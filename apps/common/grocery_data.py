"""Shared data access for the two grocery apps: SQL on the warehouse as the viewing user, and the business queries.

Auth, in order:
1. Inside a Databricks App with user authorization: the viewer's token (X-Forwarded-Access-Token header), so Unity
   Catalog applies the store row filter and the cost mask to each viewer.
2. Otherwise (app without user authorization, or a local run): the SDK's default auth (the app's service principal
   from env vars, or the CLI profile in DATABRICKS_CONFIG_PROFILE locally).
deploy_apps.py copies this file next to each app's app.py.
"""

import os

import pandas as pd
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]  # app.yaml: from the sql-warehouse resource; locally: export it
SCHEMA = "workspace.grocery_intel"


def viewer_token():
    # The app proxy adds the signed-in viewer's token when user authorization is on; None locally
    try:
        import streamlit as st
        return st.context.headers.get("X-Forwarded-Access-Token")
    except Exception:
        return None


def workspace_client():
    cfg = Config()
    token = viewer_token()
    return WorkspaceClient(host=cfg.host, token=token, auth_type="pat") if token else WorkspaceClient()


def run_sql(statement, params=None):
    """Run one statement on the warehouse and return a DataFrame. params: named :markers."""
    cfg = Config()
    token = viewer_token()
    auth = {"access_token": token} if token else {"credentials_provider": lambda: cfg.authenticate}
    with sql.connect(server_hostname=cfg.host.replace("https://", "").rstrip("/"),
                     http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}", **auth) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or {})
            rows = cur.fetchall()
            df = pd.DataFrame([list(r) for r in rows], columns=[c[0] for c in cur.description])
            kinds = [c[1] for c in cur.description]
    # SQL DECIMAL arrives as Python Decimal (object dtype), which charts and formatting can't use: make them floats
    for col, kind in zip(df.columns, kinds):
        if kind == "decimal":
            df[col] = df[col].astype(float)
    return df


def current_user():
    return run_sql("SELECT current_user() AS u").u[0]


def stores():
    # Only the stores this viewer may see: the row filter on the gold tables decides
    return run_sql(f"SELECT DISTINCT store_id, store_name, region FROM {SCHEMA}.gold_reorder_recommendation ORDER BY store_id")


# RULE: critical items = last 4 complete weeks and next week's plan, three problems valued in dollars, at least $100 each
#   (High from $300):
#   Stockouts  >= 4 days out of stock in 4 weeks; impact = lost sales (censored demand - units sold) x price
#   Waste      >= 25% of stock (sold + wasted) thrown away and >= 20 units; impact = waste at cost
#   Short next week: forecast > stock on arrival + order (shelf-life cap or allocation); impact = short units x price
# TRAP: the current week is partial (as-of Wednesday), so it is left out of the 4-week window
CRITICAL_SQL = f"""
WITH last_full AS (SELECT max(week_start) AS wk FROM {SCHEMA}.gold_store_product_week WHERE NOT is_partial_week),
recent AS (
  SELECT g.store_id, g.item_id, any_value(store_name) AS store_name, any_value(region) AS region,
         any_value(product_name) AS product_name, any_value(category) AS category, any_value(perishable) AS perishable,
         max_by(unit_price, g.week_start) AS unit_price,
         sum(GREATEST(est_demand - units_sold, 0) * unit_price) AS lost_sales_usd,
         sum(stockout_days) AS stockout_days, sum(waste_units) AS waste_units, sum(waste_value) AS waste_usd,
         try_divide(sum(waste_units), sum(units_sold + waste_units)) AS waste_rate
  FROM {SCHEMA}.gold_store_product_week g CROSS JOIN last_full
  WHERE NOT is_partial_week AND g.week_start > last_full.wk - INTERVAL 28 DAYS
  GROUP BY g.store_id, g.item_id),
plan AS (
  SELECT r.store_id, r.item_id, r.forecast_units, r.on_hand_at_arrival, r.order_units, r.order_cases, r.reason,
         GREATEST(r.forecast_units - r.on_hand_at_arrival - r.order_units, 0) AS short_units
  FROM {SCHEMA}.gold_reorder_recommendation r),
issues AS (
  SELECT store_id, item_id, 'Stockouts' AS issue,
         concat(CAST(stockout_days AS INT), ' days out of stock in the last 4 weeks') AS detail, lost_sales_usd AS impact_usd
  FROM recent WHERE stockout_days >= 4
  UNION ALL
  SELECT store_id, item_id, 'Waste',
         concat(CAST(round(waste_rate * 100) AS INT), '% of stock thrown away in the last 4 weeks'), waste_usd
  FROM recent WHERE waste_rate >= 0.25 AND waste_units >= 20
  UNION ALL
  SELECT p.store_id, p.item_id, 'Short next week',
         concat('Forecast ', CAST(round(p.forecast_units) AS INT), ', stock on arrival + order ',
                CAST(round(p.on_hand_at_arrival + p.order_units) AS INT), ': ', p.reason),
         p.short_units * r.unit_price
  FROM plan p JOIN recent r USING (store_id, item_id) WHERE p.short_units >= 5)
SELECT i.store_id, r.store_name, r.region, i.item_id, r.product_name, r.category, i.issue, i.detail,
       round(i.impact_usd, 0) AS impact_usd,
       CASE WHEN i.impact_usd >= 300 THEN 'High' ELSE 'Medium' END AS severity,
       p.order_units AS next_week_order_units, p.order_cases AS next_week_order_cases
FROM issues i JOIN recent r USING (store_id, item_id) LEFT JOIN plan p USING (store_id, item_id)
WHERE (:store_id = 'ALL' OR i.store_id = :store_id)
  AND i.impact_usd >= 100
ORDER BY impact_usd DESC
"""


def critical_items(store_id="ALL"):
    return run_sql(CRITICAL_SQL, {"store_id": store_id})


def kpis(store_id="ALL"):
    # Last 4 complete weeks: availability (in-stock days / days), revenue, waste and lost sales in dollars
    return run_sql(f"""
    WITH last_full AS (SELECT max(week_start) AS wk FROM {SCHEMA}.gold_store_product_week WHERE NOT is_partial_week)
    SELECT round(try_divide(sum(days_in_stock), sum(period_days)) * 100, 1) AS availability_pct,
           round(sum(revenue), 0) AS revenue_usd, round(sum(waste_value), 0) AS waste_usd,
           round(sum(GREATEST(est_demand - units_sold, 0) * unit_price), 0) AS lost_sales_usd
    FROM {SCHEMA}.gold_store_product_week g CROSS JOIN last_full
    WHERE NOT is_partial_week AND g.week_start > last_full.wk - INTERVAL 28 DAYS
      AND (:store_id = 'ALL' OR g.store_id = :store_id)""", {"store_id": store_id}).iloc[0]


def reorder_list(store_id="ALL", product=None):
    return run_sql(f"""
    SELECT r.store_id, r.item_id AS sku, r.product_name, r.category, r.week_start AS order_for_week,
           round(r.forecast_units, 1) AS forecast_units, round(f.forecast_low, 1) AS forecast_low,
           round(f.forecast_high, 1) AS forecast_high, r.on_hand_at_arrival, r.order_units, r.order_cases,
           round(r.order_value, 2) AS order_value, r.reason
    FROM {SCHEMA}.gold_reorder_recommendation r
    LEFT JOIN {SCHEMA}.gold_demand_forecast f USING (store_id, item_id)
    WHERE (:store_id = 'ALL' OR r.store_id = :store_id)
      AND (:product = '' OR lower(r.product_name) LIKE concat('%', lower(:product), '%'))
    ORDER BY r.order_value DESC""", {"store_id": store_id, "product": product or ""})


def data_health():
    # Freshness and trust signals: silver refresh time, quarantined rows, champion model and its gate, checks passed
    return run_sql(f"""
    SELECT (SELECT max(checked_at) FROM {SCHEMA}.silver_quality_log) AS silver_refreshed_at,
           (SELECT count(*) FROM {SCHEMA}.silver_quarantine) AS quarantined_rows,
           (SELECT max(model_version) FROM {SCHEMA}.gold_model_monitor) AS model_version,
           (SELECT max(gate_reason) FROM {SCHEMA}.gold_model_monitor) AS model_gate,
           (SELECT concat(sum(CAST(passed AS INT)), '/', count(*)) FROM {SCHEMA}.gold_quality_check) AS quality_checks""").iloc[0]


def search_documents(question, store_id=None, k=4):
    # Vector search over the parsed contracts, visit notes and delivery notes; store filter applied on the result
    hits = run_sql(f"""
    SELECT doc_name, source_format, section, store_id, chunk_text, round(search_score, 3) AS score
    FROM vector_search(index => '{SCHEMA}.doc_chunks_index', query_text => :q, num_results => 12)""", {"q": question})
    if store_id:
        hits = hits[hits.store_id.isin(['ALL', store_id])]  # chain-wide documents are tagged ALL
    return hits.head(k)
