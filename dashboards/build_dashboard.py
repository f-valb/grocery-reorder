"""05 Dashboard: build the store manager AI/BI dashboard (v2: grocery_intel, with the model panel) and create/update + publish it.

    DATABRICKS_CONFIG_PROFILE=food WAREHOUSE_ID=5d7ed27d256c7dcf python3 dashboards/build_dashboard.py

Widget helpers come from templates/dashboards/build_dashboards.py. The dashboard id is kept in
dashboards/dashboard_id_intel.<profile>.txt so reruns update the same dashboard (v1 keeps dashboard_id.txt). Prints the dashboard URL.
"""

import importlib.util
import json
import os
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")  # ids are saved per workspace profile
spec = importlib.util.spec_from_file_location("kit", HERE.parent / "templates/dashboards/build_dashboards.py")
kit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kit)
ds, text, counter, chart, table, multi_filter = kit.ds, kit.text, kit.counter, kit.chart, kit.table, kit.multi_filter

S = "workspace.grocery_intel"
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
LAST_4_WEEKS = f"""week_num > (SELECT max(week_num) FROM {S}.gold_store_product_week WHERE NOT is_partial_week) - 4
  AND NOT is_partial_week"""


def dashboard() -> dict:
    datasets = [
        ds("ds_recent", "Last 4 full weeks", f"""
SELECT store_id, product_name, category, units_sold, days_in_stock, period_days, stockout_days, waste_units, waste_value
FROM {S}.gold_store_product_week WHERE {LAST_4_WEEKS}"""),
        ds("ds_stockouts", "Top stockout items (last 4 weeks)", f"""
SELECT store_id, product_name, category, sum(stockout_days) AS stockout_days,
  round(sum(days_in_stock) / sum(period_days), 3) AS availability,
  round(sum(est_demand) - sum(units_sold)) AS lost_units
FROM {S}.gold_store_product_week WHERE {LAST_4_WEEKS}
GROUP BY ALL HAVING sum(stockout_days) > 0 ORDER BY stockout_days DESC"""),
        ds("ds_waste", "Top waste items (last 4 weeks)", f"""
SELECT store_id, product_name, category, sum(waste_units) AS waste_units, round(sum(waste_value), 2) AS waste_usd,
  round(sum(waste_units) / nullif(sum(received), 0), 3) AS waste_share_of_received
FROM {S}.gold_store_product_week WHERE {LAST_4_WEEKS}
GROUP BY ALL HAVING sum(waste_units) > 0 ORDER BY waste_usd DESC"""),
        ds("ds_reorder", "Next week's reorder list", f"""
SELECT store_id, product_name, category, round(forecast_units) AS forecast, on_hand, on_order, case_pack,
  order_cases, order_units, order_value, reason
FROM {S}.gold_reorder_recommendation WHERE order_units > 0 ORDER BY store_id, order_value DESC"""),
        ds("ds_trend", "Weekly trend", f"""
SELECT store_id, week_start, sum(days_in_stock) AS days_in_stock, sum(period_days) AS period_days,
  sum(waste_value) AS waste_value, sum(revenue) AS revenue
FROM {S}.gold_store_product_week WHERE NOT is_partial_week GROUP BY ALL"""),
        ds("ds_briefs", "AI weekly brief per store", f"""
SELECT store_id, store_name, brief, array_join(source_documents, ', ') AS sources FROM {S}.gold_store_briefs"""),
    ]
    all_ds = [d["name"] for d in datasets]  # the store filter applies to these (chain-level datasets below have no store_id)
    datasets += [
        ds("ds_model_error", "Forecast error by method (backtest)", f"""
SELECT slice, method, wape FROM {S}.gold_backtest_summary WHERE slice IN ('TOTAL', 'Promo weeks', 'Non-promo weeks')"""),
        ds("ds_model_monitor", "Model health", f"""
SELECT check_name, value, threshold, alert, champion_method, model_version, retrain_needed FROM {S}.gold_model_monitor"""),
        ds("ds_sources", "Rows per source system", f"""
SELECT 'postgres_pos' AS source_system, 'pos_sales_weekly' AS source_table, count(*) AS bronze_rows FROM {S}.bronze_pos_sales_weekly
UNION ALL SELECT 'postgres_pos', 'pos_stock_snapshots', count(*) FROM {S}.bronze_pos_stock_snapshots
UNION ALL SELECT 'mysql_erp', 'erp_products', count(*) FROM {S}.bronze_erp_products
UNION ALL SELECT 'saas_store_ops', 'ops_waste_log', count(*) FROM {S}.bronze_ops_waste_log
UNION ALL SELECT 'saas_promo_tool', 'promo_calendar', count(*) FROM {S}.bronze_promo_calendar"""),
    ]
    layout = [
        text("title", ["## Store manager: availability, waste and next week's orders\n"], 0, 0, 12, 1),
        text("subtitle", ["Counters and top lists cover the last 4 full weeks. Reorder = forecast + safety stock − stock at "
                          "arrival − on order, in whole cases; perishables capped at what sells within shelf life.\n"], 0, 1, 12, 1),
        multi_filter("filter_store", "Store", "store_id", all_ds, 0, 2, 12, 2),
        counter("kpi_avail", "Availability % (in-stock days)", "ds_recent", "availability",
                "SUM(`days_in_stock`) / SUM(`period_days`)", PCT, x=0, y=4, w=4),
        counter("kpi_waste", "Waste $ (at cost)", "ds_recent", "sum(waste_value)", "SUM(`waste_value`)", kit.USD, x=4, y=4, w=4),
        counter("kpi_stockout", "Stockout days", "ds_recent", "sum(stockout_days)", "SUM(`stockout_days`)", kit.COUNT,
                x=8, y=4, w=4),
        table("tbl_stockouts", "Top stockout items", "ds_stockouts",
              [("store_id", "Store"), ("product_name", "Product"), ("stockout_days", "Stockout days"),
               ("availability", "Availability"), ("lost_units", "Lost units (est.)")], x=0, y=7, w=6, h=7),
        table("tbl_waste", "Top waste items", "ds_waste",
              [("store_id", "Store"), ("product_name", "Product"), ("waste_units", "Waste units"),
               ("waste_usd", "Waste $"), ("waste_share_of_received", "Share of received")], x=6, y=7, w=6, h=7),
        table("tbl_reorder", "Next week's reorder list", "ds_reorder",
              [("store_id", "Store"), ("product_name", "Product"), ("forecast", "Forecast"), ("on_hand", "On hand"),
               ("on_order", "On order"), ("case_pack", "Case pack"), ("order_cases", "Cases"), ("order_units", "Units"),
               ("order_value", "Value $"), ("reason", "Reason")], x=0, y=14, w=12, h=8),
        chart("ln_avail", "line", "Weekly availability %", "ds_trend", "week_start", "`week_start`", "temporal",
              "availability", "SUM(`days_in_stock`) / SUM(`period_days`)", "Availability", fmt=PCT, x=0, y=22, w=6, h=6),
        chart("bar_waste", "bar", "Weekly waste $", "ds_trend", "week_start", "`week_start`", "temporal",
              "sum(waste_value)", "SUM(`waste_value`)", "Waste $", fmt=kit.USD, x=6, y=22, w=6, h=6),
        text("model_title", ["## Forecast model (chain level)\n"], 0, 28, 12, 1),
        chart("bar_model_error", "bar", "Forecast error (WAPE): trained model vs simple baseline", "ds_model_error", "slice",
              "`slice`", "categorical", "avg(wape)", "AVG(`wape`)", "WAPE", color=("method", "`method`"), fmt=PCT,
              description="Backtest on the last 2 full weeks; the model is champion only if it wins", x=0, y=29, w=6, h=6,
              layout="group"),
        table("tbl_monitor", "Model health: drift (PSI) and gate", "ds_model_monitor",
              [("check_name", "Check"), ("value", "Value"), ("threshold", "Threshold"), ("alert", "Alert"),
               ("champion_method", "Champion"), ("model_version", "Version"), ("retrain_needed", "Retrain needed")],
              x=6, y=29, w=6, h=6),
        table("tbl_sources", "Rows landed per source system", "ds_sources",
              [("source_system", "Source system"), ("source_table", "Table"), ("bronze_rows", "Rows")], x=0, y=35, w=12, h=4),
        table("tbl_briefs", "Weekly brief (generated from the store numbers and its documents)", "ds_briefs",
              [("store_id", "Store"), ("brief", "Brief"), ("sources", "Documents cited")],
              description="LLM answer grounded in gold numbers + vector search over supplier terms, SOPs and visit notes",
              x=0, y=39, w=12, h=8),
    ]
    return {"datasets": datasets, "pages": [{"name": "store", "displayName": "Store manager", "pageType": "PAGE_TYPE_CANVAS",
                                             "layoutVersion": "GRID_V1", "layout": layout}], "uiSettings": {"theme": kit.THEME}}


def cli(*args):
    out = subprocess.run(["databricks", *args, "-o", "json"], capture_output=True, text=True)
    if out.returncode:
        raise SystemExit(out.stderr.strip())
    return json.loads(out.stdout or "{}")


if __name__ == "__main__":
    body = dashboard()
    (HERE / "grocery_intel.lvdash.json").write_text(json.dumps(body, indent=1) + "\n")
    warehouse = os.environ["WAREHOUSE_ID"]
    user = cli("current-user", "me")["userName"]
    payload = {"display_name": "Grocery intelligence: stock, waste, forecast, reorder", "warehouse_id": warehouse,
               "serialized_dashboard": json.dumps(body), "parent_path": f"/Users/{user}/{HERE.parent.name}"}
    id_file = HERE / f"dashboard_id_intel.{PROFILE}.txt"
    if id_file.exists():
        dash = cli("lakeview", "update", id_file.read_text().strip(), "--json", json.dumps(payload))
    else:
        dash = cli("lakeview", "create", "--json", json.dumps(payload))
        id_file.write_text(dash["dashboard_id"] + "\n")
    cli("lakeview", "publish", dash["dashboard_id"], "--json", json.dumps({"embed_credentials": False, "warehouse_id": warehouse}))
    host = subprocess.run(["databricks", "auth", "describe", "-o", "json"], capture_output=True, text=True)
    host = json.loads(host.stdout)["details"]["host"].rstrip("/")
    print(f"{host}/dashboardsv3/{dash['dashboard_id']}/published")
