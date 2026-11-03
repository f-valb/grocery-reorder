"""Genie space: ask the grocery gold data product questions in plain English, then test it with the demo questions.

    DATABRICKS_CONFIG_PROFILE=food WAREHOUSE_ID=5d7ed27d256c7dcf python3 genie/build_genie_space.py [--test]

Creates the space (or updates it, using the id in genie/space_id.<profile>.txt) over the gold tables, with business
instructions and sample questions. --test asks each sample question through the Conversation API and prints
Genie's SQL and the first result rows. Auth comes from the CLI profile.
"""

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")  # ids are saved per workspace profile
S = "workspace.grocery_intel"
TITLE = "Grocery store assistant"
DESCRIPTION = "Ask about availability, stockouts, waste, next week's orders and the demand forecast for 20 stores."

TABLES = ["gold_store_product_week", "gold_demand_forecast", "gold_reorder_recommendation", "gold_backtest_summary",
          "gold_model_monitor", "silver_stores", "silver_quality_log", "gold_store_briefs",
          # TRAP: the row filter declared in the pipeline reads gov_store_managers, and Genie only runs SQL over its own
          # sources: without the mapping table every query on gold_store_product_week fails with PERMISSION_DENIED
          "gov_store_managers"]

INSTRUCTIONS = [
    "A grocery chain with 20 stores (store_id ST-01 to ST-20, names in silver_stores) and 41 products in 6 categories. "
    "Data comes from three systems (POS, ERP, store-ops app) unified on store_id and item_id.\n",
    "Grain: gold_store_product_week is one row per store, product and week (weeks start on Monday); gold_demand_forecast and "
    "gold_reorder_recommendation are one row per store and product for next week (week starting 2026-11-09).\n",
    "The current week (2026-11-02) is partial (data through 2026-11-04, is_partial_week = true): exclude it from trends, "
    "averages and totals unless asked about this week. 'Last 4 weeks' means the last 4 full weeks.\n",
    "Availability = sum(days_in_stock) / sum(period_days). Stockout days = period_days - days_in_stock. "
    "Sales are censored by stockouts: use est_demand (units_sold / days_in_stock * 7) for demand; missed demand = est_demand - units_sold.\n",
    "Waste: waste_units and waste_value (at unit cost, USD); expired_units are the ordering problem, damaged_units the handling problem. "
    "Waste rate = expired_units / received.\n",
    "Next week's order is gold_reorder_recommendation.order_units (whole case packs) with order_value in USD and a plain-English reason. "
    "Always show the reason column when asked why.\n",
    "Forecast accuracy: gold_backtest_summary, WAPE (lower is better) for method 'model' (trained gradient-boosted model, the champion) "
    "vs 'baseline' (moving average + trend), per slice (TOTAL, Promo weeks, Non-promo weeks, week, product segment). "
    "Model health and drift are in gold_model_monitor.\n",
    "gold_backtest_summary.slice has exact values 'TOTAL', 'Promo weeks', 'Non-promo weeks', week dates and segment names: filter with =, "
    "never LIKE. Show WAPE as a percentage with one decimal (wape * 100). The model is champion only if it beats the baseline.\n",
    "Category growth means the sustained trend, not one week: compare average weekly est_demand of the last 4 full weeks with the 4 full "
    "weeks before, excluding promo and spike weeks, as a percentage change per category.\n",
    "Spike weeks (is_spike_week) are real one-off events, kept in sales but excluded from forecasts. Promo weeks have is_promo = true.\n",
    "gold_store_briefs holds an AI-generated weekly brief per store (from its numbers and its documents: supplier terms, SOPs, "
    "visit notes), with source_documents. When asked for a store brief or why a store has a problem, show the brief and its sources.\n",
    "Show money in USD rounded to the nearest dollar, percentages with one decimal. For 'top' questions return the top 10.",
]

EXAMPLE_SQL = {  # curated question -> SQL pairs Genie can reuse
    "Compare the forecast error of the model and the baseline": f"""
SELECT slice, ROUND(100 * MAX(CASE WHEN method = 'model' THEN wape END), 1) AS model_wape_pct,
       ROUND(100 * MAX(CASE WHEN method = 'baseline' THEN wape END), 1) AS baseline_wape_pct
FROM {S}.gold_backtest_summary WHERE slice IN ('TOTAL', 'Promo weeks', 'Non-promo weeks') GROUP BY slice ORDER BY slice""",
    "Which category is growing fastest": f"""
WITH clean AS (
  SELECT category, week_num, SUM(est_demand) AS demand FROM {S}.gold_store_product_week
  WHERE NOT is_partial_week AND NOT is_promo AND NOT is_spike_week GROUP BY category, week_num),
last_week AS (SELECT MAX(week_num) AS w FROM clean)
SELECT category,
       ROUND(100 * (AVG(CASE WHEN week_num > w - 4 THEN demand END) / AVG(CASE WHEN week_num BETWEEN w - 7 AND w - 4 THEN demand END) - 1), 1) AS growth_pct
FROM clean CROSS JOIN last_week GROUP BY category ORDER BY growth_pct DESC""",
}

SAMPLE_QUESTIONS = [
    "Which 10 products had the most stockout days in the last 4 weeks, and how much demand did we miss?",
    "Which store wastes the most money compared with the average store, and on which products?",
    "What should store ST-07 order next week, and why?",
    "How accurate is the demand model compared with the simple forecast, especially in promo weeks?",
    "Which category is growing fastest over the last 8 weeks?",
]


def cli(*args):
    out = subprocess.run(["databricks", *args, "-o", "json"], capture_output=True, text=True)
    if out.returncode:
        raise SystemExit(out.stderr.strip())
    return json.loads(out.stdout or "{}")


def serialized_space():
    ids = sorted(uuid.uuid4().hex for _ in range(len(SAMPLE_QUESTIONS) + 1))
    sql_ids = sorted(uuid.uuid4().hex for _ in EXAMPLE_SQL)
    return json.dumps({
        "version": 2,
        "config": {"sample_questions": [{"id": i, "question": [q]} for i, q in zip(ids[1:], SAMPLE_QUESTIONS)]},
        "data_sources": {"tables": [{"identifier": f"{S}.{t}"} for t in sorted(TABLES)]},
        "instructions": {"text_instructions": [{"id": ids[0], "content": INSTRUCTIONS}],
                         "example_question_sqls": [{"id": i, "question": [q], "sql": [ln + "\n" for ln in sql.strip().splitlines()]}
                                                   for i, (q, sql) in zip(sql_ids, EXAMPLE_SQL.items())]},
    })


def ask(space_id, question):
    started = cli("genie", "start-conversation", space_id, question)
    conv, msg = started["conversation_id"], started["message_id"]
    for _ in range(60):
        m = cli("genie", "get-message", space_id, conv, msg)
        if m.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(5)
    print(f"\nQ: {question}\n   status: {m.get('status')}")
    for a in m.get("attachments", []):
        if a.get("text"):
            print("   text:", a["text"].get("content", "")[:400])
        if a.get("query"):
            print("   sql:", " ".join(a["query"]["query"].split())[:600])
            res = cli("genie", "get-message-attachment-query-result", space_id, conv, msg, a["attachment_id"])
            rows = res.get("statement_response", {}).get("result", {}).get("data_array", [])
            cols = [c["name"] for c in res.get("statement_response", {}).get("manifest", {}).get("schema", {}).get("columns", [])]
            print("   columns:", cols)
            for r in rows[:5]:
                print("   ", r)


if __name__ == "__main__":
    warehouse = os.environ["WAREHOUSE_ID"]
    user = cli("current-user", "me")["userName"]
    id_file = HERE / f"space_id.{PROFILE}.txt"
    if id_file.exists():
        space_id = id_file.read_text().strip()
        cli("genie", "update-space", space_id, "--serialized-space", serialized_space(), "--title", TITLE,
            "--description", DESCRIPTION, "--warehouse-id", warehouse)
    else:
        space = cli("genie", "create-space", warehouse, serialized_space(), "--title", TITLE, "--description", DESCRIPTION,
                    "--parent-path", f"/Users/{user}/{HERE.parent.name}")
        space_id = space["space_id"]
        id_file.write_text(space_id + "\n")
    host = json.loads(subprocess.run(["databricks", "auth", "describe", "-o", "json"], capture_output=True, text=True).stdout)
    print(f"{host['details']['host'].rstrip('/')}/genie/rooms/{space_id}")
    if "--test" in sys.argv:
        for q in SAMPLE_QUESTIONS[3:] if "--last2" in sys.argv else SAMPLE_QUESTIONS:
            ask(space_id, q)
