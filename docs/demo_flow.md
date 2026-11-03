# Demo flow: Genie → dashboard → lineage (about 12 minutes)

Workspace: https://dbc-0a174280-b54a.cloud.databricks.com/?o=7474648703926670 · schema `workspace.grocery_intel`

**Story in one line:** three systems that never agreed (POS, ERP, store-ops app) now feed one governed store-item data product; a trained forecast sits on top; managers ask questions in plain English, see the predictions in a dashboard, and can trace every number back to its source.

---

## 1. Genie: ask the existing data in plain English (5 min)
Open **Grocery store assistant**: https://dbc-0a174280-b54a.cloud.databricks.com/genie/rooms/01f1d74831d813fe87330e09c853ca47

Before typing, say: Genie only sees the governed gold tables, plus our business definitions (what "availability", "waste rate" and "last 4 weeks" mean). It writes SQL, and that SQL runs as the person asking, so the store row filter applies here too.

| # | Ask (copy-paste) | Expect | Talking point |
|---|---|---|---|
| 1 | Which 10 products had the most stockout days in the last 4 weeks, and how much demand did we miss? | Pasta 143 days, Sparkling Water 135, Coffee 132, Eggs 119; ~2,000–2,600 units missed each | Genie uses *estimated* demand, because empty shelves hide what customers wanted. That definition lives in the instructions, not in someone's head |
| 2 | Which store wastes the most money compared with the average store, and on which products? | ST-07 Westgate $13,085 vs $6,049 average; Sourdough, Muffins, Strawberries | One store, same size as its peers, more than twice the waste: an ordering habit, not a demand problem |
| 3 | What should store ST-07 order next week, and why? | Coffee 168 units ("stockout risk"), Almond Yogurt 54 ("trending up"), produce "capped by shelf life" | The prediction is already in the data product, with a reason managers can act on |
| 4 | How accurate is the demand model compared with the simple forecast, especially in promo weeks? | Promo weeks: model 10.4% vs baseline 43.8% WAPE | The trained model earns its place: it only serves if it beats the simple method on weeks it never saw |
| 5 | Which category is growing fastest over the last 8 weeks? | Plant-based first at about +25–29% (Genie's exact window varies run to run); Dry Goods +11–12% is partly the new Protein Bar launch | Ask a follow-up live: *"Show that by week as a line chart"* |

Click **Show code** on one answer: the SQL is readable and auditable. If Genie asks a clarifying question, answer it: that's the feature, not a bug.

**Optional, if they ask about trust:** "How many rows did we quarantine per rule?" (reads `silver_quality_log`).

---

## 2. Reports and dashboard, including predictions (4 min)
Open **Grocery intelligence**: https://dbc-0a174280-b54a.cloud.databricks.com/dashboardsv3/01f1d72d64ab1c8b92f78b4c747b325a/published

1. **All stores first:** availability 97.6% and waste $35.6k (last 4 weeks), top stockout and waste items, weekly trends.
2. **Store filter → ST-07:** the waste counter and tables change; the reorder list shows next week's quantities with the reason column.
3. **Scroll to "Forecast model":**
   - error by method: the model wins clearly in promo weeks
   - model health: drift scores (PSI) are under the 0.25 alert, champion = model, `retrain_needed` = false
   - say: this is how we'd know when to retrain
4. **Rows per source system:** the three systems landing side by side.

Talking point: the dashboard is published without embedded credentials, so a store manager opening it sees only their own store (row filter `gov_store_filter`), and only finance sees unit cost (column mask).

---

## 3. Data lineage: where does this number come from? (3 min)
**Catalog** → `workspace` → `grocery_intel` → `gold_reorder_recommendation` → **Lineage** tab → **See lineage graph**.

Walk it right to left:
1. `gold_reorder_recommendation` ← `gold_store_product_week` (next week's orders are built from the weekly store-item facts)
2. `gold_store_product_week` ← `silver_sales_weekly`, `silver_stock_snapshots`, `silver_waste_events`, `silver_products`, `silver_promotions`, `silver_stores`
3. silver ← `bronze_pos_*` (Postgres POS), `bronze_erp_*` (MySQL ERP), `bronze_ops_waste_log` / `bronze_promo_calendar` (SaaS)
4. bronze ← the raw extract files in the `raw` volume

Then:
- Click `gold_store_product_week` → **Columns** → `est_demand`: column-level lineage back to `units_sold` and `days_in_stock`.
- Open `silver_quarantine`: every rejected row with the rule it failed (unknown product, negative stock, waste above available stock).
- Point at the table **Overview**: grain and column comments, and the row filter shown on the table.

Talking point: lineage, quality and access control come from Unity Catalog automatically, because every step is a governed Delta table. Nobody maintains a lineage diagram by hand. In production, the bronze step becomes Lakeflow Connect (change data capture from Postgres/MySQL) without changing anything downstream.

---

## Backup if something misbehaves
- Genie gives a different answer: open the SQL and say how you'd fix it (add the definition to the instructions). The same 5 questions were tested through the Genie API before the demo.
- Dashboard slow on first open: the serverless warehouse is starting. Use the `04_demo_queries` output of the last job run instead: same numbers, plus charts.
- Rebuild the Genie space or dashboard: `python3 genie/build_genie_space.py --test` · `python3 dashboards/build_dashboard.py` (with `DATABRICKS_CONFIG_PROFILE=food WAREHOUSE_ID=5d7ed27d256c7dcf`).
