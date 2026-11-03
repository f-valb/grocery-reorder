# Demo flow: Genie + documents → dashboard → lineage (about 16 minutes)

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

### 1b. Documents: vector search finds the *why* (4 min)
Genie answers **what** from tables. The **why** is in documents: supplier contracts, the fresh SOP, area-manager visit notes, promo briefs, customer feedback, a delivery note. 25 documents in the `raw` volume, in four formats: the Coffee contract is a **PDF**, the ST-07 visit note a **Word** file, the delivery note a **photo (PNG)**, the rest markdown. Open the three real files in the volume (Catalog → `grocery_intel` → `raw` → `document_files`) before you start: "this is what the business actually has".

**Process to explain** (notebook `05_documents_rag`):
1. Files land in the volume → `bronze_documents` (text files) and `bronze_document_files` (PDF, Word, images as bytes)
2. Parsed into text with their structure (`silver_document_parse_log` shows each file, method and word count):
   - PDF → `pypdf`, page by page, so answers cite the page
   - Word → the .docx XML, by heading
   - Image → Llama 4 Maverick reads the photo (OCR by a vision model)
   - On a paid workspace, `ai_parse_document` does all three in one SQL function; on Free Edition it isn't available, so the notebook parses in Python
3. Chunked by section or PDF page (short notes stay whole) → `silver_doc_chunks` (38 chunks, with format / doc type / store / product metadata)
4. Vector Search index `doc_chunks_index` on endpoint `grocery_docs_vs` embeds every chunk with `databricks-gte-large-en` and re-syncs when the table changes
5. A question is embedded the same way; the closest chunks come back with a score
6. Llama 3.3 70B answers **only** from those chunks plus the gold numbers, citing `[doc_name]`

**Live in the SQL editor:**
```sql
SELECT doc_name, source_format, section, ROUND(search_score, 3) AS score
FROM vector_search(index => 'workspace.grocery_intel.doc_chunks_index',
                   query_text => 'why does our coffee supplier deliver less than we order', num_results => 3);
```
Expect three formats in one result: `delivery_note_st07_2026_10_12.png` (the photo), then `supplier_coffee.pdf` Page 2 and Page 1. Say: "the photo of a delivery note and a PDF contract, searched by meaning, no keywords".

Then retrieval + generation in one statement:
```sql
WITH passages AS (
  SELECT doc_name, chunk_text FROM vector_search(index => 'workspace.grocery_intel.doc_chunks_index',
    query_text => 'Why does our coffee supplier deliver less than we order, and what can we do?', num_results => 4)),
context AS (SELECT concat_ws('\n', collect_list(concat('[', doc_name, '] ', chunk_text))) AS passages FROM passages)
SELECT ai_query('databricks-meta-llama-3-3-70b-instruct',
  concat('Answer only from these passages and cite each one you use as [doc_name]. At most 80 words.\n', passages,
         '\nQuestion: Why does our coffee supplier deliver less than we order, and what can we do?')) AS answer
FROM context;
```
Expect: the order was cut back to the allocation (clause 4.1) [delivery_note…png, supplier_coffee.pdf]; emergency order or higher allocation.

**Pre-computed grounded answers** (numbers + documents): `SELECT store_id, question, answer, source_documents FROM workspace.grocery_intel.gold_doc_answers`:
- ST-07 Coffee: allocation capped at 70% [supplier_coffee.pdf, page 2]; emergency order or a higher allocation with 4 weeks of evidence
- ST-07 delivery, week of 2026-10-12: ordered 264, received 96, cut back per clause 4.1 [delivery_note_st07_2026_10_12.png] + [supplier_coffee.pdf, page 2]: the photo and the contract tell one story
- ST-07 waste: the manager orders ~50% above the system suggestion on fresh lines [visit_st07_2026_09.docx]; Sourdough $532, Muffins $424 wasted
- ST-12 week of 2026-09-07: Bayside Summer Festival, one-off: don't raise standing drink orders

Back in Genie: *"Show me the weekly brief for ST-07 and the documents it cites"* returns the generated brief with its sources.

Talking point: the documents are governed like the tables (volume, Delta tables and index all in Unity Catalog); retrieval is checked (`gold_rag_check`: 6 of 6 demo questions found the document that holds the answer, including the PDF, Word and image); every answer cites its source.

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
5. **Weekly brief** at the bottom: with ST-07 selected, the generated brief names its stockouts (allocation clause) and waste (ordering above the system), with the documents cited.

Talking point: the dashboard is published without embedded credentials, so a store manager opening it sees only their own store (row filter `gov_store_filter`), and only finance sees unit cost (column mask).

---

## 2b. The pipeline as a job (1 min)
**Jobs & Pipelines** → *Grocery intelligence: sources to forecast, reorder and documents* (https://dbc-0a174280-b54a.cloud.databricks.com/jobs/921270369760005).
- **Tasks** tab: the graph ingest_sources → bronze_silver_pipeline → gold_forecast_model → demo_checks + documents_vector_index; each task has a one-line description. `bronze_silver_pipeline` is a pipeline task: click it → **View pipeline**.
- **The pipeline** (*Grocery sources: POS, ERP and SaaS extracts to bronze and silver*): the graph shows the 6 source extracts → bronze → silver, plus `silver_quarantine` and `silver_quality_log`. Click a `*_checked` node → **Data quality**: rows failing each named rule (unknown store or item, negative stock, waste above available stock). Say: the pipeline declares *what* each table is; Databricks works out the order, retries and lineage.
- **Runs** tab: open the latest run to show every task's duration and the notebook output behind it.
- Schedule: every Monday 06:00 UTC, created paused. Say: in production this runs after the Sunday stock count; Lakeflow Connect would trigger it when new source data lands.
- **Models**: Catalog → `grocery_intel` → `demand_forecaster`: the latest version (11) holds the alias `champion`, linked to its MLflow run. Scoring points at the alias, so a rollback is moving the alias back.

---

## 3. Data lineage: where does this number come from? (3 min)
**Catalog** → `workspace` → `grocery_intel` → `gold_reorder_recommendation` → **Lineage** tab → **See lineage graph**.

Walk it right to left:
1. `gold_reorder_recommendation` ← `gold_store_product_week` (next week's orders are built from the weekly store-item facts)
2. `gold_store_product_week` ← `silver_sales_weekly`, `silver_stock_snapshots`, `silver_waste_events`, `silver_products`, `silver_promotions`, `silver_stores`
3. silver ← `bronze_pos_*` (Postgres POS), `bronze_erp_*` (MySQL ERP), `bronze_ops_waste_log` / `bronze_promo_calendar` (SaaS)
4. bronze ← the raw extract files in the `raw` volume
5. Click any bronze or silver table: the lineage panel names the **pipeline** that produces it; click a gold table: it names the **job** and the `03_gold` notebook. So every table answers "which process wrote me, and when".

Then the documents chain: `gold_store_briefs` ← `silver_doc_chunks` ← `bronze_documents` + `bronze_document_files` ← the files in `raw/documents` and `raw/document_files`; the vector index `doc_chunks_index` hangs off `silver_doc_chunks`.

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
