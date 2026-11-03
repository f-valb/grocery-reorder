# Demo brief: grocery intelligence (v2, branch `multi-source-ml`)

**Problem:** a 20-store grocery chain runs out of popular items while throwing away fresh produce. Data sits in a POS database, an ERP and two SaaS apps that don't agree on store or product names, so nobody trusts one number and ordering runs on habit.

**Built:** three mocked source systems → bronze (explicit schemas, lineage columns) → silver (one store-item view via the ERP crosswalk, quarantine with reasons) → gold data product (weekly store-item facts, trained demand model gated against a simple baseline, reorder list with reasons, backtest, drift monitor, row filter and cost mask) → demo notebook and AI/BI dashboard.
Schema `workspace.grocery_intel`. v1 (single source, no ML) stays on `main` in `grocery_reorder`.

**Verified locally only** (account locked): everything below ran on local PySpark 3.5 (`scripts/local_spark.py`). Not yet run on serverless: UC registry + champion alias, row filter / column mask, UC comments, DESCRIBE HISTORY rerun check, matplotlib chart, dashboard publish.

## Row counts per layer
| Source system | Bronze | Silver | Quarantined | Re-sends removed |
|---|---|---|---|---|
| Postgres POS: sales | 9,721 | 9,680 | 41 | 0 |
| Postgres POS: stock snapshots | 9,840 | 9,680 | 40 | 120 |
| SaaS store-ops: waste log | 6,545 | 6,500 | 45 | 0 |
| SaaS promo tool | 12 | 12 | 0 | 0 |
| MySQL ERP: products / stores | 46 / 20 | 46 versions of 41 products / 20 | 0 | 0 |

Every bronze row is accounted for (0 unexplained). Gold: `gold_store_product_week` 9,680 · forecast and reorder 820 store-items each · backtest 3,278 rows (2 methods × 2 weeks).

## Key numbers (gold)
- Last 4 full weeks: availability **97.6%**, waste **$35.6k** at cost; next week's order $97.2k.
- Forecast error (WAPE, last 2 full weeks): **model 10.5% vs baseline 13.6%**. Promo weeks 10.4% vs 43.8%; normal weeks 10.5% vs 12.2%. The model is champion.
- Backtest dollars (waste + lost margin over 2 weeks): actual $20.5k → baseline orders $13.9k → **model orders $13.0k** (≈ $7.5k better than what happened). Promo-week stockout days 81 → 1 with the model.
- Model health: largest drift score (PSI) 0.18 on `trend_rel`, below the 0.25 alert; `retrain_needed` = false.

## Planted signals recovered
| Signal | Recovered | Evidence (gold) |
|---|---|---|
| Popular items stock out | yes | Pasta, Eggs, Sparkling Water, Coffee in stock 81–84% of days vs ≥99.5% next; demand 19–23% above sales |
| Over-ordered fresh produce | yes | Strawberries 46%, Sourdough 44%, Salad Mix 42% of received expired (next: 28%) |
| One store over-orders | yes | ST-07 waste $13.1k = 2.16× the average store; receives 1.21× demand vs ~1.0× elsewhere |
| Promo weeks spike sales | yes | lift 1.80× |
| One category trending up | yes | Plant-based trend +4.1%/week above the other categories |

## Edge cases
| Case | How it shows up | Decision | Why | Rows affected |
|---|---|---|---|---|
| Orphan keys | POS item ids 901–910, waste item codes not in the ERP | quarantine | can't be valued or planned | 30 sales, 20 waste |
| Late-arriving records | POS snapshot re-sent 2 days later; waste logs synced in a later week | fix: drop exact re-sends; bucket waste by event time | the business event didn't move | 120 snapshots; 68 waste logs |
| Values changing mid-period | 5 products change cost/price in week 7 | keep both versions; join on the week | revenue and waste $ must use the price of that week | 5 products (46 versions) |
| Timezone day shift | waste logged Sunday night local = Monday UTC | fix: week from store-local date | otherwise waste lands in the wrong week | 585 waste logs |
| Small sample / new item | Protein Bar launched week 9 | keep, with fallback: <3 weeks → average of what exists; 0 weeks → category median at the store | a trend on 1–2 points is noise | 80 rows (20 stores) |
| Legitimate outlier vs error | ST-12 festival (Beverages ×3, week 6) vs a 99,999 typo | keep the festival, flagged and excluded from forecasts (median + 3.5×MAD); quarantine the typo | real sales stay in reporting, one-offs don't drive orders | 7 festival rows (+2 other flags); 1 typo |
| Rerun idempotency | gold rerun | check: last two data writes compared with `exceptAll` both ways | proves reruns don't change the data | serverless only |

## Decisions and trade-offs
- Core entity **store-item**: the ERP is the key master (holds POS ids and store names), so one crosswalk unifies all sources.
- Flaws are corrupted *copies* (later `updated_at`), never edits; rules run before keep-latest, so a bad copy can't replace the true row.
- Demand = units sold / days in stock × 7, because empty shelves censor sales.
- Model: one global gradient-boosted model predicting actual / baseline, plus 10%/90% quantiles; it only serves if it beats the baseline by >2% in the backtest. Explainable as "baseline × adjustment for promo, trend, category".
- Safety stock 1.65σ non-perishable (95% target), 0.84σ perishable; whole case packs; perishables capped at what sells within shelf life.
- Full log: `docs/decisions.md`.

## Traps handled
- Censored demand → estimate from in-stock days.
- Leakage → features use only weeks before the target week and the planned promo calendar; backtest trains only on earlier weeks with the same code as production.
- Partial current week → excluded from training and baseline; order uses stock expected at arrival.
- One-off spikes and promos → excluded from the baseline (the model learns promos explicitly).
- Dashboard with embedded credentials would bypass the row filter → publish without them for per-viewer filtering.
- Delta history: COMMENT/ALTER create versions, so the rerun check compares the last two *write* operations only.

## Honest caveats
- Short-life perishables (≤7 days) with weekly delivery: the shelf-life cap cuts waste $14.4k → $0.2k but adds $7.0k lost margin. Net positive; the real fix is two deliveries a week.
- Long-life perishables (>7 days): the simulated expiry rule is stricter than real stores, so their simulated waste is a high estimate (shows as −$2.0k).
- The backtest assumes suppliers fill the full order; the popular items are supplier-constrained.

## Spark talking points
- **Execution plan** (Q-plan in 04 section 5, captured to `gold_execution_plan`): sales ⨝ products is a `BroadcastHashJoin` (products broadcast via `BroadcastExchange`, price-valid range checked inside the join), then partial `HashAggregate` → `Exchange` (hash by store, category) → final `HashAggregate`, wrapped in `AdaptiveSparkPlan`. On serverless the same shape appears as Photon nodes.
- `02_silver` line 65: broadcast the 20-store / 41-item crosswalks so the fact tables are never shuffled for key mapping.
- `02_silver` line 131: the dedupe window is a shuffle by key; at scale it would be a MERGE on the key or Lakeflow `AUTO CDC`.
- `01_bronze` line 69: Delta overwrite in UC keeps reruns idempotent; at scale, liquid clustering by `week_start, store_id` lets a store manager's query prune files.

## Section map
| Notebook | Section | Talking point |
|---|---|---|
| 00_generate_data | 2 Entity profiles | one store and product list feeds every source, with behaviour profiles for the signals |
| 00_generate_data | 5 Planted flaws + edge cases | flaws are corrupted copies; each edge case tagged `# EDGE:` |
| 01_bronze | 1 Read raw files | explicit schema per source; SaaS stays text until silver |
| 02_silver | 2 Standardise types & timezones | ERP crosswalk = single source of truth; local-time waste |
| 02_silver | 3 Deduplicate / 4 Rules | re-sends dropped, then pass-rules → quarantine with reasons |
| 03_gold | 3 Core aggregates | censored demand, price version per week, robust spike flag |
| 03_gold | 4 Forecast / recommendation | baseline vs gradient-boosted correction; reorder formula and reasons |
| 03_gold | 5 Backtest | same code, earlier weeks only; gate decides champion; MLflow + UC alias |
| 03_gold | 6 Write + UC comments | grain comments, row filter, cost mask |
| 03_gold | 7 Signal check | signals in range, drift monitor, quality checks |
| 04_demo_queries | 2 Business questions | Q1 lineage → Q4 model vs baseline → Q5 ST-07 orders → Q7 governance |
| 04_demo_queries | 5 Execution plan | broadcast join, partial/final aggregate, AQE |

## What I'd do next
1. Run the chain on serverless once the account is back (`run_notebooks.py 00_generate_data 01_bronze 02_silver 03_gold 04_demo_queries`, then `03_gold 04_demo_queries` again so the rerun check has two writes to compare), publish the dashboard with `dashboards/build_dashboard.py`, and open it.
2. GenAI layer: a store manager assistant over the gold tables plus documents (supplier terms, shelf-life SOPs, promo plans).
3. Two deliveries a week for short-life perishables; re-run the backtest.
4. Supplier fill rates per item, so constrained items are ordered earlier or substituted.
5. Schedule train → gate → score as a job with a condition task on `retrain_needed`; Lakeflow Connect for the real Postgres/MySQL sources.
