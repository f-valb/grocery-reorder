# Decisions and trade-offs

- Workspace profile `food` (DEFAULT is out of credits).
- Raw data lands as CSV in a UC Volume; bronze keeps every column as a string so bad values survive to silver, where they are cast with try_cast and quarantined.
- Weekly grain everywhere (12 weeks, the last is partial: data through Wednesday). Daily data would be better for stockouts but is not in the brief.
- Synthetic data comes from a stock simulation (on hand + received − sold − waste), so stockouts censor sales the way they do in a real store.
- Silver dedupes snapshots before validating, so a corrected re-extract wins (one planted negative was fixed this way: 40 planted, 39 quarantined).
- Waste is checked against stock available that week (prior on hand + received), not received alone: waste of older stock is legitimate. Week 1 has no prior snapshot, so on hand + received stands in for opening stock.
- Missing shelf life is filled with the category median and flagged rather than quarantined: dropping a product would drop its sales too.
- Quarantined rows keep the full record as JSON in one `silver_quarantine` table; `silver_quality_log` holds per-rule counts.
- Forecast = 4-week moving average of estimated demand + 8-week linear trend; range ±1.28σ of the trend residuals (floored at √forecast). Same function feeds the backtest, so the backtest tests the real code.
- Safety stock z = 1.65 for non-perishables (95% target), 0.84 for perishables: perishables trade a little availability for much less waste.
- Order uses stock expected at arrival (on hand − rest of this week's forecast), because the order lands next week.
- Shelf-life cap as specified: perishables ordered only for demand that sells within shelf life. Backtest shows the cost: for items with ≤7 days shelf life, weekly delivery + cap cuts waste $13.9k → $0.4k but adds $7.2k lost margin. Net still positive; the real fix is 2 deliveries a week for those items.
- Backtest replays the recommendation against estimated demand, assuming the supplier fills the full order (the popular items are supplier-constrained, so their stockout gain is an upper bound). The simulated expiry rule is stricter than reality for long-life perishables, so their simulated waste is a high estimate.
- Promo weeks are excluded from the baseline and from the backtest comparison; a production system would add the planned promo lift on top.
- Dashboard built from the kit's widget helpers and deployed with the CLI (lakeview create/update + publish) from `dashboards/build_dashboard.py`; tables instead of top-N bar charts because they stay readable with the store filter.

## v2 (branch multi-source-ml, schema grocery_intel)
- Three mocked source systems instead of one generator output: Postgres POS, MySQL ERP, SaaS store-ops + promo tool. Real ingestion is not wired up (Lakeflow Connect / SaaS connector in production); the raw extracts land as parquet per source table.
- Core entity: store-item. The ERP is the key master (it holds the POS ids and store names), so silver can map every source onto it.
- Flaws are extra corrupted copies of true rows (later `updated_at`), never edits, and quality rules run before dedupe so a corrupted copy can't win.
- Bronze reads every extract with an explicit schema; SaaS fields stay text and are parsed in silver, where misfits are quarantined.
- Spike rule for one-off events: residual from the store-item's trend line above median + 3.5 × max(1.4826 × MAD, √mean). Robust stats, because the spike itself would inflate a standard deviation. Locally it flags all 7 festival rows plus 2 others.
- Silver keys: ERP codes (`ST-07`, `SKU-00012`) are the conformed keys; POS ids and SaaS store names map onto them through broadcast-joined crosswalks.
- Dedupe in two steps: exact re-sends (identical values) are dropped before the rules; after the rules, the latest valid version per key wins. Locally: 120 re-sends removed, 0 superseded, and silver sales/snapshots = 9,680 rows = the true row count.
- Waste is bucketed by store-local event date (585 of 6,545 logs would move week if bucketed in UTC) and by event time, not sync time (68 late-synced logs stay in their week).
- Value rules only apply when keys are known (`NOT keys_known OR ...`), so an orphan row fails only the key rule and the per-rule counts add up.
- Model: one global scikit-learn HistGradientBoosting model across all store-items that predicts actual demand / baseline forecast (a multiplier), plus 10%/90% quantile models for the range. Inputs are known at order time: history up to last week and the planned promo calendar. Chosen over one model per item: cheap to train, and new items borrow from their category.
- Gate: the model is champion only if its backtest WAPE beats the baseline by more than 2% (relative). Locally: model 10.5% vs baseline 13.6% (non-promo 10.5% vs 12.2%, promo weeks 10.4% vs 43.8%). If it ever loses, the baseline keeps serving and `retrain_needed` is raised.
- Safety stock for the model uses the width of its own 80% range; for the baseline, the trend residuals.
- Backtest now includes promo weeks (the model plans for them); spike weeks are left out of the evaluation as one-offs.
- MLflow: logged runs, model registered in UC (`grocery_intel.demand_forecaster`) with a `champion` alias on Databricks; locally a file store and no registry.
- Drift: PSI on the 4 continuous model inputs, training rows vs next week's scoring rows, alert above 0.25 (kit default).
- Governance: `gov_store_managers` mapping table, row filter on store_id for the 3 store-facing gold tables, column mask on unit_cost (admin and finance only). Written for Unity Catalog, not testable on local Spark.
- `CREATE OR REPLACE TABLE ... AS` is avoided in gold: local Spark has no Delta; DataFrame overwrite works in both.
- Serverless (Free Edition) findings: UC model registry upload is denied (S3 AccessDenied), so gold tries UC registration and falls back to logging the model in the MLflow experiment; the gate result and registry status are stored in `gold_model_monitor`. Unity Catalog blocks time travel on tables with a row filter or column mask, so the rerun check compares per-run fingerprints (row count + sum of xxhash64, `model_version` excluded as run metadata) in `gold_run_fingerprint`.
- The v2 dashboard is published without embedded credentials, so the row filter applies per viewer.
- Genie space over the gold tables (plus silver_stores and silver_quality_log) with business definitions as text instructions and two curated example SQLs (forecast error by slice, sustained category growth). Without them Genie matched 'Non-promo weeks' with LIKE and called a single noisy week the fastest growth. All 5 demo questions tested through the Conversation API.
- Forecast and reorder tables are written through a Spark join to gold_store_product_week (adds store_name, region) so Unity Catalog lineage links them to their sources; a pandas -> createDataFrame write showed only the notebook upstream.
