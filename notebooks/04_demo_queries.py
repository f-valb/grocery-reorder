# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Demo queries
# MAGIC **Purpose:** the end-to-end story for the business leader: three source systems → one store-item view → a trained forecast → next week's orders, with proof that it is right.
# MAGIC **Inputs:** `bronze_*`, `silver_*`, `gold_*`, `gov_store_managers`
# MAGIC **Outputs:** printed results and charts; `gold_execution_plan` (the captured query plan)

# COMMAND ----------

# Setup
import io
from contextlib import redirect_stdout

from pyspark.sql import functions as F

spark.sql("USE workspace.grocery_intel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Headline KPIs

# COMMAND ----------

# Headline numbers: availability and waste over the last 4 full weeks, next week's order, and who is forecasting
display(spark.sql("""
WITH recent AS (
  SELECT * FROM gold_store_product_week
  WHERE NOT is_partial_week AND week_num > (SELECT MAX(week_num) FROM gold_store_product_week WHERE NOT is_partial_week) - 4),
model AS (SELECT MAX(champion_method) AS champion_method, MAX(model_version) AS model_version, MAX(gate_reason) AS gate_reason
          FROM gold_model_monitor)
SELECT ROUND(100 * SUM(days_in_stock) / SUM(period_days), 1) AS availability_pct_last_4w,
       ROUND(SUM(waste_value)) AS waste_usd_last_4w,
       (SELECT ROUND(SUM(order_value)) FROM gold_reorder_recommendation) AS next_week_order_usd,
       (SELECT champion_method FROM model) AS forecast_method,
       (SELECT gate_reason FROM model) AS why
FROM recent"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Answer each business question

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1. Did the three systems really come together?** Rows per source: landed in bronze, clean in silver, quarantined.

# COMMAND ----------

# Lineage in numbers: every source row is either in silver or in quarantine with a reason
display(spark.sql("""
WITH landed AS (
  SELECT 'postgres_pos' AS source_system, 'pos_sales_weekly' AS source_table, COUNT(*) AS bronze_rows FROM bronze_pos_sales_weekly
  UNION ALL SELECT 'postgres_pos', 'pos_stock_snapshots', COUNT(*) FROM bronze_pos_stock_snapshots
  UNION ALL SELECT 'saas_store_ops', 'ops_waste_log', COUNT(*) FROM bronze_ops_waste_log
  UNION ALL SELECT 'saas_promo_tool', 'promo_calendar', COUNT(*) FROM bronze_promo_calendar),
clean AS (
  SELECT 'pos_sales_weekly' AS source_table, COUNT(*) AS silver_rows FROM silver_sales_weekly
  UNION ALL SELECT 'pos_stock_snapshots', COUNT(*) FROM silver_stock_snapshots
  UNION ALL SELECT 'ops_waste_log', COUNT(*) FROM silver_waste_events
  UNION ALL SELECT 'promo_calendar', COUNT(*) FROM silver_promotions),
quarantined AS (SELECT source_table, COUNT(*) AS quarantined_rows FROM silver_quarantine GROUP BY source_table),
fixed AS (SELECT table_name AS source_table, SUM(row_count) AS resends_removed FROM silver_quality_log
          WHERE rule = 'exact_resend_removed' GROUP BY table_name)
SELECT l.source_system, l.source_table, l.bronze_rows, c.silver_rows, COALESCE(q.quarantined_rows, 0) AS quarantined_rows,
       COALESCE(f.resends_removed, 0) AS resends_removed,
       l.bronze_rows - c.silver_rows - COALESCE(q.quarantined_rows, 0) - COALESCE(f.resends_removed, 0) AS unexplained_rows
FROM landed l JOIN clean c USING (source_table)
LEFT JOIN quarantined q USING (source_table) LEFT JOIN fixed f USING (source_table)
ORDER BY l.source_system, l.source_table"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Q2. Which items run out most, and how much demand do raw sales hide?**

# COMMAND ----------

# SIGNAL: popular items with frequent stockouts; estimated demand vs raw sales shows the hidden (censored) demand
display(spark.sql("""
SELECT product_name, category, ROUND(100 * AVG(availability_pct), 1) AS availability_pct, SUM(stockout_days) AS stockout_days,
       SUM(units_sold) AS units_sold, ROUND(SUM(est_demand)) AS est_demand,
       ROUND(100 * (SUM(est_demand) / SUM(units_sold) - 1), 1) AS hidden_demand_pct
FROM gold_store_product_week WHERE NOT is_partial_week
GROUP BY product_name, category ORDER BY availability_pct LIMIT 6"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Q3. Where does waste come from: which items, and which store?**

# COMMAND ----------

# SIGNAL: over-ordered fresh produce, and the store that chronically over-orders (ST-07)
display(spark.sql("""
SELECT product_name, SUM(received) AS received, SUM(expired_units) AS expired_units,
       ROUND(100 * SUM(expired_units) / SUM(received), 1) AS expired_pct_of_received, ROUND(SUM(waste_value)) AS waste_usd
FROM gold_store_product_week WHERE perishable AND NOT is_partial_week
GROUP BY product_name ORDER BY expired_pct_of_received DESC LIMIT 5"""))
display(spark.sql("""
SELECT store_id, store_name, store_size, ROUND(SUM(waste_value)) AS waste_usd,
       ROUND(SUM(waste_value) / AVG(SUM(waste_value)) OVER (), 2) AS waste_vs_avg_store,
       ROUND(SUM(received) / SUM(est_demand), 2) AS received_vs_demand
FROM gold_store_product_week WHERE NOT is_partial_week
GROUP BY store_id, store_name, store_size ORDER BY waste_usd DESC LIMIT 5"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Q4. Does the trained model beat the simple forecast?** Same backtest weeks, same inputs; error = WAPE (total absolute error / total demand).

# COMMAND ----------

# The champion decision in numbers: error by method, overall, in promo weeks and in normal weeks
display(spark.sql("""
SELECT slice,
       MAX(CASE WHEN method = 'baseline' THEN wape END) AS wape_baseline,
       MAX(CASE WHEN method = 'model' THEN wape END) AS wape_model,
       MAX(CASE WHEN method = 'baseline' THEN sim_stockout_days END) AS stockout_days_baseline,
       MAX(CASE WHEN method = 'model' THEN sim_stockout_days END) AS stockout_days_model,
       MAX(store_product_weeks) AS store_product_weeks
FROM gold_backtest_summary WHERE slice IN ('TOTAL', 'Promo weeks', 'Non-promo weeks')
GROUP BY slice ORDER BY slice"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Q5. What should ST-07 order next week, and why?**

# COMMAND ----------

# Reorder list for one store: the quantity, the numbers behind it, and the reason in plain English
display(spark.sql("""
SELECT product_name, ROUND(forecast_units) AS forecast, ROUND(safety_stock) AS safety_stock, on_hand, on_order,
       ROUND(on_hand_at_arrival) AS on_hand_at_arrival, case_pack, order_cases, order_units, order_value, reason
FROM gold_reorder_recommendation WHERE store_id = 'ST-07' ORDER BY order_value DESC LIMIT 12"""))
display(spark.sql("""
SELECT regexp_extract(reason, '^([^:(]+)', 1) AS reason_type, COUNT(*) AS store_items, SUM(order_units) AS order_units
FROM gold_reorder_recommendation GROUP BY 1 ORDER BY store_items DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Q6. Would these orders have cut stockouts and waste?** Backtest on the last 2 full weeks: what actually happened vs the recommendation from each method.

# COMMAND ----------

# Dollars: waste at cost plus lost sales margin, actual vs each method, and the net saving
display(spark.sql("""
SELECT slice, method, actual_waste_usd, sim_waste_usd, actual_lost_margin_usd, sim_lost_margin_usd,
       (actual_waste_usd + actual_lost_margin_usd) - (sim_waste_usd + sim_lost_margin_usd) AS net_saving_usd
FROM gold_backtest_summary
WHERE slice IN ('TOTAL', 'Non-perishable', 'Perishable, shelf life <= 7d', 'Perishable, shelf life > 7d')
ORDER BY slice, method"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Q7. Is the model healthy, and can a manager see only their own store?**

# COMMAND ----------

# Model health: drift per input (PSI), gate result, retrain flag
display(spark.sql("SELECT check_name, value, threshold, alert, champion_method, model_version, retrain_needed FROM gold_model_monitor"))

# COMMAND ----------

# Governance: rows each role would see under the row filter (same predicate as gov_store_filter, evaluated per user)
display(spark.sql("""
SELECT g.user_email, g.role, g.store_id AS allowed_store, COUNT(r.store_id) AS reorder_rows_visible
FROM gov_store_managers g
LEFT JOIN gold_reorder_recommendation r ON g.store_id = 'ALL' OR r.store_id = g.store_id
WHERE g.role != 'store_manager' OR g.store_id IN ('ST-07', 'ST-12')
GROUP BY g.user_email, g.role, g.store_id ORDER BY g.role, g.user_email"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 Chart

# COMMAND ----------

# Chart: backtest dollars by method (headline result) and forecast error by method
import matplotlib.pyplot as plt

summary = spark.sql("SELECT slice, method, wape, sim_waste_usd, sim_lost_margin_usd, actual_waste_usd, actual_lost_margin_usd "
                    "FROM gold_backtest_summary WHERE slice IN ('TOTAL', 'Promo weeks', 'Non-promo weeks')").toPandas()
total = summary[summary.slice == "TOTAL"].set_index("method")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
labels = ["Actual", "Baseline orders", "Model orders"]
waste = [total.actual_waste_usd.iloc[0], total.loc["baseline", "sim_waste_usd"], total.loc["model", "sim_waste_usd"]]
lost = [total.actual_lost_margin_usd.iloc[0], total.loc["baseline", "sim_lost_margin_usd"], total.loc["model", "sim_lost_margin_usd"]]
ax1.bar(labels, waste, label="Waste $ (at cost)", color="#D55E00")
ax1.bar(labels, lost, bottom=waste, label="Lost sales margin $", color="#0072B2")
ax1.set_title("Backtest, last 2 full weeks: waste + lost margin"); ax1.set_ylabel("USD"); ax1.legend()
err = summary.pivot(index="slice", columns="method", values="wape").loc[["Non-promo weeks", "Promo weeks", "TOTAL"]]
err.plot.bar(ax=ax2, color=["#999999", "#0072B2"], rot=0)
ax2.set_title("Forecast error (WAPE) by method"); ax2.set_ylabel("WAPE")
display(fig)

# COMMAND ----------

# Weekly demand by category: the Plant-based trend the forecast picks up
display(spark.sql("""
SELECT week_start, category, ROUND(SUM(est_demand)) AS est_demand FROM gold_store_product_week
WHERE NOT is_partial_week AND NOT is_promo AND NOT is_spike_week GROUP BY week_start, category ORDER BY week_start, category"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 Data product check (grain, freshness, quality)

# COMMAND ----------

# QUALITY: grain: each gold table has exactly one row per its stated key
display(spark.sql("""
SELECT 'gold_store_product_week' AS table_name, 'store x product x week' AS grain, COUNT(*) AS row_count,
       COUNT(DISTINCT store_id, item_id, week_start) AS distinct_keys FROM gold_store_product_week
UNION ALL SELECT 'gold_demand_forecast', 'store x product', COUNT(*), COUNT(DISTINCT store_id, item_id) FROM gold_demand_forecast
UNION ALL SELECT 'gold_reorder_recommendation', 'store x product', COUNT(*), COUNT(DISTINCT store_id, item_id) FROM gold_reorder_recommendation
UNION ALL SELECT 'gold_backtest', 'method x store x product x week', COUNT(*), COUNT(DISTINCT method, store_id, item_id, week_start) FROM gold_backtest"""))

# COMMAND ----------

# QUALITY: freshness, consistency and planted-signal checks written by gold
display(spark.sql("SELECT check_name, value, passed FROM gold_quality_check ORDER BY check_name"))
display(spark.sql("SELECT signal_name, ROUND(value, 3) AS value, low, high, passed FROM gold_signal_check ORDER BY signal_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Rerun check (idempotency):** rerunning gold must not change its data. Compare the last two *data writes* in the Delta history.
# MAGIC COMMENT, ALTER and row-filter statements also create versions, so "latest − 1" would compare a table with itself and always pass: only write operations are used.

# COMMAND ----------

# QUALITY: idempotency: the last two data writes of a gold table hold exactly the same rows (exceptAll both ways)
# SPARK: Delta time travel (versionAsOf) reads an older snapshot of the same table
for table in ["gold_store_product_week", "gold_reorder_recommendation"]:
    writes = (spark.sql(f"DESCRIBE HISTORY {table}")
              .where(F.col("operation").isin("CREATE OR REPLACE TABLE AS SELECT", "WRITE", "CREATE TABLE AS SELECT"))
              .orderBy(F.col("version").desc()).limit(2).collect())
    if len(writes) < 2:
        print(f"{table}: only {len(writes)} data write so far: rerun 03_gold, then this check")
        continue
    newer, older = (spark.read.option("versionAsOf", w.version).table(table) for w in writes)
    added, removed = newer.exceptAll(older).count(), older.exceptAll(newer).count()
    print(f"{table}: versions {writes[1].version} -> {writes[0].version}: {added} rows added, {removed} removed "
          f"-> {'IDEMPOTENT' if added == removed == 0 else 'CHANGED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 Execution plan
# MAGIC The main join + aggregate behind the waste-by-store question: weekly sales joined to the product price version valid that week, then grouped by store and category.
# MAGIC Print output is lost in jobs, so the plan text is written to `gold_execution_plan` and read back.

# COMMAND ----------

# SPARK: capture the formatted physical plan of the main join + aggregate
waste_by_store_category = spark.sql("""
SELECT s.store_id, p.category, SUM(s.units_sold * p.unit_price) AS revenue, COUNT(*) AS store_item_weeks
FROM silver_sales_weekly s
JOIN silver_products p ON p.item_id = s.item_id AND s.week_start BETWEEN p.valid_from AND p.valid_to
GROUP BY s.store_id, p.category""")
plan_text = io.StringIO()
with redirect_stdout(plan_text):
    waste_by_store_category.explain("formatted")
waste_by_store_category.collect()  # run it once so AQE has executed the plan
spark.createDataFrame([(i, line) for i, line in enumerate(plan_text.getvalue().splitlines())], "line_no INT, plan_line STRING") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_execution_plan")
display(spark.table("gold_execution_plan").orderBy("line_no"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the plan, in plain English**
# MAGIC - **Join strategy: broadcast hash join.** The products table (46 price versions) is far below the broadcast threshold, so Spark copies it to every task and joins on `item_id`; the "price valid that week" range check runs as a filter inside the join. No shuffle of the 9,680 sales rows is needed for the join. On serverless the node is `PhotonBroadcastHashJoin`.
# MAGIC - **Exchange (shuffle):** there is one, between the two aggregates: rows are hash-partitioned by `store_id, category` so every store-category ends up in one partition. Plus a `BroadcastExchange` that ships the small products table.
# MAGIC - **Partial vs final HashAggregate:** each task first sums its own rows per store-category (partial), so only one small row per group per task crosses the network; after the shuffle a final aggregate adds those partial sums up. On serverless: `PhotonGroupingAgg` partial → final merge.
# MAGIC - **AQE (adaptive query execution):** the plan is wrapped in `AdaptiveSparkPlan`; at run time AQE coalesces the shuffle partitions to a handful (the data is tiny) and could switch a sort-merge join to a broadcast join if a side turned out small. `isFinalPlan=false` in the printed plan only means it was printed before execution.
# MAGIC - **At TB scale:** the products dimension still broadcasts (thousands of rows), but sales would be partitioned or liquid-clustered by `week_start` so a week filter prunes files; a skewed store (one huge flagship) would be handled by AQE skew-join splitting or salting the key; a large-to-large join (sales to snapshots) would become a sort-merge join with a shuffle on both sides.
