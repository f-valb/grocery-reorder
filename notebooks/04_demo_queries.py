# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Demo queries
# MAGIC **Purpose:** answer the store manager's questions from the gold data product, one cell per question.
# MAGIC **Inputs:** `gold_*`, `silver_quality_log`
# MAGIC **Outputs:** printed results and charts (nothing written)

# COMMAND ----------

# Setup
import matplotlib.pyplot as plt

spark.sql("USE workspace.grocery_reorder")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Which items run out most, and how much demand do raw sales hide?

# COMMAND ----------

# SIGNAL: popular items with frequent stockouts; estimated demand vs raw sales shows the censored demand
display(spark.sql("""
SELECT product_name, category,
       round(avg(availability_pct) * 100, 1) AS availability_pct,
       sum(stockout_days) AS stockout_days,
       sum(units_sold) AS units_sold,
       round(sum(est_demand)) AS est_demand,
       round((sum(est_demand) / sum(units_sold) - 1) * 100, 1) AS hidden_demand_pct
FROM gold_store_product_week
WHERE NOT is_partial_week
GROUP BY ALL ORDER BY availability_pct LIMIT 8"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Where does waste come from: which items, and which store?

# COMMAND ----------

# SIGNAL: over-ordered fresh produce (waste as a share of what was received)
display(spark.sql("""
SELECT product_name, category, sum(received) AS received, sum(waste_units) AS waste_units,
       round(sum(waste_units) / sum(received) * 100, 1) AS waste_pct_of_received, round(sum(waste_value)) AS waste_usd
FROM gold_store_product_week WHERE perishable AND NOT is_partial_week
GROUP BY ALL ORDER BY waste_pct_of_received DESC LIMIT 6"""))

# COMMAND ----------

# SIGNAL: the store that chronically over-orders: receives more than its demand and wastes ~2x the average
display(spark.sql("""
SELECT store_id, store_size,
       round(sum(waste_value)) AS waste_usd,
       round(sum(waste_value) / avg(sum(waste_value)) OVER (), 2) AS waste_vs_avg_store,
       round(sum(received) / sum(est_demand), 2) AS received_vs_demand
FROM gold_store_product_week WHERE NOT is_partial_week
GROUP BY ALL ORDER BY waste_usd DESC LIMIT 5"""))

# COMMAND ----------

# Chart: waste $ per store (S07 stands out)
waste_by_store = spark.sql("""
SELECT store_id, sum(waste_value) AS waste_usd FROM gold_store_product_week GROUP BY 1 ORDER BY 1""").toPandas()
fig, ax = plt.subplots(figsize=(10, 3.5))
ax.bar(waste_by_store.store_id, waste_by_store.waste_usd,
       color=["#D55E00" if s == "S07" else "#0072B2" for s in waste_by_store.store_id])
ax.axhline(waste_by_store.waste_usd.mean(), color="grey", linestyle="--", label="average store")
ax.set_title("Waste $ per store, 12 weeks"); ax.set_ylabel("USD at cost"); ax.legend()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. What should store S07 order next week, and why?

# COMMAND ----------

# Reorder list for one store: quantity, the numbers behind it, and the reason in plain English
display(spark.sql("""
SELECT product_name, category, round(forecast_units) AS forecast, round(safety_stock) AS safety_stock,
       on_hand, on_order, round(on_hand_at_arrival) AS on_hand_at_arrival, case_pack, order_cases, order_units,
       order_value, reason
FROM gold_reorder_recommendation WHERE store_id = 'S07'
ORDER BY order_value DESC"""))

# COMMAND ----------

# Why the list looks the way it does across the chain: how many items fall under each reason
display(spark.sql("""
SELECT regexp_extract(reason, '^([^:(]+)', 1) AS reason_type, count(*) AS store_products,
       sum(order_units) AS order_units, round(sum(order_value)) AS order_value_usd
FROM gold_reorder_recommendation GROUP BY 1 ORDER BY store_products DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. How good is the forecast, and would the recommendations have cut stockouts and waste?

# COMMAND ----------

# Backtest summary on the last 2 full weeks: forecast error and outcomes vs what actually happened
display(spark.sql("""
SELECT slice, wape_forecast, wape_naive_raw_sales,
       actual_stockout_days, sim_stockout_days, actual_waste_units, sim_waste_units,
       actual_waste_usd, sim_waste_usd, actual_lost_margin_usd, sim_lost_margin_usd,
       (actual_waste_usd + actual_lost_margin_usd) - (sim_waste_usd + sim_lost_margin_usd) AS net_saving_usd
FROM gold_backtest_summary"""))

# COMMAND ----------

# Chart: waste $ and lost margin $, actual vs recommendation, per product segment
bt = spark.sql("""SELECT slice, actual_waste_usd, sim_waste_usd, actual_lost_margin_usd, sim_lost_margin_usd
                  FROM gold_backtest_summary WHERE slice NOT LIKE '20%' AND slice != 'TOTAL'""").toPandas()
fig, axes = plt.subplots(1, 2, figsize=(12, 3.8), sharey=True)
for ax, (metric, title) in zip(axes, [("waste_usd", "Waste $ (at cost)"), ("lost_margin_usd", "Lost sales margin $")]):
    x = range(len(bt))
    ax.bar([i - 0.2 for i in x], bt[f"actual_{metric}"], width=0.4, label="actual", color="#999999")
    ax.bar([i + 0.2 for i in x], bt[f"sim_{metric}"], width=0.4, label="with recommendation", color="#0072B2")
    ax.set_xticks(list(x)); ax.set_xticklabels(bt.slice, fontsize=8); ax.set_title(title); ax.legend()
fig.suptitle("Backtest, last 2 full weeks")
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. What does a promo do to sales, and which category is trending up?

# COMMAND ----------

# SIGNAL: promo lift on promoted products (promo weeks vs their normal weeks)
display(spark.sql("""
SELECT product_name,
       round(avg(CASE WHEN is_promo THEN units_sold END)) AS avg_units_promo_week,
       round(avg(CASE WHEN NOT is_promo THEN units_sold END)) AS avg_units_normal_week,
       round(avg(CASE WHEN is_promo THEN units_sold END) / avg(CASE WHEN NOT is_promo THEN units_sold END), 2) AS promo_lift
FROM gold_store_product_week
WHERE product_id IN (SELECT product_id FROM silver_promotions) AND NOT is_partial_week
GROUP BY ALL ORDER BY promo_lift DESC"""))

# COMMAND ----------

# SIGNAL: category trend: forecast trend as % of demand per week, and weekly demand per category
display(spark.sql("""
SELECT p.category, round(avg(f.trend_per_week / f.forecast_units) * 100, 1) AS trend_pct_per_week
FROM gold_demand_forecast f JOIN silver_products p USING (product_id)
GROUP BY 1 ORDER BY 2 DESC"""))

trend = spark.sql("""
SELECT week_start, category, sum(est_demand) AS est_demand
FROM gold_store_product_week WHERE NOT is_partial_week AND NOT is_promo GROUP BY 1, 2 ORDER BY 1""").toPandas()
fig, ax = plt.subplots(figsize=(10, 3.5))
for cat, g in trend.groupby("category"):
    ax.plot(g.week_start, g.est_demand, marker="o", linewidth=3 if cat == "Plant-based" else 1.2, label=cat)
ax.set_title("Weekly estimated demand by category (non-promo)"); ax.set_ylabel("units"); ax.legend(fontsize=8, ncol=3)
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Can we trust the data?

# COMMAND ----------

# QUALITY: rows quarantined or fixed per rule, and the gold freshness/consistency checks
display(spark.sql("SELECT table_name, rule, row_count FROM silver_quality_log ORDER BY 1, 2"))
display(spark.sql("SELECT check_name, value, passed FROM gold_quality_check"))
