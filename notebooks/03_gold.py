# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: the store manager data product
# MAGIC **Purpose:** weekly performance, next-week demand forecast, reorder recommendation, and a backtest of the recommendation on the last 2 full weeks.
# MAGIC **Inputs:** `silver_*`
# MAGIC **Outputs:**
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `gold_store_product_week` | store × product × week |
# MAGIC | `gold_demand_forecast` | store × product (next week) |
# MAGIC | `gold_reorder_recommendation` | store × product (order placed now, arrives next week) |
# MAGIC | `gold_backtest` | store × product × backtest week |
# MAGIC | `gold_backtest_summary` | backtest week (plus a total row) |
# MAGIC | `gold_quality_check` | one row per check |

# COMMAND ----------

# Setup
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

spark.sql("USE workspace.grocery_reorder")
LEAD_TIME_WEEKS = 1
Z_NON_PERISHABLE = 1.65  # RULE: 95% availability target on non-perishables
Z_PERISHABLE = 0.84      # RULE: 80% on perishables: a little less availability for much less waste
RANGE_Z = 1.28           # forecast low/high = 80% range
BACKTEST_WEEKS = 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weekly store/product performance
# MAGIC Demand is censored by stockouts: a shelf that was empty for 3 days sold less than customers wanted.
# MAGIC Estimated demand = units sold / days in stock × 7. The last week is partial (data through the as-of date), so its days are counted from the as-of date.

# COMMAND ----------

# Build weekly performance: sales, stockout days, availability, estimated demand, waste units and value
spark.sql("""
CREATE OR REPLACE TABLE gold_store_product_week AS
WITH as_of AS (SELECT max(snapshot_date) AS as_of_date FROM silver_stock_snapshots),
waste_week AS (
  SELECT store_id, product_id, week_start,
         sum(waste_units) AS waste_units,
         sum(CASE WHEN reason = 'expired' THEN waste_units ELSE 0 END) AS expired_units,
         sum(CASE WHEN reason = 'damaged' THEN waste_units ELSE 0 END) AS damaged_units
  FROM silver_waste GROUP BY ALL)
SELECT s.store_id, st.region, st.size AS store_size, s.product_id, p.product_name, p.category, p.perishable,
       s.week_start,
       int(datediff(s.week_start, (SELECT min(week_start) FROM silver_sales_weekly)) / 7) + 1 AS week_num,
       -- RULE: a week has 7 days except the current one, which runs to the as-of date
       least(7, datediff(a.as_of_date, s.week_start) + 1) AS period_days,
       least(7, datediff(a.as_of_date, s.week_start) + 1) < 7 AS is_partial_week,
       pr.discount_pct IS NOT NULL AS is_promo,
       s.units_sold, s.days_in_stock,
       least(7, datediff(a.as_of_date, s.week_start) + 1) - s.days_in_stock AS stockout_days,
       round(s.days_in_stock / least(7, datediff(a.as_of_date, s.week_start) + 1), 4) AS availability_pct,
       -- TRAP: demand from in-stock days, not raw sales (raw sales hide demand lost to empty shelves)
       CASE WHEN s.days_in_stock > 0 THEN round(s.units_sold / s.days_in_stock * 7, 1) END AS est_demand,
       round(s.units_sold * p.unit_price * (1 - coalesce(pr.discount_pct, 0) / 100), 2) AS revenue,
       n.on_hand, n.on_order, n.received,
       coalesce(w.waste_units, 0) AS waste_units, coalesce(w.expired_units, 0) AS expired_units,
       coalesce(w.damaged_units, 0) AS damaged_units,
       round(coalesce(w.waste_units, 0) * p.unit_cost, 2) AS waste_value
FROM silver_sales_weekly s
CROSS JOIN as_of a
JOIN silver_products p USING (product_id)
JOIN silver_stores st USING (store_id)
LEFT JOIN silver_promotions pr ON pr.product_id = s.product_id AND pr.week_start = s.week_start
LEFT JOIN silver_stock_snapshots n ON n.store_id = s.store_id AND n.product_id = s.product_id AND n.week_start = s.week_start
LEFT JOIN waste_week w ON w.store_id = s.store_id AND w.product_id = s.product_id AND w.week_start = s.week_start
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast: moving average + trend (no ML)
# MAGIC Per store/product, on the 8 weeks before the target week:
# MAGIC - baseline = average estimated demand of the last 4 usable weeks
# MAGIC - trend = slope of a straight line through the 8 weeks, added for the gap between those 4 weeks and the target week
# MAGIC - range = ± 1.28 × the scatter around that line (80% range), never below the Poisson noise √forecast

# COMMAND ----------

# Forecast function shared by next week's forecast and the backtest (same code, so the backtest is honest)
weekly = spark.table("gold_store_product_week").toPandas()
products = spark.table("silver_products").toPandas()
products["shelf_life_days"] = products.shelf_life_days.astype(int)
weekly.attrs = {}


def fit_one(g, target_week):
    g = g.sort_values("week_num")
    x, y = g.week_num.to_numpy(float), g.est_demand.to_numpy(float)
    slope, intercept = np.polyfit(x, y, 1) if len(g) >= 3 else (0.0, y.mean())
    last4 = g.tail(4)
    forecast = max(0.0, last4.est_demand.mean() + slope * (target_week - last4.week_num.mean()))
    resid_sd = np.std(y - (slope * x + intercept), ddof=1) if len(g) >= 3 else y.std()
    sigma = max(float(np.nan_to_num(resid_sd)), np.sqrt(forecast))
    return pd.Series({"forecast_units": forecast, "sigma": sigma, "trend_per_week": slope,
                      "baseline_weeks": len(g), "raw_sales_ma4": last4.units_sold.mean()})


def forecast_week(target_week):
    # TRAP: baseline excludes promo weeks, the partial current week and weeks with no stock (no demand estimate)
    base = weekly[(weekly.week_num < target_week) & (weekly.week_num >= target_week - 8)
                  & ~weekly.is_promo & ~weekly.is_partial_week & weekly.est_demand.notna()]
    fc = base.groupby(["store_id", "product_id"]).apply(fit_one, target_week=target_week).reset_index()
    fc["target_week"] = target_week
    fc["forecast_low"] = (fc.forecast_units - RANGE_Z * fc.sigma).clip(lower=0)
    fc["forecast_high"] = fc.forecast_units + RANGE_Z * fc.sigma
    return fc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reorder rule
# MAGIC `order = forecast + safety stock − on hand at arrival − on order`, rounded up to whole case packs.
# MAGIC - Safety stock = z × σ × √lead time (z = 1.65 non-perishable for 95% availability, 0.84 perishable).
# MAGIC - On hand at arrival: the order arrives at the start of next week, so the rest of this week's demand is used up first.
# MAGIC - Perishables are capped at what sells within shelf life: forecast / 7 × shelf life days, minus stock already there (rounded to the nearest case).

# COMMAND ----------

# Reorder function shared by next week's list and the backtest
def recommend(fc, stock, remaining_days):
    r = fc.merge(products, on="product_id").merge(stock, on=["store_id", "product_id"], how="left")
    r["has_snapshot"] = r.on_hand.notna()
    r[["on_hand", "on_order"]] = r[["on_hand", "on_order"]].fillna(0)
    # TRAP: stock still on the shelf when the order lands, not stock today
    r["on_hand_at_arrival"] = (r.on_hand - r.forecast_units * remaining_days / 7).clip(lower=0)
    z = np.where(r.perishable, Z_PERISHABLE, Z_NON_PERISHABLE)
    r["safety_stock"] = z * r.sigma * np.sqrt(LEAD_TIME_WEEKS)
    # RULE: order = forecast + safety stock − on hand − on order, rounded up to case packs
    r["net_need"] = (r.forecast_units + r.safety_stock - r.on_hand_at_arrival - r.on_order).clip(lower=0)
    r["need_units"] = np.ceil(r.net_need / r.case_pack) * r.case_pack
    # RULE: perishables capped at what sells within shelf life
    shelf_cap = (r.forecast_units / 7 * r.shelf_life_days - r.on_hand_at_arrival - r.on_order).clip(lower=0)
    r["shelf_life_cap"] = np.where(r.perishable, np.round(shelf_cap / r.case_pack) * r.case_pack, np.nan)
    r["capped_by_shelf_life"] = r.perishable & (r.shelf_life_cap < r.need_units)
    r["order_units"] = np.where(r.capped_by_shelf_life, r.shelf_life_cap, r.need_units).astype(int)
    r["order_cases"] = (r.order_units // r.case_pack).astype(int)
    return r

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next week's forecast and reorder list

# COMMAND ----------

# Forecast next week and build the reorder list from the current (partial) week's stock snapshot
current_week = int(weekly.week_num.max())
target_week = current_week + 1
forecast = forecast_week(target_week)

now = weekly[weekly.week_num == current_week]
remaining_days = 7 - int(now.period_days.iloc[0])
stock_now = now[["store_id", "product_id", "on_hand", "on_order"]]
reorder = recommend(forecast, stock_now, remaining_days)

# Recent availability (last 4 full weeks) explains why an item is on the list
recent = (weekly[~weekly.is_partial_week & (weekly.week_num > current_week - 5)]
          .groupby(["store_id", "product_id"]).agg(recent_availability=("availability_pct", "mean"),
                                                    recent_waste_units=("waste_units", "mean")).reset_index())
reorder = reorder.merge(recent, on=["store_id", "product_id"], how="left")


def reason(r):
    if not r.has_snapshot:
        return "No valid stock snapshot this week: assumed 0 on hand, check the shelf"
    if r.capped_by_shelf_life:
        return f"Capped by shelf life ({r.shelf_life_days}d): more would expire before it sells"
    if r.order_units == 0 and r.on_hand_at_arrival > r.forecast_units:
        return "Overstocked: stock on hand covers next week"
    if r.order_units == 0:
        return "Enough stock: no order needed"
    if r.recent_availability < 0.9:
        return f"Stockout risk: in stock {r.recent_availability:.0%} of days lately, ordering true demand"
    if r.trend_per_week > 0.04 * max(r.forecast_units, 1):
        return f"Trending up: +{r.trend_per_week:.1f} units/week"
    return "Regular replenishment"


reorder["reason"] = reorder.apply(reason, axis=1)
reorder["order_value"] = (reorder.order_units * reorder.unit_cost).round(2)
week_start_next = (pd.Timestamp(weekly.week_start.max()) + pd.Timedelta(weeks=1)).date()

forecast_out = forecast.assign(week_start=week_start_next)[
    ["store_id", "product_id", "week_start", "target_week", "forecast_units", "forecast_low", "forecast_high",
     "trend_per_week", "sigma", "baseline_weeks", "raw_sales_ma4"]].round(1)
reorder_out = reorder.assign(week_start=week_start_next)[
    ["store_id", "product_id", "product_name", "category", "perishable", "week_start", "forecast_units", "safety_stock",
     "on_hand", "on_order", "on_hand_at_arrival", "case_pack", "shelf_life_days", "shelf_life_cap", "order_units",
     "order_cases", "order_value", "capped_by_shelf_life", "recent_availability", "reason"]].round(2)
spark.createDataFrame(forecast_out).write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_demand_forecast")
spark.createDataFrame(reorder_out).write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_reorder_recommendation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backtest on the last 2 full weeks
# MAGIC For each backtest week: forecast with only the weeks before it, place the order at the end of the previous week (its real on-hand snapshot), then replay the week against the actual estimated demand.
# MAGIC - Forecast error = WAPE = Σ|forecast − actual demand| / Σ actual demand (non-promo weeks, also shown for a naive "average of raw sales").
# MAGIC - Simulated stockouts: days the order + stock would not have covered demand. Simulated waste: perishable stock left over that cannot sell within shelf life.
# MAGIC - Caveats: the simulation assumes the supplier delivers the full order, and its expiry rule is stricter than real stores (so simulated waste is a high estimate).

# COMMAND ----------

# TRAP: each backtest week only sees data from before it (no leakage); order placed with the previous week's snapshot
full_weeks = sorted(weekly.loc[~weekly.is_partial_week, "week_num"].unique())
backtest_rows = []
for wk in full_weeks[-BACKTEST_WEEKS:]:
    fc = forecast_week(wk)
    prev = weekly[weekly.week_num == wk - 1][["store_id", "product_id", "on_hand"]].assign(on_order=0)
    rec = recommend(fc, prev, remaining_days=0)
    actual = weekly[weekly.week_num == wk][["store_id", "product_id", "week_start", "is_promo", "est_demand", "units_sold",
                                            "stockout_days", "expired_units", "received"]]
    bt = rec.merge(actual, on=["store_id", "product_id"])
    bt["available"] = bt.on_hand + bt.order_units
    bt["sim_stockout_days"] = np.where(bt.est_demand > bt.available,
                                       7 - np.ceil(7 * bt.available / bt.est_demand.where(bt.est_demand > 0, 1)), 0)
    leftover = (bt.available - bt.est_demand).clip(lower=0)
    bt["sim_waste_units"] = np.where(bt.perishable, leftover * np.minimum(1, 7 / bt.shelf_life_days), 0).round()
    # Lost sales = demand the shelf could not serve; valued at margin, waste valued at cost, so both are in dollars
    bt["actual_lost_units"] = (bt.est_demand - bt.units_sold).clip(lower=0)
    bt["sim_lost_units"] = (bt.est_demand - bt.available).clip(lower=0)
    bt["segment"] = np.where(~bt.perishable, "Non-perishable",
                             np.where(bt.shelf_life_days <= 7, "Perishable, shelf life <= 7d", "Perishable, shelf life > 7d"))
    backtest_rows.append(bt)

backtest = pd.concat(backtest_rows)
# Promo weeks are left out of the comparison: the baseline deliberately ignores promos (a real system adds the planned lift)
backtest = backtest[~backtest.is_promo & backtest.est_demand.notna()]
backtest_out = backtest[["store_id", "product_id", "product_name", "category", "perishable", "week_start", "target_week",
                         "forecast_units", "raw_sales_ma4", "est_demand", "units_sold", "order_units", "received",
                         "segment", "unit_cost", "unit_price", "stockout_days", "sim_stockout_days", "actual_lost_units",
                         "sim_lost_units", "expired_units", "sim_waste_units"]].round(1)
spark.createDataFrame(backtest_out).write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_backtest")

# COMMAND ----------

# Backtest summary: forecast error and stockouts/waste with the recommendation vs what actually happened
spark.sql("""
CREATE OR REPLACE TABLE gold_backtest_summary AS
-- Rows: one per backtest week, one per product segment, and a TOTAL
SELECT CASE WHEN grouping(week_start) = 0 THEN string(week_start) WHEN grouping(segment) = 0 THEN segment ELSE 'TOTAL' END AS slice,
       count(*) AS store_product_weeks,
       round(sum(abs(forecast_units - est_demand)) / sum(est_demand), 3) AS wape_forecast,
       round(sum(abs(raw_sales_ma4 - est_demand)) / sum(est_demand), 3) AS wape_naive_raw_sales,
       round(sum(stockout_days)) AS actual_stockout_days,
       round(sum(sim_stockout_days)) AS sim_stockout_days,
       round(1 - sum(stockout_days) / (7 * count(*)), 4) AS actual_availability,
       round(1 - sum(sim_stockout_days) / (7 * count(*)), 4) AS sim_availability,
       round(sum(expired_units)) AS actual_waste_units,
       round(sum(sim_waste_units)) AS sim_waste_units,
       round(sum(received)) AS actual_units_received,
       round(sum(order_units)) AS sim_units_ordered,
       round(sum(expired_units * unit_cost)) AS actual_waste_usd,
       round(sum(sim_waste_units * unit_cost)) AS sim_waste_usd,
       round(sum(actual_lost_units * (unit_price - unit_cost))) AS actual_lost_margin_usd,
       round(sum(sim_lost_units * (unit_price - unit_cost))) AS sim_lost_margin_usd
FROM gold_backtest
GROUP BY GROUPING SETS ((week_start), (segment), ())
ORDER BY grouping(week_start), grouping(segment), slice
""")
display(spark.table("gold_backtest_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data product: comments and quality checks

# COMMAND ----------

# Table and column comments in Unity Catalog (grain first, so consumers know what one row is)
comments = {
    "gold_store_product_week": ("Grain: store x product x week. Weekly sales, stock, stockouts and waste. Last week is partial (see period_days).",
                                {"est_demand": "Estimated weekly demand = units_sold / days_in_stock * 7 (corrects for stockouts)",
                                 "availability_pct": "Share of days in the period the item was in stock",
                                 "stockout_days": "Days in the period with no stock",
                                 "waste_value": "Waste units x unit cost (USD)",
                                 "period_days": "Days in the week covered by data (7, or fewer for the current week)"}),
    "gold_demand_forecast": ("Grain: store x product for next week. Moving average + trend on estimated demand, excluding promo and partial weeks.",
                             {"forecast_units": "Expected demand next week (units)",
                              "forecast_low": "Low end of the 80% range", "forecast_high": "High end of the 80% range",
                              "trend_per_week": "Change in weekly demand per week (units)"}),
    "gold_reorder_recommendation": ("Grain: store x product. Order to place now for delivery next week (1-week lead time).",
                                    {"order_units": "Recommended order in units (whole case packs)",
                                     "safety_stock": "Buffer: 1.65 sigma non-perishable (95% target), 0.84 sigma perishable",
                                     "on_hand_at_arrival": "Stock expected to remain when the order arrives",
                                     "shelf_life_cap": "Most a perishable can sell within shelf life (units)",
                                     "reason": "Why this quantity, in plain English"}),
    "gold_backtest": ("Grain: store x product x backtest week. Recommendation replayed against actual demand (non-promo weeks).", {}),
    "gold_backtest_summary": ("Grain: one row per backtest week, per product segment, plus TOTAL. Forecast error and simulated vs actual stockouts and waste.", {}),
}
for table, (table_comment, cols) in comments.items():
    spark.sql(f"COMMENT ON TABLE {table} IS '{table_comment}'")
    for col, text in cols.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{text}'")

# COMMAND ----------

# QUALITY: freshness and consistency checks on the data product, stored so the dashboard can show them
checks = spark.sql("""
SELECT 'data_freshness_days' AS check_name, datediff(current_date(), max(week_start + period_days - 1)) AS value,
       datediff(current_date(), max(week_start + period_days - 1)) <= 7 AS passed FROM gold_store_product_week
UNION ALL SELECT 'reorder_rows_vs_store_products', count(*), count(*) = 800 FROM gold_reorder_recommendation
UNION ALL SELECT 'negative_orders', count_if(order_units < 0), count_if(order_units < 0) = 0 FROM gold_reorder_recommendation
UNION ALL SELECT 'orders_not_in_case_packs', count_if(order_units % case_pack != 0), count_if(order_units % case_pack != 0) = 0 FROM gold_reorder_recommendation
UNION ALL SELECT 'est_demand_below_sales', count_if(est_demand < units_sold), count_if(est_demand < units_sold) = 0 FROM gold_store_product_week
UNION ALL SELECT 'quarantined_rows', count(*), count(*) < 0.02 * 26000 FROM silver_quarantine
""").withColumn("checked_at", F.current_timestamp())
checks.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_quality_check")
display(checks)
