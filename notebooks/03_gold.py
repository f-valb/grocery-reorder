# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: the store-item data product and the demand model
# MAGIC **Purpose:** weekly store-item performance, a trained demand model that must beat a simple forecast to be used, next week's reorder list, a backtest of both, governance, and checks.
# MAGIC **Inputs:** `silver_*`
# MAGIC **Outputs:**
# MAGIC | Table | Grain |
# MAGIC |---|---|
# MAGIC | `gold_store_product_week` | store × product × week |
# MAGIC | `gold_demand_forecast` | store × product, next week (champion method) |
# MAGIC | `gold_reorder_recommendation` | store × product, order placed now for next week |
# MAGIC | `gold_backtest` / `gold_backtest_summary` | store × product × backtest week × method / slice × method |
# MAGIC | `gold_model_monitor` | one row per monitored feature, plus the gate decision |
# MAGIC | `gold_signal_check`, `gold_quality_check` | one row per check |
# MAGIC | `gov_store_managers` | one row per user and store they may see |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Load silver

# COMMAND ----------

# Setup: environment (Databricks or local Spark), constants, silver tables
import os
import tempfile

import mlflow
import mlflow.models
import mlflow.sklearn
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.ensemble import HistGradientBoostingRegressor

spark.sql("USE workspace.grocery_intel")
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
AS_OF = pd.Timestamp("2026-10-21")  # same fixed as-of date as the generator
MODEL_NAME = "workspace.grocery_intel.demand_forecaster"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Business definitions

# COMMAND ----------

# RULE: definitions used everywhere below
LEAD_TIME_WEEKS = 1      # order placed now arrives at the start of next week
Z_NON_PERISHABLE = 1.65  # RULE: 95% availability target on non-perishables
Z_PERISHABLE = 0.84      # RULE: 80% on perishables: a little less availability for much less waste
RANGE_Z = 1.28           # low/high = 80% range
MIN_HISTORY_WEEKS = 3    # EDGE: small sample: below this, no trend is fitted
BASELINE_WINDOW = 8      # weeks of history used for the moving average and trend
BACKTEST_WEEKS = 2
GATE_TOLERANCE = 0.02    # the model must beat the baseline error by at least 2% (relative) to be champion
PSI_ALERT = 0.25         # drift score above this raises retrain_needed (kit default)
# Estimated demand = units sold / days in stock x 7 (sales are censored when the shelf is empty)
# Baseline-eligible week = full week, not a promo, not a one-off spike, at least 1 day in stock

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 Core aggregates
# MAGIC One row per store × product × week: sales, stockouts, availability, estimated demand, waste units and value, with the price valid in that week.

# COMMAND ----------

# Weekly store-item facts from the three sources joined on the conformed keys
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW store_product_week_base AS
WITH waste_week AS (
  SELECT store_id, item_id, week_start,
         SUM(waste_units) AS waste_units,
         SUM(CASE WHEN reason = 'expired' THEN waste_units ELSE 0 END) AS expired_units,
         SUM(CASE WHEN reason = 'damaged' THEN waste_units ELSE 0 END) AS damaged_units
  FROM silver_waste_events GROUP BY store_id, item_id, week_start),
first_week AS (SELECT MIN(week_start) AS first_week FROM silver_sales_weekly)
SELECT s.store_id, st.store_name, st.region, st.store_size, s.item_id, p.product_name, p.category, p.perishable,
       p.shelf_life_days, p.case_pack, p.unit_cost, p.unit_price,
       s.week_start,
       CAST(datediff(s.week_start, fw.first_week) / 7 AS INT) + 1 AS week_num,
       LEAST(7, datediff(DATE'{AS_OF.date()}', s.week_start) + 1) AS period_days,
       LEAST(7, datediff(DATE'{AS_OF.date()}', s.week_start) + 1) < 7 AS is_partial_week,
       pr.discount_pct IS NOT NULL AS is_promo, COALESCE(pr.discount_pct, 0) AS discount_pct,
       s.units_sold, s.days_in_stock,
       LEAST(7, datediff(DATE'{AS_OF.date()}', s.week_start) + 1) - s.days_in_stock AS stockout_days,
       ROUND(s.days_in_stock / LEAST(7, datediff(DATE'{AS_OF.date()}', s.week_start) + 1), 4) AS availability_pct,
       CASE WHEN s.days_in_stock > 0 THEN ROUND(s.units_sold / s.days_in_stock * 7, 1) END AS est_demand,
       ROUND(s.units_sold * p.unit_price * (1 - COALESCE(pr.discount_pct, 0) / 100), 2) AS revenue,
       n.on_hand, n.on_order, n.received,
       COALESCE(w.waste_units, 0) AS waste_units, COALESCE(w.expired_units, 0) AS expired_units,
       COALESCE(w.damaged_units, 0) AS damaged_units,
       ROUND(COALESCE(w.waste_units, 0) * p.unit_cost, 2) AS waste_value
FROM silver_sales_weekly s
CROSS JOIN first_week fw
JOIN silver_stores st ON st.store_id = s.store_id
JOIN silver_products p ON p.item_id = s.item_id AND s.week_start BETWEEN p.valid_from AND p.valid_to
LEFT JOIN silver_promotions pr ON pr.item_id = s.item_id AND pr.week_start = s.week_start
LEFT JOIN silver_stock_snapshots n ON n.store_id = s.store_id AND n.item_id = s.item_id AND n.week_start = s.week_start
LEFT JOIN waste_week w ON w.store_id = s.store_id AND w.item_id = s.item_id AND w.week_start = s.week_start
""")
# EDGE: price changed mid-period: the join picks the price version valid in each week (revenue and waste $ use it)
# TRAP: demand from in-stock days (est_demand), not raw sales: raw sales hide demand lost to empty shelves
# SPARK: the joins to stores, products and promotions are small dimension joins; at this size Spark broadcasts them itself

# COMMAND ----------

# MAGIC %md
# MAGIC One-off spikes (a local festival) are real sales, so they stay in the table, but they must not drive next week's forecast.
# MAGIC A week is a spike when it sits more than 3.5 robust deviations above the store-item's own trend line. Robust = median and MAD, because the spike itself would inflate a standard deviation.

# COMMAND ----------

# EDGE: legitimate outlier: flag one-off spikes with median + 3.5 x MAD of the trend residuals (keep the row, exclude from the baseline)
weekly = spark.table("store_product_week_base").toPandas()
weekly.attrs = {}
weekly["week_start"] = pd.to_datetime(weekly.week_start)


def spike_flags(weekly):
    flags = []
    base = weekly[~weekly.is_partial_week & ~weekly.is_promo & weekly.est_demand.notna()]
    for (store, item), g in base.groupby(["store_id", "item_id"]):
        if len(g) < 6:
            continue
        x, y = g.week_num.to_numpy(float), g.est_demand.to_numpy(float)
        slope, intercept = np.polyfit(x, y, 1)
        resid = y - (slope * x + intercept)
        med = np.median(resid)
        mad = 1.4826 * np.median(np.abs(resid - med))
        limit = 3.5 * max(mad, np.sqrt(y.mean()))  # floor at Poisson noise so very stable items don't flag every wiggle
        for week_num, r in zip(g.week_num, resid):
            if r - med > limit:
                flags.append({"store_id": store, "item_id": item, "week_num": int(week_num)})
    return pd.DataFrame(flags, columns=["store_id", "item_id", "week_num"])


spikes = spike_flags(weekly).assign(is_spike_week=True)
weekly = weekly.merge(spikes, on=["store_id", "item_id", "week_num"], how="left")
weekly["is_spike_week"] = weekly.is_spike_week.astype("boolean").fillna(False).astype(bool)

spike_df = spark.createDataFrame(spikes if len(spikes) else pd.DataFrame({"store_id": ["-"], "item_id": ["-"], "week_num": [0], "is_spike_week": [True]}))
(spark.table("store_product_week_base")
 .join(F.broadcast(spike_df), ["store_id", "item_id", "week_num"], "left")  # SPARK: a handful of flags, broadcast
 .withColumn("is_spike_week", F.coalesce("is_spike_week", F.lit(False)))
 .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_store_product_week"))

# At scale: this table would be partitioned or liquid-clustered by week_start and store_id, so a store manager's query
# reads one store's files only; the spike scan would run per store-item with applyInPandas instead of a driver loop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 Forecast / score / recommendation
# MAGIC Two methods, same inputs:
# MAGIC - **Baseline:** 4-week moving average + 8-week trend on baseline-eligible weeks. Fewer than 3 weeks: average of what there is. No history: category median at the store.
# MAGIC - **Model:** gradient-boosted trees (scikit-learn) that learn how actual demand departs from the baseline: promo and discount, trend, recent level, category, store size, region. It predicts a multiplier on the baseline, plus 10% and 90% quantiles for the range.
# MAGIC Every input is known when the order is placed: history up to last week, and the promo calendar, which is planned in advance.

# COMMAND ----------

# Baseline history features per store-item for a target week (only weeks before it)
def history_features(weekly, target_week):
    # TRAP: no leakage: only weeks before the target week, and only clean weeks (no promo, spike, partial or empty shelf)
    eligible = weekly[(weekly.week_num < target_week) & (weekly.week_num >= target_week - BASELINE_WINDOW)
                      & ~weekly.is_partial_week & ~weekly.is_promo & ~weekly.is_spike_week & weekly.est_demand.notna()]
    rows = []
    for (store, item), g in eligible.groupby(["store_id", "item_id"]):
        g = g.sort_values("week_num")
        x, y = g.week_num.to_numpy(float), g.est_demand.to_numpy(float)
        last4 = g.tail(4)
        if len(g) >= MIN_HISTORY_WEEKS:
            slope, intercept = np.polyfit(x, y, 1)
            level = last4.est_demand.mean() + slope * (target_week - last4.week_num.mean())
            resid_sd = np.std(y - (slope * x + intercept), ddof=1)
            method = "moving_average_trend"
        else:  # EDGE: small sample: 1-2 weeks of history: their average, no trend
            slope, level, resid_sd, method = 0.0, y.mean(), np.nan, "low_history_average"
        rows.append({"store_id": store, "item_id": item, "n_hist_weeks": len(g), "baseline_forecast": max(level, 0.0),
                     "trend_per_week": slope, "resid_sd": resid_sd, "last_clean_demand": y[-1], "baseline_method": method})
    return pd.DataFrame(rows, columns=["store_id", "item_id", "n_hist_weeks", "baseline_forecast", "trend_per_week",
                                       "resid_sd", "last_clean_demand", "baseline_method"])


store_items = weekly[["store_id", "item_id", "product_name", "category", "perishable", "shelf_life_days", "case_pack",
                      "store_size", "region"]].drop_duplicates(["store_id", "item_id"])
first_week_of = weekly.groupby(["store_id", "item_id"]).week_num.min().rename("first_week").reset_index()
promo_by_week = weekly[["item_id", "week_num", "is_promo", "discount_pct"]].drop_duplicates(["item_id", "week_num"])


def frame_for(target_week):
    # One row per store-item that exists by the target week, with baseline features and the planned promo
    exists = first_week_of[first_week_of.first_week <= target_week][["store_id", "item_id"]]
    frame = store_items.merge(exists, on=["store_id", "item_id"]).merge(history_features(weekly, target_week),
                                                                         on=["store_id", "item_id"], how="left")
    frame["target_week"] = target_week
    frame = frame.merge(promo_by_week[promo_by_week.week_num == target_week].drop(columns="week_num"), on="item_id", how="left")
    frame["is_promo"] = frame.is_promo.astype("boolean").fillna(False).astype(bool)
    frame["discount_pct"] = frame.discount_pct.fillna(0)
    # EDGE: small sample: no history yet: category median at the same store
    category_median = frame.groupby(["store_id", "category"]).baseline_forecast.transform("median")
    no_history = frame.baseline_forecast.isna()
    frame.loc[no_history, "baseline_method"] = "category_median"
    frame.loc[no_history, "baseline_forecast"] = category_median[no_history]
    frame["n_hist_weeks"] = frame.n_hist_weeks.fillna(0)
    frame["baseline_sigma"] = np.maximum(frame.resid_sd.fillna(0), np.sqrt(frame.baseline_forecast.clip(lower=1)))
    return frame

# COMMAND ----------

# Model inputs and training: the model predicts actual demand / baseline forecast
SIZES, CATEGORIES, REGIONS = ["S", "M", "L"], sorted(weekly.category.unique()), sorted(weekly.region.unique())
FEATURES = ["is_promo", "discount_pct", "trend_rel", "last_vs_baseline", "cv", "n_hist_weeks", "log_baseline",
            "perishable", "store_size", "category", "region"]
CATEGORICAL = {"store_size", "category", "region"}
DRIFT_FEATURES = ["trend_rel", "last_vs_baseline", "cv", "log_baseline"]  # continuous inputs monitored with PSI


def model_matrix(frame):
    base = frame.baseline_forecast.clip(lower=1)
    return pd.DataFrame({
        "is_promo": frame.is_promo.astype(int), "discount_pct": frame.discount_pct.astype(float),
        "trend_rel": frame.trend_per_week / base, "last_vs_baseline": frame.last_clean_demand / base,
        "cv": frame.baseline_sigma / base, "n_hist_weeks": frame.n_hist_weeks.astype(float), "log_baseline": np.log1p(base),
        "perishable": frame.perishable.astype(int), "store_size": frame.store_size.map(SIZES.index),
        "category": frame.category.map(CATEGORIES.index), "region": frame.region.map(REGIONS.index)})[FEATURES]


def training_set(first_target, last_target):
    parts = []
    for target_week in range(first_target, last_target + 1):
        actual = weekly[(weekly.week_num == target_week) & ~weekly.is_partial_week & ~weekly.is_spike_week
                        & weekly.est_demand.notna()][["store_id", "item_id", "est_demand"]]
        parts.append(frame_for(target_week).merge(actual, on=["store_id", "item_id"]))
    return pd.concat(parts, ignore_index=True)


MODEL_PARAMS = {"max_iter": 200, "learning_rate": 0.05, "max_depth": 4, "min_samples_leaf": 30, "random_state": 42}


def train_models(train):
    X = model_matrix(train)
    y = (train.est_demand / train.baseline_forecast.clip(lower=1)).clip(0, 5)
    cat_mask = [c in CATEGORICAL for c in FEATURES]
    fit = lambda **loss: HistGradientBoostingRegressor(categorical_features=cat_mask, **MODEL_PARAMS, **loss).fit(X, y)
    return {"point": fit(), "low": fit(loss="quantile", quantile=0.1), "high": fit(loss="quantile", quantile=0.9)}


def model_forecast(models, frame):
    X, base = model_matrix(frame), frame.baseline_forecast.clip(lower=1)
    point, low, high = (np.clip(base * models[k].predict(X), 0, None) for k in ("point", "low", "high"))
    # sigma for safety stock from the model's own 80% range, never below Poisson noise
    return point, np.minimum(low, point), np.maximum(high, point), np.maximum((high - low) / (2 * RANGE_Z), np.sqrt(np.maximum(point, 1)))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reorder rule:** order = forecast + safety stock − stock left when the order arrives − on order, rounded up to whole cases.
# MAGIC Perishables are capped at what sells within shelf life (forecast / 7 × shelf life days − stock already there, to the nearest case).

# COMMAND ----------

# Reorder function shared by next week's list and the backtest
def recommend(frame, forecast_col, sigma_col, stock, remaining_days):
    r = frame.merge(stock, on=["store_id", "item_id"], how="left")
    r["has_snapshot"] = r.on_hand.notna()
    r[["on_hand", "on_order"]] = r[["on_hand", "on_order"]].fillna(0)
    # TRAP: stock still on the shelf when the order lands, not stock today
    r["on_hand_at_arrival"] = (r.on_hand - r[forecast_col] * remaining_days / 7).clip(lower=0)
    r["safety_stock"] = np.where(r.perishable, Z_PERISHABLE, Z_NON_PERISHABLE) * r[sigma_col] * np.sqrt(LEAD_TIME_WEEKS)
    # RULE: order = forecast + safety stock - on hand at arrival - on order, rounded up to case packs
    r["net_need"] = (r[forecast_col] + r.safety_stock - r.on_hand_at_arrival - r.on_order).clip(lower=0)
    r["need_units"] = np.ceil(r.net_need / r.case_pack) * r.case_pack
    # RULE: perishables capped at what sells within shelf life
    shelf_cap = (r[forecast_col] / 7 * r.shelf_life_days - r.on_hand_at_arrival - r.on_order).clip(lower=0)
    r["shelf_life_cap"] = np.where(r.perishable, np.round(shelf_cap / r.case_pack) * r.case_pack, np.nan)
    r["capped_by_shelf_life"] = r.perishable & (r.shelf_life_cap < r.need_units)
    r["order_units"] = np.where(r.capped_by_shelf_life, r.shelf_life_cap, r.need_units).astype(int)
    r["order_cases"] = (r.order_units // r.case_pack).astype(int)
    return r

# At scale: one global model across all store-items (not one per item) keeps training cheap and lets new items borrow
# from their category; with millions of store-items the frames would be built in Spark and scored with a pandas UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 Backtest
# MAGIC For each of the last 2 full weeks: train only on earlier weeks, forecast with both methods, place the order with the previous week's real stock, and replay the week against the actual estimated demand.
# MAGIC The model becomes **champion** only if its error beats the baseline by more than the tolerance; otherwise the baseline stays in charge.

# COMMAND ----------

# TRAP: each backtest week only sees data from before it; the same functions produce next week's numbers
full_weeks = sorted(weekly.loc[~weekly.is_partial_week, "week_num"].unique())
current_week = int(weekly.week_num.max())
backtest_rows = []
for target_week in full_weeks[-BACKTEST_WEEKS:]:
    models = train_models(training_set(3, target_week - 1))
    frame = frame_for(target_week)
    frame["model_forecast"], frame["model_low"], frame["model_high"], frame["model_sigma"] = model_forecast(models, frame)
    prev_stock = weekly[weekly.week_num == target_week - 1][["store_id", "item_id", "on_hand"]].assign(on_order=0)
    actual = weekly[weekly.week_num == target_week][["store_id", "item_id", "week_start", "est_demand", "units_sold",
                                                     "stockout_days", "expired_units", "received", "unit_cost",
                                                     "unit_price", "is_spike_week"]]
    for method, f_col, s_col in [("baseline", "baseline_forecast", "baseline_sigma"), ("model", "model_forecast", "model_sigma")]:
        bt = recommend(frame, f_col, s_col, prev_stock, remaining_days=0).merge(actual, on=["store_id", "item_id"])
        bt = bt[bt.est_demand.notna() & ~bt.is_spike_week]  # one-off spikes are not a fair test of any forecast
        bt["method"], bt["forecast_units"] = method, bt[f_col]
        bt["available"] = bt.on_hand + bt.order_units
        bt["sim_stockout_days"] = np.where(bt.est_demand > bt.available,
                                           7 - np.ceil(7 * bt.available / bt.est_demand.where(bt.est_demand > 0, 1)), 0)
        leftover = (bt.available - bt.est_demand).clip(lower=0)
        bt["sim_waste_units"] = np.where(bt.perishable, leftover * np.minimum(1, 7 / bt.shelf_life_days), 0).round()
        bt["actual_lost_units"] = (bt.est_demand - bt.units_sold).clip(lower=0)
        bt["sim_lost_units"] = (bt.est_demand - bt.available).clip(lower=0)
        bt["segment"] = np.where(~bt.perishable, "Non-perishable",
                                 np.where(bt.shelf_life_days <= 7, "Perishable, shelf life <= 7d", "Perishable, shelf life > 7d"))
        backtest_rows.append(bt)

backtest = pd.concat(backtest_rows, ignore_index=True)
backtest["week_start"] = backtest.week_start.dt.date
backtest_out = backtest[["method", "store_id", "item_id", "product_name", "category", "segment", "perishable", "week_start",
                         "target_week", "is_promo", "baseline_method", "forecast_units", "est_demand", "units_sold",
                         "order_units", "received", "unit_cost", "unit_price", "stockout_days", "sim_stockout_days",
                         "actual_lost_units", "sim_lost_units", "expired_units", "sim_waste_units"]].round(2)
spark.createDataFrame(backtest_out).write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_backtest")

# COMMAND ----------

# Backtest summary per slice and method: forecast error (WAPE) and outcomes vs what actually happened
spark.sql("""
WITH sliced AS (
  SELECT 'TOTAL' AS slice, * FROM gold_backtest
  UNION ALL SELECT CAST(week_start AS STRING), * FROM gold_backtest
  UNION ALL SELECT CASE WHEN is_promo THEN 'Promo weeks' ELSE 'Non-promo weeks' END, * FROM gold_backtest
  UNION ALL SELECT segment, * FROM gold_backtest)
SELECT slice, method, COUNT(*) AS store_product_weeks,
       ROUND(SUM(ABS(forecast_units - est_demand)) / SUM(est_demand), 3) AS wape,
       ROUND(SUM(stockout_days)) AS actual_stockout_days, ROUND(SUM(sim_stockout_days)) AS sim_stockout_days,
       ROUND(SUM(expired_units)) AS actual_waste_units, ROUND(SUM(sim_waste_units)) AS sim_waste_units,
       ROUND(SUM(expired_units * unit_cost)) AS actual_waste_usd, ROUND(SUM(sim_waste_units * unit_cost)) AS sim_waste_usd,
       ROUND(SUM(actual_lost_units * (unit_price - unit_cost))) AS actual_lost_margin_usd,
       ROUND(SUM(sim_lost_units * (unit_price - unit_cost))) AS sim_lost_margin_usd
FROM sliced GROUP BY slice, method
""").write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_backtest_summary")
display(spark.sql("SELECT * FROM gold_backtest_summary WHERE slice IN ('TOTAL', 'Promo weeks', 'Non-promo weeks') ORDER BY slice, method"))

# COMMAND ----------

# Gate: the model is champion only if it beats the baseline WAPE by more than the tolerance (templates/ml/registry.py logic)
wape = {r.method: r.wape for r in spark.sql("SELECT method, wape FROM gold_backtest_summary WHERE slice = 'TOTAL'").collect()}
model_is_champion = wape["model"] <= wape["baseline"] * (1 - GATE_TOLERANCE)
gate_reason = f"model WAPE {wape['model']:.3f} vs baseline {wape['baseline']:.3f} (tolerance {GATE_TOLERANCE:.0%})"
champion_method = "model" if model_is_champion else "baseline"
print(champion_method, gate_reason)

# COMMAND ----------

# Train the final model on all full weeks, log it to MLflow, register it in Unity Catalog and set the champion alias
train_all = training_set(3, current_week - 1)
final_models = train_models(train_all)
if IS_DATABRICKS:
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/grocery_intel_demand_forecaster")
else:
    mlflow.set_tracking_uri(f"file:{tempfile.gettempdir()}/mlruns_grocery_intel")
    mlflow.set_experiment("grocery_intel_demand_forecaster")

with mlflow.start_run(run_name=f"demand_forecaster_week_{current_week + 1}") as run:
    mlflow.log_params({**MODEL_PARAMS, "features": ",".join(FEATURES), "train_rows": len(train_all),
                       "target": "est_demand / baseline_forecast"})
    mlflow.log_metrics({"backtest_wape_model": wape["model"], "backtest_wape_baseline": wape["baseline"],
                        "champion_is_model": int(model_is_champion)})
    signature = mlflow.models.infer_signature(model_matrix(train_all), final_models["point"].predict(model_matrix(train_all)))
    registry_status, info = "not attempted (local run)", None
    if IS_DATABRICKS:
        try:
            info = mlflow.sklearn.log_model(final_models["point"], "model", signature=signature, registered_model_name=MODEL_NAME)
            registry_status = "registered in Unity Catalog"
        except Exception as e:  # Free Edition denies writes to UC model storage: keep the run, log the model to the experiment
            registry_status = f"UC registry unavailable ({type(e).__name__}): model logged to the experiment only"
    if info is None:
        try:
            mlflow.sklearn.log_model(final_models["point"], "model", signature=signature)
        except Exception as e:
            registry_status += f"; experiment artifact upload failed ({type(e).__name__}): params and metrics only"
    mlflow.set_tags({"gate": gate_reason, "champion_method": champion_method, "registry_status": registry_status})
model_version = f"run-{run.info.run_id[:8]}"
if info is not None and info.registered_model_version:
    client = mlflow.MlflowClient()
    model_version = str(info.registered_model_version)
    if model_is_champion:
        client.set_registered_model_alias(MODEL_NAME, "champion", model_version)
    client.set_model_version_tag(MODEL_NAME, model_version, "gate", gate_reason)
print(model_version, registry_status)

# At scale: the gate and retrain run as a scheduled job (train -> compare -> condition task -> set alias), and scoring
# loads models:/demand_forecaster@champion, so a rollback is moving the alias back.

# COMMAND ----------

# Score next week with the champion method and build the reorder list from the current (partial) week's stock
target_week = current_week + 1
next_week = frame_for(target_week)
next_week["model_forecast"], next_week["model_low"], next_week["model_high"], next_week["model_sigma"] = model_forecast(final_models, next_week)
if model_is_champion:
    next_week["forecast_units"], next_week["forecast_low"], next_week["forecast_high"], next_week["sigma"] = (
        next_week.model_forecast, next_week.model_low, next_week.model_high, next_week.model_sigma)
else:
    next_week["forecast_units"], next_week["sigma"] = next_week.baseline_forecast, next_week.baseline_sigma
    next_week["forecast_low"] = (next_week.baseline_forecast - RANGE_Z * next_week.baseline_sigma).clip(lower=0)
    next_week["forecast_high"] = next_week.baseline_forecast + RANGE_Z * next_week.baseline_sigma
next_week["forecast_method"], next_week["model_version"] = champion_method, model_version

now = weekly[weekly.week_num == current_week]
remaining_days = 7 - int(now.period_days.iloc[0])
reorder = recommend(next_week, "forecast_units", "sigma", now[["store_id", "item_id", "on_hand", "on_order"]], remaining_days)
recent = (weekly[~weekly.is_partial_week & (weekly.week_num > current_week - 5)]
          .groupby(["store_id", "item_id"]).availability_pct.mean().rename("recent_availability").reset_index())
reorder = reorder.merge(recent, on=["store_id", "item_id"], how="left")
price_now = now[["store_id", "item_id", "unit_cost"]]
reorder = reorder.merge(price_now, on=["store_id", "item_id"], how="left")


def reason(r):
    if not r.has_snapshot:
        return "No valid stock snapshot this week: assumed 0 on hand, check the shelf"
    if r.baseline_method != "moving_average_trend":
        return f"New item: only {int(r.n_hist_weeks)} weeks of history, forecast from {r.baseline_method.replace('_', ' ')}"
    if r.capped_by_shelf_life:
        return f"Capped by shelf life ({int(r.shelf_life_days)}d): more would expire before it sells"
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
week_start_next = (weekly.week_start.max() + pd.Timedelta(weeks=1)).date()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 Write + UC comments
# MAGIC Gold tables are the data product: each states its grain in a Unity Catalog comment. Governance: a row filter so a store manager sees only their own store, and a column mask so only finance sees unit cost.

# COMMAND ----------

# Write next week's forecast and reorder list
forecast_out = next_week.assign(week_start=week_start_next)[
    ["store_id", "item_id", "product_name", "category", "week_start", "target_week", "forecast_method", "model_version",
     "forecast_units", "forecast_low", "forecast_high", "baseline_forecast", "model_forecast", "baseline_method",
     "n_hist_weeks", "trend_per_week"]].round(2)
reorder_out = reorder.assign(week_start=week_start_next)[
    ["store_id", "item_id", "product_name", "category", "perishable", "week_start", "forecast_method", "forecast_units",
     "safety_stock", "on_hand", "on_order", "on_hand_at_arrival", "case_pack", "shelf_life_days", "shelf_life_cap",
     "order_units", "order_cases", "order_value", "capped_by_shelf_life", "recent_availability", "reason"]].round(2)
# SPARK: join back to gold_store_product_week in Spark (adds store name and region) so Unity Catalog lineage links these
# tables to their sources; a plain pandas -> createDataFrame write would show only the notebook as upstream
store_context = (spark.table("gold_store_product_week").where(F.col("week_num") == current_week)
                 .select("store_id", "item_id", "store_name", "region"))
for out, table in [(forecast_out, "gold_demand_forecast"), (reorder_out, "gold_reorder_recommendation")]:
    (spark.createDataFrame(out).join(store_context, ["store_id", "item_id"], "left")
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table))

# COMMAND ----------

# Table and column comments in Unity Catalog (no apostrophes: they break the SQL string)
comments = {
    "gold_store_product_week": ("Grain: store x product x week. Sales, stock, stockouts and waste, priced with the version valid that week.",
                                {"est_demand": "Estimated weekly demand = units_sold / days_in_stock * 7 (corrects for stockouts)",
                                 "availability_pct": "Share of days in the period the item was in stock",
                                 "is_spike_week": "One-off spike (robust MAD rule): kept, but excluded from forecasts",
                                 "period_days": "Days covered by data: 7, fewer for the current partial week"}),
    "gold_demand_forecast": ("Grain: store x product for next week. Champion method (model or baseline) chosen by the backtest gate.",
                             {"forecast_units": "Expected demand next week from the champion method",
                              "forecast_low": "Low end of the 80% range", "forecast_high": "High end of the 80% range",
                              "model_version": "Registered model version used (or local run id)"}),
    "gold_reorder_recommendation": ("Grain: store x product. Order to place now for delivery next week (1-week lead time).",
                                    {"order_units": "Recommended order in units (whole case packs)",
                                     "on_hand_at_arrival": "Stock expected to remain when the order arrives",
                                     "reason": "Why this quantity, in plain English"}),
    "gold_backtest": ("Grain: method x store x product x backtest week. Recommendations replayed against actual demand.", {}),
    "gold_backtest_summary": ("Grain: slice x method. Forecast error and simulated vs actual stockouts, waste and lost margin.", {}),
}
for table, (table_comment, cols) in comments.items():
    spark.sql(f"COMMENT ON TABLE {table} IS '{table_comment}'")
    for col, text in cols.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{text}'")

# COMMAND ----------

# Governance: who may see which store (mock managers plus the pipeline owner, who sees everything)
owner = spark.sql("SELECT current_user()").first()[0]
spark.createDataFrame(
    [(owner, "ALL", "admin")] + [(f"manager.{s.lower()}@example.com", s, "store_manager") for s in sorted(weekly.store_id.unique())]
    + [("finance.lead@example.com", "ALL", "finance")],
    "user_email STRING, store_id STRING, role STRING").write.mode("overwrite").saveAsTable("gov_store_managers")

# COMMAND ----------

# Row filter (store managers see their own store) and column mask (only admin and finance see unit cost); Unity Catalog only
if IS_DATABRICKS:
    spark.sql("""CREATE OR REPLACE FUNCTION gov_store_filter(store STRING) RETURNS BOOLEAN
      RETURN EXISTS (SELECT 1 FROM workspace.grocery_intel.gov_store_managers g
                     WHERE g.user_email = current_user() AND (g.store_id = store OR g.store_id = 'ALL'))""")
    spark.sql("""CREATE OR REPLACE FUNCTION gov_cost_mask(cost DOUBLE) RETURNS DOUBLE
      RETURN CASE WHEN EXISTS (SELECT 1 FROM workspace.grocery_intel.gov_store_managers g
                               WHERE g.user_email = current_user() AND g.role IN ('admin', 'finance')) THEN cost END""")
    for table in ["gold_store_product_week", "gold_reorder_recommendation", "gold_demand_forecast"]:
        spark.sql(f"ALTER TABLE {table} SET ROW FILTER gov_store_filter ON (store_id)")
    spark.sql("ALTER TABLE gold_store_product_week ALTER COLUMN unit_cost SET MASK gov_cost_mask")
# TRAP: a dashboard published with embedded credentials runs as its owner (role admin) and skips this filter:
# publish without embedded credentials so each viewer sees only their own store.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 Signal check
# MAGIC Each planted signal is checked against gold with an expected range taken from the generator's true data (section 4 of 00), plus model health and data product checks.

# COMMAND ----------

# SIGNAL: every planted signal must be recovered in gold within its expected range
signals = spark.sql("""
WITH full_weeks AS (SELECT * FROM gold_store_product_week WHERE NOT is_partial_week),
items AS (
  SELECT product_name, AVG(availability_pct) AS availability, SUM(expired_units) / SUM(received) AS expiry_rate
  FROM full_weeks GROUP BY product_name),
store_waste AS (SELECT store_id, SUM(waste_value) AS waste_usd FROM full_weeks GROUP BY store_id),
promo AS (
  SELECT AVG(CASE WHEN is_promo THEN units_sold END) / AVG(CASE WHEN NOT is_promo THEN units_sold END) AS lift
  FROM full_weeks f JOIN (SELECT DISTINCT item_id FROM silver_promotions) p ON p.item_id = f.item_id),
trend AS (
  SELECT category, AVG(trend_per_week / forecast_units) AS trend_rel FROM gold_demand_forecast
  WHERE forecast_units > 0 GROUP BY category)
SELECT 'popular items availability (max of 4)' AS signal_name,
       (SELECT MAX(availability) FROM items WHERE product_name IN ('Pasta', 'Coffee', 'Sparkling Water', 'Eggs 12pk')) AS value, 0.0 AS low, 0.90 AS high
UNION ALL SELECT 'over-ordered fresh expiry rate (min of 3)',
       (SELECT MIN(expiry_rate) FROM items WHERE product_name IN ('Strawberries', 'Salad Mix', 'Sourdough')), 0.35, 1.0
UNION ALL SELECT 'ST-07 waste vs average store',
       (SELECT MAX(CASE WHEN store_id = 'ST-07' THEN waste_usd END) / AVG(waste_usd) FROM store_waste), 1.6, 5.0
UNION ALL SELECT 'promo lift', (SELECT lift FROM promo), 1.5, 2.2
UNION ALL SELECT 'Plant-based trend minus other categories (per week)',
       (SELECT MAX(CASE WHEN category = 'Plant-based' THEN trend_rel END) - AVG(CASE WHEN category != 'Plant-based' THEN trend_rel END) FROM trend), 0.03, 0.2
UNION ALL SELECT 'ST-12 festival week flagged as spike (rows)',
       (SELECT CAST(COUNT(*) AS DOUBLE) FROM gold_store_product_week WHERE store_id = 'ST-12' AND week_num = 6 AND is_spike_week), 5, 7
""").withColumn("passed", F.col("value").between(F.col("low"), F.col("high")))
signals.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_signal_check")
display(spark.table("gold_signal_check"))

# COMMAND ----------

# Model monitor: drift (PSI) of each continuous model input between training rows and next week's scoring rows
def bin_edges(values, n_bins=10):
    v = values[~np.isnan(values)]
    return np.unique(np.quantile(v, np.linspace(0, 1, n_bins + 1)))[1:-1].tolist()


def proportions(values, edges):
    v = values[~np.isnan(values)]
    counts = np.bincount(np.searchsorted(np.asarray(edges), v, side="right"), minlength=len(edges) + 1).astype(float)
    return counts / max(counts.sum(), 1)


def psi(expected, actual, eps=1e-4):
    e, a = np.clip(expected, eps, None), np.clip(actual, eps, None)
    return float(np.sum((a - e) * np.log(a / e)))


train_matrix, score_matrix = model_matrix(train_all), model_matrix(next_week)
monitor_rows = []
for feature in DRIFT_FEATURES:
    edges = bin_edges(train_matrix[feature].to_numpy(float))
    value = psi(proportions(train_matrix[feature].to_numpy(float), edges), proportions(score_matrix[feature].to_numpy(float), edges))
    monitor_rows.append({"check_name": f"psi_{feature}", "value": round(value, 4), "threshold": PSI_ALERT, "alert": value > PSI_ALERT})
monitor_rows.append({"check_name": "backtest_wape_model", "value": wape["model"], "threshold": wape["baseline"], "alert": not model_is_champion})
monitor = pd.DataFrame(monitor_rows).assign(champion_method=champion_method, model_version=model_version, gate_reason=gate_reason,
                                             registry_status=registry_status)
monitor["retrain_needed"] = bool(monitor.alert.any())
spark.createDataFrame(monitor).write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_model_monitor")
display(spark.table("gold_model_monitor"))

# COMMAND ----------

# QUALITY: data product checks: completeness, case packs, no negative orders, demand never below sales, quarantine share
checks = spark.sql(f"""
SELECT 'data_complete_to_as_of' AS check_name, CAST(datediff(MAX(week_start + period_days - 1), DATE'{AS_OF.date()}') AS DOUBLE) AS value,
       MAX(week_start + period_days - 1) >= DATE'{AS_OF.date()}' AS passed FROM gold_store_product_week
UNION ALL SELECT 'reorder_rows_vs_store_items', CAST(COUNT(*) AS DOUBLE),
       COUNT(*) = (SELECT COUNT(DISTINCT store_id, item_id) FROM gold_store_product_week) FROM gold_reorder_recommendation
UNION ALL SELECT 'negative_orders', CAST(COUNT_IF(order_units < 0) AS DOUBLE), COUNT_IF(order_units < 0) = 0 FROM gold_reorder_recommendation
UNION ALL SELECT 'orders_not_in_case_packs', CAST(COUNT_IF(order_units % case_pack != 0) AS DOUBLE),
       COUNT_IF(order_units % case_pack != 0) = 0 FROM gold_reorder_recommendation
UNION ALL SELECT 'est_demand_below_sales', CAST(COUNT_IF(est_demand < units_sold) AS DOUBLE),
       COUNT_IF(est_demand < units_sold) = 0 FROM gold_store_product_week
UNION ALL SELECT 'quarantine_share_pct', ROUND(100 * (SELECT COUNT(*) FROM silver_quarantine) / COUNT(*), 2),
       (SELECT COUNT(*) FROM silver_quarantine) < 0.02 * COUNT(*) FROM gold_store_product_week
""").withColumn("checked_at", F.current_timestamp())
checks.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_quality_check")
display(spark.table("gold_quality_check"))

# At scale: these checks would be Lakeflow expectations or Lakehouse Monitoring on the gold tables, with SQL alerts
# on gold_quality_check and gold_model_monitor.retrain_needed.

# COMMAND ----------

# QUALITY: run fingerprint for the rerun (idempotency) check: row count + sum of row hashes per gold table, appended per run
# TRAP: time travel (versionAsOf) is not allowed on tables with a row filter or column mask, so reruns are compared by fingerprint
run_at = spark.sql("SELECT current_timestamp() AS ts").first().ts
fingerprints = None
for table in ["gold_store_product_week", "gold_demand_forecast", "gold_reorder_recommendation", "gold_backtest"]:
    df = spark.table(table)
    content_cols = [c for c in df.columns if c != "model_version"]  # run metadata: a new MLflow run id every run, by design
    fp = df.select(F.lit(table).alias("table_name"), F.count("*").alias("row_count"),
                   F.sum(F.xxhash64(*content_cols).cast("decimal(38,0)")).cast("string").alias("content_hash"))
    fingerprints = fp if fingerprints is None else fingerprints.unionByName(fp)
fingerprints.withColumn("run_at", F.lit(run_at)).write.mode("append").saveAsTable("gold_run_fingerprint")
