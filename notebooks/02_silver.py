# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: one store-item view of three source systems
# MAGIC **Purpose:** map every source onto one store and one product key (the ERP crosswalk), parse types and timezones, drop re-sent rows, and quarantine rows that break a rule.
# MAGIC **Inputs:** `bronze_*` (POS, ERP, SaaS)
# MAGIC **Outputs:** `silver_stores`, `silver_products` (price history), `silver_store_xref`, `silver_item_xref`, `silver_sales_weekly`, `silver_stock_snapshots`, `silver_waste_events`, `silver_promotions`, `silver_quarantine`, `silver_quality_log`
# MAGIC
# MAGIC Pattern from `templates/pipelines/medallion_template.py`: named rules → `failed_rules` array → one quarantine table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Load bronze

# COMMAND ----------

# Load the six bronze extracts; quality_log collects one row per rule or fix for the summary
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE workspace.grocery_intel")
bronze = {name: spark.table(f"bronze_{name}") for name in
          ["pos_sales_weekly", "pos_stock_snapshots", "erp_stores", "erp_products", "ops_waste_log", "promo_calendar"]}
quality_log, quarantine_parts = [], []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Standardise types & timezones
# MAGIC The ERP is the master for stores and products. Its codes (`ST-07`, `SKU-00012`) become the conformed keys; the POS ids and SaaS store names map onto them.

# COMMAND ----------

# Stores and the store crosswalk: ERP code is the key; POS id and SaaS store name map onto it
stores = bronze["erp_stores"].select(
    F.col("store_code").alias("store_id"), "store_name", "region", F.col("size_band").alias("store_size"), "timezone",
    "pos_store_id", "_source_system")
store_xref = stores.select("store_id", "pos_store_id", "store_name")

# COMMAND ----------

# Products with price history (one row per price version) and the item crosswalk
products = bronze["erp_products"].select(
    F.col("item_code").alias("item_id"), "pos_item_id", "product_name", "category",
    (F.col("is_perishable") == 1).alias("perishable"), F.col("shelf_life_days").cast("int").alias("shelf_life_days"),
    F.col("case_qty").cast("int").alias("case_pack"), "unit_cost", F.col("list_price").alias("unit_price"),
    F.expr("try_cast(valid_from AS DATE)").alias("valid_from"),
    # EDGE: price changed mid-period: keep every version; open-ended versions run to 9999-12-31 so gold can join on the week
    F.coalesce(F.expr("try_cast(valid_to AS DATE)"), F.lit("9999-12-31").cast("date")).alias("valid_to"), "_source_system")

# QUALITY: missing shelf life would break the perishable cap: fill with the category median and flag it
category_median = products.groupBy("category").agg(F.expr("percentile_approx(shelf_life_days, 0.5)").alias("median_shelf_life"))
products = (products.join(F.broadcast(category_median), "category")  # SPARK: 6-row lookup, broadcast avoids a shuffle
            .withColumn("shelf_life_imputed", F.col("shelf_life_days").isNull())
            .withColumn("shelf_life_days", F.coalesce("shelf_life_days", "median_shelf_life"))
            .drop("median_shelf_life"))
item_xref = products.select("item_id", "pos_item_id").distinct()
quality_log += [("products", "null_shelf_life_imputed", products.where("shelf_life_imputed").select("item_id").distinct().count()),
                ("products", "price_versions_kept", products.groupBy("item_id").count().where("count > 1").count())]

# COMMAND ----------

# POS sales and snapshots: map integer ids to conformed keys, parse dates and UTC timestamps
def map_pos_keys(df):
    # SPARK: broadcast the small crosswalks (20 stores, 41 items) so the 10k-row fact table is never shuffled for this join
    store_map = F.broadcast(store_xref.select(F.col("pos_store_id").alias("pos_store_id"), F.col("store_id").alias("store_key")))
    item_map = F.broadcast(item_xref.select(F.col("pos_item_id").alias("pos_item_id"), F.col("item_id").alias("item_key")))
    return (df.withColumnRenamed("store_id", "pos_store_id").withColumnRenamed("item_id", "pos_item_id")
            .join(store_map, "pos_store_id", "left").join(item_map, "pos_item_id", "left")
            .withColumnRenamed("store_key", "store_id").withColumnRenamed("item_key", "item_id"))


sales = map_pos_keys(bronze["pos_sales_weekly"]).select(
    "store_id", "item_id", "pos_item_id", F.expr("try_cast(week_start AS DATE)").alias("week_start"),
    F.col("units_sold").cast("int").alias("units_sold"), F.col("days_in_stock").cast("int").alias("days_in_stock"),
    F.expr("try_to_timestamp(updated_at)").alias("updated_at"), "_source_system", "_ingested_at")
snapshots = map_pos_keys(bronze["pos_stock_snapshots"]).select(
    "store_id", "item_id", "pos_item_id", F.expr("try_cast(week_start AS DATE)").alias("week_start"),
    F.expr("try_cast(snapshot_date AS DATE)").alias("snapshot_date"),
    F.col("on_hand").cast("int").alias("on_hand"), F.col("on_order").cast("int").alias("on_order"),
    F.col("received").cast("int").alias("received"), F.expr("try_to_timestamp(updated_at)").alias("updated_at"),
    "_source_system", "_ingested_at")

# COMMAND ----------

# MAGIC %md
# MAGIC The store-ops app logs waste in **store local time**. A Sunday 22:30 log in New York is already Monday 03:30 in UTC, so bucketing by UTC would move it into the next week.

# COMMAND ----------

# SaaS waste log: store name → store key, local time → local date and UTC, text quantity → int
waste_events = (bronze["ops_waste_log"]
    .join(F.broadcast(stores.select("store_id", F.col("store_name").alias("storeName"), "timezone")), "storeName", "left")
    .join(F.broadcast(item_xref.select(F.col("item_id").alias("itemCode"), F.lit(True).alias("_known_item"))), "itemCode", "left")
    .select(
        "store_id", F.when(F.col("_known_item"), F.col("itemCode")).alias("item_id"), F.col("itemCode").alias("source_item_code"),
        F.expr("try_to_timestamp(loggedAt)").alias("event_ts_local"), "timezone",
        F.expr("try_cast(wasteQty AS INT)").alias("waste_units"), F.lower("reason").alias("reason"),
        F.expr("try_to_timestamp(syncedAt)").alias("synced_at"), "_source_system", "_ingested_at")
    # EDGE: timezone: the week comes from the store-local date; UTC is kept for ordering and sync lag only
    .withColumn("event_ts_utc", F.to_utc_timestamp("event_ts_local", F.col("timezone")))
    .withColumn("event_date_local", F.to_date("event_ts_local"))
    .withColumn("week_start", F.to_date(F.date_trunc("week", "event_date_local")))
    # EDGE: late-arriving logs are assigned by event time, not sync time; flag the ones synced more than a day late
    .withColumn("is_late_sync", F.col("synced_at") > F.col("event_ts_utc") + F.expr("INTERVAL 1 DAY")))
quality_log += [
    ("ops_waste_log", "utc_bucketing_would_shift_week",
     waste_events.where(F.to_date(F.date_trunc("week", "event_ts_utc")) != F.col("week_start")).count()),
    ("ops_waste_log", "late_synced_kept_in_event_week", waste_events.where("is_late_sync").count())]

# COMMAND ----------

# SaaS promo calendar: product code → item key, text dates and "25%" → numbers
promotions = (bronze["promo_calendar"]
    .join(F.broadcast(item_xref.select(F.col("item_id").alias("productSku"), F.lit(True).alias("_known_item"))), "productSku", "left")
    .select(F.col("promoId").alias("promo_id"), F.when(F.col("_known_item"), F.col("productSku")).alias("item_id"),
            F.expr("try_cast(startDate AS DATE)").alias("week_start"), F.expr("try_cast(endDate AS DATE)").alias("end_date"),
            F.expr("try_cast(regexp_replace(discountPct, '%', '') AS INT)").alias("discount_pct"), "_source_system"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 Deduplicate
# MAGIC Only exact re-sends are dropped here (same values, later `updated_at`). Copies with *different* values are left for the rules in section 4, so a corrupted later copy can't replace the true row.

# COMMAND ----------

# EDGE: late-arriving re-sends: identical business values, keep the first arrival
def drop_resends(df, table, value_cols):
    first_arrival = Window.partitionBy(*value_cols).orderBy("updated_at")
    deduped = df.withColumn("_rn", F.row_number().over(first_arrival)).where("_rn = 1").drop("_rn")  # SPARK: window = shuffle by key
    quality_log.append((table, "exact_resend_removed", df.count() - deduped.count()))
    return deduped


sales = drop_resends(sales, "pos_sales_weekly", ["store_id", "pos_item_id", "week_start", "units_sold", "days_in_stock"])
snapshots = drop_resends(snapshots, "pos_stock_snapshots",
                         ["store_id", "pos_item_id", "week_start", "snapshot_date", "on_hand", "on_order", "received"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 Quality rules → quarantine
# MAGIC Each rule says what a *good* row looks like. A row that fails goes to `silver_quarantine` with the names of the rules it failed.
# MAGIC Rules on values only apply once the keys are known, so an orphan row fails only the key rule and the per-rule counts add up.

# COMMAND ----------

# QUALITY: helper: evaluate named pass-rules, quarantine failures with their rule names, return the clean rows
def apply_rules(df, table, rules):
    checks = [F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name)) for name, rule in rules.items()]
    flagged = df.withColumn("failed_rules", F.filter(F.array(*checks), lambda r: r.isNotNull()))
    bad = flagged.where(F.size("failed_rules") > 0)
    quarantine_parts.append(bad.select(F.lit(table).alias("source_table"), "failed_rules",
                                       F.to_json(F.struct(*[c for c in df.columns])).alias("record")))
    return flagged.where(F.size("failed_rules") == 0).drop("failed_rules")


KEYS_KNOWN = "store_id IS NOT NULL AND item_id IS NOT NULL"
# EDGE: orphan keys: rows whose store or item is not in the ERP are quarantined (they can't be valued or planned)
sales = apply_rules(sales, "pos_sales_weekly", {
    "known_store_and_item": KEYS_KNOWN,
    "plausible_units_sold": f"NOT ({KEYS_KNOWN}) OR units_sold BETWEEN 0 AND 5000",
    "days_in_stock_0_to_7": f"NOT ({KEYS_KNOWN}) OR days_in_stock BETWEEN 0 AND 7",
})
snapshots = apply_rules(snapshots, "pos_stock_snapshots", {
    "known_store_and_item": KEYS_KNOWN,
    "non_negative_on_hand": f"NOT ({KEYS_KNOWN}) OR on_hand >= 0",
})
promotions = apply_rules(promotions, "promo_calendar", {
    "known_item": "item_id IS NOT NULL",
    "discount_1_to_90_pct": "item_id IS NULL OR discount_pct BETWEEN 1 AND 90",
})

# COMMAND ----------

# MAGIC %md
# MAGIC A store can't throw away more than it had that week: prior week's on hand + received. Week 1 (or a product's first week) has no prior snapshot, so that week's on hand + received stands in for the opening stock.

# COMMAND ----------

# QUALITY: waste must not exceed the stock available that week (uses the clean snapshots)
by_item = Window.partitionBy("store_id", "item_id").orderBy("week_start")
available = (snapshots.withColumn("prior_on_hand", F.lag("on_hand").over(by_item))
             .select("store_id", "item_id", "week_start",
                     (F.coalesce("prior_on_hand", F.col("on_hand") + F.col("received")) + F.col("received")).alias("available_units")))
waste_events = waste_events.join(available, ["store_id", "item_id", "week_start"], "left")
waste_events = apply_rules(waste_events, "ops_waste_log", {
    "known_store_and_item": KEYS_KNOWN,
    "positive_waste_qty": f"NOT ({KEYS_KNOWN}) OR waste_units > 0",
    "waste_within_available_stock": f"NOT ({KEYS_KNOWN}) OR waste_units IS NULL OR available_units IS NULL OR waste_units <= available_units",
}).drop("available_units")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 Write silver

# COMMAND ----------

# Keep the latest valid version per key (corrupted copies are already quarantined, so this should remove nothing)
def keep_latest(df, table, keys):
    latest = Window.partitionBy(*keys).orderBy(F.col("updated_at").desc())
    kept = df.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
    quality_log.append((table, "superseded_valid_versions", df.count() - kept.count()))
    return kept


sales = keep_latest(sales, "pos_sales_weekly", ["store_id", "item_id", "week_start"])
snapshots = keep_latest(snapshots, "pos_stock_snapshots", ["store_id", "item_id", "week_start"])

# COMMAND ----------

# Write the silver tables (Delta, overwrite = idempotent reruns)
silver = {"silver_stores": stores, "silver_products": products, "silver_store_xref": store_xref, "silver_item_xref": item_xref,
          "silver_sales_weekly": sales, "silver_stock_snapshots": snapshots, "silver_waste_events": waste_events,
          "silver_promotions": promotions}
for name, df in silver.items():
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(name)

quarantine = quarantine_parts[0]
for part in quarantine_parts[1:]:
    quarantine = quarantine.unionByName(part)
quarantine.withColumn("quarantined_at", F.current_timestamp()) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 Quarantine summary

# COMMAND ----------

# QUALITY: one row per rule or fix with its row count, stored for the dashboard and the demo
per_rule = (spark.table("silver_quarantine").select("source_table", F.explode("failed_rules").alias("rule"))  # explode before groupBy
            .groupBy("source_table", "rule").count().collect())
log_rows = quality_log + [(r.source_table, f"quarantined: {r.rule}", r["count"]) for r in per_rule]
spark.createDataFrame(log_rows, "table_name STRING, rule STRING, row_count LONG") \
    .withColumn("checked_at", F.current_timestamp()) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_quality_log")
display(spark.table("silver_quality_log").orderBy("table_name", "rule"))

# COMMAND ----------

# Rows per source system that made it into the conformed silver facts (lineage from bronze to silver)
display(spark.sql("""
SELECT 'postgres_pos' AS source_system, 'silver_sales_weekly' AS table_name, count(*) AS row_count FROM silver_sales_weekly
UNION ALL SELECT 'postgres_pos', 'silver_stock_snapshots', count(*) FROM silver_stock_snapshots
UNION ALL SELECT 'saas_store_ops', 'silver_waste_events', count(*) FROM silver_waste_events
UNION ALL SELECT 'saas_promo_tool', 'silver_promotions', count(*) FROM silver_promotions
UNION ALL SELECT 'mysql_erp', 'silver_products (versions)', count(*) FROM silver_products
UNION ALL SELECT 'mysql_erp', 'silver_stores', count(*) FROM silver_stores"""))
