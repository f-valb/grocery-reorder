# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver
# MAGIC **Purpose:** typed, deduplicated, validated tables. Rows that break a rule go to one `silver_quarantine` table with the rules they failed.
# MAGIC **Inputs:** `bronze_*`
# MAGIC **Outputs:** `silver_stores`, `silver_products`, `silver_sales_weekly`, `silver_stock_snapshots`, `silver_waste`, `silver_promotions`, `silver_quarantine`, `silver_quality_log`

# COMMAND ----------

# Setup: helpers to split a table into clean rows and quarantined rows by named rules
from pyspark.sql import functions as F

spark.sql("USE workspace.grocery_reorder")
quarantine_parts, quality_log = [], []


def apply_rules(df, table, rules):
    # QUALITY: each rule is a named condition that marks a row as bad; failed rule names go into an array column
    failed = F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()]))
    df = df.withColumn("failed_rules", failed)
    bad = df.where(F.size("failed_rules") > 0)
    quarantine_parts.append(bad.select(F.lit(table).alias("source_table"), "failed_rules",
                                       F.to_json(F.struct(*[c for c in df.columns if c != "failed_rules"])).alias("record")))
    return df.where(F.size("failed_rules") == 0).drop("failed_rules")

# COMMAND ----------

# Stores and promotions: cast types (no planted flaws here)
stores = spark.table("bronze_stores").select("store_id", "region", "size")
stores.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_stores")

promotions = spark.table("bronze_promotions").select(
    "product_id", F.expr("try_cast(week_start AS DATE)").alias("week_start"),
    F.expr("try_cast(discount_pct AS INT)").alias("discount_pct"))
promotions.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_promotions")

# COMMAND ----------

# MAGIC %md
# MAGIC Products: missing shelf life on a perishable would break the shelf-life cap, so fill it with the category median and flag it (a product row can't be quarantined: its sales would be lost).

# COMMAND ----------

# QUALITY: fill null shelf life with the category median and flag the filled rows
products = spark.table("bronze_products").select(
    "product_id", "product_name", "category",
    (F.lower("perishable") == "true").alias("perishable"),
    F.expr("try_cast(shelf_life_days AS INT)").alias("shelf_life_days"),
    F.expr("try_cast(case_pack AS INT)").alias("case_pack"),
    F.expr("try_cast(unit_cost AS DOUBLE)").alias("unit_cost"),
    F.expr("try_cast(unit_price AS DOUBLE)").alias("unit_price"))
category_median = products.groupBy("category").agg(F.expr("percentile_disc(0.5) WITHIN GROUP (ORDER BY shelf_life_days)").alias("median_shelf_life"))
products = (products.join(category_median, "category")
            .withColumn("shelf_life_imputed", F.col("shelf_life_days").isNull())
            .withColumn("shelf_life_days", F.coalesce("shelf_life_days", "median_shelf_life"))
            .drop("median_shelf_life"))
products.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_products")
quality_log.append(("products", "null_shelf_life_imputed", products.where("shelf_life_imputed").count()))
known_products = products.select("product_id", F.lit(True).alias("_known"))

# COMMAND ----------

# Weekly sales: cast, then quarantine unknown products and impossible values
sales = (spark.table("bronze_sales_weekly")
         .select("store_id", "product_id", F.expr("try_cast(week_start AS DATE)").alias("week_start"),
                 F.expr("try_cast(units_sold AS INT)").alias("units_sold"),
                 F.expr("try_cast(days_in_stock AS INT)").alias("days_in_stock"), "_ingested_at")
         .join(known_products, "product_id", "left"))
sales = apply_rules(sales, "sales_weekly", {
    "unknown_product": F.col("_known").isNull(),
    "invalid_sales_values": F.col("_known").isNotNull() & (F.col("units_sold").isNull() | (F.col("units_sold") < 0)
                                                         | ~F.col("days_in_stock").between(0, 7)),
}).drop("_known")
sales.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_sales_weekly")

# COMMAND ----------

# MAGIC %md
# MAGIC Stock snapshots: the source re-extracts some rows (same store/product/week, later `extracted_at`, corrected values). Dedupe first so the correction wins, then validate.

# COMMAND ----------

# QUALITY: dedupe snapshots, keeping the latest extract per store/product/week
from pyspark.sql.window import Window

snaps_raw = (spark.table("bronze_stock_snapshots")
             .select("store_id", "product_id", F.expr("try_cast(week_start AS DATE)").alias("week_start"),
                     F.expr("try_cast(snapshot_date AS DATE)").alias("snapshot_date"),
                     F.expr("try_cast(on_hand AS INT)").alias("on_hand"),
                     F.expr("try_cast(on_order AS INT)").alias("on_order"),
                     F.expr("try_cast(received AS INT)").alias("received"),
                     F.expr("try_to_timestamp(extracted_at)").alias("extracted_at"), "_ingested_at"))
latest_first = Window.partitionBy("store_id", "product_id", "week_start").orderBy(F.col("extracted_at").desc())
snaps = snaps_raw.withColumn("_rn", F.row_number().over(latest_first)).where("_rn = 1").drop("_rn")
quality_log.append(("stock_snapshots", "duplicate_snapshot_removed", snaps_raw.count() - snaps.count()))

# QUALITY: negative stock is a system error, unknown products can't be planned
snaps = apply_rules(snaps.join(known_products, "product_id", "left"), "stock_snapshots", {
    "unknown_product": F.col("_known").isNull(),
    "negative_on_hand": F.col("_known").isNotNull() & (F.col("on_hand") < 0),
}).drop("_known")
snaps.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_stock_snapshots")

# COMMAND ----------

# MAGIC %md
# MAGIC Waste: a store can't throw away more than it had that week (prior week's on hand + received).
# MAGIC Week 1 has no prior snapshot, so its opening stock is estimated as that week's on hand + received.

# COMMAND ----------

# QUALITY: quarantine waste from unknown products and waste above the stock available that week
prior = Window.partitionBy("store_id", "product_id").orderBy("week_start")
available = (snaps_raw.withColumn("_rn", F.row_number().over(latest_first)).where("_rn = 1")
             .withColumn("prior_on_hand", F.lag("on_hand").over(prior))
             # TRAP: greatest() ignores nulls, so clamp negatives only when a prior week exists
             .withColumn("prior_on_hand", F.when(F.col("prior_on_hand").isNotNull(), F.greatest("prior_on_hand", F.lit(0))))
             .select("store_id", "product_id", "week_start",
                     (F.coalesce("prior_on_hand", F.greatest("on_hand", F.lit(0)) + F.col("received")) + F.col("received")).alias("available_units")))
waste = (spark.table("bronze_waste")
         .select("store_id", "product_id", F.expr("try_cast(week_start AS DATE)").alias("week_start"),
                 F.expr("try_cast(waste_units AS INT)").alias("waste_units"), "reason", "_ingested_at")
         .join(known_products, "product_id", "left")
         .join(available, ["store_id", "product_id", "week_start"], "left"))
waste = apply_rules(waste, "waste", {
    "unknown_product": F.col("_known").isNull(),
    "waste_gt_available": F.col("_known").isNotNull() & (F.col("waste_units") > F.col("available_units")),
}).drop("_known", "available_units")
waste.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_waste")

# COMMAND ----------

# Write the quarantine table and the quality log (one row per rule with the row count)
quarantine = quarantine_parts[0]
for part in quarantine_parts[1:]:
    quarantine = quarantine.unionByName(part)
quarantine.withColumn("quarantined_at", F.current_timestamp()) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_quarantine")

per_rule = (spark.table("silver_quarantine").select("source_table", F.explode("failed_rules").alias("rule"))
            .groupBy("source_table", "rule").count().collect())
log_rows = quality_log + [(r.source_table, r.rule, r["count"]) for r in per_rule]
spark.createDataFrame(log_rows, "table_name STRING, rule STRING, row_count LONG") \
    .withColumn("checked_at", F.current_timestamp()) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_quality_log")
display(spark.table("silver_quality_log").orderBy("table_name", "rule"))
