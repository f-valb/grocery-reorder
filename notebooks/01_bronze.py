# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze
# MAGIC **Purpose:** land each source system's extract as a Delta table exactly as received, with an explicit schema and lineage columns.
# MAGIC **Inputs:** `/Volumes/workspace/grocery_intel/raw/*.parquet` (POS, ERP and SaaS extracts)
# MAGIC **Outputs:** `bronze_<source table>` for each of the 6 extracts, with `_source_system`, `_source_file`, `_ingested_at`
# MAGIC
# MAGIC In production these extracts would arrive through Lakeflow Connect (Postgres and MySQL change data capture) and a SaaS connector or Auto Loader; the bronze contract stays the same.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Read raw files

# COMMAND ----------

# Explicit schema per source table: a source that changes its columns fails here instead of silently shifting data
import os
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

spark.sql("USE workspace.grocery_intel")
RAW = os.environ.get("RAW_DIR", "/Volumes/workspace/grocery_intel/raw")


def schema(*cols):
    return StructType([StructField(name, dtype, True) for name, dtype in cols])


S, L, D = StringType(), LongType(), DoubleType()
SOURCES = {
    # table: (source system, schema)
    "pos_sales_weekly": ("postgres_pos", schema(("store_id", L), ("item_id", L), ("week_start", S), ("units_sold", L),
                                                ("days_in_stock", L), ("updated_at", S))),
    "pos_stock_snapshots": ("postgres_pos", schema(("store_id", L), ("item_id", L), ("week_start", S), ("snapshot_date", S),
                                                   ("on_hand", L), ("on_order", L), ("received", L), ("updated_at", S))),
    "erp_stores": ("mysql_erp", schema(("store_code", S), ("store_name", S), ("region", S), ("size_band", S),
                                       ("timezone", S), ("pos_store_id", L))),
    "erp_products": ("mysql_erp", schema(("item_code", S), ("pos_item_id", L), ("product_name", S), ("category", S),
                                         ("is_perishable", L), ("shelf_life_days", L), ("case_qty", L), ("unit_cost", D),
                                         ("list_price", D), ("valid_from", S), ("valid_to", S))),
    # SaaS exports arrive as text: silver parses them with try_cast and quarantines what doesn't fit
    "ops_waste_log": ("saas_store_ops", schema(*[(c, S) for c in ["storeName", "itemCode", "loggedAt", "wasteQty", "reason", "syncedAt"]])),
    "promo_calendar": ("saas_promo_tool", schema(*[(c, S) for c in ["promoId", "productSku", "startDate", "endDate", "discountPct"]])),
}
raw_tables = {name: spark.read.schema(struct).parquet(f"{RAW}/{name}.parquet") for name, (_, struct) in SOURCES.items()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Add ingest metadata (source, ingested_at)

# COMMAND ----------

# Lineage columns: which system and file each row came from, and when it landed
bronze_tables = {
    name: df.withColumn("_source_system", F.lit(SOURCES[name][0]))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    for name, df in raw_tables.items()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 Write bronze tables

# COMMAND ----------

# SPARK: Delta tables in Unity Catalog; overwrite keeps reruns idempotent (same input, same table)
for name, df in bronze_tables.items():
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"bronze_{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 Row counts

# COMMAND ----------

# Row counts per source system and table (should match the generator's printout)
display(spark.sql(" UNION ALL ".join(
    f"SELECT '{SOURCES[n][0]}' AS source_system, 'bronze_{n}' AS table_name, count(*) AS row_count FROM bronze_{n}" for n in SOURCES)))
