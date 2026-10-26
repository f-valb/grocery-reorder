# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze
# MAGIC **Purpose:** land the raw CSVs as Delta tables exactly as received, plus load metadata.
# MAGIC **Inputs:** `/Volumes/workspace/grocery_reorder/raw/*.csv`
# MAGIC **Outputs:** `bronze_stores`, `bronze_products`, `bronze_sales_weekly`, `bronze_stock_snapshots`, `bronze_waste`, `bronze_promotions`
# MAGIC
# MAGIC Every column stays a string: bad values (negative stock, unknown IDs) must survive to silver, where they are cast and quarantined.

# COMMAND ----------

# Load each CSV as all-string columns and add where/when it was loaded
from pyspark.sql import functions as F

spark.sql("USE workspace.grocery_reorder")
RAW = "/Volumes/workspace/grocery_reorder/raw"
TABLES = ["stores", "products", "sales_weekly", "stock_snapshots", "waste", "promotions"]

for name in TABLES:
    raw = (spark.read.option("header", True).option("inferSchema", False).csv(f"{RAW}/{name}.csv")
           .withColumn("_source_file", F.col("_metadata.file_path"))
           .withColumn("_ingested_at", F.current_timestamp()))
    raw.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"bronze_{name}")

# COMMAND ----------

# Row counts per bronze table (should match the generator's printout)
display(spark.sql(" UNION ALL ".join(f"SELECT 'bronze_{n}' AS table_name, count(*) AS row_count FROM bronze_{n}" for n in TABLES)))
