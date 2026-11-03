# Databricks notebook source
# MAGIC %md
# MAGIC # 00b Governance setup
# MAGIC **Purpose:** create who-may-see-what before any gold table exists, because the pipeline declares the row filter and column mask on `gold_store_product_week` and Unity Catalog needs the functions to exist first.
# MAGIC **Inputs:** none (mock store managers, plus the user running the job as admin)
# MAGIC **Outputs:** `gov_store_managers`, functions `gov_store_filter` (row filter) and `gov_cost_mask` (column mask)

# COMMAND ----------

# Governance: who may see which store (mock managers plus the job owner, who sees everything)
spark.sql("USE workspace.grocery_intel")
owner = spark.sql("SELECT current_user()").first()[0]
stores = [f"ST-{n:02d}" for n in range(1, 21)]  # the 20 ERP store codes
spark.createDataFrame(
    [(owner, "ALL", "admin")] + [(f"manager.{s.lower()}@example.com", s, "store_manager") for s in stores]
    + [("finance.lead@example.com", "ALL", "finance")],
    "user_email STRING, store_id STRING, role STRING").write.mode("overwrite").saveAsTable("gov_store_managers")

# COMMAND ----------

# Row filter (store managers see their own store) and column mask (only admin and finance see unit cost)
spark.sql("""CREATE OR REPLACE FUNCTION gov_store_filter(store STRING) RETURNS BOOLEAN
  RETURN EXISTS (SELECT 1 FROM workspace.grocery_intel.gov_store_managers g
                 WHERE g.user_email = current_user() AND (g.store_id = store OR g.store_id = 'ALL'))""")
spark.sql("""CREATE OR REPLACE FUNCTION gov_cost_mask(cost DOUBLE) RETURNS DOUBLE
  RETURN CASE WHEN EXISTS (SELECT 1 FROM workspace.grocery_intel.gov_store_managers g
                           WHERE g.user_email = current_user() AND g.role IN ('admin', 'finance')) THEN cost END""")
display(spark.sql("SELECT role, count(*) AS users FROM gov_store_managers GROUP BY role"))
