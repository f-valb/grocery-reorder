"""Lakeflow (SDP) medallion template: Auto Loader -> bronze -> translate -> quarantine | clean ->
content-hash dedupe -> AUTO CDC silver -> gold MVs (row filter, freshness).

Copy into pipelines/<name>/transformations/, set the TODOs, and point a bundle pipeline resource at it
(templates/bundle/databricks.yml). Proven on serverless in pipelines/pos_country/transformations/*.

Rules this encodes (each one cost time once):
* `from pyspark import pipelines as dp` (not `import dlt`); `dp.create_auto_cdc_flow` (not apply_changes).
* Fully qualified names when publishing to more than one schema; read them back by the same FQN.
* try_cast / try_to_timestamp / try_element_at everywhere: serverless runs ANSI mode, bad data must
  become NULL and fail a named rule, never crash the update.
* Dedupe watermark on PROCESSING time (_bronze_ts), never on event time: an event-time watermark
  silently drops late rows when the pipeline catches up.
* Masks / row filters need an explicit schema and the UC functions must exist before the first run.
* A table can be owned by only ONE pipeline: never deploy the same target by hand and from CI.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("app.catalog")  # TODO: pipeline configuration keys
ENV = spark.conf.get("app.env")
LANDING = spark.conf.get("app.landing_path")  # /Volumes/<cat>/<env>_bronze/landing/raw
BRONZE = f"{CAT}.{ENV}_bronze.events_raw"
SILVER = f"{CAT}.{ENV}_silver"
GOLD = f"{CAT}.{ENV}_gold"
KEYS = ["entity_id", "line_no"]  # TODO: business key
RULES = {  # TODO: one named rule per defect you generate; names show up in quarantine
    "has_key": "entity_id IS NOT NULL",
    "valid_ts": "event_ts IS NOT NULL",
    # Keep rules mutually exclusive (a null amount fails only has_amount), so per-rule quarantine counts add up
    "has_amount": "amount IS NOT NULL",
    "non_negative_amount": "amount IS NULL OR amount >= 0",
}
ALL_PASS = " AND ".join(f"coalesce(({r}), false)" for r in RULES.values())


@dp.table(name=BRONZE, cluster_by=["_window_start"],
          table_properties={"delta.enableRowTracking": "true",
                            "pipelines.reset.allowed": "false" if ENV == "prod" else "true"})
def events_raw():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.partitionColumns", "")  # hive-style dirs are not data columns
            .load(LANDING)
            .withColumn("_landed_ts", F.col("_metadata.file_modification_time"))
            .withColumn("_bronze_ts", F.current_timestamp()))


@dp.temporary_view(name="events_translated")
def events_translated():
    s = spark.readStream.table(BRONZE)
    return s.select(  # TODO: legacy -> typed columns
        F.expr("nullif(RAW_ID, '')").alias("entity_id"),  # F.nullif is broken in PySpark 3.5
        F.expr("try_cast(LN AS INT)").alias("line_no"),
        F.try_to_timestamp(F.col("RAW_TS"), F.lit("dd/MM/yyyy HH:mm:ss")).alias("event_ts"),
        F.expr("try_cast(replace(RAW_AMT, ',', '.') AS DECIMAL(12,2))").alias("amount"),
        "_record_hash", "_ingest_ts", "_landed_ts", "_bronze_ts", "_raw_line",
    )


@dp.table(name=f"{SILVER}.events_quarantine")
def events_quarantine():
    failed = F.filter(F.array(*[F.when(~F.coalesce(F.expr(r), F.lit(False)), F.lit(n)) for n, r in RULES.items()]),
                      lambda x: x.isNotNull())
    return (spark.readStream.table("events_translated").where(f"NOT ({ALL_PASS})")
            .select("_raw_line", failed.alias("failed_rules"), "_ingest_ts", F.current_timestamp().alias("_quarantined_ts")))


@dp.temporary_view(name="events_clean")
@dp.expect_all_or_drop(RULES)
def events_clean():
    return (spark.readStream.table("events_translated")
            .withWatermark("_bronze_ts", "30 minutes")
            .dropDuplicatesWithinWatermark(["_record_hash"])
            .withColumn("_silver_ts", F.current_timestamp())
            .drop("_raw_line"))


dp.create_streaming_table(name=f"{SILVER}.events", cluster_by=["event_ts"],
                          table_properties={"delta.enableRowTracking": "true"})
dp.create_auto_cdc_flow(target=f"{SILVER}.events", source="events_clean", keys=KEYS,
                        sequence_by=F.col("_ingest_ts"), stored_as_scd_type=1)


@dp.materialized_view(name=f"{GOLD}.events_by_hour",
                      row_filter=f"ROW FILTER {CAT}.security.rf_country ON (country)")  # TODO or remove
def events_by_hour():
    return (spark.read.table(f"{SILVER}.events")
            .groupBy(F.lit("PH").alias("country"), F.date_trunc("hour", "event_ts").alias("hour"))
            .agg(F.count("*").alias("events"), F.sum("amount").alias("amount")))


@dp.materialized_view(name=f"{GOLD}.data_as_of")
def data_as_of():
    return (spark.read.table(f"{SILVER}.events")
            .agg(F.max("event_ts").alias("data_as_of"), F.max("_silver_ts").alias("last_silver_ts"))
            .withColumn("refreshed_at", F.current_timestamp()))
