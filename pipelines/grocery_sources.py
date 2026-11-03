"""Lakeflow Declarative Pipeline: raw source extracts -> bronze -> conformed silver (quarantine, quality log) -> gold facts.

Same logic as notebooks/01_bronze.py, 02_silver.py and 03_gold.py section 3 (kept as the local-test version), declared
as materialized views so the pipeline owns the tables: Unity Catalog lineage then shows this pipeline as the producer of
every bronze, silver and gold fact table, and the pipeline graph shows the flow from the six source files to gold.
The model (training, forecast, reorder) stays a notebook task after the pipeline: retraining is not a table refresh.

Target: workspace.grocery_intel (set on the pipeline). Config: app.raw_dir (raw volume), app.as_of (fixed as-of date).
Built and started by jobs/build_pipeline.py; the saved job runs it as a pipeline task after ingest_sources.
"""

import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

RAW = spark.conf.get("app.raw_dir", "/Volumes/workspace/grocery_intel/raw")


def schema(*cols):
    return StructType([StructField(name, dtype, True) for name, dtype in cols])


# --- Bronze: one materialized view per source extract, explicit schema, lineage columns ---------------------------
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


def bronze_view(name, system, struct):
    # SPARK: materialized view = full batch read of the extract each update, so a rerun on the same files gives the same table
    # (a streaming table would skip extracts that the source re-sends under the same file name)
    @dp.materialized_view(name=f"bronze_{name}", comment=f"Raw {name} extract from {system}, as received, with lineage columns")
    def _bronze():
        return (spark.read.schema(struct).parquet(f"{RAW}/{name}.parquet")
                .withColumn("_source_system", F.lit(system))
                .withColumn("_source_file", F.col("_metadata.file_path"))
                .withColumn("_ingested_at", F.current_timestamp()))


for source_name, (source_system, source_schema) in SOURCES.items():
    bronze_view(source_name, source_system, source_schema)


# --- Silver: master data and crosswalks (ERP codes are the conformed keys) -----------------------------------------
@dp.materialized_view(name="silver_stores", comment="One row per store, ERP store code is the key")
def silver_stores():
    return spark.read.table("bronze_erp_stores").select(
        F.col("store_code").alias("store_id"), "store_name", "region", F.col("size_band").alias("store_size"), "timezone",
        "pos_store_id", "_source_system")


@dp.materialized_view(name="silver_store_xref", comment="POS store id and store name to ERP store code")
def silver_store_xref():
    return spark.read.table("silver_stores").select("store_id", "pos_store_id", "store_name")


@dp.materialized_view(name="silver_products", comment="One row per product price version (SCD2), shelf life imputed where missing")
def silver_products():
    products = spark.read.table("bronze_erp_products").select(
        F.col("item_code").alias("item_id"), "pos_item_id", "product_name", "category",
        (F.col("is_perishable") == 1).alias("perishable"), F.col("shelf_life_days").cast("int").alias("shelf_life_days"),
        F.col("case_qty").cast("int").alias("case_pack"), "unit_cost", F.col("list_price").alias("unit_price"),
        F.expr("try_cast(valid_from AS DATE)").alias("valid_from"),
        # EDGE: price changed mid-period: keep every version; open-ended versions run to 9999-12-31 so gold can join on the week
        F.coalesce(F.expr("try_cast(valid_to AS DATE)"), F.lit("9999-12-31").cast("date")).alias("valid_to"), "_source_system")
    # QUALITY: missing shelf life would break the perishable cap: fill with the category median and flag it
    category_median = products.groupBy("category").agg(F.expr("percentile_approx(shelf_life_days, 0.5)").alias("median_shelf_life"))
    return (products.join(F.broadcast(category_median), "category")  # SPARK: 6-row lookup, broadcast avoids a shuffle
            .withColumn("shelf_life_imputed", F.col("shelf_life_days").isNull())
            .withColumn("shelf_life_days", F.coalesce("shelf_life_days", "median_shelf_life"))
            .drop("median_shelf_life"))


@dp.materialized_view(name="silver_item_xref", comment="POS item id to ERP item code")
def silver_item_xref():
    return spark.read.table("silver_products").select("item_id", "pos_item_id").distinct()


# --- Silver: POS facts (map keys, parse types, drop exact re-sends) ------------------------------------------------
def map_pos_keys(df):
    # SPARK: broadcast the small crosswalks (20 stores, 41 items) so the 10k-row fact table is never shuffled for this join
    store_map = F.broadcast(spark.read.table("silver_store_xref").select("pos_store_id", F.col("store_id").alias("store_key")))
    item_map = F.broadcast(spark.read.table("silver_item_xref").select("pos_item_id", F.col("item_id").alias("item_key")))
    return (df.withColumnRenamed("store_id", "pos_store_id").withColumnRenamed("item_id", "pos_item_id")
            .join(store_map, "pos_store_id", "left").join(item_map, "pos_item_id", "left")
            .withColumnRenamed("store_key", "store_id").withColumnRenamed("item_key", "item_id"))


def first_arrival(df, value_cols):
    # EDGE: late-arriving re-sends: identical business values, keep the first arrival
    window = Window.partitionBy(*value_cols).orderBy("updated_at")  # SPARK: window = shuffle by key
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


@dp.temporary_view(name="sales_typed")
def sales_typed():
    return map_pos_keys(spark.read.table("bronze_pos_sales_weekly")).select(
        "store_id", "item_id", "pos_item_id", F.expr("try_cast(week_start AS DATE)").alias("week_start"),
        F.col("units_sold").cast("int").alias("units_sold"), F.col("days_in_stock").cast("int").alias("days_in_stock"),
        F.expr("try_to_timestamp(updated_at)").alias("updated_at"), "_source_system", "_ingested_at")


@dp.temporary_view(name="snapshots_typed")
def snapshots_typed():
    return map_pos_keys(spark.read.table("bronze_pos_stock_snapshots")).select(
        "store_id", "item_id", "pos_item_id", F.expr("try_cast(week_start AS DATE)").alias("week_start"),
        F.expr("try_cast(snapshot_date AS DATE)").alias("snapshot_date"),
        F.col("on_hand").cast("int").alias("on_hand"), F.col("on_order").cast("int").alias("on_order"),
        F.col("received").cast("int").alias("received"), F.expr("try_to_timestamp(updated_at)").alias("updated_at"),
        "_source_system", "_ingested_at")


@dp.temporary_view(name="sales_deduped")
def sales_deduped():
    return first_arrival(spark.read.table("sales_typed"), ["store_id", "pos_item_id", "week_start", "units_sold", "days_in_stock"])


@dp.temporary_view(name="snapshots_deduped")
def snapshots_deduped():
    return first_arrival(spark.read.table("snapshots_typed"),
                         ["store_id", "pos_item_id", "week_start", "snapshot_date", "on_hand", "on_order", "received"])


# --- Silver: SaaS waste log and promo calendar ---------------------------------------------------------------------
@dp.temporary_view(name="waste_typed")
def waste_typed():
    stores = spark.read.table("silver_stores")
    item_xref = spark.read.table("silver_item_xref")
    return (spark.read.table("bronze_ops_waste_log")
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


@dp.temporary_view(name="promotions_typed")
def promotions_typed():
    item_xref = spark.read.table("silver_item_xref")
    return (spark.read.table("bronze_promo_calendar")
        .join(F.broadcast(item_xref.select(F.col("item_id").alias("productSku"), F.lit(True).alias("_known_item"))), "productSku", "left")
        .select(F.col("promoId").alias("promo_id"), F.when(F.col("_known_item"), F.col("productSku")).alias("item_id"),
                F.expr("try_cast(startDate AS DATE)").alias("week_start"), F.expr("try_cast(endDate AS DATE)").alias("end_date"),
                F.expr("try_cast(regexp_replace(discountPct, '%', '') AS INT)").alias("discount_pct"), "_source_system"))


# --- Quality rules -> quarantine (named pass-rules, failed_rules array) --------------------------------------------
KEYS_KNOWN = "store_id IS NOT NULL AND item_id IS NOT NULL"
# EDGE: orphan keys: rows whose store or item is not in the ERP are quarantined (they can't be valued or planned)
RULES = {
    "pos_sales_weekly": {
        "known_store_and_item": KEYS_KNOWN,
        "plausible_units_sold": f"NOT ({KEYS_KNOWN}) OR units_sold BETWEEN 0 AND 5000",
        "days_in_stock_0_to_7": f"NOT ({KEYS_KNOWN}) OR days_in_stock BETWEEN 0 AND 7",
    },
    "pos_stock_snapshots": {
        "known_store_and_item": KEYS_KNOWN,
        "non_negative_on_hand": f"NOT ({KEYS_KNOWN}) OR on_hand >= 0",
    },
    "promo_calendar": {
        "known_item": "item_id IS NOT NULL",
        "discount_1_to_90_pct": "item_id IS NULL OR discount_pct BETWEEN 1 AND 90",
    },
    "ops_waste_log": {
        "known_store_and_item": KEYS_KNOWN,
        "positive_waste_qty": f"NOT ({KEYS_KNOWN}) OR waste_units > 0",
        "waste_within_available_stock": f"NOT ({KEYS_KNOWN}) OR waste_units IS NULL OR available_units IS NULL OR waste_units <= available_units",
    },
}


def with_failed_rules(df, rules):
    # QUALITY: evaluate every named pass-rule; failed_rules lists the ones a row breaks (empty = clean)
    checks = [F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name)) for name, rule in rules.items()]
    return df.withColumn("failed_rules", F.filter(F.array(*checks), lambda r: r.isNotNull()))


def checked_view(table, source_view):
    # QUALITY: expectations (warn) on the same rules: the pipeline UI shows failed rows per rule; the split below routes them
    rules = RULES[table]

    @dp.temporary_view(name=f"{table}_checked")
    @dp.expect_all({name: f"coalesce(({rule}), false)" for name, rule in rules.items()})
    def _checked():
        return with_failed_rules(spark.read.table(source_view), rules)


checked_view("pos_sales_weekly", "sales_deduped")
checked_view("pos_stock_snapshots", "snapshots_deduped")
checked_view("promo_calendar", "promotions_typed")


def keep_latest(df, keys):
    # Keep the latest valid version per key (corrupted copies are already quarantined, so this should remove nothing)
    window = Window.partitionBy(*keys).orderBy(F.col("updated_at").desc())
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def clean(table):
    return spark.read.table(f"{table}_checked").where(F.size("failed_rules") == 0).drop("failed_rules")


@dp.temporary_view(name="sales_valid")
def sales_valid():
    return clean("pos_sales_weekly")


@dp.temporary_view(name="snapshots_valid")
def snapshots_valid():
    return clean("pos_stock_snapshots")


@dp.materialized_view(name="silver_sales_weekly", comment="Weekly POS sales per store and item (ERP keys), clean rows only")
def silver_sales_weekly():
    return keep_latest(spark.read.table("sales_valid"), ["store_id", "item_id", "week_start"])


@dp.materialized_view(name="silver_stock_snapshots", comment="Weekly stock snapshot per store and item (ERP keys), clean rows only")
def silver_stock_snapshots():
    return keep_latest(spark.read.table("snapshots_valid"), ["store_id", "item_id", "week_start"])


@dp.materialized_view(name="silver_promotions", comment="Promo weeks per item with discount percent, clean rows only")
def silver_promotions():
    return clean("promo_calendar")


@dp.temporary_view(name="waste_with_stock")
def waste_with_stock():
    # QUALITY: waste must not exceed the stock available that week: prior on hand + received (first week: own on hand)
    by_item = Window.partitionBy("store_id", "item_id").orderBy("week_start")
    available = (spark.read.table("silver_stock_snapshots").withColumn("prior_on_hand", F.lag("on_hand").over(by_item))
                 .select("store_id", "item_id", "week_start",
                         (F.coalesce("prior_on_hand", F.col("on_hand") + F.col("received")) + F.col("received")).alias("available_units")))
    return spark.read.table("waste_typed").join(available, ["store_id", "item_id", "week_start"], "left")


checked_view("ops_waste_log", "waste_with_stock")


@dp.materialized_view(name="silver_waste_events", comment="Waste events per store and item, store-local week, clean rows only")
def silver_waste_events():
    return clean("ops_waste_log").drop("available_units")


@dp.materialized_view(name="silver_quarantine", comment="Rows that broke a quality rule, with the rule names and the full record as JSON")
def silver_quarantine():
    parts = []
    for table in RULES:
        bad = spark.read.table(f"{table}_checked").where(F.size("failed_rules") > 0)
        record_cols = [c for c in bad.columns if c != "failed_rules"]
        parts.append(bad.select(F.lit(table).alias("source_table"), "failed_rules",
                                F.to_json(F.struct(*record_cols)).alias("record")))
    quarantine = parts[0]
    for part in parts[1:]:
        quarantine = quarantine.unionByName(part)
    return quarantine.withColumn("quarantined_at", F.current_timestamp())


# --- Quality log: one row per rule or fix with its row count (same rows as 02_silver section 6) ---------------------
def count_of(df, table, rule):
    return df.agg(F.count(F.lit(1)).alias("row_count")).select(F.lit(table).alias("table_name"), F.lit(rule).alias("rule"), "row_count")


def removed(before, after, table, rule):
    # Rows removed by a step = rows before minus rows after, as a one-row DataFrame (no actions inside a pipeline definition)
    b = before.agg(F.count(F.lit(1)).alias("b"))
    a = after.agg(F.count(F.lit(1)).alias("a"))
    return b.crossJoin(a).select(F.lit(table).alias("table_name"), F.lit(rule).alias("rule"), (F.col("b") - F.col("a")).alias("row_count"))


@dp.materialized_view(name="silver_quality_log", comment="One row per quality rule or fix with its row count")
def silver_quality_log():
    products = spark.read.table("silver_products")
    waste = spark.read.table("waste_typed")
    parts = [
        count_of(products.where("shelf_life_imputed").select("item_id").distinct(), "products", "null_shelf_life_imputed"),
        count_of(products.groupBy("item_id").count().where("count > 1"), "products", "price_versions_kept"),
        count_of(waste.where(F.to_date(F.date_trunc("week", "event_ts_utc")) != F.col("week_start")),
                 "ops_waste_log", "utc_bucketing_would_shift_week"),
        count_of(waste.where("is_late_sync"), "ops_waste_log", "late_synced_kept_in_event_week"),
        removed(spark.read.table("sales_typed"), spark.read.table("sales_deduped"), "pos_sales_weekly", "exact_resend_removed"),
        removed(spark.read.table("snapshots_typed"), spark.read.table("snapshots_deduped"), "pos_stock_snapshots", "exact_resend_removed"),
        removed(spark.read.table("sales_valid"), spark.read.table("silver_sales_weekly"), "pos_sales_weekly", "superseded_valid_versions"),
        removed(spark.read.table("snapshots_valid"), spark.read.table("silver_stock_snapshots"), "pos_stock_snapshots",
                "superseded_valid_versions"),
        # SPARK: explode before groupBy (explode can't sit inside the aggregation)
        spark.read.table("silver_quarantine").select("source_table", F.explode("failed_rules").alias("rule"))
             .groupBy("source_table", "rule").count()
             .select(F.col("source_table").alias("table_name"), F.concat(F.lit("quarantined: "), "rule").alias("rule"),
                     F.col("count").alias("row_count")),
    ]
    log = parts[0]
    for part in parts[1:]:
        log = log.unionByName(part)
    return log.withColumn("checked_at", F.current_timestamp())


# --- Gold: the weekly store-item data product (store x product x week) ---------------------------------------------
AS_OF = spark.conf.get("app.as_of", "2026-10-21")  # same fixed as-of date as the generator: the current week is partial

# Explicit schema so Unity Catalog can attach the row filter and the column mask (both need the functions from 00b_governance)
GOLD_WEEK_SCHEMA = """
  store_id STRING, item_id STRING, week_num INT, store_name STRING, region STRING, store_size STRING,
  product_name STRING, category STRING, perishable BOOLEAN, shelf_life_days INT, case_pack INT,
  unit_cost DOUBLE MASK workspace.grocery_intel.gov_cost_mask, unit_price DOUBLE, week_start DATE,
  period_days INT COMMENT 'Days covered by data: 7, fewer for the current partial week',
  is_partial_week BOOLEAN, is_promo BOOLEAN, discount_pct INT, units_sold INT, days_in_stock INT, stockout_days INT,
  availability_pct DOUBLE COMMENT 'Share of days in the period the item was in stock',
  est_demand DOUBLE COMMENT 'Estimated weekly demand = units_sold / days_in_stock * 7 (corrects for stockouts)',
  revenue DOUBLE, on_hand INT, on_order INT, received INT, waste_units BIGINT, expired_units BIGINT, damaged_units BIGINT,
  waste_value DOUBLE,
  is_spike_week BOOLEAN COMMENT 'One-off spike (robust MAD rule): kept, but excluded from forecasts'
"""
GOLD_WEEK_COLUMNS = [c.split()[0] for c in re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", GOLD_WEEK_SCHEMA) if c.strip()]  # commas outside quotes


@dp.temporary_view(name="store_product_week_base")
def store_product_week_base():
    # Weekly store-item facts from the three sources joined on the conformed keys
    # EDGE: price changed mid-period: the join picks the price version valid in each week (revenue and waste $ use it)
    # TRAP: demand from in-stock days (est_demand), not raw sales: raw sales hide demand lost to empty shelves
    period = f"LEAST(7, datediff(DATE'{AS_OF}', s.week_start) + 1)"
    return spark.sql(f"""
    WITH waste_week AS (
      SELECT store_id, item_id, week_start,
             SUM(waste_units) AS waste_units,
             SUM(CASE WHEN reason = 'expired' THEN waste_units ELSE 0 END) AS expired_units,
             SUM(CASE WHEN reason = 'damaged' THEN waste_units ELSE 0 END) AS damaged_units
      FROM silver_waste_events GROUP BY store_id, item_id, week_start),
    first_week AS (SELECT MIN(week_start) AS first_week FROM silver_sales_weekly)
    SELECT s.store_id, s.item_id, CAST(datediff(s.week_start, fw.first_week) / 7 AS INT) + 1 AS week_num,
           st.store_name, st.region, st.store_size, p.product_name, p.category, p.perishable,
           p.shelf_life_days, p.case_pack, p.unit_cost, p.unit_price, s.week_start,
           {period} AS period_days, {period} < 7 AS is_partial_week,
           pr.discount_pct IS NOT NULL AS is_promo, COALESCE(pr.discount_pct, 0) AS discount_pct,
           s.units_sold, s.days_in_stock, {period} - s.days_in_stock AS stockout_days,
           ROUND(s.days_in_stock / {period}, 4) AS availability_pct,
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
    LEFT JOIN waste_week w ON w.store_id = s.store_id AND w.item_id = s.item_id AND w.week_start = s.week_start""")


def spike_weeks(g):
    # EDGE: legitimate outlier: a week more than 3.5 robust deviations (median + MAD) above the store-item's own trend line
    import numpy as np
    import pandas as pd
    out = pd.DataFrame({"store_id": pd.Series(dtype=str), "item_id": pd.Series(dtype=str), "week_num": pd.Series(dtype="int32")})
    if len(g) < 6:
        return out
    x, y = g.week_num.to_numpy(float), g.est_demand.to_numpy(float)
    slope, intercept = np.polyfit(x, y, 1)
    resid = y - (slope * x + intercept)
    med = np.median(resid)
    mad = 1.4826 * np.median(np.abs(resid - med))
    limit = 3.5 * max(mad, np.sqrt(y.mean()))  # floor at Poisson noise so very stable items don't flag every wiggle
    hits = g.week_num[(resid - med) > limit].astype("int32")
    return pd.DataFrame({"store_id": g.store_id.iloc[0], "item_id": g.item_id.iloc[0], "week_num": hits.to_numpy()})


@dp.materialized_view(
    name="gold_store_product_week", schema=GOLD_WEEK_SCHEMA,
    row_filter="ROW FILTER workspace.grocery_intel.gov_store_filter ON (store_id)",
    comment="Grain: store x product x week. Sales, stock, stockouts and waste, priced with the version valid that week.")
def gold_store_product_week():
    base = spark.read.table("store_product_week_base")
    # SPARK: applyInPandas runs the spike scan per store-item in parallel (a shuffle by store_id, item_id), not on the driver
    spikes = (base.where("NOT is_partial_week AND NOT is_promo AND est_demand IS NOT NULL")
              .select("store_id", "item_id", "week_num", "est_demand")
              .groupBy("store_id", "item_id").applyInPandas(spike_weeks, "store_id STRING, item_id STRING, week_num INT")
              .withColumn("is_spike_week", F.lit(True)))
    return (base.join(spikes, ["store_id", "item_id", "week_num"], "left")
            .withColumn("is_spike_week", F.coalesce("is_spike_week", F.lit(False)))
            .select(*GOLD_WEEK_COLUMNS))
