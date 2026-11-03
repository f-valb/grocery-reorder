# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Generate data (mocked source systems)
# MAGIC **Purpose:** synthetic data for 20 stores × 41 products × 12 weeks, from a stock simulation (stockouts really censor sales), written in the shape of three source systems.
# MAGIC **Inputs:** none (fixed seed 42, fixed as-of date 2026-11-04).
# MAGIC **Outputs:** one parquet file per source table in `/Volumes/workspace/grocery_intel/raw/`:
# MAGIC | Source system | Tables | Look and feel |
# MAGIC |---|---|---|
# MAGIC | Postgres POS + inventory | `pos_sales_weekly`, `pos_stock_snapshots` | integer store and item IDs, UTC `updated_at` |
# MAGIC | MySQL ERP master data | `erp_stores`, `erp_products` | codes `ST-07` / `SKU-00012`, price history with valid dates |
# MAGIC | SaaS store-ops app + promo tool | `ops_waste_log`, `promo_calendar` | camelCase, store names, local-time timestamps, everything as text |
# MAGIC
# MAGIC Runs locally too: `OUT_DIR=/tmp/raw ~/.local/venvs/pyspark/bin/python notebooks/00_generate_data.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Config & seed

# COMMAND ----------

# Fixed seed and fixed as-of date, so every rerun produces identical data
import os
import numpy as np
import pandas as pd

try:
    spark  # noqa: F821
except NameError:
    spark = None

SEED = 42
rng = np.random.default_rng(SEED)
N_WEEKS = 12
FIRST_WEEK = pd.Timestamp("2026-08-17")  # a Monday
AS_OF = pd.Timestamp("2026-11-04")       # RULE: data runs through Wednesday of week 12 (a partial week)
PARTIAL_DAYS = (AS_OF - (FIRST_WEEK + pd.Timedelta(weeks=N_WEEKS - 1))).days + 1

if spark is not None:
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.grocery_intel")
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.grocery_intel.raw")
    OUT_DIR = "/Volumes/workspace/grocery_intel/raw"
else:
    OUT_DIR = os.environ.get("OUT_DIR", "/tmp/grocery_intel_raw")
os.makedirs(OUT_DIR, exist_ok=True)


def week_start(w):
    return FIRST_WEEK + pd.Timedelta(weeks=w - 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Entity profiles
# MAGIC One list of stores and one of products. Every source table is generated from them, so the silver crosswalk can join everything back to one store-item.

# COMMAND ----------

# Stores: 20 stores in 4 regions with a local timezone; size drives volume
REGION_TZ = {"North": "America/Chicago", "South": "America/New_York", "East": "America/New_York", "West": "America/Los_Angeles"}
neighbourhoods = ["Riverside", "Oak Park", "Harbor", "Midtown", "Lakeview", "Hillcrest", "Westgate", "Elm Street", "Fairview",
                  "Kingsway", "Northgate", "Bayside", "Maple Court", "Old Town", "Southbank", "Parkside", "Brookfield",
                  "Sunset", "Greenwood", "Market Square"]
sizes = rng.choice(["S", "M", "L"], size=20, p=[0.3, 0.45, 0.25])
sizes[6] = "M"  # keep store 7 a normal-sized store so its waste stands out
stores = pd.DataFrame({
    "store_num": range(1, 21),
    "region": [list(REGION_TZ)[i % 4] for i in range(20)],
    "size": sizes,
})
stores["store_code"] = stores.store_num.map(lambda n: f"ST-{n:02d}")
stores["store_name"] = [f"Store {n:02d} - {neighbourhoods[n - 1]}" for n in stores.store_num]
stores["timezone"] = stores.region.map(REGION_TZ)
SIZE_FACTOR = {"S": 0.7, "M": 1.0, "L": 1.4}

# COMMAND ----------

# Products: 40 regular products in 6 categories plus 1 new product launched in week 9
catalog = [
    # category, perishable, shelf life range, case pack, cost range, names
    ("Produce", True, (4, 7), 12, (0.6, 2.5), ["Bananas", "Strawberries", "Salad Mix", "Tomatoes", "Avocados", "Apples", "Spinach", "Grapes"]),
    ("Dairy", True, (10, 14), 6, (0.8, 3.5), ["Whole Milk", "Greek Yogurt", "Cheddar", "Butter", "Eggs 12pk", "Cream"]),
    ("Bakery", True, (3, 5), 8, (1.0, 3.0), ["Sourdough", "Bagels", "Croissants", "Baguette", "Muffins"]),
    ("Dry Goods", False, (365, 365), 24, (0.8, 4.0), ["Pasta", "Rice", "Cereal", "Peanut Butter", "Canned Tomatoes", "Olive Oil", "Flour", "Coffee"]),
    ("Beverages", False, (365, 365), 24, (0.5, 2.5), ["Sparkling Water", "Cola", "Orange Juice", "Iced Tea", "Energy Drink", "Lemonade", "Kombucha"]),
    ("Plant-based", True, (21, 30), 6, (1.5, 4.5), ["Oat Milk", "Tofu", "Veggie Burger", "Almond Yogurt", "Plant Mince", "Soy Milk"]),
]
rows = []
for cat, perishable, (sl_lo, sl_hi), pack, (c_lo, c_hi), names in catalog:
    for name in names:
        cost = round(rng.uniform(c_lo, c_hi), 2)
        rows.append({"item_num": len(rows) + 1, "product_name": name, "category": cat, "perishable": perishable,
                     "shelf_life_days": int(rng.integers(sl_lo, sl_hi + 1)), "case_pack": pack,
                     "unit_cost": cost, "unit_price": round(cost * rng.uniform(1.3, 1.6), 2), "launch_week": 1})
NEW_ITEM_LAUNCH_WEEK = 9  # EDGE: new product with only 3 full weeks of history (small sample)
rows.append({"item_num": 41, "product_name": "Protein Bar", "category": "Dry Goods", "perishable": False,
             "shelf_life_days": 365, "case_pack": 24, "unit_cost": 1.40, "unit_price": 2.10, "launch_week": NEW_ITEM_LAUNCH_WEEK})
products = pd.DataFrame(rows)
products["item_code"] = products.item_num.map(lambda n: f"SKU-{n:05d}")
products["base_demand"] = rng.uniform(25, 70, len(products)).round(1)
by_name = dict(zip(products.product_name, products.item_num))

# COMMAND ----------

# SIGNAL: behaviour profiles for the five business signals the gold layer must recover
POPULAR_STOCKOUT = [by_name[n] for n in ["Pasta", "Coffee", "Sparkling Water", "Eggs 12pk"]]  # supplier fills only ~70%
OVER_ORDERED_FRESH = [by_name[n] for n in ["Strawberries", "Salad Mix", "Sourdough"]]           # ordered ~1.6x demand
OVER_ORDER_STORE = 7                                                                          # orders ~1.5x on everything
TRENDING_CATEGORY, TREND_PER_WEEK = "Plant-based", 1.06                                       # demand +6% a week
PROMO_LIFT = 1.8                                                                              # promo weeks sell ~1.8x
products.loc[products.item_num.isin(POPULAR_STOCKOUT), "base_demand"] = [120, 110, 130, 115]

# EDGE: a legitimate one-off spike (local festival): Beverages at store 12 sell ~3x in week 6. Keep it, but out of the baseline.
FESTIVAL_STORE, FESTIVAL_WEEK, FESTIVAL_CATEGORY, FESTIVAL_LIFT = 12, 6, "Beverages", 3.0

# EDGE: cost and price change in week 7 for 5 products (ERP keeps both versions with valid dates)
PRICE_CHANGE_WEEK = 7
price_change_items = [by_name[n] for n in ["Coffee", "Olive Oil", "Butter", "Avocados", "Oat Milk"]]

# Promotions: chain-wide product × week discounts, not on stockout items so the lift is visible
promo_candidates = [by_name[n] for n in ["Cereal", "Cola", "Greek Yogurt", "Olive Oil", "Orange Juice", "Cheddar"]]
promo_weeks = {}
for item in promo_candidates:
    for w in sorted(rng.choice(range(2, 12), size=2, replace=False)):
        promo_weeks[(item, int(w))] = int(rng.choice([15, 20, 25, 30]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 Fact tables
# MAGIC Each week: start stock + received → sell min(stock, demand) → leftover perishables expire → the store orders next week (1-week lead time).
# MAGIC The planted signals are profile settings (section 2) used as multipliers inside this loop; section 4 checks them.

# COMMAND ----------

# Simulate store × product × week: demand, sales capped by stock, waste, and the store's own ordering
def expiry_share(shelf_life):
    # Share of leftover stock that expires by the end of the week
    if shelf_life <= 7:
        return 0.85
    if shelf_life <= 14:
        return 0.35
    if shelf_life <= 60:
        return 0.08
    return 0.0


sales, snaps, waste = [], [], []
for s in stores.itertuples():
    for p in products.itertuples():
        mean = p.base_demand * SIZE_FACTOR[s.size] * rng.uniform(0.8, 1.2)
        mult = 1.15  # normal policy: order up to 115% of expected demand
        if p.item_num in OVER_ORDERED_FRESH:
            mult = 1.6
        if s.store_num == OVER_ORDER_STORE:
            mult *= 1.5
        on_hand = int(round(mean))
        incoming = int(np.ceil(mean * mult / p.case_pack) * p.case_pack)
        for w in range(p.launch_week, N_WEEKS + 1):
            days = PARTIAL_DAYS if w == N_WEEKS else 7
            trend = TREND_PER_WEEK ** (w - 1) if p.category == TRENDING_CATEGORY else 1.0
            promo = (p.item_num, w) in promo_weeks
            festival = (s.store_num, w, p.category) == (FESTIVAL_STORE, FESTIVAL_WEEK, FESTIVAL_CATEGORY)
            weekly_mean = mean * trend * (PROMO_LIFT if promo else 1.0) * (FESTIVAL_LIFT if festival else 1.0)
            demand = rng.poisson(weekly_mean * days / 7)

            received = incoming
            if p.item_num in POPULAR_STOCKOUT:  # the supplier only fills ~70% of normal demand
                received = min(received, int(np.floor(mean * 0.7 / p.case_pack) * p.case_pack) + p.case_pack * int(rng.integers(0, 2)))
            available = on_hand + received
            sold = min(available, demand)
            if demand > available:
                days_in_stock = int(np.ceil(days * available / demand)) if available > 0 else 0
            else:
                days_in_stock = days
            leftover = available - sold
            expired = int(round(leftover * expiry_share(p.shelf_life_days) * days / 7)) if p.perishable else 0
            damaged = int(min(leftover - expired, rng.poisson(0.6))) if rng.random() < 0.3 else 0
            on_hand = leftover - expired - damaged

            # Store's ordering policy for next week (what the recommendation should improve on)
            next_promo = (p.item_num, w + 1) in promo_weeks
            expected = mean * (TREND_PER_WEEK ** w if p.category == TRENDING_CATEGORY else 1.0) * (PROMO_LIFT if next_promo else 1.0)
            incoming = int(max(0, np.ceil((expected * mult - on_hand) / p.case_pack)) * p.case_pack)
            on_order = incoming if w < N_WEEKS else (p.case_pack if rng.random() < 0.25 else 0)

            snap_date = week_start(w) + pd.Timedelta(days=days - 1)
            sales.append((s.store_num, p.item_num, w, int(sold), days_in_stock, snap_date))
            snaps.append((s.store_num, p.item_num, w, snap_date, on_hand, on_order, received))
            for units, reason in [(expired, "EXPIRED"), (damaged, "DAMAGED")]:
                if units > 0:
                    waste.append((s.store_num, p.item_num, w, units, reason, days))

sales = pd.DataFrame(sales, columns=["store_id", "item_id", "week", "units_sold", "days_in_stock", "snap_date"])
snaps = pd.DataFrame(snaps, columns=["store_id", "item_id", "week", "snap_date", "on_hand", "on_order", "received"])
waste = pd.DataFrame(waste, columns=["store_num", "item_num", "week", "units", "reason", "days"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 Planted signals
# MAGIC Check each signal on the true simulated data before any flaws are added (the expected ranges used in gold come from here).

# COMMAND ----------

# SIGNAL: measure each planted signal on the true data
truth = sales.merge(products[["item_num", "product_name", "category"]], left_on="item_id", right_on="item_num")
full = truth[truth.week < N_WEEKS]
avail = (full.groupby("product_name").days_in_stock.mean() / 7).sort_values()
waste_rate = (waste[waste.reason == "EXPIRED"].groupby("item_num").units.sum() / snaps.groupby("item_id").received.sum()).dropna()
store_waste = waste.merge(products[["item_num", "unit_cost"]], on="item_num").assign(usd=lambda d: d.units * d.unit_cost) \
    .groupby("store_num").usd.sum()
promo_flag = full.apply(lambda r: (r.item_id, r.week) in promo_weeks, axis=1)
promo_items = full.item_id.isin(promo_candidates)
lift = full[promo_items & promo_flag].units_sold.mean() / full[promo_items & ~promo_flag].units_sold.mean()
cat_week = full[~promo_flag].groupby(["category", "week"]).units_sold.sum().unstack(0)
growth = cat_week.iloc[-1] / cat_week.iloc[0] - 1
signal_check = {
    "stockout items availability": round(avail[[products.product_name[i - 1] for i in POPULAR_STOCKOUT]].mean(), 3),
    "other items availability": round(avail.drop([products.product_name[i - 1] for i in POPULAR_STOCKOUT]).mean(), 3),
    "over-ordered fresh expiry rate": round(waste_rate[OVER_ORDERED_FRESH].mean(), 3),
    "other perishables expiry rate": round(waste_rate.drop(OVER_ORDERED_FRESH)[lambda r: r > 0].mean(), 3),
    "store 7 waste vs average store": round(store_waste[OVER_ORDER_STORE] / store_waste.mean(), 2),
    "promo lift": round(lift, 2),
    "Plant-based growth minus peers": round(growth[TRENDING_CATEGORY] - growth.drop(TRENDING_CATEGORY).mean(), 2),
}
for k, v in signal_check.items():
    print(f"{k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 Planted flaws and edge cases
# MAGIC Reported rows are built from the true arrays first; flaws are added as extra corrupted copies, never as edits to true rows.

# COMMAND ----------

# Build the reported POS rows (Postgres: integer IDs, UTC timestamps as text)
def utc_text(ts):
    return pd.to_datetime(ts).dt.strftime("%Y-%m-%d %H:%M:%S")


pos_sales = sales.assign(week_start=sales.week.map(lambda w: str(week_start(w).date())),
                         updated_at=utc_text(sales.snap_date + pd.Timedelta(hours=23)))
pos_sales = pos_sales[["store_id", "item_id", "week_start", "units_sold", "days_in_stock", "updated_at"]]
pos_snaps = snaps.assign(week_start=snaps.week.map(lambda w: str(week_start(w).date())),
                         snapshot_date=snaps.snap_date.dt.strftime("%Y-%m-%d"),
                         updated_at=utc_text(snaps.snap_date + pd.Timedelta(hours=23)))
pos_snaps = pos_snaps[["store_id", "item_id", "week_start", "snapshot_date", "on_hand", "on_order", "received", "updated_at"]]

# COMMAND ----------

# QUALITY: generic flaws: unknown items, a typo, negative stock, waste above available, null shelf life
unknown_sales = pos_sales.sample(30, random_state=1).assign(item_id=lambda d: 900 + rng.integers(1, 11, len(d)))
typo_sales = pos_sales.sample(1, random_state=2).assign(  # same key, later update, impossible quantity
    units_sold=99999, updated_at=lambda d: utc_text(pd.to_datetime(d.updated_at) + pd.Timedelta(hours=1)))
bad_days = pos_sales.sample(10, random_state=3).assign(
    days_in_stock=9, updated_at=lambda d: utc_text(pd.to_datetime(d.updated_at) + pd.Timedelta(hours=1)))
negative_stock = pos_snaps.sample(40, random_state=4).assign(  # a buggy resend with negative on hand
    on_hand=lambda d: -rng.integers(1, 30, len(d)), updated_at=lambda d: utc_text(pd.to_datetime(d.updated_at) + pd.Timedelta(hours=1)))

# EDGE: late-arriving snapshot re-sends: same values, updated 2 days later (dedupe must keep one)
late_snaps = pos_snaps.sample(120, random_state=5).assign(
    updated_at=lambda d: utc_text(pd.to_datetime(d.updated_at) + pd.Timedelta(days=2)))

pos_sales_out = pd.concat([pos_sales, unknown_sales, typo_sales, bad_days], ignore_index=True)
pos_snaps_out = pd.concat([pos_snaps, negative_stock, late_snaps], ignore_index=True)

# COMMAND ----------

# Build the ERP master data (MySQL): store and product codes, 0/1 flags, price history with valid dates
erp_stores = stores.assign(pos_store_id=stores.store_num)[["store_code", "store_name", "region", "size", "timezone", "pos_store_id"]] \
    .rename(columns={"size": "size_band"})

change_from = str(week_start(PRICE_CHANGE_WEEK).date())
change_to = str((week_start(PRICE_CHANGE_WEEK) - pd.Timedelta(days=1)).date())
erp = products.assign(pos_item_id=products.item_num, is_perishable=products.perishable.astype(int),
                      valid_from=str(FIRST_WEEK.date()), valid_to=None)
before = erp[erp.item_num.isin(price_change_items)].assign(valid_to=change_to)
after = erp[erp.item_num.isin(price_change_items)].assign(
    valid_from=change_from, unit_cost=lambda d: (d.unit_cost * 1.12).round(2), unit_price=lambda d: (d.unit_price * 1.10).round(2))
erp_products = pd.concat([erp[~erp.item_num.isin(price_change_items)], before, after], ignore_index=True)
null_shelf_life = [by_name[n] for n in ["Avocados", "Croissants", "Cream", "Tofu"]]
erp_products["shelf_life_days"] = erp_products.shelf_life_days.astype("Int64")
erp_products.loc[erp_products.item_num.isin(null_shelf_life), "shelf_life_days"] = pd.NA
erp_products = erp_products[["item_code", "pos_item_id", "product_name", "category", "is_perishable", "shelf_life_days",
                             "case_pack", "unit_cost", "unit_price", "valid_from", "valid_to"]] \
    .rename(columns={"case_pack": "case_qty", "unit_price": "list_price"})

# COMMAND ----------

# Build the SaaS store-ops waste log: one logging event per store/item/week/reason, local time, store names, text values
store_lookup = stores.set_index("store_num")
item_lookup = products.set_index("item_num")
event_day = rng.integers(0, 7, len(waste)) % waste.days.to_numpy()  # partial week: only days with data
event_hour = rng.integers(8, 21, len(waste))
# EDGE: ~8% of events are logged late on Sunday evening (local), which is already Monday in UTC
sunday_night = (rng.random(len(waste)) < 0.08) & (waste.days.to_numpy() == 7)
event_day = np.where(sunday_night, 6, event_day)
event_hour = np.where(sunday_night, rng.integers(21, 24, len(waste)), event_hour)
logged_local = (waste.week.map(week_start) + pd.to_timedelta(event_day, unit="D") + pd.to_timedelta(event_hour, unit="h")
                + pd.to_timedelta(rng.integers(0, 60, len(waste)), unit="m"))
ops_waste = pd.DataFrame({
    "storeName": waste.store_num.map(store_lookup.store_name),
    "itemCode": waste.item_num.map(item_lookup.item_code),
    "loggedAt": logged_local.dt.strftime("%Y-%m-%dT%H:%M:%S"),
    "wasteQty": waste.units.astype(str),
    "reason": waste.reason,
    "syncedAt": (logged_local + pd.Timedelta(hours=8)).dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
})

# EDGE: late-arriving waste: ~80 week-11 logs only synced in week 12 (they belong to week 11 by event time)
late_waste = ops_waste.index[(waste.week == 11) & (rng.random(len(waste)) < 0.12)]
ops_waste.loc[late_waste, "syncedAt"] = (logged_local[late_waste] + pd.Timedelta(days=4)).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

# QUALITY: waste far above what the store could have had that week, and EDGE: orphan item codes not in the ERP
too_much = ops_waste[ops_waste.reason == "EXPIRED"].sample(25, random_state=6).index
ops_waste.loc[too_much, "wasteQty"] = (ops_waste.loc[too_much, "wasteQty"].astype(int) * 10 + 500).astype(str)
orphans = ops_waste.sample(20, random_state=7).assign(itemCode=lambda d: [f"SKU-09{i:03d}" for i in rng.integers(1, 50, len(d))])
ops_waste_out = pd.concat([ops_waste, orphans], ignore_index=True)

# COMMAND ----------

# Build the SaaS promo calendar: camelCase, discount as text with a % sign
promo_calendar = pd.DataFrame([
    {"promoId": f"PR-{i + 1:03d}", "productSku": item_lookup.loc[item, "item_code"],
     "startDate": str(week_start(w).date()), "endDate": str((week_start(w) + pd.Timedelta(days=6)).date()),
     "discountPct": f"{pct}%"}
    for i, ((item, w), pct) in enumerate(sorted(promo_weeks.items()))])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 Write + row counts

# COMMAND ----------

# Write each source table as parquet with explicit column types (the same types the bronze schemas expect)
RAW_TYPES = {
    "pos_sales_weekly": {"store_id": "int64", "item_id": "int64", "week_start": "string", "units_sold": "int64",
                         "days_in_stock": "int64", "updated_at": "string"},
    "pos_stock_snapshots": {"store_id": "int64", "item_id": "int64", "week_start": "string", "snapshot_date": "string",
                            "on_hand": "int64", "on_order": "int64", "received": "int64", "updated_at": "string"},
    "erp_stores": {"store_code": "string", "store_name": "string", "region": "string", "size_band": "string",
                   "timezone": "string", "pos_store_id": "int64"},
    "erp_products": {"item_code": "string", "pos_item_id": "int64", "product_name": "string", "category": "string",
                     "is_perishable": "int64", "shelf_life_days": "Int64", "case_qty": "int64", "unit_cost": "float64",
                     "list_price": "float64", "valid_from": "string", "valid_to": "string"},
    "ops_waste_log": {c: "string" for c in ["storeName", "itemCode", "loggedAt", "wasteQty", "reason", "syncedAt"]},
    "promo_calendar": {c: "string" for c in ["promoId", "productSku", "startDate", "endDate", "discountPct"]},
}
tables = {"pos_sales_weekly": pos_sales_out.sample(frac=1, random_state=8), "pos_stock_snapshots": pos_snaps_out.sample(frac=1, random_state=9),
          "erp_stores": erp_stores, "erp_products": erp_products, "ops_waste_log": ops_waste_out, "promo_calendar": promo_calendar}
for name, df in tables.items():
    df = df[list(RAW_TYPES[name])].astype(RAW_TYPES[name]).reset_index(drop=True)
    df.attrs = {}
    df.to_parquet(f"{OUT_DIR}/{name}.parquet", index=False)
    print(f"{name}: {len(df)} rows")
    assert len(df) < 10_000, name
