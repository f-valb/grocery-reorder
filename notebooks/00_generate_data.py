# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Generate data
# MAGIC **Purpose:** synthetic grocery data for 20 stores × 40 products × 12 weeks, produced by a simple stock simulation so stockouts really censor sales.
# MAGIC **Inputs:** none (fixed seed 42).
# MAGIC **Outputs:** CSV files in `/Volumes/workspace/grocery_reorder/raw/`: stores, products, sales_weekly, stock_snapshots, waste, promotions.
# MAGIC
# MAGIC Planted signals: popular items that stock out, over-ordered produce, store S07 over-orders, promo spikes, Plant-based trending up.
# MAGIC Planted flaws: negative stock, duplicate snapshots, unknown product IDs, waste greater than available, null shelf life.
# MAGIC
# MAGIC Runs locally too: `OUT_DIR=/tmp/raw python3 notebooks/00_generate_data.py`.

# COMMAND ----------

# Set up: fixed seed, output folder (UC Volume on Databricks, OUT_DIR env var locally)
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
FIRST_WEEK = pd.Timestamp("2026-08-03")  # a Monday
PARTIAL_DAYS = 3  # RULE: the last week is partial, data through Wednesday

if spark is not None:
    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.grocery_reorder")
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.grocery_reorder.raw")
    OUT_DIR = "/Volumes/workspace/grocery_reorder/raw"
else:
    OUT_DIR = os.environ.get("OUT_DIR", "/tmp/grocery_raw")
os.makedirs(OUT_DIR, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared entity profiles
# MAGIC Stores and products are defined once; every fact table is generated from them, so joins work.

# COMMAND ----------

# Stores: 20 stores in 4 regions; size drives volume. S07 is the chronic over-orderer.
regions = ["North", "South", "East", "West"]
sizes = rng.choice(["S", "M", "L"], size=20, p=[0.3, 0.45, 0.25])
sizes[6] = "M"  # keep S07 a normal-sized store so its waste stands out
stores = pd.DataFrame({
    "store_id": [f"S{i:02d}" for i in range(1, 21)],
    "region": [regions[i % 4] for i in range(20)],
    "size": sizes,
})
SIZE_FACTOR = {"S": 0.7, "M": 1.0, "L": 1.4}
OVER_ORDER_STORE = "S07"  # SIGNAL: orders ~1.5x what it needs

# COMMAND ----------

# Products: 40 products in 6 categories with perishability, shelf life, case pack and price
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
        rows.append({
            "product_id": f"P{len(rows) + 1:03d}",
            "product_name": name,
            "category": cat,
            "perishable": perishable,
            "shelf_life_days": int(rng.integers(sl_lo, sl_hi + 1)),
            "case_pack": pack,
            "unit_cost": cost,
            "unit_price": round(cost * rng.uniform(1.3, 1.6), 2),
        })
products = pd.DataFrame(rows)
pid = dict(zip(products.product_name, products.product_id))

# Base weekly demand per product for a medium store
products["base_demand"] = rng.uniform(25, 70, len(products)).round(1)
POPULAR_STOCKOUT = [pid[n] for n in ["Pasta", "Coffee", "Sparkling Water", "Eggs 12pk"]]  # SIGNAL: supplier can't keep up
OVER_ORDERED_FRESH = [pid[n] for n in ["Strawberries", "Salad Mix", "Sourdough"]]  # SIGNAL: ordered ~1.6x demand
TRENDING_CATEGORY = "Plant-based"  # SIGNAL: demand +6% a week
products.loc[products.product_id.isin(POPULAR_STOCKOUT), "base_demand"] = [120, 110, 130, 115]

# COMMAND ----------

# Promotions: chain-wide product × week discounts (not on stockout items, so the lift is visible)
promo_candidates = [pid[n] for n in ["Cereal", "Cola", "Greek Yogurt", "Olive Oil", "Orange Juice", "Cheddar"]]
promo_rows = []
for i, p in enumerate(promo_candidates):
    for w in sorted(rng.choice(range(2, 12), size=2, replace=False)):
        promo_rows.append({"product_id": p, "week_start": (FIRST_WEEK + pd.Timedelta(weeks=int(w) - 1)).date(),
                           "discount_pct": int(rng.choice([15, 20, 25, 30]))})
promotions = pd.DataFrame(promo_rows)
promo_set = {(r.product_id, str(r.week_start)) for r in promotions.itertuples()}
PROMO_LIFT = 1.8  # SIGNAL: promo weeks sell ~1.8x

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stock simulation
# MAGIC Each week: start stock + received → sell min(stock, demand) → leftover perishables expire → order next week (1-week lead time).

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
        if p.product_id in OVER_ORDERED_FRESH:
            mult = 1.6
        if s.store_id == OVER_ORDER_STORE:
            mult *= 1.5
        on_hand = int(round(mean))
        incoming = int(np.ceil(mean * mult / p.case_pack) * p.case_pack)
        for w in range(1, N_WEEKS + 1):
            week_start = FIRST_WEEK + pd.Timedelta(weeks=w - 1)
            days = PARTIAL_DAYS if w == N_WEEKS else 7
            trend = 1.06 ** (w - 1) if p.category == TRENDING_CATEGORY else 1.0
            promo = (p.product_id, str(week_start.date())) in promo_set
            weekly_mean = mean * trend * (PROMO_LIFT if promo else 1.0)
            demand = rng.poisson(weekly_mean * days / 7)

            # Popular items: the supplier only fills ~70% of normal demand
            received = incoming
            if p.product_id in POPULAR_STOCKOUT:
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

            # Store's ordering policy for next week (what we are trying to improve)
            next_promo = (p.product_id, str((week_start + pd.Timedelta(weeks=1)).date())) in promo_set
            expected = mean * (1.06 ** w if p.category == TRENDING_CATEGORY else 1.0) * (PROMO_LIFT if next_promo else 1.0)
            incoming = int(max(0, np.ceil((expected * mult - on_hand) / p.case_pack)) * p.case_pack)
            on_order = incoming
            if w == N_WEEKS:  # current week: next order not placed yet, only some open orders
                on_order = p.case_pack if rng.random() < 0.25 else 0

            snap_date = week_start + pd.Timedelta(days=days - 1)
            sales.append((s.store_id, p.product_id, week_start.date(), int(sold), days_in_stock))
            snaps.append((s.store_id, p.product_id, week_start.date(), snap_date.date(), on_hand, on_order, received,
                          (snap_date + pd.Timedelta(hours=23)).isoformat()))
            for units, reason in [(expired, "expired"), (damaged, "damaged")]:
                if units > 0:
                    waste.append((s.store_id, p.product_id, week_start.date(), units, reason))

sales = pd.DataFrame(sales, columns=["store_id", "product_id", "week_start", "units_sold", "days_in_stock"])
snaps = pd.DataFrame(snaps, columns=["store_id", "product_id", "week_start", "snapshot_date", "on_hand", "on_order", "received", "extracted_at"])
waste = pd.DataFrame(waste, columns=["store_id", "product_id", "week_start", "waste_units", "reason"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Planted flaws (silver must clean these)

# COMMAND ----------

# QUALITY: plant flaws: negative stock, duplicate snapshots, unknown products, impossible waste, null shelf life
neg_idx = rng.choice(snaps.index, 40, replace=False)
snaps.loc[neg_idx, "on_hand"] = -rng.integers(1, 30, 40)

dupes = snaps.sample(120, random_state=SEED).copy()  # re-extracted rows: same key, later extract, corrected on_hand
dupes["extracted_at"] = (pd.to_datetime(dupes.extracted_at) + pd.Timedelta(hours=2)).dt.strftime("%Y-%m-%dT%H:%M:%S")
dupes["on_hand"] = dupes["on_hand"].abs()
snaps = pd.concat([snaps, dupes], ignore_index=True)

unknown_sales = sales.sample(30, random_state=1).copy()
unknown_sales["product_id"] = [f"P9{i:02d}" for i in rng.integers(1, 11, 30)]
sales = pd.concat([sales, unknown_sales], ignore_index=True)
unknown_waste = waste.sample(20, random_state=2).copy()
unknown_waste["product_id"] = [f"P9{i:02d}" for i in rng.integers(1, 11, 20)]
waste = pd.concat([waste, unknown_waste], ignore_index=True)

# Waste far above what the store could have had that week (on hand + received ≤ ~400)
bad_waste = waste[waste.reason == "expired"].sample(25, random_state=3).index
waste.loc[bad_waste, "waste_units"] = waste.loc[bad_waste, "waste_units"] * 10 + 500

null_sl = [pid[n] for n in ["Avocados", "Croissants", "Cream", "Tofu"]]
products_out = products.drop(columns=["base_demand"]).astype({"shelf_life_days": "Int64"})
products_out.loc[products_out.product_id.isin(null_sl), "shelf_life_days"] = pd.NA

# COMMAND ----------

# Write CSVs and print row counts (every table must stay under 10,000 rows)
tables = {"stores": stores, "products": products_out, "sales_weekly": sales.sample(frac=1, random_state=4),
          "stock_snapshots": snaps.sample(frac=1, random_state=5), "waste": waste, "promotions": promotions}
for name, df in tables.items():
    df.to_csv(f"{OUT_DIR}/{name}.csv", index=False)
    print(f"{name}: {len(df)} rows")
    assert len(df) < 10_000, name
