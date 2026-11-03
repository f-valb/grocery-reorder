# Data requirements: sales, stock and waste

Goal: tell store managers where they lose sales (stockouts) and money (waste), and what to order next week.
Every fact table joins on `store_id` + `product_id` + `week_start`.

| Entity | Grain | Key fields | Why it is needed |
|---|---|---|---|
| stores | one row per store | store_id, region, size (S/M/L) | Filter the dashboard by store; compare stores of the same size; find stores that over-order |
| products | one row per product | product_id, category, perishable, shelf_life_days, case_pack, unit_cost, unit_price | Case pack rounds orders; shelf life caps perishable orders; cost values waste; category shows trends |
| sales_weekly | store × product × week | units_sold, days_in_stock | Sales alone understate demand when the shelf is empty. Days in stock lets us estimate true demand: units_sold / days_in_stock × 7 |
| stock_snapshots | store × product × week (end of week, or the as-of date in the current week) | on_hand, on_order, received, snapshot_date, extracted_at | On hand and on order are subtracted from the order; received shows over-ordering; snapshot_date marks the partial week; extracted_at breaks ties between duplicate snapshots |
| waste | store × product × week (only weeks with waste) | waste_units, reason (expired/damaged) | Waste units × cost = money thrown away; expired vs damaged separates ordering problems from handling problems |
| promotions | product × week (chain-wide) | discount_pct | Promo weeks spike sales; they must be excluded from the forecast baseline so we don't over-order the week after |

## Source systems (mocked in this build)
| System | Tables | Keys it uses | Quirks silver must handle |
|---|---|---|---|
| Postgres POS + inventory | pos_sales_weekly, pos_stock_snapshots | integer store_id, item_id | re-sent rows (latest `updated_at` wins), UTC timestamps |
| MySQL ERP | erp_stores, erp_products | store_code `ST-07`, item_code `SKU-00012`, plus the POS ids | price/cost history with valid_from/valid_to, 0/1 flags |
| SaaS store-ops app | ops_waste_log | store *name*, item_code | everything as text, local-time timestamps, late sync |
| SaaS promo tool | promo_calendar | item_code | discount as "25%" text, date ranges |

The ERP is the master for stores and products: its crosswalk (pos_store_id, pos_item_id, store_name) maps every source onto one **store-item** (store × product), the unit every reorder decision is made on.

## Rules the data must satisfy (checked in silver)
- on_hand ≥ 0 (negative stock is a system error)
- one snapshot per store/product/week (keep the latest extract)
- product_id exists in products
- waste_units ≤ stock available that week (prior on hand + received)
- shelf_life_days is known for perishables (filled with the category median if missing, and flagged)

## Not in scope (would add next)
Daily sales (exact stockout timing), supplier lead times and fill rates per supplier, price changes, store-level promotions, transfers between stores.
