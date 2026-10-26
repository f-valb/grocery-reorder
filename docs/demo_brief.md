# Demo brief: grocery stock, waste and reorder

**Problem:** 20 stores run out of popular items while throwing away fresh produce. Managers need to see sales, stock and waste, and get next week's order quantities.

**Built:** synthetic data from a stock simulation → bronze (raw strings) → silver (typed, deduped, quarantined) → gold data product (performance, forecast, reorder, backtest, quality checks) → demo notebook with charts → AI/BI dashboard filterable by store.
Workspace `food`, schema `workspace.grocery_reorder`. Dashboard: https://dbc-0a174280-b54a.cloud.databricks.com/dashboardsv3/01f1d1330d101192a42648ba36cd6e8e/published

## Row counts
| Table | Bronze | Silver | Gold |
|---|---|---|---|
| sales_weekly | 9,630 | 9,600 | `gold_store_product_week` 9,600 |
| stock_snapshots | 9,720 | 9,561 | |
| waste | 6,545 | 6,500 | |
| stores / products / promotions | 20 / 40 / 12 | same | |
| quarantine | | 114 rows | |
| forecast / reorder | | | 800 / 800 (store × product) |
| backtest | | | 1,580 store-product-weeks |

**Silver rules:** unknown_product (30 sales rows, 20 waste rows), waste_gt_available 25, negative_on_hand 39 (40 planted, 1 fixed by a later re-extract), duplicate snapshots removed 120, null shelf life filled 4.

## Key numbers (gold)
- Availability over the last 4 full weeks: 97.7% chain-wide; waste $34.7k at cost.
- Forecast error (WAPE, last 2 weeks): **12.3%**, vs 15.2% for a naive average of raw sales.
- Backtest, recommendation vs actual: waste **$16.8k → $5.2k**, lost margin $2.8k → $7.9k, net **≈ $6.5k better over 2 weeks**. Non-perishable stockout days 183 → 1.

## Planted signals recovered
| Signal | Recovered | Evidence |
|---|---|---|
| Popular items with frequent stockouts | yes | Eggs, Pasta, Sparkling Water, Coffee in stock 82–84% of days vs ≥99.7% for others; true demand ~22% above sales |
| Over-ordered fresh produce, high waste | yes | Strawberries 46%, Salad Mix 42%, Sourdough 41% of received wasted (next: 27%) |
| One store over-orders | yes | S07 waste $14.3k = 2.17× the store average; receives 1.22× its demand vs 1.01× elsewhere |
| Promo weeks spike sales | yes | 1.81× lift on promoted products |
| One category trending up | yes | Plant-based +4.5%/week in the forecast trend (others between −0.8% and +0.1%) |

## Decisions and trade-offs
- Demand = units sold / days in stock × 7: sales are censored by empty shelves.
- Forecast = 4-week moving average + 8-week trend, no ML; range ±1.28σ (80%). Baseline excludes promo weeks, the partial week and zero-stock weeks.
- Safety stock 1.65σ for non-perishables (95% target), 0.84σ for perishables; order rounded up to case packs; perishables capped at what sells within shelf life.
- Full log: `docs/decisions.md`.

## Traps handled
- Censored demand (stockouts hide demand) → estimate from in-stock days.
- Partial current week → excluded from the baseline; availability uses its real day count.
- Promo spikes → excluded from the baseline so the week after isn't over-ordered.
- Order lands next week → subtract stock expected at arrival, not today's stock.
- Backtest leakage → each week forecast only from earlier weeks, using the same code as production.
- Duplicate snapshots → dedupe before validating, so corrections win; `greatest()` ignores nulls (caught a wrong quarantine count).

## Honest caveats
- The shelf-life cap with **weekly** delivery starves short-life items (≤7 days): waste $13.9k → $0.4k but lost margin $20 → $7.2k. Still a net win, but the right fix is 2 deliveries a week for those items.
- The backtest assumes the supplier fills the full order; the popular items are supplier-constrained, so their stockout gain is an upper bound.
- The simulated expiry rule is stricter than reality for long-life perishables (their simulated waste is a high estimate).

## Next steps
1. Twice-weekly ordering for perishables with ≤7 days shelf life; re-run the backtest.
2. Add planned promo lift to the forecast instead of only excluding promo weeks.
3. Supplier fill rate per item, so orders for constrained items are placed earlier or substituted.
4. Daily sales grain, a scheduled job, and an alert on the quality-check table.
