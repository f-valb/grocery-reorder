# Decisions and trade-offs

- Workspace profile `food` (DEFAULT is out of credits).
- Raw data lands as CSV in a UC Volume; bronze keeps every column as a string so bad values survive to silver, where they are cast with try_cast and quarantined.
- Weekly grain everywhere (12 weeks, the last is partial: data through Wednesday). Daily data would be better for stockouts but is not in the brief.
- Synthetic data comes from a stock simulation (on hand + received − sold − waste), so stockouts censor sales the way they do in a real store.
- Silver dedupes snapshots before validating, so a corrected re-extract wins (one planted negative was fixed this way: 40 planted, 39 quarantined).
- Waste is checked against stock available that week (prior on hand + received), not received alone: waste of older stock is legitimate. Week 1 has no prior snapshot, so on hand + received stands in for opening stock.
- Missing shelf life is filled with the category median and flagged rather than quarantined: dropping a product would drop its sales too.
- Quarantined rows keep the full record as JSON in one `silver_quarantine` table; `silver_quality_log` holds per-rule counts.
- Forecast = 4-week moving average of estimated demand + 8-week linear trend; range ±1.28σ of the trend residuals (floored at √forecast). Same function feeds the backtest, so the backtest tests the real code.
- Safety stock z = 1.65 for non-perishables (95% target), 0.84 for perishables: perishables trade a little availability for much less waste.
- Order uses stock expected at arrival (on hand − rest of this week's forecast), because the order lands next week.
- Shelf-life cap as specified: perishables ordered only for demand that sells within shelf life. Backtest shows the cost: for items with ≤7 days shelf life, weekly delivery + cap cuts waste $13.9k → $0.4k but adds $7.2k lost margin. Net still positive; the real fix is 2 deliveries a week for those items.
- Backtest replays the recommendation against estimated demand, assuming the supplier fills the full order (the popular items are supplier-constrained, so their stockout gain is an upper bound). The simulated expiry rule is stricter than reality for long-life perishables, so their simulated waste is a high estimate.
- Promo weeks are excluded from the baseline and from the backtest comparison; a production system would add the planned promo lift on top.
