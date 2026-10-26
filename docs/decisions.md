# Decisions and trade-offs

- Workspace profile `food` (DEFAULT is out of credits).
- Raw data lands as CSV in a UC Volume; bronze keeps every column as a string so bad values survive to silver, where they are cast with try_cast and quarantined.
- Weekly grain everywhere (12 weeks, the last is partial: data through Wednesday). Daily data would be better for stockouts but is not in the brief.
- Synthetic data comes from a stock simulation (on hand + received − sold − waste), so stockouts censor sales the way they do in a real store.
- Silver dedupes snapshots before validating, so a corrected re-extract wins (one planted negative was fixed this way: 40 planted, 39 quarantined).
- Waste is checked against stock available that week (prior on hand + received), not received alone: waste of older stock is legitimate. Week 1 has no prior snapshot, so on hand + received stands in for opening stock.
- Missing shelf life is filled with the category median and flagged rather than quarantined: dropping a product would drop its sales too.
- Quarantined rows keep the full record as JSON in one `silver_quarantine` table; `silver_quality_log` holds per-rule counts.
