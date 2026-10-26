# Decisions and trade-offs

- Workspace profile `food` (DEFAULT is out of credits).
- Raw data lands as CSV in a UC Volume; bronze keeps every column as a string so bad values survive to silver, where they are cast with try_cast and quarantined.
- Weekly grain everywhere (12 weeks, the last is partial: data through Wednesday). Daily data would be better for stockouts but is not in the brief.
- Synthetic data comes from a stock simulation (on hand + received − sold − waste), so stockouts censor sales the way they do in a real store.
