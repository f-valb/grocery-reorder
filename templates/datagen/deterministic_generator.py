"""Deterministic synthetic event generator (pattern from mock_pos_api/generator.py).

Why each property matters:
* Seed per (entity, minute): the same window always yields the same rows, so a retried pull is
  byte-identical (idempotency tests become possible).
* Late updates as a function of wall-clock time (status flips N minutes after the event): exercises
  "latest version wins" in silver.
* Deterministic defects (~0.3%) and in-response duplicates (~0.5%): quarantine + dedupe have work to do.
* Volume follows a local-time curve normalised to mean 1.0: peaks exist and totals are predictable.
* Entity behaviour (e.g. customer visit rate, churn date) comes from SHARED profiles
  (shared_profiles.py) that the ML history seed also uses. Without that, live data contradicts the
  history and ML labels are meaningless (this cost two full rebuilds once).
"""

from __future__ import annotations

import hashlib
import math
from functools import lru_cache

import numpy as np


def seed(*parts) -> int:
    return int.from_bytes(hashlib.sha256("|".join(map(str, parts)).encode()).digest()[:8], "little")


def _curve(h: float) -> float:
    return 0.15 + 0.9 * math.exp(-(((h - 12.5) / 1.3) ** 2)) + 1.1 * math.exp(-(((h - 19) / 1.8) ** 2))


_NORM = sum(_curve(m / 60) for m in range(1440)) / 1440


def volume(total_per_minute: int, local_hour: float) -> int:
    return max(1, int(total_per_minute * _curve(local_hour) / _NORM))


@lru_cache(maxsize=512)
def minute_rows(partition: str, minute_epoch: int, total_per_minute: int, local_hour: float) -> tuple:
    """All base rows for one (partition, minute). Immutable -> cacheable -> deterministic."""
    rng = np.random.default_rng(seed("minute", partition, minute_epoch))
    n = volume(total_per_minute, local_hour)
    secs = np.sort(rng.uniform(0, 60, n))
    late_update = rng.random(n) < 0.02
    update_after = rng.integers(180, 1200, n)
    defect = rng.random(n) < 0.003
    dup = rng.random(n) < 0.005
    rows = []
    for i in range(n):
        ts = minute_epoch + float(secs[i])
        fields = {"id": f"{partition}{minute_epoch // 60:09d}{i:05d}", "ts": ts, "amount": f"{rng.uniform(1, 200):.2f}"}
        if defect[i]:
            fields["amount"] = "-" + fields["amount"]  # TODO: one defect type per quality rule
        rows.append((ts, fields, ts + float(update_after[i]) if late_update[i] else None, bool(dup[i])))
    return tuple(rows)


def render(rows: tuple, now_epoch: float) -> list[str]:
    out = []
    for ts, f, update_at, dup in rows:
        status, seq = ("VOID", 2) if update_at is not None and now_epoch >= update_at else ("OK", 1)
        line = f"{f['id']}|{ts:.0f}|{f['amount']}|{status}|{seq}"
        out += [line, line] if dup else [line]
    return out
