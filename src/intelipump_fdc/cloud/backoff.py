"""Bounded exponential backoff with jitter."""

from __future__ import annotations

import random


def compute_backoff_seconds(
    attempt: int,
    *,
    base: float = 1.0,
    maximum: float = 60.0,
    jitter: float = 0.2,
) -> float:
    """attempt is 1-based failure count."""
    exp = max(0, attempt - 1)
    delay = min(maximum, base * (2**exp))
    if jitter > 0:
        delay *= 1.0 + random.uniform(-jitter, jitter)
    return float(max(0.1, delay))
