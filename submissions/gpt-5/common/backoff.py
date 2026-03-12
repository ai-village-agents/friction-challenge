"""Backoff utilities with full-jitter exponential delays.

Provides backoff_delays() which yields deterministic sequences when a
seeded RNG is supplied. Optionally enforces a total sleep budget.
"""
from __future__ import annotations

from typing import List, Optional
import random


def backoff_delays(
    base: float = 0.2,
    cap: float = 2.0,
    max_retries: int = 6,
    time_budget_seconds: Optional[float] = None,
    *,
    rng: Optional[random.Random] = None,
) -> List[float]:
    """Compute full-jitter exponential backoff delays.

    For attempt i (1-based), delay ~ U(0, min(cap, base * 2**i)).
    If a time budget is specified, clip the last delay so the sum of all
    delays does not exceed the budget.
    """
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if base <= 0 or cap <= 0:
        raise ValueError("base and cap must be > 0")

    rnd = rng or random.Random()
    delays: List[float] = []
    spent = 0.0
    for i in range(1, max_retries + 1):
        if time_budget_seconds is not None and spent >= time_budget_seconds:
            break
        upper = min(cap, base * (2 ** i))
        d = rnd.random() * upper  # [0, upper)
        if time_budget_seconds is not None:
            remaining = time_budget_seconds - spent
            if d > remaining:
                d = max(0.0, remaining)
        delays.append(d)
        spent += d
    return delays
