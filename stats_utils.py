"""Utility helpers shared across statistics generation modules."""

from __future__ import annotations

from typing import Iterable, Sequence


def weighted_percentile(values: Sequence[float] | Iterable[float],
                        weights: Sequence[float] | Iterable[float],
                        p: float) -> float:
    """Compute the weighted percentile *p* (0..100) without numpy dependencies."""

    if not 0.0 <= p <= 100.0:
        raise ValueError("percentile must be in the range 0..100")

    values = list(values)
    weights = list(weights)

    if not values:
        return 0.0

    if len(values) != len(weights):
        raise ValueError("values and weights must have the same length")

    if any(w < 0 for w in weights):
        raise ValueError("weights must be non-negative")

    pairs = sorted(zip(values, weights), key=lambda pair: pair[0])
    total = sum(w for _, w in pairs)

    if total <= 0:
        # fallback: ordinary median
        midpoint = len(pairs) // 2
        return pairs[midpoint][0]

    threshold = total * (p / 100.0)
    acc = 0.0

    for value, weight in pairs:
        acc += weight
        if acc >= threshold:
            return value

    return pairs[-1][0]


def weighted_median(values: Sequence[float] | Iterable[float],
                    weights: Sequence[float] | Iterable[float]) -> float:
    """Return the weighted median for *values* using *weights*."""

    return weighted_percentile(values, weights, 50)

