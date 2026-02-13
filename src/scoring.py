"""Quality scoring based on Bayesian average and Wilson lower bound."""
from __future__ import annotations

import math


def bayesian_average(rating: float, v: int, c: float, m: int) -> float:
    if v <= 0:
        return c
    return (v / (v + m)) * rating + (m / (v + m)) * c


def wilson_lower_bound(rating: float, v: int, z: float = 1.96) -> float:
    if v <= 0:
        return 0.0
    p = rating / 5.0
    denom = 1 + (z ** 2) / v
    center = p + (z ** 2) / (2 * v)
    margin = z * math.sqrt((p * (1 - p) + (z ** 2) / (4 * v)) / v)
    return max(0.0, (center - margin) / denom)


def quality_score(rating: float, v: int, c: float, m: int) -> dict:
    bayes = bayesian_average(rating, v, c, m)
    wilson = wilson_lower_bound(rating, v)
    quality_bayes = 20 * bayes
    quality_wilson = 100 * wilson
    quality = 0.75 * quality_bayes + 0.25 * quality_wilson
    return {
        "quality_bayes": quality_bayes,
        "quality_wilson": quality_wilson,
        "quality": quality,
    }
