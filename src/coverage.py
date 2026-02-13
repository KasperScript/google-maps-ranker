"""Coverage metrics and grid harvesting helpers."""
from __future__ import annotations

from typing import Dict, Iterable, List, Set, Tuple

from .geo import grid_points


def compute_uplift(u_hubs: int, u_union_total: int) -> float:
    base = max(1, u_hubs)
    return (u_union_total - u_hubs) / base


def unique_contributions(sets_by_key: Dict[str, Set[str]]) -> Dict[str, int]:
    keys = list(sets_by_key.keys())
    contributions: Dict[str, int] = {}
    for key in keys:
        others = set()
        for other_key in keys:
            if other_key == key:
                continue
            others.update(sets_by_key[other_key])
        contributions[key] = len(sets_by_key[key] - others)
    return contributions


def jaccard_similarity(a: Set[str], b: Set[str]) -> float:
    if not a and not b:
        return 1.0
    return len(a & b) / max(1, len(a | b))


def pairwise_jaccard(sets_by_key: Dict[str, Set[str]]) -> Dict[str, float]:
    keys = sorted(sets_by_key.keys())
    scores: Dict[str, float] = {}
    for i, k1 in enumerate(keys):
        for k2 in keys[i + 1 :]:
            scores[f"{k1}|{k2}"] = jaccard_similarity(sets_by_key[k1], sets_by_key[k2])
    return scores


def build_grid(bbox: Dict[str, float], size: int) -> List[Dict[str, float]]:
    return grid_points(bbox, size)


def flatten_place_ids(sets: Iterable[Set[str]]) -> Set[str]:
    out: Set[str] = set()
    for s in sets:
        out.update(s)
    return out
