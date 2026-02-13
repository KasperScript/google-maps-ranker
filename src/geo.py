"""Geospatial helpers."""
from __future__ import annotations

import math
from typing import Dict, List


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def grid_points(bbox: Dict[str, float], n: int) -> List[Dict[str, float]]:
    if n <= 1:
        raise ValueError("Grid size must be > 1")
    lat_min = bbox["lat_min"]
    lat_max = bbox["lat_max"]
    lon_min = bbox["lon_min"]
    lon_max = bbox["lon_max"]

    lat_step = (lat_max - lat_min) / (n - 1)
    lon_step = (lon_max - lon_min) / (n - 1)

    points = []
    for r in range(n):
        for c in range(n):
            points.append(
                {
                    "id": f"grid_{r}_{c}",
                    "name": f"Grid {r},{c}",
                    "lat": lat_min + r * lat_step,
                    "lon": lon_min + c * lon_step,
                }
            )
    return points
