from src.geo import haversine_km
from src.pipeline import build_radius_scan_points


def test_radius_scan_points_within_radius():
    center_lat = 52.0
    center_lon = 21.0
    radius_km = 5.0
    points = build_radius_scan_points(center_lat, center_lon, radius_km, grid_step_km=2.0)
    assert points
    for p in points:
        dist = haversine_km(p["lat"], p["lon"], center_lat, center_lon)
        assert dist <= radius_km + 1e-6


def test_radius_scan_point_count_increases_with_smaller_step():
    center_lat = 52.0
    center_lon = 21.0
    radius_km = 5.0
    coarse = build_radius_scan_points(center_lat, center_lon, radius_km, grid_step_km=3.0)
    fine = build_radius_scan_points(center_lat, center_lon, radius_km, grid_step_km=1.5)
    assert len(fine) > len(coarse)


def test_radius_scan_deterministic_ordering():
    center_lat = 52.0
    center_lon = 21.0
    radius_km = 5.0
    points1 = build_radius_scan_points(center_lat, center_lon, radius_km, grid_step_km=2.0)
    points2 = build_radius_scan_points(center_lat, center_lon, radius_km, grid_step_km=2.0)
    assert points1 == points2
