from datetime import datetime, timezone

from src import config
from src.routes_client import bucket_departure_key, build_routes_body


def test_bucket_departure_key_same_bucket():
    dt1 = datetime(2026, 1, 26, 16, 10, tzinfo=timezone.utc)
    dt2 = datetime(2026, 1, 26, 16, 14, tzinfo=timezone.utc)
    key1 = bucket_departure_key(dt1, config.ROUTES_CACHE_DEPARTURE_BUCKET_MINUTES)
    key2 = bucket_departure_key(dt2, config.ROUTES_CACHE_DEPARTURE_BUCKET_MINUTES)
    assert key1 == key2
    assert key1 == "wd0_1700"


def test_bucket_departure_key_different_buckets():
    dt1 = datetime(2026, 1, 26, 16, 10, tzinfo=timezone.utc)
    dt2 = datetime(2026, 1, 26, 16, 25, tzinfo=timezone.utc)
    assert bucket_departure_key(
        dt1, config.ROUTES_CACHE_DEPARTURE_BUCKET_MINUTES
    ) != bucket_departure_key(dt2, config.ROUTES_CACHE_DEPARTURE_BUCKET_MINUTES)


def test_build_routes_body_uses_exact_departure_time():
    origin = {"lat": 52.2, "lon": 21.0}
    dest = {"place_id": "p1", "lat": 52.3, "lon": 21.1}
    departure = "2026-01-26T16:10:00Z"
    body = build_routes_body(origin, dest, departure, config.TRANSIT_MODE)
    assert body["departureTime"] == departure
