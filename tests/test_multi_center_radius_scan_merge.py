import json

from src import config
from src.pipeline import run


class FakePlacesClient:
    def __init__(self, places):
        self.places = places
        self.budget = None
        self.metrics = None

    def set_budget(self, budget):
        self.budget = budget

    def set_metrics(self, metrics):
        self.metrics = metrics

    def search_text_all(self, query, point, type_filter=None, max_pages=None, radius_m=None):
        return list(self.places)


class FakeRoutesClient:
    def __init__(self):
        self.calls = 0

    def set_budget(self, budget):
        return None

    def set_metrics(self, metrics):
        return None

    def compute_route_duration(self, origin_id, origin, destination, departure_time_rfc3339, mode=None):
        self.calls += 1
        return None


def test_multi_center_radius_scan_merge(tmp_path, monkeypatch):
    monkeypatch.setattr(
        config,
        "HUBS",
        {
            "c1": {"name": "C1", "lat": 1.0, "lon": 1.0},
            "c2": {"name": "C2", "lat": 1.0, "lon": 2.0},
        },
    )
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", [])
    monkeypatch.setattr(config, "PLACES_SUPPORTS_TYPE_FILTER", False)
    monkeypatch.setattr(config, "MIN_USER_RATING_COUNT", 10)

    places = [
        {
            "place_id": "p_overlap",
            "name": "Overlap",
            "rating": 4.5,
            "user_rating_count": 50,
            "lat": 1.0,
            "lon": 1.2,
            "types": [],
            "business_status": config.BUSINESS_STATUS_OPERATIONAL,
        },
        {
            "place_id": "p_b",
            "name": "OnlyB",
            "rating": 4.6,
            "user_rating_count": 30,
            "lat": 1.0,
            "lon": 2.0,
            "types": [],
            "business_status": config.BUSINESS_STATUS_OPERATIONAL,
        },
    ]

    fake_places = FakePlacesClient(places)
    fake_routes = FakeRoutesClient()
    out_dir = tmp_path / "out"

    result = run(
        api_key=None,
        cache_db_path=":memory:",
        max_places=50,
        max_routes=0,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir=str(out_dir),
        write_outputs=True,
        places_client=fake_places,
        routes_client=fake_routes,
        coverage_mode="off",
        radius_scan=True,
        radius_scan_centers=["c1", "c2"],
        radius_scan_radius_km=100.0,
        radius_scan_grid_step_km=10.0,
        radius_scan_scan_radius_m=1000,
        radius_scan_queries=["ortodonta"],
        radius_scan_max_pages=1,
    )

    assert result.summary["places_requests"] == 0
    assert result.summary["cache_hits_places"] == 0
    assert result.summary["dedup_skips_places"] >= 0
    assert result.summary["eligible_count"] > 0
    assert result.summary["rejected_count"] >= 0
    assert result.summary["total_unique_place_ids"] == 2
    assert fake_routes.calls == 0

    merged_json = out_dir / "radius_scan_merged_results.json"
    assert merged_json.exists()
    data = json.loads(merged_json.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    by_id = {row["place_id"]: row for row in data}
    overlap = by_id["p_overlap"]
    onlyb = by_id["p_b"]

    assert set(overlap["distance_km_by_center"].keys()) == {"c1", "c2"}
    assert overlap["nearest_center_id"] in {"c1", "c2"}
    assert set(overlap["centers_in_range"]) == {"c1", "c2"}

    assert set(onlyb["distance_km_by_center"].keys()) == {"c1", "c2"}
    assert onlyb["nearest_center_id"] == "c2"
    assert onlyb["centers_in_range"] == ["c2"]

    for center_id in ("c1", "c2"):
        center_dir = out_dir / "by_center" / center_id
        assert (center_dir / "radius_scan_results.csv").exists()
        assert (center_dir / "radius_scan_results.json").exists()
        assert (center_dir / "radius_scan_summary.txt").exists()
