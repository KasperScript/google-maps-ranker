import json

from src import config
from src.pipeline import compute_min_distance_km_to_any_hub, run


class FakePlacesClient:
    def __init__(self, places):
        self.places = places
        self.budget = None
        self.metrics = None

    def set_budget(self, budget):
        self.budget = budget

    def set_metrics(self, metrics):
        self.metrics = metrics

    def search_text_all(self, query, point, type_filter=None, max_pages=None):
        return list(self.places)


class FakeRoutesClient:
    def __init__(self):
        self.calls = 0
        self.budget = None
        self.metrics = None

    def set_budget(self, budget):
        self.budget = budget

    def set_metrics(self, metrics):
        self.metrics = metrics

    def compute_route_duration(self, origin_id, origin, destination, departure_time_rfc3339, mode=None):
        self.calls += 1
        return None


def test_min_distance_and_nearest_hub():
    hubs = [
        {"id": "h1", "lat": 0.0, "lon": 0.0},
        {"id": "h2", "lat": 0.0, "lon": 1.0},
    ]
    place = {"lat": 0.0, "lon": 0.2}
    dist, hub_id = compute_min_distance_km_to_any_hub(place, hubs)
    assert hub_id == "h1"
    assert dist is not None
    assert dist >= 0.0


def test_list_mode_outputs_and_skips_routes(tmp_path, monkeypatch):
    monkeypatch.setattr(
        config,
        "HUBS",
        {
            "h1": {"name": "Hub1", "lat": 1.0, "lon": 1.0},
            "h2": {"name": "Hub2", "lat": 1.0, "lon": 2.0},
        },
    )
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", [])
    monkeypatch.setattr(config, "PLACES_SUPPORTS_TYPE_FILTER", False)
    monkeypatch.setattr(config, "MIN_USER_RATING_COUNT", 10)
    monkeypatch.setattr(config, "MAX_DISTANCE_KM", 50.0)

    places = [
        {
            "place_id": "p1",
            "name": "Near",
            "rating": 4.5,
            "user_rating_count": 25,
            "lat": 1.0,
            "lon": 1.1,
            "types": [],
            "business_status": config.BUSINESS_STATUS_OPERATIONAL,
        },
        {
            "place_id": "p2",
            "name": "Far",
            "rating": 4.5,
            "user_rating_count": 25,
            "lat": 1.0,
            "lon": 10.0,
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
        max_places=10,
        max_routes=10,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir=str(out_dir),
        write_outputs=True,
        places_client=fake_places,
        routes_client=fake_routes,
        coverage_mode="off",
        list_mode=True,
    )

    assert result.summary["routes_requests"] == 0
    assert fake_routes.calls == 0

    csv_path = out_dir / "list_mode_results.csv"
    json_path = out_dir / "list_mode_results.json"
    summary_path = out_dir / "list_mode_summary.txt"
    assert csv_path.exists()
    assert json_path.exists()
    assert summary_path.exists()

    header = csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert "place_id" in header
    assert "nearest_hub_id" in header
    assert "min_distance_km_to_any_hub" in header

    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert data
    assert "place_id" in data[0]
    assert "nearest_hub_id" in data[0]
    assert "min_distance_km_to_any_hub" in data[0]
