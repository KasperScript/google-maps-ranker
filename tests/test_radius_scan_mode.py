import json
from types import SimpleNamespace

import run as run_module
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


def test_radius_scan_outputs_and_skips_routes(tmp_path, monkeypatch):
    monkeypatch.setattr(
        config,
        "HUBS",
        {
            "centralny": {"name": "Central", "lat": 1.0, "lon": 1.0},
            "alk": {"name": "ALK", "lat": 1.1, "lon": 1.1},
            "galeria_polnocna": {"name": "Gallery", "lat": 1.2, "lon": 1.2},
        },
    )
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", [])
    monkeypatch.setattr(config, "PLACES_SUPPORTS_TYPE_FILTER", False)
    monkeypatch.setattr(config, "MIN_USER_RATING_COUNT", 10)

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
        radius_scan_center_lat=1.0,
        radius_scan_center_lon=1.0,
        radius_scan_radius_km=50.0,
        radius_scan_grid_step_km=10.0,
        radius_scan_scan_radius_m=1000,
        radius_scan_queries=["ortodonta"],
        radius_scan_max_pages=1,
    )

    assert result.summary["routes_requests"] == 0
    assert fake_routes.calls == 0

    csv_path = out_dir / "radius_scan_results.csv"
    json_path = out_dir / "radius_scan_results.json"
    summary_path = out_dir / "radius_scan_summary.txt"
    assert csv_path.exists()
    assert json_path.exists()
    assert summary_path.exists()

    header = csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert "place_id" in header
    assert "distance_km_to_center" in header

    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert data
    assert "place_id" in data[0]
    assert "distance_km_to_center" in data[0]


def test_extreme_defaults_apply():
    args = SimpleNamespace(
        extreme=True,
        radius_km=None,
        grid_step_km=None,
        scan_radius_m=None,
        max_pages=None,
    )
    run_module.apply_extreme_defaults(args)
    assert args.radius_km == 20.0
    assert args.grid_step_km == 0.5
    assert args.scan_radius_m == 800
    assert args.max_pages == 3
