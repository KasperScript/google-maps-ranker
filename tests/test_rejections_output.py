import csv
import json
from pathlib import Path

import pytest

from src import config
from src.pipeline import run
from src.places_client import parse_places_response


def _load_fixture(name: str):
    path = Path(__file__).parent / "fixtures" / name
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class FakePlacesClient:
    def __init__(self, places_fixture):
        self.places_fixture = places_fixture

    def set_budget(self, budget):  # pragma: no cover - interface hook
        self.budget = budget

    def set_metrics(self, metrics):  # pragma: no cover - interface hook
        self.metrics = metrics

    def search_text_all(self, query, point, type_filter=None, max_pages=None):
        return parse_places_response(self.places_fixture)


class FakeRoutesClient:
    def __init__(self, routes_fixture):
        self.routes_fixture = routes_fixture

    def set_budget(self, budget):  # pragma: no cover - interface hook
        self.budget = budget

    def set_metrics(self, metrics):  # pragma: no cover - interface hook
        self.metrics = metrics

    def compute_route_duration(self, origin_id, origin, destination, departure_time_rfc3339, mode=None):
        key = f"{origin_id}|{destination['place_id']}"
        return self.routes_fixture.get("durations", {}).get(key)


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    monkeypatch.setattr(
        config,
        "HUBS",
        {
            "galeria_polnocna": {"name": "Galeria Północna", "lat": 52.30, "lon": 20.95},
            "alk": {"name": "ALK", "lat": 52.25, "lon": 21.00},
            "centralny": {"name": "Central", "lat": 52.23, "lon": 21.01},
        },
    )
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", [])
    monkeypatch.setattr(config, "COVERAGE_QUERIES", ["ortodonta"])
    monkeypatch.setattr(
        config,
        "WARSAW_BBOX",
        {"lat_min": 52.1, "lat_max": 52.3, "lon_min": 20.9, "lon_max": 21.1},
    )
    monkeypatch.setattr(
        config,
        "COVERAGE_CONFIG",
        config.CoverageConfig(grid_size_initial=2, grid_max_iterations=0),
    )
    monkeypatch.setattr(config, "DEPARTURE_TIME_RFC3339_OVERRIDE", "2026-01-26T16:00:00Z")
    monkeypatch.setattr(config, "PLACES_SUPPORTS_TYPE_FILTER", False)


def test_rejections_csv_written_with_headers_and_rows(tmp_path):
    places_fixture = _load_fixture("places_text_search.json")
    routes_fixture = _load_fixture("routes_compute_routes.json")

    fake_places = FakePlacesClient(places_fixture)
    fake_routes = FakeRoutesClient(routes_fixture)

    run(
        api_key=None,
        cache_db_path=":memory:",
        max_places=100,
        max_routes=100,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir=str(tmp_path),
        write_outputs=True,
        places_client=fake_places,
        routes_client=fake_routes,
        coverage_mode="off",
    )

    rejections_path = tmp_path / "rejections.csv"
    assert rejections_path.exists()

    with open(rejections_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = set(reader.fieldnames or [])

    required_headers = {
        "place_id",
        "name",
        "rating",
        "user_rating_count",
        "lat",
        "lon",
        "reject_reason",
        "stage",
        "found_by_queries",
    }
    assert required_headers.issubset(headers)
    assert len(rows) >= 1
