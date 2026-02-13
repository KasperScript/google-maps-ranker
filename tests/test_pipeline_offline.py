import json
from pathlib import Path

import pytest

from src import config
from src.pipeline import render_summary, result_sort_key, run
from src.places_client import parse_places_response


class FakePlacesClient:
    def __init__(
        self,
        places_fixture,
        budget=None,
        simulate_live: bool = False,
        simulate_cache: bool = False,
        pages_per_call: int = 1,
        metrics=None,
    ):
        self.places_fixture = places_fixture
        self.budget = budget
        self.simulate_live = simulate_live
        self.simulate_cache = simulate_cache
        self.pages_per_call = pages_per_call
        self.metrics = metrics

    def set_budget(self, budget):
        self.budget = budget

    def set_metrics(self, metrics):
        self.metrics = metrics

    def search_text_all(self, query, point, type_filter=None, max_pages=None):
        if self.simulate_live and self.budget:
            pages = max_pages if max_pages is not None else self.pages_per_call
            try:
                pages = int(pages)
            except (TypeError, ValueError):
                pages = 1
            pages = max(1, pages)
            for _ in range(pages):
                self.budget.consume("places")
        elif self.simulate_cache and self.metrics:
            self.metrics.inc_cache_hit("places")
        return parse_places_response(self.places_fixture)


class FakeRoutesClient:
    def __init__(self, routes_fixture, budget=None, simulate_live: bool = False, simulate_cache: bool = False, metrics=None):
        self.routes_fixture = routes_fixture
        self.budget = budget
        self.simulate_live = simulate_live
        self.simulate_cache = simulate_cache
        self.metrics = metrics

    def set_budget(self, budget):
        self.budget = budget

    def set_metrics(self, metrics):
        self.metrics = metrics

    def compute_route_duration(self, origin_id, origin, destination, departure_time_rfc3339, mode=None):
        if self.simulate_live and self.budget:
            self.budget.consume("routes")
        elif self.simulate_cache and self.metrics:
            self.metrics.inc_cache_hit("routes")
        key = f"{origin_id}|{destination['place_id']}"
        return self.routes_fixture.get("durations", {}).get(key)


def load_fixture(name):
    path = Path(__file__).parent / "fixtures" / name
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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
    monkeypatch.setattr(config, "WARSAW_BBOX", {"lat_min": 52.1, "lat_max": 52.3, "lon_min": 20.9, "lon_max": 21.1})
    monkeypatch.setattr(config, "COVERAGE_CONFIG", config.CoverageConfig(grid_size_initial=2, grid_max_iterations=0))
    monkeypatch.setattr(config, "DEPARTURE_TIME_RFC3339_OVERRIDE", "2026-01-26T16:00:00Z")
    monkeypatch.setattr(config, "PLACES_SUPPORTS_TYPE_FILTER", False)


def test_pipeline_offline_stable():
    places_fixture = load_fixture("places_text_search.json")
    routes_fixture = load_fixture("routes_compute_routes.json")

    fake_places = FakePlacesClient(places_fixture)
    fake_routes = FakeRoutesClient(routes_fixture)

    result1 = run(
        api_key=None,
        cache_db_path=":memory:",
        max_places=100,
        max_routes=100,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir="out",
        write_outputs=False,
        places_client=fake_places,
        routes_client=fake_routes,
    )

    result2 = run(
        api_key=None,
        cache_db_path=":memory:",
        max_places=100,
        max_routes=100,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir="out",
        write_outputs=False,
        places_client=fake_places,
        routes_client=fake_routes,
    )

    results = result1.results
    ids1 = [r["place_id"] for r in results]
    ids2 = [r["place_id"] for r in result2.results]

    assert ids1 == ids2
    assert results
    assert "p3" not in ids1

    required_keys = {"place_id", "name", "rating", "user_rating_count", "final"}
    for row in results:
        assert required_keys.issubset(row)

    finals = [row["final"] for row in results]
    assert all(a >= b for a, b in zip(finals, finals[1:]))

    sort_keys = [result_sort_key(row) for row in results]
    assert all(a <= b for a, b in zip(sort_keys, sort_keys[1:]))

    for prev, curr in zip(results, results[1:]):
        if prev["final"] == curr["final"]:
            assert result_sort_key(prev)[1:] <= result_sort_key(curr)[1:]


def test_offline_request_counters_increment_when_simulating_live():
    places_fixture = load_fixture("places_text_search.json")
    routes_fixture = load_fixture("routes_compute_routes.json")

    fake_places = FakePlacesClient(places_fixture, simulate_live=True)
    fake_routes = FakeRoutesClient(routes_fixture, simulate_live=True)

    result = run(
        api_key=None,
        cache_db_path=":memory:",
        max_places=200,
        max_routes=200,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir="out",
        write_outputs=False,
        places_client=fake_places,
        routes_client=fake_routes,
    )

    assert result.summary["places_requests"] > 0
    assert result.summary["routes_requests"] > 0


def test_summary_reports_requests_after_transit_and_coverage_off():
    places_fixture = load_fixture("places_text_search.json")
    routes_fixture = load_fixture("routes_compute_routes.json")

    fake_places = FakePlacesClient(places_fixture, simulate_live=True)
    fake_routes = FakeRoutesClient(routes_fixture, simulate_live=True)

    result = run(
        api_key=None,
        cache_db_path=":memory:",
        max_places=200,
        max_routes=200,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir="out",
        write_outputs=False,
        places_client=fake_places,
        routes_client=fake_routes,
        coverage_mode="off",
    )

    lines = render_summary(result.summary)
    places_line = next(line for line in lines if line.startswith("Places requests (network):"))
    routes_line = next(line for line in lines if line.startswith("Routes requests (network):"))

    assert int(places_line.split(":", 1)[1].strip()) > 0
    assert int(routes_line.split(":", 1)[1].strip()) > 0
    assert "Coverage: DISABLED" in lines
    assert not any(line.startswith("Coverage: U_hubs") for line in lines)


def test_offline_cache_hits_reported_when_simulating_cache():
    places_fixture = load_fixture("places_text_search.json")
    routes_fixture = load_fixture("routes_compute_routes.json")

    fake_places = FakePlacesClient(places_fixture, simulate_cache=True)
    fake_routes = FakeRoutesClient(routes_fixture, simulate_cache=True)

    result = run(
        api_key=None,
        cache_db_path=":memory:",
        max_places=200,
        max_routes=200,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir="out",
        write_outputs=False,
        places_client=fake_places,
        routes_client=fake_routes,
    )

    assert result.summary["places_requests"] == 0
    assert result.summary["routes_requests"] == 0
    assert result.summary["cache_hits_places"] > 0
    assert result.summary["cache_hits_routes"] > 0


def test_summary_includes_dedup_stats_section():
    summary = {
        "places_requests": 2,
        "routes_requests": 1,
        "cache_hits_places": 3,
        "cache_hits_routes": 4,
        "dedup_skips_places": 5,
        "dedup_skips_routes": 6,
        "routes_skipped": False,
        "coverage": {"mode": "off"},
        "rejection_counts": {},
        "top10": [],
    }

    lines = render_summary(summary)
    assert "Request stats:" in lines
    assert any("dedup_skips=5" in line for line in lines)
    assert any("dedup_skips=6" in line for line in lines)
