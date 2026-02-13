import json
from pathlib import Path

import pytest

from src import config, pipeline


def load_fixture(name: str):
    path = Path(__file__).parent / "fixtures" / name
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def make_fake_http_client(fixture):
    class FakeHttpClient:
        def __init__(self, *args, **kwargs):
            self.calls = 0

        def post_json(self, url, body, field_mask, extra_headers=None):
            self.calls += 1
            return fixture

    return FakeHttpClient


def patch_basic_config(monkeypatch, hubs):
    monkeypatch.setattr(config, "HUBS", hubs)
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", [])
    monkeypatch.setattr(config, "PLACES_SUPPORTS_TYPE_FILTER", False)
    monkeypatch.setattr(config, "PLACES_MAX_PAGES_PER_QUERY", 1)
    monkeypatch.setattr(config, "COVERAGE_MAX_PAGES_PER_QUERY", 1)
    monkeypatch.setattr(
        config,
        "WARSAW_BBOX",
        {"lat_min": 52.1, "lat_max": 52.3, "lon_min": 20.9, "lon_max": 21.1},
    )


def test_coverage_off_skips_and_completes(monkeypatch):
    places_fixture = load_fixture("places_text_search.json")
    fake_http = make_fake_http_client(places_fixture)
    monkeypatch.setattr(pipeline, "HttpClient", fake_http)

    patch_basic_config(
        monkeypatch,
        {"hub1": {"name": "Hub 1", "lat": 52.2, "lon": 21.0}},
    )
    monkeypatch.setattr(config, "COVERAGE_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "COVERAGE_CONFIG", config.CoverageConfig(grid_size_initial=2, grid_max_iterations=0))

    result = pipeline.run(
        api_key="dummy",
        cache_db_path=":memory:",
        max_places=1,
        max_routes=0,
        no_cache=True,
        refresh_routes=False,
        top_n=1,
        output_dir="out",
        write_outputs=False,
        coverage_mode="off",
    )

    assert result.summary["places_requests"] == 1
    coverage = result.summary["coverage"]
    assert coverage["mode"] == "off"
    assert coverage["skipped"] is True


def test_coverage_light_within_budget(monkeypatch):
    places_fixture = load_fixture("places_text_search.json")
    fake_http = make_fake_http_client(places_fixture)
    monkeypatch.setattr(pipeline, "HttpClient", fake_http)

    patch_basic_config(
        monkeypatch,
        {
            "hub1": {"name": "Hub 1", "lat": 52.2, "lon": 21.0},
            "hub2": {"name": "Hub 2", "lat": 52.25, "lon": 21.02},
            "hub3": {"name": "Hub 3", "lat": 52.23, "lon": 21.01},
        },
    )
    monkeypatch.setattr(config, "COVERAGE_CONFIG", config.CoverageConfig(grid_size_initial=2, grid_max_iterations=0))

    result = pipeline.run(
        api_key="dummy",
        cache_db_path=":memory:",
        max_places=200,
        max_routes=0,
        no_cache=True,
        refresh_routes=False,
        top_n=5,
        output_dir="out",
        write_outputs=False,
        coverage_mode="light",
    )

    assert result.summary["places_requests"] <= 200
    coverage = result.summary["coverage"]
    assert coverage["mode"] == "light"
    assert coverage["coverage_consumed"] <= coverage["coverage_cap"]


def test_coverage_full_stops_on_cap(monkeypatch):
    places_fixture = load_fixture("places_text_search.json")
    fake_http = make_fake_http_client(places_fixture)
    monkeypatch.setattr(pipeline, "HttpClient", fake_http)

    patch_basic_config(
        monkeypatch,
        {"hub1": {"name": "Hub 1", "lat": 52.2, "lon": 21.0}},
    )
    monkeypatch.setattr(config, "COVERAGE_QUERIES", ["ortodonta", "orthodontist"])
    monkeypatch.setattr(config, "COVERAGE_CONFIG", config.CoverageConfig(grid_size_initial=3, grid_max_iterations=0))

    result = pipeline.run(
        api_key="dummy",
        cache_db_path=":memory:",
        max_places=20,
        max_routes=0,
        no_cache=True,
        refresh_routes=False,
        top_n=5,
        output_dir="out",
        write_outputs=False,
        coverage_mode="full",
        coverage_budget_share=0.10,
    )

    coverage = result.summary["coverage"]
    assert coverage["mode"] == "full"
    assert coverage["stopped_early"] is True
    assert coverage["coverage_cap"] == 2
    assert coverage["coverage_consumed"] == 2
