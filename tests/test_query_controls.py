from __future__ import annotations

from typing import Dict, List, Tuple

import pytest

from src import config
from src.pipeline import run


Place = Dict[str, object]
Key = Tuple[str, str]


def _place(place_id: str, name: str | None = None) -> Place:
    return {
        "place_id": place_id,
        "name": name or place_id,
        "rating": 4.8,
        "user_rating_count": 200,
        "lat": 52.23,
        "lon": 21.01,
        "types": ["dentist", "health"],
        "business_status": "OPERATIONAL",
    }


class ScenarioPlacesClient:
    def __init__(self, mapping: Dict[Key, List[Place]]):
        self.mapping = mapping
        self.calls: List[Dict[str, object]] = []

    def set_budget(self, budget):  # pragma: no cover - interface hook
        self.budget = budget

    def set_metrics(self, metrics):  # pragma: no cover - interface hook
        self.metrics = metrics

    def search_text_all(self, query, point, type_filter=None, max_pages=None):
        point_id = str(point.get("id"))
        point_kind = "grid" if point_id.startswith("grid_") else "hub"
        self.calls.append(
            {
                "query": query,
                "point_id": point_id,
                "point_kind": point_kind,
                "type_filter": type_filter,
                "max_pages": max_pages,
            }
        )
        return list(self.mapping.get((query, point_kind), []))


class DummyRoutesClient:
    def set_budget(self, budget):  # pragma: no cover - interface hook
        self.budget = budget

    def set_metrics(self, metrics):  # pragma: no cover - interface hook
        self.metrics = metrics

    def compute_route_duration(self, origin_id, origin, destination, departure_time_rfc3339, mode=None):
        return None


def _patch_base_config(monkeypatch):
    monkeypatch.setattr(
        config,
        "HUBS",
        {
            "hub1": {"name": "Hub 1", "lat": 52.23, "lon": 21.01},
        },
    )
    monkeypatch.setattr(
        config,
        "WARSAW_BBOX",
        {"lat_min": 52.2, "lat_max": 52.24, "lon_min": 21.0, "lon_max": 21.04},
    )
    monkeypatch.setattr(
        config,
        "COVERAGE_CONFIG",
        config.CoverageConfig(grid_size_initial=2, grid_max_iterations=0, uplift_threshold=0.0),
    )
    monkeypatch.setattr(config, "COVERAGE_MAX_PAGES_PER_QUERY", 1)
    monkeypatch.setattr(config, "COVERAGE_QUERIES", [])
    monkeypatch.setattr(config, "PLACES_SUPPORTS_TYPE_FILTER", False)
    monkeypatch.setattr(config, "PLACES_MAX_PAGES_PER_QUERY", 1)
    monkeypatch.setattr(config, "DEPARTURE_TIME_RFC3339_OVERRIDE", "2026-01-26T16:00:00Z")


def _run_with_clients(places_client: ScenarioPlacesClient, coverage_mode: str = "off"):
    return run(
        api_key=None,
        cache_db_path=":memory:",
        max_places=200,
        max_routes=0,
        no_cache=True,
        refresh_routes=True,
        top_n=10,
        output_dir="out",
        write_outputs=False,
        places_client=places_client,
        routes_client=DummyRoutesClient(),
        coverage_mode=coverage_mode,
        coverage_budget_share=1.0,
    )


def test_general_queries_skipped_when_ortho_candidates_sufficient(monkeypatch):
    _patch_base_config(monkeypatch)
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", ["stomatolog", "dentist"])
    monkeypatch.setattr(config, "MIN_CANDIDATES", 2)

    mapping: Dict[Key, List[Place]] = {
        ("ortodonta", "hub"): [_place("o1"), _place("o2")],
    }
    places_client = ScenarioPlacesClient(mapping)

    _run_with_clients(places_client, coverage_mode="off")

    general_calls = [c for c in places_client.calls if c["query"] in set(config.GENERAL_QUERIES)]
    assert not general_calls


def test_general_queries_run_when_ortho_candidates_insufficient(monkeypatch):
    _patch_base_config(monkeypatch)
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", ["stomatolog", "dentist"])
    monkeypatch.setattr(config, "MIN_CANDIDATES", 3)

    mapping: Dict[Key, List[Place]] = {
        ("ortodonta", "hub"): [_place("o1")],
        ("stomatolog", "hub"): [_place("g1")],
        ("dentist", "hub"): [_place("g2")],
    }
    places_client = ScenarioPlacesClient(mapping)

    _run_with_clients(places_client, coverage_mode="full")

    general_calls = [c for c in places_client.calls if c["query"] in set(config.GENERAL_QUERIES)]
    assert general_calls
    assert all(c["max_pages"] == 1 for c in general_calls)
    general_grid_calls = [c for c in general_calls if c["point_kind"] == "grid"]
    assert not general_grid_calls


def test_general_query_cost_controls_allow_grid_only_when_still_insufficient(monkeypatch):
    _patch_base_config(monkeypatch)
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["ortodonta"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", ["stomatolog", "dentist"])
    monkeypatch.setattr(config, "MIN_CANDIDATES", 3)

    mapping: Dict[Key, List[Place]] = {
        ("ortodonta", "hub"): [_place("o1")],
        ("stomatolog", "hub"): [_place("g1")],
        ("dentist", "hub"): [],
        ("stomatolog", "grid"): [_place("g_grid")],
        ("dentist", "grid"): [],
    }
    places_client = ScenarioPlacesClient(mapping)

    _run_with_clients(places_client, coverage_mode="full")

    general_calls = [c for c in places_client.calls if c["query"] in set(config.GENERAL_QUERIES)]
    assert general_calls
    assert all(c["max_pages"] == 1 for c in general_calls)
    general_grid_calls = [c for c in general_calls if c["point_kind"] == "grid"]
    assert general_grid_calls
