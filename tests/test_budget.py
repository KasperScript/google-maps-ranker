import pytest

from src import config, pipeline
from src.http import BudgetExceededError


class FakeHttpClient:
    def __init__(self, *args, **kwargs):
        self.calls = 0

    def post_json(self, url, body, field_mask, extra_headers=None):
        self.calls += 1
        return {"places": []}


def test_budget_guard_stops_requests(monkeypatch):
    monkeypatch.setattr(
        config,
        "HUBS",
        {
            "hub1": {"name": "Hub 1", "lat": 52.2, "lon": 21.0},
        },
    )
    monkeypatch.setattr(config, "ORTHO_QUERIES", ["q1", "q2"])
    monkeypatch.setattr(config, "GENERAL_QUERIES", [])
    monkeypatch.setattr(config, "COVERAGE_QUERIES", [])
    monkeypatch.setattr(config, "PLACES_SUPPORTS_TYPE_FILTER", False)

    created = {}

    def fake_http_client(*args, **kwargs):
        client = FakeHttpClient()
        created["client"] = client
        return client

    monkeypatch.setattr(pipeline, "HttpClient", fake_http_client)

    with pytest.raises(BudgetExceededError):
        pipeline.run(
            api_key="dummy",
            cache_db_path=":memory:",
            max_places=1,
            max_routes=0,
            no_cache=True,
            refresh_routes=True,
            top_n=1,
            write_outputs=False,
        )

    assert created["client"].calls == 1
