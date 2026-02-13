from datetime import datetime, timezone

from src import config
from src.cache import Cache, make_request_cache_key
from src.http import HttpClient, RequestBudget, RequestMetrics
from src.places_client import PlacesClient, build_text_search_body
from src.routes_client import RoutesClient, bucket_departure_key


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200
        self.headers = {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, responses_by_url):
        self.responses_by_url = responses_by_url
        self.calls = []

    def post(self, url, data=None, headers=None, timeout=None):
        self.calls.append(url)
        payload = self.responses_by_url.get(url, {})
        return FakeResponse(payload)


def make_http_client(responses_by_url):
    client = HttpClient(
        api_key="dummy",
        timeout=1,
        retry_max=1,
        backoff_base=0.0,
        backoff_max=0.0,
    )
    client.session = FakeSession(responses_by_url)
    return client


def test_dedup_integration_with_temp_cache(tmp_path):
    metrics = RequestMetrics()
    budget = RequestBudget(max_places=10, max_routes=10, metrics=metrics)
    cache_path = tmp_path / "cache.db"
    cache = Cache(str(cache_path))
    responses = {
        config.PLACES_TEXT_SEARCH_URL: {"places": []},
        config.ROUTES_COMPUTE_URL: {"routes": [{"duration": "120s"}]},
    }
    http_client = make_http_client(responses)
    places_client = PlacesClient(http_client, cache, budget, no_cache=False, metrics=metrics)
    routes_client = RoutesClient(
        http_client, cache, budget, no_cache=False, refresh_routes=True, metrics=metrics
    )

    point = {"lat": 52.2, "lon": 21.0}
    places_client.search_text("ortodonta", point)
    places_client.search_text("ortodonta", point)

    departure = "2026-01-26T16:00:00Z"
    dest = {"place_id": "p0", "lat": 52.2, "lon": 21.0}
    routes_client.compute_route_duration("hub", point, dest, departure)
    routes_client.compute_route_duration("hub", point, dest, departure)

    assert metrics.network_places == 1
    assert metrics.network_routes == 1
    assert metrics.dedup_skips_places == 1
    assert metrics.dedup_skips_routes == 1
    assert http_client.session.calls.count(config.PLACES_TEXT_SEARCH_URL) == 1
    assert http_client.session.calls.count(config.ROUTES_COMPUTE_URL) == 1
    cache.close()


def test_network_counters_increment_with_mock_http():
    metrics = RequestMetrics()
    budget = RequestBudget(max_places=10, max_routes=10, metrics=metrics)
    cache = Cache(":memory:")
    responses = {
        config.PLACES_TEXT_SEARCH_URL: {"places": []},
        config.ROUTES_COMPUTE_URL: {"routes": [{"duration": "120s"}]},
    }
    http_client = make_http_client(responses)
    places_client = PlacesClient(http_client, cache, budget, no_cache=True, metrics=metrics)
    routes_client = RoutesClient(
        http_client, cache, budget, no_cache=True, refresh_routes=True, metrics=metrics
    )

    point = {"lat": 52.2, "lon": 21.0}
    for _ in range(2):
        places_client.search_text("ortodonta", point)

    departure = "2026-01-26T16:00:00Z"
    for idx in range(3):
        dest = {"place_id": f"p{idx}", "lat": 52.2, "lon": 21.0}
        routes_client.compute_route_duration("hub", point, dest, departure)

    assert metrics.network_places == 1
    assert metrics.network_routes == 3
    assert metrics.cache_hits_places == 0
    assert metrics.cache_hits_routes == 0
    cache.close()


def test_cache_hits_increment_without_network():
    metrics = RequestMetrics()
    budget = RequestBudget(max_places=10, max_routes=10, metrics=metrics)
    cache = Cache(":memory:")
    responses = {
        config.PLACES_TEXT_SEARCH_URL: {"places": []},
        config.ROUTES_COMPUTE_URL: {"routes": [{"duration": "120s"}]},
    }
    http_client = make_http_client(responses)
    places_client = PlacesClient(http_client, cache, budget, no_cache=False, metrics=metrics)
    routes_client = RoutesClient(
        http_client, cache, budget, no_cache=False, refresh_routes=False, metrics=metrics
    )

    point = {"lat": 52.2, "lon": 21.0}
    body = build_text_search_body("ortodonta", point, None, None)
    key = make_request_cache_key(config.PLACES_TEXT_SEARCH_URL, places_client.field_mask, body)
    cache.set_search_cache(key, {"places": []})

    departure = "2026-01-26T16:00:00Z"
    dest = {"place_id": "p0", "lat": 52.2, "lon": 21.0}
    dep_dt = datetime.fromisoformat(departure.replace("Z", "+00:00")).astimezone(timezone.utc)
    dep_bucket = bucket_departure_key(dep_dt, config.ROUTES_CACHE_DEPARTURE_BUCKET_MINUTES)
    routes_key = f"hub|{dest['place_id']}|{config.TRANSIT_MODE}|{dep_bucket}"
    cache.set_routes_cache(routes_key, "hub", dest["place_id"], config.TRANSIT_MODE, 123)

    places_client.search_text("ortodonta", point)
    routes_client.compute_route_duration("hub", point, dest, departure)

    assert metrics.network_places == 0
    assert metrics.network_routes == 0
    assert metrics.cache_hits_places == 1
    assert metrics.cache_hits_routes == 1
    cache.close()
