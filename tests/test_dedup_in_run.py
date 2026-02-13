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


class CountingCache(Cache):
    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.search_calls = 0
        self.routes_calls = 0

    def get_search_cache(self, key):
        self.search_calls += 1
        return super().get_search_cache(key)

    def get_routes_cache(self, key):
        self.routes_calls += 1
        return super().get_routes_cache(key)


def test_places_dedup_with_cached_response():
    cache = CountingCache(":memory:")
    responses = {config.PLACES_TEXT_SEARCH_URL: {"places": [{"id": "p1"}]}}
    http_client = make_http_client(responses)
    budget = RequestBudget(max_places=10, max_routes=10)
    places_client = PlacesClient(http_client, cache, budget, no_cache=False)

    point = {"lat": 52.2, "lon": 21.0}
    body = build_text_search_body("ortodonta", point, None, None)
    key = make_request_cache_key(config.PLACES_TEXT_SEARCH_URL, places_client.field_mask, body)
    cached_payload = {"places": [{"id": "p1"}]}
    cache.set_search_cache(key, cached_payload)

    first = places_client.search_text("ortodonta", point)
    second = places_client.search_text("ortodonta", point)

    assert first == cached_payload
    assert second == cached_payload
    assert http_client.session.calls == []
    assert cache.search_calls == 1
    cache.close()


def test_routes_dedup_with_cached_response():
    cache = CountingCache(":memory:")
    responses = {config.ROUTES_COMPUTE_URL: {"routes": [{"duration": "120s"}]}}
    http_client = make_http_client(responses)
    budget = RequestBudget(max_places=10, max_routes=10)
    routes_client = RoutesClient(
        http_client, cache, budget, no_cache=False, refresh_routes=False
    )

    origin = {"lat": 52.2, "lon": 21.0}
    dest = {"place_id": "p0", "lat": 52.2, "lon": 21.0}
    departure = "2026-01-26T16:00:00Z"
    dep_dt = datetime.fromisoformat(departure.replace("Z", "+00:00")).astimezone(timezone.utc)
    dep_bucket = bucket_departure_key(dep_dt, config.ROUTES_CACHE_DEPARTURE_BUCKET_MINUTES)
    routes_key = f"hub|{dest['place_id']}|{config.TRANSIT_MODE}|{dep_bucket}"
    cache.set_routes_cache(routes_key, "hub", dest["place_id"], config.TRANSIT_MODE, 123)

    first = routes_client.compute_route_duration("hub", origin, dest, departure)
    second = routes_client.compute_route_duration("hub", origin, dest, departure)

    assert first == 123
    assert second == 123
    assert http_client.session.calls == []
    assert cache.routes_calls == 1
    cache.close()


def test_dedup_without_cache_skips_network_on_second_call():
    cache = Cache(":memory:")
    responses = {
        config.PLACES_TEXT_SEARCH_URL: {"places": [{"id": "p1"}]},
        config.ROUTES_COMPUTE_URL: {"routes": [{"duration": "120s"}]},
    }
    http_client = make_http_client(responses)
    metrics = RequestMetrics()
    budget = RequestBudget(max_places=10, max_routes=10, metrics=metrics)
    places_client = PlacesClient(http_client, cache, budget, no_cache=True, metrics=metrics)
    routes_client = RoutesClient(
        http_client, cache, budget, no_cache=True, refresh_routes=True, metrics=metrics
    )

    point = {"lat": 52.2, "lon": 21.0}
    first_places = places_client.search_text("ortodonta", point)
    second_places = places_client.search_text("ortodonta", point)

    departure = "2026-01-26T16:00:00Z"
    dest = {"place_id": "p0", "lat": 52.2, "lon": 21.0}
    first_route = routes_client.compute_route_duration("hub", point, dest, departure)
    second_route = routes_client.compute_route_duration("hub", point, dest, departure)

    assert first_places == {"places": [{"id": "p1"}]}
    assert second_places == {}
    assert first_route == 120
    assert second_route is None
    assert http_client.session.calls.count(config.PLACES_TEXT_SEARCH_URL) == 1
    assert http_client.session.calls.count(config.ROUTES_COMPUTE_URL) == 1
    assert metrics.dedup_skips_places == 1
    assert metrics.dedup_skips_routes == 1
    cache.close()
