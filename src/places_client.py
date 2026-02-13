"""Places API client with caching and response parsing."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import config
from .cache import Cache, make_request_cache_key
from .http import HttpClient, RequestBudget, RequestMetrics


class PlacesClient:
    def __init__(
        self,
        http_client: HttpClient,
        cache: Cache,
        budget: RequestBudget,
        no_cache: bool = False,
        refresh_places: bool = False,
        field_mask: str = config.PLACES_FIELD_MASK_MIN,
        metrics: Optional[RequestMetrics] = None,
    ) -> None:
        self.http = http_client
        self.cache = cache
        self.budget = budget
        self.no_cache = no_cache
        self.refresh_places = refresh_places
        self.field_mask = field_mask
        self.metrics = metrics
        self._seen_cache_keys: set[str] = set()
        self._memory_cache: Dict[str, Dict[str, Any]] = {}

    def search_text(
        self,
        query: str,
        point: Dict[str, Any],
        type_filter: Optional[str] = None,
        page_token: Optional[str] = None,
        radius_m: Optional[int] = None,
    ) -> Dict[str, Any]:
        body = build_text_search_body(query, point, type_filter, page_token, radius_m=radius_m)
        key = make_request_cache_key(config.PLACES_TEXT_SEARCH_URL, self.field_mask, body)
        if key in self._seen_cache_keys:
            if self.metrics is not None:
                self.metrics.inc_dedup_skip("places")
            cached = self._memory_cache.get(key)
            if cached is not None and not self.no_cache:
                return cached
            return {}
        if not self.no_cache and not self.refresh_places:
            cached = self.cache.get_search_cache(key)
            if cached is not None:
                if self.metrics is not None:
                    self.metrics.inc_cache_hit("places")
                self._seen_cache_keys.add(key)
                self._memory_cache[key] = cached
                return cached

        self._seen_cache_keys.add(key)
        self.budget.consume("places")
        response = self.http.post_json(config.PLACES_TEXT_SEARCH_URL, body, self.field_mask)
        if not self.no_cache:
            self.cache.set_search_cache(key, response)
            self._memory_cache[key] = response
        return response

    def search_text_all(
        self,
        query: str,
        point: Dict[str, Any],
        type_filter: Optional[str] = None,
        max_pages: int = config.PLACES_MAX_PAGES_PER_QUERY,
        radius_m: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        places: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        for _ in range(max_pages):
            resp = self.search_text(
                query, point, type_filter=type_filter, page_token=page_token, radius_m=radius_m
            )
            places.extend(parse_places_response(resp))
            page_token = resp.get("nextPageToken") or resp.get("next_page_token")
            if not page_token:
                break
        return places

    def search_nearby(
        self,
        point: Dict[str, Any],
        type_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        body = build_nearby_search_body(point, type_filter)
        key = make_request_cache_key(config.PLACES_NEARBY_SEARCH_URL, self.field_mask, body)
        if key in self._seen_cache_keys:
            if self.metrics is not None:
                self.metrics.inc_dedup_skip("places")
            cached = self._memory_cache.get(key)
            if cached is not None and not self.no_cache:
                return cached
            return {}
        if not self.no_cache and not self.refresh_places:
            cached = self.cache.get_search_cache(key)
            if cached is not None:
                if self.metrics is not None:
                    self.metrics.inc_cache_hit("places")
                self._seen_cache_keys.add(key)
                self._memory_cache[key] = cached
                return cached

        self._seen_cache_keys.add(key)
        self.budget.consume("places")
        response = self.http.post_json(config.PLACES_NEARBY_SEARCH_URL, body, self.field_mask)
        if not self.no_cache:
            self.cache.set_search_cache(key, response)
            self._memory_cache[key] = response
        return response


def build_text_search_body(
    query: str,
    point: Dict[str, Any],
    type_filter: Optional[str],
    page_token: Optional[str],
    radius_m: Optional[int] = None,
) -> Dict[str, Any]:
    radius = (
        int(radius_m)
        if radius_m is not None
        else config.PLACES_LOCATION_BIAS_RADIUS_M
    )
    body: Dict[str, Any] = {
        "textQuery": query,
        "locationBias": {
            "circle": {
                "center": {"latitude": point["lat"], "longitude": point["lon"]},
                "radius": radius,
            }
        },
    }
    if page_token:
        body["pageToken"] = page_token
    if type_filter and config.PLACES_SUPPORTS_TYPE_FILTER:
        body["includedType"] = type_filter
    if config.PLACES_TEXT_SEARCH_BODY_EXTRA:
        body.update(config.PLACES_TEXT_SEARCH_BODY_EXTRA)
    return body


def build_nearby_search_body(point: Dict[str, Any], type_filter: Optional[str]) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "locationRestriction": {
            "circle": {
                "center": {"latitude": point["lat"], "longitude": point["lon"]},
                "radius": config.PLACES_LOCATION_BIAS_RADIUS_M,
            }
        },
    }
    if type_filter and config.PLACES_SUPPORTS_TYPE_FILTER:
        body["includedType"] = type_filter
    if config.PLACES_NEARBY_BODY_EXTRA:
        body.update(config.PLACES_NEARBY_BODY_EXTRA)
    return body


# Adapter/mapper for Places response fields

def parse_places_response(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    places = response.get("places") or []
    parsed: List[Dict[str, Any]] = []
    for p in places:
        place_id = p.get("id") or p.get("placeId")
        if not place_id:
            continue
        display = p.get("displayName")
        if isinstance(display, dict):
            name = display.get("text") or display.get("value")
        else:
            name = display
        rating = p.get("rating")
        user_rating_count = p.get("userRatingCount") or p.get("user_ratings_total")
        location = p.get("location") or p.get("latLng") or {}
        lat = location.get("latitude") or location.get("lat")
        lon = location.get("longitude") or location.get("lng") or location.get("lon")
        types = p.get("types") or []
        business_status = p.get("businessStatus") or p.get("business_status")
        parsed.append(
            {
                "place_id": place_id,
                "name": name,
                "rating": rating,
                "user_rating_count": int(user_rating_count) if user_rating_count is not None else None,
                "lat": lat,
                "lon": lon,
                "types": types,
                "business_status": business_status,
            }
        )
    return parsed
