"""Routes API client with caching and response parsing."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from . import config
from .cache import Cache
from .http import HttpClient, RequestBudget, RequestMetrics


class RoutesClient:
    def __init__(
        self,
        http_client: HttpClient,
        cache: Cache,
        budget: RequestBudget,
        no_cache: bool = False,
        refresh_routes: bool = False,
        field_mask: str = config.ROUTES_FIELD_MASK_MIN,
        metrics: Optional[RequestMetrics] = None,
    ) -> None:
        self.http = http_client
        self.cache = cache
        self.budget = budget
        self.no_cache = no_cache
        self.refresh_routes = refresh_routes
        self.field_mask = field_mask
        self.metrics = metrics
        self._seen_cache_keys: set[str] = set()
        self._memory_cache: Dict[str, Optional[int]] = {}

    def compute_route_duration(
        self,
        origin_id: str,
        origin: Dict[str, Any],
        destination: Dict[str, Any],
        departure_time_rfc3339: str,
        mode: str = config.TRANSIT_MODE,
    ) -> Optional[int]:
        dep_bucket = _departure_bucket_from_rfc3339(
            departure_time_rfc3339, config.ROUTES_CACHE_DEPARTURE_BUCKET_MINUTES
        )
        cache_key = f"{origin_id}|{destination['place_id']}|{mode}|{dep_bucket}"
        if cache_key in self._seen_cache_keys:
            if self.metrics is not None:
                self.metrics.inc_dedup_skip("routes")
            if not self.no_cache and cache_key in self._memory_cache:
                return self._memory_cache[cache_key]
            return None
        if not self.no_cache and not self.refresh_routes:
            cached = self.cache.get_routes_cache(cache_key)
            if cached is not None:
                if self.metrics is not None:
                    self.metrics.inc_cache_hit("routes")
                self._seen_cache_keys.add(cache_key)
                self._memory_cache[cache_key] = cached
                return cached

        self._seen_cache_keys.add(cache_key)
        self.budget.consume("routes")
        body = build_routes_body(origin, destination, departure_time_rfc3339, mode)
        response = self.http.post_json(config.ROUTES_COMPUTE_URL, body, self.field_mask)
        duration = parse_routes_duration(response)
        if not self.no_cache:
            self.cache.set_routes_cache(cache_key, origin_id, destination["place_id"], mode, duration)
            self._memory_cache[cache_key] = duration
        return duration


def build_routes_body(
    origin: Dict[str, Any],
    destination: Dict[str, Any],
    departure_time_rfc3339: str,
    mode: str,
) -> Dict[str, Any]:
    body = {
        "origin": {
            "location": {
                "latLng": {
                    "latitude": origin["lat"],
                    "longitude": origin["lon"],
                }
            }
        },
        "destination": {
            "location": {
                "latLng": {
                    "latitude": destination["lat"],
                    "longitude": destination["lon"],
                }
            }
        },
        "travelMode": mode,
        "departureTime": departure_time_rfc3339,
    }
    if config.ROUTES_BODY_EXTRA:
        body.update(config.ROUTES_BODY_EXTRA)
    return body


def parse_routes_duration(response: Dict[str, Any]) -> Optional[int]:
    routes = response.get("routes") or []
    if not routes:
        return None
    duration = routes[0].get("duration")
    if duration is None:
        return None
    if isinstance(duration, str):
        # Typically in seconds, like "123s"
        if duration.endswith("s"):
            duration = duration[:-1]
        try:
            return int(float(duration))
        except ValueError:
            return None
    if isinstance(duration, (int, float)):
        return int(duration)
    return None


def compute_departure_time_rfc3339(
    policy: str = config.DEPARTURE_POLICY,
    now_utc: Optional[datetime] = None,
) -> str:
    if config.DEPARTURE_TIME_RFC3339_OVERRIDE:
        return config.DEPARTURE_TIME_RFC3339_OVERRIDE

    if policy != "next_weekday_17_00":
        raise ValueError(f"Unknown departure policy: {policy}")

    tz = ZoneInfo(config.WARSAW_TIMEZONE)
    now = (now_utc or datetime.now(timezone.utc)).astimezone(tz)
    target_time = time(17, 0)

    candidate = datetime.combine(now.date(), target_time, tz)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)

    while candidate.weekday() >= 5:  # 5=Sat, 6=Sun
        candidate = candidate + timedelta(days=1)

    utc_dt = candidate.astimezone(timezone.utc).replace(microsecond=0)
    return utc_dt.isoformat().replace("+00:00", "Z")


def bucket_departure_key(dt: datetime, bucket_minutes: int) -> str:
    if bucket_minutes <= 0:
        raise ValueError("bucket_minutes must be positive")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local = dt.astimezone(ZoneInfo(config.WARSAW_TIMEZONE))
    total_minutes = local.hour * 60 + local.minute
    bucket_start = (total_minutes // bucket_minutes) * bucket_minutes
    bucket_hour = bucket_start // 60
    bucket_minute = bucket_start % 60
    return f"wd{local.weekday()}_{bucket_hour:02d}{bucket_minute:02d}"


def _departure_bucket_from_rfc3339(departure_time_rfc3339: str, bucket_minutes: int) -> str:
    try:
        ts = departure_time_rfc3339.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(ts)
    except ValueError:
        return departure_time_rfc3339
    return bucket_departure_key(parsed, bucket_minutes)
