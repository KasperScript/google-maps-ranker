"""HTTP client with retry/backoff and request budgeting."""
from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import requests

logger = logging.getLogger(__name__)


class BudgetExceededError(RuntimeError):
    pass


@dataclass
class RequestMetrics:
    network_places: int = 0
    network_routes: int = 0
    cache_hits_places: int = 0
    cache_hits_routes: int = 0
    dedup_skips_places: int = 0
    dedup_skips_routes: int = 0

    @property
    def places_count(self) -> int:
        return self.network_places

    @property
    def routes_count(self) -> int:
        return self.network_routes

    def inc_network(self, kind: str) -> None:
        if kind == "places":
            self.network_places += 1
        elif kind == "routes":
            self.network_routes += 1
        else:
            raise ValueError(f"Unknown request kind: {kind}")

    def inc_cache_hit(self, kind: str) -> None:
        if kind == "places":
            self.cache_hits_places += 1
        elif kind == "routes":
            self.cache_hits_routes += 1
        else:
            raise ValueError(f"Unknown request kind: {kind}")

    def inc_dedup_skip(self, kind: str) -> None:
        if kind == "places":
            self.dedup_skips_places += 1
        elif kind == "routes":
            self.dedup_skips_routes += 1
        else:
            raise ValueError(f"Unknown request kind: {kind}")


class RequestBudget:
    def __init__(
        self,
        max_places: int,
        max_routes: int,
        on_consume: Optional[Callable[[str, int, int], None]] = None,
        metrics: Optional[RequestMetrics] = None,
    ) -> None:
        self.max_places = max_places
        self.max_routes = max_routes
        self.on_consume = on_consume
        self.metrics = metrics
        self._places_count = 0
        self._routes_count = 0

    @property
    def places_count(self) -> int:
        if self.metrics is not None:
            return int(self.metrics.network_places)
        return self._places_count

    @property
    def routes_count(self) -> int:
        if self.metrics is not None:
            return int(self.metrics.network_routes)
        return self._routes_count

    def consume(self, kind: str) -> None:
        if kind == "places":
            if self.places_count >= self.max_places:
                raise BudgetExceededError(
                    f"Places request budget exceeded: {self.places_count} >= {self.max_places}"
                )
            if self.metrics is not None:
                self.metrics.inc_network("places")
            else:
                self._places_count += 1
        elif kind == "routes":
            if self.routes_count >= self.max_routes:
                raise BudgetExceededError(
                    f"Routes request budget exceeded: {self.routes_count} >= {self.max_routes}"
                )
            if self.metrics is not None:
                self.metrics.inc_network("routes")
            else:
                self._routes_count += 1
        else:
            raise ValueError(f"Unknown budget kind: {kind}")
        if self.on_consume:
            self.on_consume(kind, self.places_count, self.routes_count)


class HttpClient:
    def __init__(
        self,
        api_key: str,
        timeout: int = 20,
        retry_max: int = 5,
        backoff_base: float = 0.5,
        backoff_max: float = 8.0,
    ) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.retry_max = retry_max
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.session = requests.Session()

    def post_json(
        self,
        url: str,
        body: Dict[str, Any],
        field_mask: str,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        headers = {
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": field_mask,
            "Content-Type": "application/json",
        }
        if extra_headers:
            headers.update(extra_headers)

        payload = json.dumps(body)
        for attempt in range(1, self.retry_max + 1):
            try:
                resp = self.session.post(url, data=payload, headers=headers, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt >= self.retry_max:
                    raise
                self._sleep_backoff(attempt)
                continue

            status = resp.status_code
            if status == 200:
                try:
                    return resp.json()
                except ValueError:
                    logger.error("Non-JSON response from %s", url)
                    raise

            if status in (429, 500, 502, 503, 504):
                logger.warning("HTTP %s from %s (attempt %s)", status, url, attempt)
                if attempt >= self.retry_max:
                    resp.raise_for_status()
                if not self._sleep_retry_after(resp):
                    self._sleep_backoff(attempt)
                continue

            # Non-retryable
            logger.error("HTTP %s from %s", status, url)
            resp.raise_for_status()

        raise RuntimeError("Unexpected HTTP retry loop exit")

    def _sleep_backoff(self, attempt: int) -> None:
        base = min(self.backoff_base * (2 ** (attempt - 1)), self.backoff_max)
        jitter = random.uniform(0, self.backoff_base)
        time.sleep(base + jitter)

    def _sleep_retry_after(self, resp: requests.Response) -> bool:
        retry_after = resp.headers.get("Retry-After")
        if not retry_after:
            return False
        try:
            delay = float(retry_after)
        except ValueError:
            return False
        delay = max(0.0, min(delay, self.backoff_max))
        time.sleep(delay)
        return True
