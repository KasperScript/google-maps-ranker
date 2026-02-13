"""Output reporting helpers."""
from __future__ import annotations

import csv
import json
import os
import logging
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Protocol, TextIO, Tuple


class RequestCounters(Protocol):
    places_count: int
    routes_count: int


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _fsync_dir(path: str) -> None:
    try:
        dir_fd = os.open(path, os.O_DIRECTORY)
    except Exception:
        return
    try:
        os.fsync(dir_fd)
    except Exception:
        pass
    finally:
        os.close(dir_fd)


@contextmanager
def atomic_writer(
    path: str,
    mode: str = "w",
    encoding: str = "utf-8",
    newline: Optional[str] = None,
) -> Iterator[TextIO]:
    dir_path = os.path.dirname(path) or "."
    base = os.path.basename(path)
    fd, tmp_path = tempfile.mkstemp(prefix=f".{base}.", suffix=".tmp", dir=dir_path)
    try:
        with os.fdopen(fd, mode, encoding=encoding, newline=newline) as f:
            yield f
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(dir_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise


def atomic_write_text(path: str, text: str) -> None:
    with atomic_writer(path, mode="w", encoding="utf-8") as f:
        f.write(text)


def write_results_csv(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    fieldnames = [
        "place_id",
        "name",
        "rating",
        "user_rating_count",
        "lat",
        "lon",
        "business_status",
        "types",
        "quality_bayes",
        "quality_wilson",
        "quality",
        "transit_min_minutes",
        "transit_score",
        "final",
        "found_by_queries",
        "found_by_points",
        "rejected_reason",
    ]
    with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["types"] = json.dumps(out.get("types", []), ensure_ascii=False)
            out["found_by_queries"] = json.dumps(out.get("found_by_queries", []), ensure_ascii=False)
            out["found_by_points"] = json.dumps(out.get("found_by_points", []), ensure_ascii=False)
            writer.writerow(out)


def write_results_json(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    with atomic_writer(path, mode="w", encoding="utf-8") as f:
        json.dump(list(rows), f, ensure_ascii=False, indent=2)


def write_json_object(path: str, payload: Dict[str, Any]) -> None:
    with atomic_writer(path, mode="w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_list_mode_results_csv(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    fieldnames = [
        "place_id",
        "name",
        "lat",
        "lon",
        "rating",
        "user_rating_count",
        "business_status",
        "nearest_hub_id",
        "min_distance_km_to_any_hub",
        "quality",
        "rejected_reason",
        "found_by_points",
        "found_by_queries",
    ]
    with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["found_by_points"] = json.dumps(out.get("found_by_points", []), ensure_ascii=False)
            out["found_by_queries"] = json.dumps(out.get("found_by_queries", []), ensure_ascii=False)
            writer.writerow(out)


def write_radius_scan_results_csv(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    fieldnames = [
        "place_id",
        "name",
        "lat",
        "lon",
        "rating",
        "user_rating_count",
        "business_status",
        "distance_km_to_center",
        "quality",
        "rejected_reason",
        "found_by_points",
        "found_by_queries",
    ]
    with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["found_by_points"] = json.dumps(out.get("found_by_points", []), ensure_ascii=False)
            out["found_by_queries"] = json.dumps(out.get("found_by_queries", []), ensure_ascii=False)
            writer.writerow(out)


def write_radius_scan_merged_results_csv(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    fieldnames = [
        "place_id",
        "name",
        "lat",
        "lon",
        "rating",
        "user_rating_count",
        "business_status",
        "distance_km_by_center",
        "min_distance_km_to_any_center",
        "nearest_center_id",
        "centers_in_range",
        "quality",
        "rejected_reason",
        "found_by_points",
        "found_by_queries",
    ]
    with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["distance_km_by_center"] = json.dumps(
                out.get("distance_km_by_center", {}), ensure_ascii=False
            )
            out["centers_in_range"] = json.dumps(
                out.get("centers_in_range", []), ensure_ascii=False
            )
            out["found_by_points"] = json.dumps(out.get("found_by_points", []), ensure_ascii=False)
            out["found_by_queries"] = json.dumps(out.get("found_by_queries", []), ensure_ascii=False)
            writer.writerow(out)


def write_summary(path: str, summary_lines: List[str]) -> None:
    atomic_write_text(path, "\n".join(summary_lines))


def write_rejections_csv(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    fieldnames = [
        "place_id",
        "name",
        "rating",
        "user_rating_count",
        "lat",
        "lon",
        "reject_reason",
        "stage",
        "found_by_queries",
    ]
    with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["found_by_queries"] = json.dumps(
                out.get("found_by_queries", []), ensure_ascii=False
            )
            writer.writerow(out)


def write_rejections_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    with atomic_writer(path, mode="w", encoding="utf-8", newline="") as f:
        for row in rows:
            line = json.dumps(row, ensure_ascii=False)
            f.write(line)
            f.write("\n")


class ProgressReporter:
    def __init__(
        self,
        output_path: Optional[str],
        log_every: int = 50,
        write_interval_seconds: float = 5.0,
        logger: Optional[logging.Logger] = None,
        counters: Optional[RequestCounters] = None,
    ) -> None:
        self.output_path = output_path
        self.log_every = max(1, int(log_every)) if log_every else 0
        self.write_interval_seconds = float(write_interval_seconds)
        self.logger = logger or logging.getLogger(__name__)
        self._counters = counters
        self.stage = "init"
        self.processed_count = 0
        self.total_estimate: Optional[int] = None
        self.places_requests = 0
        self.routes_requests = 0
        self._next_log = self.log_every if self.log_every else 0
        self._last_write = 0.0

    def set_stage(self, stage: str, total_estimate: Optional[int] = None) -> None:
        self.stage = stage
        self.processed_count = 0
        self.total_estimate = total_estimate
        self._next_log = self.log_every if self.log_every else 0
        self._write_if_due(force=True)

    def set_counters(self, counters: Optional[RequestCounters]) -> None:
        self._counters = counters
        self._write_if_due(force=True)

    def advance(self, count: int = 1) -> None:
        if count <= 0:
            return
        self.processed_count += count
        places_requests, routes_requests = self._get_counts()
        if self.log_every and self.processed_count >= self._next_log:
            if self.total_estimate is None:
                self.logger.info(
                    "Progress: stage=%s processed=%s places_requests=%s routes_requests=%s",
                    self.stage,
                    self.processed_count,
                    places_requests,
                    routes_requests,
                )
            else:
                self.logger.info(
                    "Progress: stage=%s processed=%s/%s places_requests=%s routes_requests=%s",
                    self.stage,
                    self.processed_count,
                    self.total_estimate,
                    places_requests,
                    routes_requests,
                )
            self._next_log += self.log_every
        self._write_if_due()

    def on_request(self, _kind: str, places_count: int, routes_count: int) -> None:
        if self._counters is None:
            self.places_requests = places_count
            self.routes_requests = routes_count
        self._write_if_due()

    def flush(self) -> None:
        self._write_if_due(force=True)

    def _get_counts(self) -> Tuple[int, int]:
        if self._counters is not None:
            return (
                int(getattr(self._counters, "places_count", 0)),
                int(getattr(self._counters, "routes_count", 0)),
            )
        return (self.places_requests, self.routes_requests)

    def _write_if_due(self, force: bool = False) -> None:
        if not self.output_path:
            return
        now = time.monotonic()
        if not force and (now - self._last_write) < self.write_interval_seconds:
            return
        places_requests, routes_requests = self._get_counts()
        payload = {
            "stage": self.stage,
            "processed_count": self.processed_count,
            "total_estimate": self.total_estimate,
            "places_requests": places_requests,
            "routes_requests": routes_requests,
            "timestamp": utc_now_iso(),
        }
        with atomic_writer(self.output_path, mode="w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        self._last_write = now
