"""Pipeline orchestration."""
from __future__ import annotations

import math
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from . import config
from .cache import Cache
from .coverage import (
    build_grid,
    compute_uplift,
    pairwise_jaccard,
    unique_contributions,
)
from .geo import haversine_km
from .http import BudgetExceededError, HttpClient, RequestBudget, RequestMetrics
from .places_client import PlacesClient
from .reporting import (
    ProgressReporter,
    ensure_dir,
    write_rejections_csv,
    write_rejections_jsonl,
    write_results_csv,
    write_results_json,
    write_json_object,
    write_list_mode_results_csv,
    write_radius_scan_results_csv,
    write_radius_scan_merged_results_csv,
    write_summary,
)
from .routes_client import RoutesClient, compute_departure_time_rfc3339
from .scoring import quality_score

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    results: List[Dict[str, Any]]
    all_places: List[Dict[str, Any]]
    summary: Dict[str, Any]


@dataclass
class CoverageBudgetTracker:
    cap: int
    consumed: int = 0

    def remaining(self, budget: RequestBudget) -> int:
        remaining_cap = self.cap - self.consumed
        remaining_global = budget.max_places - budget.places_count
        return max(0, min(remaining_cap, remaining_global))


def run(
    api_key: Optional[str],
    cache_db_path: str = config.CACHE_DB_PATH,
    max_places: int = config.MAX_PLACES_REQUESTS_PER_RUN,
    max_routes: int = config.MAX_ROUTES_REQUESTS_PER_RUN,
    no_cache: bool = False,
    refresh_routes: bool = False,
    top_n: int = config.TOP_N_QUALITY,
    output_dir: str = config.OUTPUT_DIR,
    write_outputs: bool = True,
    places_client: Optional[PlacesClient] = None,
    routes_client: Optional[RoutesClient] = None,
    skip_routes: bool = False,
    coverage_mode: str = "light",
    coverage_budget_share: float = 0.20,
    metrics: Optional[RequestMetrics] = None,
    dedup_probe: bool = False,
    list_mode: bool = False,
    radius_scan: bool = False,
    radius_scan_center_lat: Optional[float] = None,
    radius_scan_center_lon: Optional[float] = None,
    radius_scan_center_id: Optional[str] = None,
    radius_scan_radius_km: Optional[float] = None,
    radius_scan_grid_step_km: Optional[float] = None,
    radius_scan_scan_radius_m: Optional[int] = None,
    radius_scan_queries: Optional[List[str]] = None,
    radius_scan_types: Optional[List[str]] = None,
    radius_scan_max_pages: Optional[int] = None,
    radius_scan_centers: Optional[List[str]] = None,
    refresh_places: bool = False,
) -> PipelineResult:
    validate_hubs(config.HUBS)

    if metrics is None:
        metrics = RequestMetrics()

    if write_outputs:
        ensure_dir(output_dir)

    progress = ProgressReporter(
        output_path=f"{output_dir}/progress.json" if write_outputs else None,
        log_every=config.PROGRESS_LOG_EVERY,
        write_interval_seconds=config.PROGRESS_WRITE_INTERVAL_SECONDS,
        logger=logger,
        counters=metrics,
    )

    coverage_mode = (coverage_mode or "light").lower()
    if coverage_mode not in {"off", "light", "full"}:
        raise ValueError("coverage_mode must be one of: off, light, full")
    if coverage_budget_share < 0.0 or coverage_budget_share > 1.0:
        raise ValueError("coverage_budget_share must be between 0 and 1")

    cache = Cache(cache_db_path)
    coverage_cap = 0
    if coverage_mode != "off":
        coverage_cap = max(0, int(math.floor(max_places * coverage_budget_share)))
    coverage_budget = CoverageBudgetTracker(cap=coverage_cap)
    coverage_active = False

    def on_consume(kind: str, places_count: int, routes_count: int) -> None:
        progress.on_request(kind, places_count, routes_count)
        if coverage_active and kind == "places":
            coverage_budget.consumed += 1

    budget = RequestBudget(
        max_places=max_places,
        max_routes=max_routes,
        on_consume=on_consume,
        metrics=metrics,
    )
    progress.set_counters(metrics)
    skip_routes = skip_routes or max_routes == 0

    def attach_budget(client: Optional[object]) -> None:
        if client is None:
            return
        setter = getattr(client, "set_budget", None)
        if callable(setter):
            try:
                setter(budget)
                return
            except Exception:
                pass
        if hasattr(client, "budget"):
            try:
                client.budget = budget
            except Exception:
                pass

    def attach_metrics(client: Optional[object]) -> None:
        if client is None:
            return
        setter = getattr(client, "set_metrics", None)
        if callable(setter):
            try:
                setter(metrics)
                return
            except Exception:
                pass
        if hasattr(client, "metrics"):
            try:
                client.metrics = metrics
            except Exception:
                pass

    attach_budget(places_client)
    attach_budget(routes_client)
    attach_metrics(places_client)
    attach_metrics(routes_client)

    if places_client is None or routes_client is None:
        if not api_key:
            raise ValueError("API key is required when using real API clients")
        http_client = HttpClient(
            api_key,
            timeout=config.HTTP_TIMEOUT_SECONDS,
            retry_max=config.HTTP_RETRY_MAX,
            backoff_base=config.HTTP_BACKOFF_BASE,
            backoff_max=config.HTTP_BACKOFF_MAX,
        )
        if places_client is None:
            places_client = PlacesClient(
                http_client,
                cache,
                budget,
                no_cache=no_cache,
                refresh_places=refresh_places,
                metrics=metrics,
            )
        if routes_client is None:
            routes_client = RoutesClient(
                http_client,
                cache,
                budget,
                no_cache=no_cache,
                refresh_routes=refresh_routes,
                metrics=metrics,
            )

    if dedup_probe:
        hub_id = sorted(config.HUBS.keys())[0]
        hub = config.HUBS[hub_id]
        point = {"id": hub_id, "name": hub["name"], "lat": hub["lat"], "lon": hub["lon"]}
        query = config.PRIMARY_QUERIES[0] if config.PRIMARY_QUERIES else "place"
        places_client.search_text(query, point, type_filter=None)
        places_client.search_text(query, point, type_filter=None)
        departure_time = compute_departure_time_rfc3339()
        dest = {"place_id": "dedup_probe", "lat": hub["lat"], "lon": hub["lon"]}
        routes_client.compute_route_duration(
            hub_id, hub, dest, departure_time, mode=config.TRANSIT_MODE
        )
        routes_client.compute_route_duration(
            hub_id, hub, dest, departure_time, mode=config.TRANSIT_MODE
        )

    hubs = hubs_list(config.HUBS)
    points_by_id = {p["id"]: p for p in hubs}

    # Queries and query-level cost controls
    ortho_queries = list(dict.fromkeys(config.ORTHO_QUERIES))
    general_queries = list(dict.fromkeys(config.GENERAL_QUERIES))
    ortho_query_set = set(ortho_queries)
    general_query_set = set(general_queries)
    hubs_only_queries = set(config.HUBS_ONLY_QUERIES) | set(general_queries)
    query_max_pages_overrides = dict(config.QUERY_MAX_PAGES_OVERRIDES)
    for query in general_queries:
        query_max_pages_overrides.setdefault(query, config.GENERAL_MAX_PAGES_PER_QUERY)
    ortho_grid_queries = [q for q in ortho_queries if q not in hubs_only_queries]

    # Harvest stores
    places_by_id: Dict[str, Dict[str, Any]] = {}
    found_by_sets: Dict[str, Set[Tuple[str, str, str]]] = {}
    results_by_query: Dict[str, Set[str]] = {}
    results_by_point: Dict[str, Set[str]] = {}
    results_by_query_point: Dict[Tuple[str, str], Set[str]] = {}
    results_by_group: Dict[str, Set[str]] = {}

    def record_place(place: Dict[str, Any], query: str, point_id: str, mode: str) -> None:
        place_id = place["place_id"]
        if query in ortho_query_set:
            group = "ortho"
        elif query in general_query_set:
            group = "general"
        else:
            group = "other"
        if place_id not in places_by_id:
            place_copy = dict(place)
            place_copy["found_by"] = []
            place_copy["found_by_groups"] = set()
            places_by_id[place_id] = place_copy
            found_by_sets[place_id] = set()
        else:
            place_copy = places_by_id[place_id]
            for key in ("name", "rating", "user_rating_count", "lat", "lon", "types", "business_status"):
                if place.get(key) is not None:
                    place_copy[key] = place.get(key)

        place_copy.setdefault("found_by_groups", set()).add(group)
        fb_key = (query, point_id, mode)
        if fb_key not in found_by_sets[place_id]:
            found_by_sets[place_id].add(fb_key)
            places_by_id[place_id]["found_by"].append(
                {"query": query, "point_id": point_id, "mode": mode, "group": group}
            )

        results_by_query.setdefault(query, set()).add(place_id)
        results_by_point.setdefault(point_id, set()).add(place_id)
        results_by_query_point.setdefault((query, point_id), set()).add(place_id)
        results_by_group.setdefault(group, set()).add(place_id)

        cache.upsert_place(places_by_id[place_id])

    def harvest(
        points: Iterable[Dict[str, Any]],
        queries: List[str],
        progress_reporter: Optional[ProgressReporter] = None,
        type_filters: Optional[List[Optional[str]]] = None,
        max_pages: Optional[int] = None,
        per_query_max_pages: Optional[Dict[str, int]] = None,
        coverage_budget_guard: Optional[CoverageBudgetTracker] = None,
        budget_guard: Optional[RequestBudget] = None,
    ) -> bool:
        stopped_early = False
        if type_filters is None:
            type_filters = [None]
            if config.PLACES_SUPPORTS_TYPE_FILTER:
                type_filters.extend(config.PLACES_TYPE_FILTERS)
        max_pages_default = (
            config.PLACES_MAX_PAGES_PER_QUERY if max_pages is None else max_pages
        )

        for point in sorted(points, key=lambda x: x["id"]):
            for query in queries:
                query_max_pages = max_pages_default
                if per_query_max_pages:
                    override = per_query_max_pages.get(query)
                    if override is not None:
                        query_max_pages = override
                try:
                    query_max_pages = int(query_max_pages)
                except (TypeError, ValueError):
                    query_max_pages = int(max_pages_default)
                query_max_pages = max(1, query_max_pages)
                for type_filter in type_filters:
                    if coverage_budget_guard and budget_guard:
                        remaining = coverage_budget_guard.remaining(budget_guard)
                        if remaining <= 0:
                            stopped_early = True
                            break
                        max_pages_local = min(query_max_pages, remaining)
                    else:
                        max_pages_local = query_max_pages
                    mode = "text" if not type_filter else f"text+type:{type_filter}"
                    try:
                        places = places_client.search_text_all(
                            query,
                            point,
                            type_filter=type_filter,
                            max_pages=max_pages_local,
                        )
                    except BudgetExceededError:
                        if coverage_budget_guard:
                            stopped_early = True
                            break
                        raise
                    for place in places:
                        record_place(place, query, point["id"], mode)
                    progress.advance()
                if stopped_early:
                    break
            if stopped_early:
                break

        return stopped_early

    if radius_scan:
        radius_km = (
            float(radius_scan_radius_km)
            if radius_scan_radius_km is not None
            else float(config.MAX_DISTANCE_KM)
        )
        grid_step_km = (
            float(radius_scan_grid_step_km)
            if radius_scan_grid_step_km is not None
            else 2.0
        )
        scan_radius_m = (
            int(radius_scan_scan_radius_m)
            if radius_scan_scan_radius_m is not None
            else 1500
        )
        scan_queries = radius_scan_queries
        if scan_queries is None:
            scan_queries = list(dict.fromkeys(config.ORTHO_QUERIES + config.GENERAL_QUERIES))
        scan_types = radius_scan_types
        if scan_types is None:
            scan_types = list(config.PLACES_TYPE_FILTERS)
        type_filters: List[Optional[str]] = [None]
        if config.PLACES_SUPPORTS_TYPE_FILTER and scan_types:
            type_filters.extend(scan_types)
        max_pages = int(radius_scan_max_pages) if radius_scan_max_pages is not None else 1
        max_pages = max(1, max_pages)

        def run_radius_scan_center(
            center_id: str,
            center_lat: float,
            center_lon: float,
            center_output_dir: Optional[str],
        ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
            places_by_id_local: Dict[str, Dict[str, Any]] = {}
            found_by_sets_local: Dict[str, set] = {}

            def record_place_local(place: Dict[str, Any], query: str, point_id: str, mode: str) -> None:
                place_id = place.get("place_id")
                if not place_id:
                    return
                if place_id not in places_by_id_local:
                    place_copy = dict(place)
                    place_copy["found_by"] = []
                    places_by_id_local[place_id] = place_copy
                    found_by_sets_local[place_id] = set()
                else:
                    place_copy = places_by_id_local[place_id]
                    for key in (
                        "name",
                        "rating",
                        "user_rating_count",
                        "lat",
                        "lon",
                        "types",
                        "business_status",
                    ):
                        if place.get(key) is not None:
                            place_copy[key] = place.get(key)

                fb_key = (query, point_id, mode)
                if fb_key not in found_by_sets_local[place_id]:
                    found_by_sets_local[place_id].add(fb_key)
                    place_copy["found_by"].append(
                        {"query": query, "point_id": point_id, "mode": mode}
                    )
                cache.upsert_place(place_copy)

            logger.info("Stage 1: radius scan (%s)", center_id)
            scan_points = build_radius_scan_points(center_lat, center_lon, radius_km, grid_step_km)
            for idx, point in enumerate(scan_points):
                point["id"] = f"{center_id}_{idx}"

            total_estimate = len(scan_points) * len(scan_queries) * len(type_filters)
            progress.set_stage(f"radius_scan_{center_id}", total_estimate=total_estimate)

            budget_exceeded = False
            for point in scan_points:
                if budget_exceeded:
                    break
                for query in scan_queries:
                    if budget_exceeded:
                        break
                    for type_filter in type_filters:
                        try:
                            places = places_client.search_text_all(
                                query,
                                point,
                                type_filter=type_filter,
                                max_pages=max_pages,
                                radius_m=scan_radius_m,
                            )
                        except BudgetExceededError:
                            budget_exceeded = True
                            break
                        mode = "text" if not type_filter else f"text+type:{type_filter}"
                        for place in places:
                            record_place_local(place, query, point["id"], mode)
                        progress.advance()

            logger.info("Stage 2: radius scan filters (%s)", center_id)
            progress.set_stage("filters", total_estimate=len(places_by_id_local))
            filtered_places, rejection_counts = apply_radius_scan_filters(
                places_by_id_local,
                center_lat,
                center_lon,
                radius_km,
                progress_reporter=progress,
            )

            logger.info("Stage 3: radius scan quality scoring (%s)", center_id)
            progress.set_stage("quality", total_estimate=len(filtered_places))
            accepted_places_sorted = compute_quality_all(
                filtered_places, progress_reporter=progress, sort_key=list_mode_sort_key
            )

            accepted_rows_sorted = [build_radius_scan_row(place) for place in accepted_places_sorted]
            rejected_rows = [
                build_radius_scan_row(place)
                for place in places_by_id_local.values()
                if place.get("rejected_reason")
            ]
            rejected_rows_sorted = sorted(
                rejected_rows,
                key=lambda r: (
                    (r.get("rejected_reason") or ""),
                    (r.get("name") or ""),
                    (r.get("place_id") or ""),
                ),
            )
            all_rows_sorted = accepted_rows_sorted + rejected_rows_sorted

            summary = {
                "unique_place_ids": len(places_by_id_local),
                "accepted": len(accepted_rows_sorted),
                "rejected": len(rejected_rows_sorted),
                "budget_exceeded": budget_exceeded,
                "places_requests": metrics.network_places,
                "routes_requests": metrics.network_routes,
                "cache_hits_places": metrics.cache_hits_places,
                "cache_hits_routes": metrics.cache_hits_routes,
                "dedup_skips_places": metrics.dedup_skips_places,
                "dedup_skips_routes": metrics.dedup_skips_routes,
                "rejection_counts": rejection_counts,
                "scan_points": len(scan_points),
                "scan_queries": scan_queries,
                "scan_types": scan_types,
                "scan_radius_m": scan_radius_m,
                "radius_km": radius_km,
                "grid_step_km": grid_step_km,
                "center_id": center_id,
                "top20": [
                    {
                        "place_id": r.get("place_id"),
                        "name": r.get("name"),
                        "quality": r.get("quality"),
                    }
                    for r in accepted_rows_sorted[:20]
                ],
            }

            if write_outputs and center_output_dir:
                ensure_dir(center_output_dir)
                logger.info("Stage 5: radius scan outputs (%s)", center_id)
                progress.set_stage("outputs", total_estimate=3)
                write_radius_scan_results_csv(
                    f"{center_output_dir}/radius_scan_results.csv", all_rows_sorted
                )
                progress.advance()
                write_results_json(f"{center_output_dir}/radius_scan_results.json", all_rows_sorted)
                progress.advance()
                summary_lines = render_radius_scan_summary(summary)
                write_summary(f"{center_output_dir}/radius_scan_summary.txt", summary_lines)
                progress.advance()

            return places_by_id_local, summary, all_rows_sorted

        if radius_scan_centers:
            centers: List[Tuple[str, float, float]] = []
            for center_id in radius_scan_centers:
                hub = config.HUBS.get(center_id)
                if not hub:
                    raise ValueError(f"Unknown center id: {center_id}")
                centers.append((center_id, float(hub["lat"]), float(hub["lon"])))
        else:
            if radius_scan_center_lat is None or radius_scan_center_lon is None:
                raise ValueError("radius_scan requires center lat/lon")
            center_id = radius_scan_center_id or "center"
            centers = [(center_id, float(radius_scan_center_lat), float(radius_scan_center_lon))]

        merged_places: Dict[str, Dict[str, Any]] = {}
        per_center_uniques: Dict[str, int] = {}
        per_center_budget: Dict[str, bool] = {}
        overall_budget_exceeded = False

        multi_center = len(centers) > 1
        for center_id, center_lat, center_lon in centers:
            if write_outputs:
                center_dir = (
                    f"{output_dir}/by_center/{center_id}" if multi_center else output_dir
                )
            else:
                center_dir = None
            places_local, summary_local, _ = run_radius_scan_center(
                center_id, center_lat, center_lon, center_dir
            )
            per_center_uniques[center_id] = int(summary_local.get("unique_place_ids", 0))
            per_center_budget[center_id] = bool(summary_local.get("budget_exceeded"))
            overall_budget_exceeded = overall_budget_exceeded or per_center_budget[center_id]
            for place_id, place in places_local.items():
                merged = merged_places.get(place_id)
                if merged is None:
                    merged = dict(place)
                    merged["found_by"] = list(place.get("found_by") or [])
                    merged_places[place_id] = merged
                else:
                    for key in (
                        "name",
                        "rating",
                        "user_rating_count",
                        "lat",
                        "lon",
                        "types",
                        "business_status",
                    ):
                        if merged.get(key) is None and place.get(key) is not None:
                            merged[key] = place.get(key)
                    merged.setdefault("found_by", [])
                    merged["found_by"].extend(place.get("found_by") or [])
                dist_map = merged.setdefault("distance_km_by_center", {})
                dist_map[center_id] = place.get("distance_km_to_center")

        rejection_counts: Dict[str, int] = {}
        eligible: List[Dict[str, Any]] = []
        for place in merged_places.values():
            dist_map = place.get("distance_km_by_center") or {}
            valid_dists = {k: v for k, v in dist_map.items() if v is not None}
            if valid_dists:
                nearest_center_id = min(valid_dists, key=lambda k: valid_dists[k])
                min_dist = valid_dists[nearest_center_id]
                centers_in_range = [k for k, v in valid_dists.items() if v <= radius_km]
            else:
                nearest_center_id = None
                min_dist = None
                centers_in_range = []

            place["nearest_center_id"] = nearest_center_id
            place["min_distance_km_to_any_center"] = min_dist
            place["centers_in_range"] = centers_in_range

            reason = None
            if place.get("rating") is None:
                reason = "rating_missing"
            elif place.get("user_rating_count") is None:
                reason = "missing_user_rating_count"
            elif place["user_rating_count"] < config.MIN_USER_RATING_COUNT:
                reason = "insufficient_reviews"
            elif place.get("lat") is None or place.get("lon") is None:
                reason = "missing_location"
            elif not centers_in_range:
                reason = "too_far"

            if reason is None:
                status = place.get("business_status")
                if status is not None and status != config.BUSINESS_STATUS_OPERATIONAL:
                    reason = "business_status_not_operational"

            if reason:
                place["rejected_reason"] = reason
                place["rejected_stage"] = "radius_scan_merge"
                rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
            else:
                eligible.append(place)

        accepted_sorted = compute_quality_all(
            eligible, progress_reporter=None, sort_key=quality_sort_key_full
        )

        accepted_rows_sorted = [build_radius_scan_merged_row(place) for place in accepted_sorted]
        rejected_rows = [
            build_radius_scan_merged_row(place)
            for place in merged_places.values()
            if place.get("rejected_reason")
        ]
        rejected_rows_sorted = sorted(
            rejected_rows,
            key=lambda r: (
                (r.get("rejected_reason") or ""),
                (r.get("name") or ""),
                (r.get("place_id") or ""),
            ),
        )
        all_rows_sorted = accepted_rows_sorted + rejected_rows_sorted

        merged_summary = {
            "total_unique_place_ids": len(merged_places),
            "eligible_count": len(accepted_rows_sorted),
            "rejected_count": len(rejected_rows_sorted),
            "rejection_counts": rejection_counts,
            "per_center_unique_counts": per_center_uniques,
            "per_center_budget_exceeded": per_center_budget,
            "budget_exceeded": overall_budget_exceeded,
            "places_requests": metrics.network_places,
            "routes_requests": metrics.network_routes,
            "cache_hits_places": metrics.cache_hits_places,
            "cache_hits_routes": metrics.cache_hits_routes,
            "dedup_skips_places": metrics.dedup_skips_places,
            "dedup_skips_routes": metrics.dedup_skips_routes,
            "top50": [
                {
                    "place_id": r.get("place_id"),
                    "name": r.get("name"),
                    "quality": r.get("quality"),
                    "min_distance_km_to_any_center": r.get("min_distance_km_to_any_center"),
                    "nearest_center_id": r.get("nearest_center_id"),
                }
                for r in accepted_rows_sorted[:50]
            ],
        }

        if write_outputs:
            logger.info("Stage 5: radius scan merged outputs")
            progress.set_stage("outputs", total_estimate=3)
            write_radius_scan_merged_results_csv(
                f"{output_dir}/radius_scan_merged_results.csv", all_rows_sorted
            )
            progress.advance()
            write_results_json(
                f"{output_dir}/radius_scan_merged_results.json", all_rows_sorted
            )
            progress.advance()
            summary_lines = render_radius_scan_merged_summary(merged_summary)
            write_summary(f"{output_dir}/radius_scan_merged_summary.txt", summary_lines)
            progress.advance()

        progress.flush()
        cache.close()

        return PipelineResult(
            results=accepted_rows_sorted, all_places=all_rows_sorted, summary=merged_summary
        )

    # Stage 1: Harvest ortho queries from hubs
    logger.info("Stage 1: harvest (hubs, ortho-first)")
    type_filters = [None]
    if config.PLACES_SUPPORTS_TYPE_FILTER:
        type_filters.extend(config.PLACES_TYPE_FILTERS)
    harvest_total = len(hubs) * len(ortho_queries) * len(type_filters)
    progress.set_stage("harvest_hubs", total_estimate=harvest_total)
    harvest(
        hubs,
        ortho_queries,
        progress_reporter=progress,
        type_filters=type_filters,
        per_query_max_pages=query_max_pages_overrides,
    )

    # Stage 1b: Coverage sanity-check (optional)
    coverage_stats: Dict[str, Any] = {
        "u_hubs": 0,
        "u_grid_total": 0,
        "u_grid_new": 0,
        "u_union_total": 0,
        "uplift": 0.0,
        "grid_size": 0,
        "grid_points": [],
    }
    coverage_skipped = False
    coverage_stopped_early = False

    coverage_queries = list(config.COVERAGE_QUERIES)
    coverage_max_pages_per_query = config.COVERAGE_MAX_PAGES_PER_QUERY
    coverage_grid_size_initial = config.COVERAGE_CONFIG.grid_size_initial
    coverage_grid_max_iterations = config.COVERAGE_CONFIG.grid_max_iterations
    coverage_harvest_queries = ortho_grid_queries
    coverage_harvest_type_filters: Optional[List[Optional[str]]] = None
    coverage_harvest_max_pages: Optional[int] = None

    if coverage_mode == "light":
        coverage_queries = list(config.COVERAGE_QUERIES[:2]) if config.COVERAGE_QUERIES else []
        coverage_max_pages_per_query = 1
        coverage_grid_size_initial = 3
        coverage_grid_max_iterations = 0
        coverage_harvest_queries = coverage_queries
        coverage_harvest_type_filters = [None]
        coverage_harvest_max_pages = 1

    # Hubs-only queries must not be used for grid coverage checks/harvest.
    coverage_queries = [q for q in coverage_queries if q not in hubs_only_queries]
    coverage_harvest_queries = [q for q in coverage_harvest_queries if q not in hubs_only_queries]

    if coverage_mode == "off":
        coverage_skipped = True
    elif coverage_cap <= 0:
        coverage_skipped = True
        coverage_stopped_early = True
    elif not coverage_queries:
        coverage_skipped = True
    else:
        logger.info("Stage 1b: coverage check")
        progress.set_stage("coverage_check")
        coverage_active = True
        coverage_stats = coverage_check(
            places_client,
            hubs,
            record_place,
            results_by_query_point,
            coverage_queries=coverage_queries,
            max_pages_per_query=coverage_max_pages_per_query,
            grid_size_initial=coverage_grid_size_initial,
            grid_max_iterations=coverage_grid_max_iterations,
            coverage_budget=coverage_budget,
            budget=budget,
            progress_reporter=progress,
        )
        coverage_active = False
        coverage_stopped_early = bool(coverage_stats.get("stopped_early"))

    # Stage 1c: Harvest ortho queries from grid when coverage is enabled.
    coverage_enabled = coverage_mode != "off" and coverage_cap > 0
    if coverage_enabled and coverage_budget.remaining(budget) > 0 and coverage_harvest_queries:
        grid_points = coverage_stats.get("grid_points") or build_grid(
            config.WARSAW_BBOX, coverage_grid_size_initial
        )
        if grid_points:
            logger.info("Stage 1c: harvest (grid, ortho)")
            harvest_total = len(grid_points) * len(coverage_harvest_queries)
            if coverage_harvest_type_filters is None and config.PLACES_SUPPORTS_TYPE_FILTER:
                harvest_total *= (1 + len(config.PLACES_TYPE_FILTERS))
            progress.set_stage("harvest_grid", total_estimate=harvest_total)
            coverage_active = True
            grid_stopped = harvest(
                grid_points,
                coverage_harvest_queries,
                progress_reporter=progress,
                type_filters=coverage_harvest_type_filters,
                max_pages=coverage_harvest_max_pages,
                per_query_max_pages=query_max_pages_overrides,
                coverage_budget_guard=coverage_budget,
                budget_guard=budget,
            )
            coverage_active = False
            if grid_stopped:
                coverage_stopped_early = True

    def preview_filtered_count() -> int:
        preview_places_by_id = {pid: dict(place) for pid, place in places_by_id.items()}
        filtered_preview, _ = apply_filters(preview_places_by_id, hubs)
        return len(filtered_preview)

    # Stage 1d: General dentistry fallback (after Stage 2 preview on ortho results)
    candidate_count_after_ortho = preview_filtered_count()
    run_general_queries = (
        bool(general_queries) and candidate_count_after_ortho < config.MIN_CANDIDATES
    )
    if run_general_queries:
        logger.info(
            "Stage 1d: harvest (general fallback, candidates=%s < %s)",
            candidate_count_after_ortho,
            config.MIN_CANDIDATES,
        )
        general_per_query_max_pages = {
            q: config.GENERAL_MAX_PAGES_PER_QUERY for q in general_queries
        }
        harvest_total = len(hubs) * len(general_queries)
        progress.set_stage("harvest_general_hubs", total_estimate=harvest_total)
        harvest(
            hubs,
            general_queries,
            progress_reporter=progress,
            type_filters=[None],
            max_pages=config.GENERAL_MAX_PAGES_PER_QUERY,
            per_query_max_pages=general_per_query_max_pages,
        )

        candidate_count_after_general_hubs = preview_filtered_count()
        general_grid_allowed = coverage_mode != "off" and coverage_budget.remaining(budget) > 0
        general_grid_needed = (
            candidate_count_after_general_hubs < config.MIN_CANDIDATES and general_grid_allowed
        )
        if general_grid_needed:
            grid_points_general = build_grid(config.WARSAW_BBOX, coverage_grid_size_initial)
            harvest_total = len(grid_points_general) * len(general_queries)
            progress.set_stage("harvest_general_grid", total_estimate=harvest_total)
            coverage_active = True
            grid_stopped = harvest(
                grid_points_general,
                general_queries,
                progress_reporter=progress,
                type_filters=[None],
                max_pages=config.GENERAL_MAX_PAGES_PER_QUERY,
                per_query_max_pages=general_per_query_max_pages,
                coverage_budget_guard=coverage_budget,
                budget_guard=budget,
            )
            coverage_active = False
            if grid_stopped:
                coverage_stopped_early = True
        else:
            logger.info(
                "Stage 1d: general grid harvest skipped (candidates=%s >= %s or coverage disabled)",
                candidate_count_after_general_hubs,
                config.MIN_CANDIDATES,
            )
    elif general_queries:
        logger.info(
            "Stage 1d: general harvest skipped (candidates=%s >= %s)",
            candidate_count_after_ortho,
            config.MIN_CANDIDATES,
        )

    # Finalize coverage stats after all coverage-budgeted work
    if coverage_mode != "off":
        cap_reached = coverage_budget.cap == 0 or coverage_budget.consumed >= coverage_budget.cap
    else:
        cap_reached = False

    coverage_stats = dict(coverage_stats)
    coverage_stats.update(
        {
            "mode": coverage_mode,
            "skipped": coverage_skipped,
            "stopped_early": coverage_stopped_early,
            "coverage_cap": coverage_budget.cap,
            "coverage_consumed": coverage_budget.consumed,
            "cap_reached": cap_reached,
        }
    )

    # Clear stale rejection fields before the real Stage 2 pass
    for place in places_by_id.values():
        place.pop("rejected_reason", None)
        place.pop("rejected_stage", None)
        place.pop("distance_km", None)

    if list_mode:
        # Stage 2: List-mode filters
        logger.info("Stage 2: list-mode filters")
        progress.set_stage("filters", total_estimate=len(places_by_id))
        filtered_places, rejection_counts = apply_list_mode_filters(
            places_by_id, hubs, progress_reporter=progress
        )

        # Stage 3: List-mode quality scoring
        logger.info("Stage 3: list-mode quality scoring")
        progress.set_stage("quality", total_estimate=len(filtered_places))
        accepted_places_sorted = compute_quality_all(
            filtered_places, progress_reporter=progress
        )

        # Build list-mode rows
        accepted_rows_sorted = [build_list_mode_row(place) for place in accepted_places_sorted]
        rejected_rows = [
            build_list_mode_row(place)
            for place in places_by_id.values()
            if place.get("rejected_reason")
        ]
        rejected_rows_sorted = sorted(
            rejected_rows,
            key=lambda r: (
                (r.get("rejected_reason") or ""),
                (r.get("name") or ""),
                (r.get("place_id") or ""),
            ),
        )
        all_rows_sorted = accepted_rows_sorted + rejected_rows_sorted

        summary = {
            "places_requests": metrics.network_places,
            "routes_requests": metrics.network_routes,
            "cache_hits_places": metrics.cache_hits_places,
            "cache_hits_routes": metrics.cache_hits_routes,
            "dedup_skips_places": metrics.dedup_skips_places,
            "dedup_skips_routes": metrics.dedup_skips_routes,
            "routes_skipped": True,
            "coverage": coverage_stats,
            "rejection_counts": rejection_counts,
            "list_mode": True,
            "list_mode_total": len(all_rows_sorted),
            "list_mode_accepted": len(accepted_rows_sorted),
            "list_mode_rejected": len(rejected_rows_sorted),
            "top20": [
                {
                    "place_id": r.get("place_id"),
                    "name": r.get("name"),
                    "quality": r.get("quality"),
                }
                for r in accepted_rows_sorted[:20]
            ],
        }

        if write_outputs:
            logger.info("Stage 5: list-mode outputs")
            progress.set_stage("outputs", total_estimate=3)
            write_list_mode_results_csv(f"{output_dir}/list_mode_results.csv", all_rows_sorted)
            progress.advance()
            write_results_json(f"{output_dir}/list_mode_results.json", all_rows_sorted)
            progress.advance()
            summary_lines = render_list_mode_summary(summary)
            write_summary(f"{output_dir}/list_mode_summary.txt", summary_lines)
            progress.advance()

        progress.flush()
        cache.close()

        return PipelineResult(
            results=accepted_rows_sorted, all_places=all_rows_sorted, summary=summary
        )

    # Stage 2: Local filters
    logger.info("Stage 2: filters")
    progress.set_stage("filters", total_estimate=len(places_by_id))
    filtered_places, rejection_counts = apply_filters(places_by_id, hubs, progress_reporter=progress)

    # Stage 3: Quality scoring and shortlist
    logger.info("Stage 3: quality scoring")
    progress.set_stage("quality", total_estimate=len(filtered_places))
    shortlist = compute_quality(filtered_places, top_n=top_n, progress_reporter=progress)

    # Stage 4: Transit scoring
    if skip_routes:
        logger.info("Stage 4: transit skipped")
        progress.set_stage("transit", total_estimate=len(shortlist))
        for place in shortlist:
            place["transit_min_minutes"] = None
            place["transit_score"] = None
            ortho_relevance = compute_ortho_relevance(place)
            place["ortho_relevance"] = ortho_relevance
            place["final"] = compute_final_score(
                place.get("quality", 0.0),
                None,
                ortho_relevance,
            )
            progress.advance()
    else:
        logger.info("Stage 4: transit scoring")
        progress.set_stage("transit", total_estimate=len(shortlist))
        departure_time = compute_departure_time_rfc3339()
        apply_transit(
            shortlist,
            hubs,
            routes_client,
            departure_time,
            rejection_counts,
            progress_reporter=progress,
        )

    # Prepare output rows (include rejected)
    all_rows = []
    for place in places_by_id.values():
        row = build_output_row(place)
        all_rows.append(row)
    rejection_rows = [
        build_rejection_row(place)
        for place in places_by_id.values()
        if place.get("rejected_reason")
    ]

    # Sort accepted results deterministically
    accepted = [r for r in all_rows if not r.get("rejected_reason")]
    accepted_sorted = sorted(accepted, key=result_sort_key)

    # Append rejected rows (sorted by name) after accepted
    rejected = [r for r in all_rows if r.get("rejected_reason")]
    rejected_sorted = sorted(
        rejected,
        key=lambda r: ((r.get("rejected_reason") or ""), (r.get("name") or ""), (r.get("place_id") or "")),
    )
    rejections_sorted = sorted(
        rejection_rows,
        key=lambda r: ((r.get("reject_reason") or ""), (r.get("name") or ""), (r.get("place_id") or "")),
    )
    all_rows_sorted = accepted_sorted + rejected_sorted

    # Coverage report
    coverage_report = build_coverage_report(results_by_query, results_by_point)
    query_group_sets = {
        group: ids for group, ids in results_by_group.items() if group in {"ortho", "general"}
    }
    query_group_totals = {group: len(ids) for group, ids in query_group_sets.items()}
    query_group_uniques = unique_contributions(query_group_sets) if query_group_sets else {}

    summary = {
        "places_requests": metrics.network_places,
        "routes_requests": metrics.network_routes,
        "cache_hits_places": metrics.cache_hits_places,
        "cache_hits_routes": metrics.cache_hits_routes,
        "dedup_skips_places": metrics.dedup_skips_places,
        "dedup_skips_routes": metrics.dedup_skips_routes,
        "routes_skipped": skip_routes,
        "coverage": coverage_stats,
        "coverage_report": coverage_report,
        "query_group_totals": query_group_totals,
        "query_group_uniques": query_group_uniques,
        "rejection_counts": rejection_counts,
        "top10": [
            {
                "place_id": r.get("place_id"),
                "name": r.get("name"),
                "quality": r.get("quality"),
                "transit_min_minutes": r.get("transit_min_minutes"),
                "final": r.get("final"),
            }
            for r in accepted_sorted[:10]
        ],
    }

    if write_outputs:
        logger.info("Stage 5: outputs")
        progress.set_stage("outputs", total_estimate=6)
        write_results_csv(f"{output_dir}/results.csv", all_rows_sorted)
        progress.advance()
        write_results_json(f"{output_dir}/results.json", all_rows_sorted)
        progress.advance()
        summary_lines = render_summary(summary)
        write_summary(f"{output_dir}/summary.txt", summary_lines)
        progress.advance()
        write_json_object(f"{output_dir}/coverage.json", coverage_report)
        progress.advance()
        write_rejections_csv(f"{output_dir}/rejections.csv", rejections_sorted)
        progress.advance()
        write_rejections_jsonl(f"{output_dir}/rejections.jsonl", rejections_sorted)
        progress.advance()

    progress.flush()
    cache.close()

    return PipelineResult(results=accepted_sorted, all_places=all_rows_sorted, summary=summary)


# Helpers

def validate_hubs(hubs: Dict[str, Dict[str, Any]]) -> None:
    invalid = [k for k, v in hubs.items() if v.get("lat") == 0.0 or v.get("lon") == 0.0]
    if invalid:
        raise ValueError(
            "Hub coordinates are not set. Please update lat/lon in config.HUBS for: "
            + ", ".join(invalid)
        )


def hubs_list(hubs: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for hub_id in sorted(hubs.keys()):
        data = hubs[hub_id]
        items.append({"id": hub_id, "name": data["name"], "lat": data["lat"], "lon": data["lon"]})
    return items


def build_output_row(place: Dict[str, Any]) -> Dict[str, Any]:
    found_by = place.get("found_by", [])
    found_by_queries = sorted({f.get("query") for f in found_by if f.get("query")})
    found_by_points = sorted({f.get("point_id") for f in found_by if f.get("point_id")})
    return {
        "place_id": place.get("place_id"),
        "name": place.get("name"),
        "rating": place.get("rating"),
        "user_rating_count": place.get("user_rating_count"),
        "lat": place.get("lat"),
        "lon": place.get("lon"),
        "business_status": place.get("business_status"),
        "types": place.get("types") or [],
        "quality_bayes": place.get("quality_bayes"),
        "quality_wilson": place.get("quality_wilson"),
        "quality": place.get("quality"),
        "transit_min_minutes": place.get("transit_min_minutes"),
        "transit_score": place.get("transit_score"),
        "final": place.get("final"),
        "found_by_queries": found_by_queries,
        "found_by_points": found_by_points,
        "rejected_reason": place.get("rejected_reason"),
    }


def build_rejection_row(place: Dict[str, Any]) -> Dict[str, Any]:
    found_by = place.get("found_by", [])
    found_by_queries = sorted({f.get("query") for f in found_by if f.get("query")})
    return {
        "place_id": place.get("place_id"),
        "name": place.get("name"),
        "rating": place.get("rating"),
        "user_rating_count": place.get("user_rating_count"),
        "lat": place.get("lat"),
        "lon": place.get("lon"),
        "reject_reason": place.get("rejected_reason"),
        "stage": place.get("rejected_stage") or "unknown",
        "found_by_queries": found_by_queries,
    }


def result_sort_key(row: Dict[str, Any]) -> Tuple[float, int, float, str, str]:
    return (
        -safe_float(row.get("final")),
        -safe_int(row.get("user_rating_count")),
        -safe_float(row.get("rating")),
        (row.get("name") or ""),
        (row.get("place_id") or ""),
    )


def list_mode_sort_key(row: Dict[str, Any]) -> Tuple[float, str, str]:
    return (
        -safe_float(row.get("quality")),
        (row.get("name") or ""),
        (row.get("place_id") or ""),
    )


def quality_sort_key_full(row: Dict[str, Any]) -> Tuple[float, int, float, str, str]:
    return (
        -safe_float(row.get("quality")),
        -safe_int(row.get("user_rating_count")),
        -safe_float(row.get("rating")),
        (row.get("name") or ""),
        (row.get("place_id") or ""),
    )


def compute_min_distance_km_to_any_hub(
    place: Dict[str, Any], hubs: List[Dict[str, Any]]
) -> Tuple[Optional[float], Optional[str]]:
    lat = place.get("lat")
    lon = place.get("lon")
    if lat is None or lon is None or not hubs:
        return None, None
    nearest_id: Optional[str] = None
    min_dist: Optional[float] = None
    for hub in hubs:
        dist = haversine_km(lat, lon, hub["lat"], hub["lon"])
        if min_dist is None or dist < min_dist:
            min_dist = dist
            nearest_id = hub["id"]
    return min_dist, nearest_id


def compute_distance_km_to_center(
    place: Dict[str, Any], center_lat: float, center_lon: float
) -> Optional[float]:
    lat = place.get("lat")
    lon = place.get("lon")
    if lat is None or lon is None:
        return None
    return haversine_km(lat, lon, center_lat, center_lon)


def build_radius_scan_points(
    center_lat: float,
    center_lon: float,
    radius_km: float,
    grid_step_km: float,
) -> List[Dict[str, Any]]:
    if grid_step_km <= 0:
        raise ValueError("grid_step_km must be positive")
    if radius_km <= 0:
        raise ValueError("radius_km must be positive")

    center_lat = float(center_lat)
    center_lon = float(center_lon)
    radius_km = float(radius_km)
    grid_step_km = float(grid_step_km)

    lat_step = grid_step_km / 111.0
    cos_center = math.cos(math.radians(center_lat))
    if abs(cos_center) < 1e-3:
        cos_center = 1e-3
    lon_step = grid_step_km / (111.0 * cos_center)

    lat_delta = radius_km / 111.0
    lon_delta = radius_km / (111.0 * cos_center)

    points: List[Dict[str, Any]] = []
    idx = 0
    lat = center_lat - lat_delta
    epsilon = 1e-6
    while lat <= center_lat + lat_delta + epsilon:
        lon = center_lon - lon_delta
        while lon <= center_lon + lon_delta + epsilon:
            if haversine_km(lat, lon, center_lat, center_lon) <= radius_km + 1e-6:
                points.append({"id": f"scan_{idx}", "lat": lat, "lon": lon})
                idx += 1
            lon += lon_step
        lat += lat_step
    return points


def apply_radius_scan_filters(
    places_by_id: Dict[str, Dict[str, Any]],
    center_lat: float,
    center_lon: float,
    radius_km: float,
    progress_reporter: Optional[ProgressReporter] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    filtered = []
    rejection_counts: Dict[str, int] = {}
    for place in places_by_id.values():
        dist_km = compute_distance_km_to_center(place, center_lat, center_lon)
        place["distance_km_to_center"] = dist_km

        reason = None
        if place.get("rating") is None:
            reason = "rating_missing"
        elif place.get("user_rating_count") is None:
            reason = "missing_user_rating_count"
        elif place["user_rating_count"] < config.MIN_USER_RATING_COUNT:
            reason = "insufficient_reviews"
        elif place.get("lat") is None or place.get("lon") is None:
            reason = "missing_location"
        elif dist_km is None:
            reason = "missing_location"
        elif dist_km > radius_km:
            reason = "too_far"

        if reason is None:
            status = place.get("business_status")
            if status is not None and status != config.BUSINESS_STATUS_OPERATIONAL:
                reason = "business_status_not_operational"

        if reason:
            place["rejected_reason"] = reason
            place["rejected_stage"] = "radius_scan_filters"
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
        else:
            filtered.append(place)
        if progress_reporter:
            progress_reporter.advance()

    return filtered, rejection_counts


def build_radius_scan_row(place: Dict[str, Any]) -> Dict[str, Any]:
    found_by = place.get("found_by") or []
    found_by_queries = sorted({f.get("query") for f in found_by if f.get("query")})
    found_by_points = sorted({f.get("point_id") for f in found_by if f.get("point_id")})
    return {
        "place_id": place.get("place_id"),
        "name": place.get("name"),
        "lat": place.get("lat"),
        "lon": place.get("lon"),
        "rating": place.get("rating"),
        "user_rating_count": place.get("user_rating_count"),
        "business_status": place.get("business_status"),
        "distance_km_to_center": place.get("distance_km_to_center"),
        "quality": place.get("quality"),
        "rejected_reason": place.get("rejected_reason") or "",
        "found_by_points": found_by_points,
        "found_by_queries": found_by_queries,
    }


def build_radius_scan_merged_row(place: Dict[str, Any]) -> Dict[str, Any]:
    found_by = place.get("found_by") or []
    found_by_queries = sorted({f.get("query") for f in found_by if f.get("query")})
    found_by_points = sorted({f.get("point_id") for f in found_by if f.get("point_id")})
    return {
        "place_id": place.get("place_id"),
        "name": place.get("name"),
        "lat": place.get("lat"),
        "lon": place.get("lon"),
        "rating": place.get("rating"),
        "user_rating_count": place.get("user_rating_count"),
        "business_status": place.get("business_status"),
        "distance_km_by_center": place.get("distance_km_by_center") or {},
        "min_distance_km_to_any_center": place.get("min_distance_km_to_any_center"),
        "nearest_center_id": place.get("nearest_center_id"),
        "centers_in_range": place.get("centers_in_range") or [],
        "quality": place.get("quality"),
        "rejected_reason": place.get("rejected_reason") or "",
        "found_by_points": found_by_points,
        "found_by_queries": found_by_queries,
    }


def apply_list_mode_filters(
    places_by_id: Dict[str, Dict[str, Any]],
    hubs: List[Dict[str, Any]],
    progress_reporter: Optional[ProgressReporter] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    filtered = []
    rejection_counts: Dict[str, int] = {}

    for place in places_by_id.values():
        min_dist, nearest_id = compute_min_distance_km_to_any_hub(place, hubs)
        place["min_distance_km_to_any_hub"] = min_dist
        place["nearest_hub_id"] = nearest_id

        reason = None
        if place.get("rating") is None:
            reason = "rating_missing"
        elif place.get("user_rating_count") is None:
            reason = "missing_user_rating_count"
        elif place["user_rating_count"] < config.MIN_USER_RATING_COUNT:
            reason = "insufficient_reviews"
        elif place.get("lat") is None or place.get("lon") is None:
            reason = "missing_location"
        elif min_dist is None:
            reason = "missing_location"
        elif min_dist > config.MAX_DISTANCE_KM:
            reason = "too_far"

        if reason is None:
            status = place.get("business_status")
            if status is not None and status != config.BUSINESS_STATUS_OPERATIONAL:
                reason = "business_status_not_operational"

        if reason:
            place["rejected_reason"] = reason
            place["rejected_stage"] = "list_mode_filters"
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
        else:
            filtered.append(place)
        if progress_reporter:
            progress_reporter.advance()

    return filtered, rejection_counts


def compute_quality_all(
    places: List[Dict[str, Any]],
    progress_reporter: Optional[ProgressReporter] = None,
    sort_key=list_mode_sort_key,
) -> List[Dict[str, Any]]:
    if not places:
        return []
    c = sum(p["rating"] for p in places if p.get("rating") is not None) / len(places)
    for place in places:
        v = int(place["user_rating_count"])
        scores = quality_score(float(place["rating"]), v, c, config.BAYES_M)
        place.update(scores)
        if progress_reporter:
            progress_reporter.advance()
    return sorted(places, key=sort_key)


def build_list_mode_row(place: Dict[str, Any]) -> Dict[str, Any]:
    found_by = place.get("found_by") or []
    found_by_queries = sorted({f.get("query") for f in found_by if f.get("query")})
    found_by_points = sorted({f.get("point_id") for f in found_by if f.get("point_id")})
    return {
        "place_id": place.get("place_id"),
        "name": place.get("name"),
        "lat": place.get("lat"),
        "lon": place.get("lon"),
        "rating": place.get("rating"),
        "user_rating_count": place.get("user_rating_count"),
        "business_status": place.get("business_status"),
        "nearest_hub_id": place.get("nearest_hub_id"),
        "min_distance_km_to_any_hub": place.get("min_distance_km_to_any_hub"),
        "quality": place.get("quality"),
        "rejected_reason": place.get("rejected_reason") or "",
        "found_by_points": found_by_points,
        "found_by_queries": found_by_queries,
    }


def apply_filters(
    places_by_id: Dict[str, Dict[str, Any]],
    hubs: List[Dict[str, Any]],
    progress_reporter: Optional[ProgressReporter] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    filtered = []
    rejection_counts: Dict[str, int] = {}

    for place in places_by_id.values():
        reason = None

        if place.get("rating") is None:
            reason = "rating_missing"
        elif place.get("user_rating_count") is None:
            reason = "missing_user_rating_count"
        elif place["user_rating_count"] < config.MIN_USER_RATING_COUNT:
            reason = "insufficient_reviews"
        elif place.get("lat") is None or place.get("lon") is None:
            reason = "missing_location"
        else:
            dist_km = min(
                haversine_km(place["lat"], place["lon"], h["lat"], h["lon"]) for h in hubs
            )
            place["distance_km"] = dist_km
            if dist_km > config.MAX_DISTANCE_KM:
                reason = "too_far"

        if reason is None:
            status = place.get("business_status")
            if status is not None and status != config.BUSINESS_STATUS_OPERATIONAL:
                reason = "business_status_not_operational"

        if reason is None:
            types = set(place.get("types") or [])
            if types and types.isdisjoint(config.ALLOWED_MEDICAL_TYPES):
                if types.intersection(config.NON_MEDICAL_TYPES):
                    reason = "non_medical_types"

        if reason is None:
            name_casefold = (place.get("name") or "").casefold()
            for banned in config.DOMAIN_REJECT_NAME_SUBSTRINGS:
                if banned and banned.casefold() in name_casefold:
                    reason = "irrelevant_domain"
                    break

        if reason:
            place["rejected_reason"] = reason
            place["rejected_stage"] = "filters"
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
        else:
            filtered.append(place)
        if progress_reporter:
            progress_reporter.advance()

    return filtered, rejection_counts


def compute_quality(
    places: List[Dict[str, Any]],
    top_n: int,
    progress_reporter: Optional[ProgressReporter] = None,
) -> List[Dict[str, Any]]:
    if not places:
        return []
    c = sum(p["rating"] for p in places if p.get("rating") is not None) / len(places)
    for place in places:
        v = int(place["user_rating_count"])
        scores = quality_score(float(place["rating"]), v, c, config.BAYES_M)
        place.update(scores)
        if progress_reporter:
            progress_reporter.advance()

    sorted_by_quality = sorted(
        places,
        key=lambda p: (
            -safe_float(p.get("quality")),
            -safe_int(p.get("user_rating_count")),
            -safe_float(p.get("rating")),
            (p.get("name") or ""),
            (p.get("place_id") or ""),
        ),
    )
    return sorted_by_quality[:top_n]


def apply_transit(
    shortlist: List[Dict[str, Any]],
    hubs: List[Dict[str, Any]],
    routes_client: RoutesClient,
    departure_time: str,
    rejection_counts: Dict[str, int],
    progress_reporter: Optional[ProgressReporter] = None,
) -> List[Dict[str, Any]]:
    for place in shortlist:
        durations = []
        for hub in hubs:
            duration = routes_client.compute_route_duration(
                hub["id"], hub, place, departure_time, mode=config.TRANSIT_MODE
            )
            if duration is not None:
                durations.append(duration)
        if not durations:
            place["rejected_reason"] = "no_transit_route"
            place["rejected_stage"] = "transit"
            rejection_counts["no_transit_route"] = rejection_counts.get("no_transit_route", 0) + 1
            if progress_reporter:
                progress_reporter.advance()
            continue

        min_seconds = min(durations)
        min_minutes = min_seconds / 60.0
        place["transit_min_minutes"] = min_minutes

        if min_minutes > config.TRANSIT_TIME_LIMIT_MIN:
            place["rejected_reason"] = "transit_over_limit"
            place["rejected_stage"] = "transit"
            rejection_counts["transit_over_limit"] = rejection_counts.get("transit_over_limit", 0) + 1
            if progress_reporter:
                progress_reporter.advance()
            continue

        transit_score = 100.0 * math.exp(-min_minutes / config.TRANSIT_SCORE_K)
        place["transit_score"] = transit_score
        ortho_relevance = compute_ortho_relevance(place)
        place["ortho_relevance"] = ortho_relevance
        place["final"] = compute_final_score(
            place.get("quality", 0.0),
            transit_score,
            ortho_relevance,
        )
        if progress_reporter:
            progress_reporter.advance()

    return shortlist


def coverage_check(
    places_client: PlacesClient,
    hubs: List[Dict[str, Any]],
    record_place_fn,
    results_by_query_point: Dict[Tuple[str, str], Set[str]],
    coverage_queries: Optional[List[str]] = None,
    max_pages_per_query: int = config.COVERAGE_MAX_PAGES_PER_QUERY,
    grid_size_initial: Optional[int] = None,
    grid_max_iterations: Optional[int] = None,
    coverage_budget: Optional[CoverageBudgetTracker] = None,
    budget: Optional[RequestBudget] = None,
    progress_reporter: Optional[ProgressReporter] = None,
) -> Dict[str, Any]:
    if coverage_queries is None:
        coverage_queries = list(config.COVERAGE_QUERIES)
    if grid_size_initial is None:
        grid_size_initial = config.COVERAGE_CONFIG.grid_size_initial
    if grid_max_iterations is None:
        grid_max_iterations = config.COVERAGE_CONFIG.grid_max_iterations

    def coverage_remaining() -> Optional[int]:
        if coverage_budget is None or budget is None:
            return None
        return coverage_budget.remaining(budget)

    # Compute U_hubs from existing hub harvest
    hub_ids = {h["id"] for h in hubs}
    u_hubs: Set[str] = set()
    for query in coverage_queries:
        for hub_id in hub_ids:
            u_hubs.update(results_by_query_point.get((query, hub_id), set()))

    grid_size = grid_size_initial
    max_iter = grid_max_iterations
    last_u_grid: Set[str] = set()
    last_u_union: Set[str] = set()
    grid_points: List[Dict[str, Any]] = []
    stopped_early = False

    if not coverage_queries:
        return {
            "u_hubs": len(u_hubs),
            "u_grid_total": 0,
            "u_grid_new": 0,
            "u_union_total": len(u_hubs),
            "uplift": 0.0,
            "grid_size": grid_size,
            "grid_points": [],
            "stopped_early": False,
        }

    for idx in range(max_iter + 1):
        grid_points = build_grid(config.WARSAW_BBOX, grid_size)
        u_grid: Set[str] = set()
        for point in grid_points:
            for query in coverage_queries:
                remaining = coverage_remaining()
                if remaining is not None and remaining <= 0:
                    stopped_early = True
                    break
                max_pages = max_pages_per_query
                if remaining is not None:
                    max_pages = min(max_pages_per_query, remaining)
                if max_pages <= 0:
                    stopped_early = True
                    break
                try:
                    places = places_client.search_text_all(
                        query, point, type_filter=None, max_pages=max_pages
                    )
                except BudgetExceededError:
                    stopped_early = True
                    break
                for place in places:
                    record_place_fn(place, query, point["id"], "text")
                    u_grid.add(place["place_id"])
                if progress_reporter:
                    progress_reporter.advance()
            if stopped_early:
                break
        last_u_grid = u_grid
        last_u_union = u_hubs | u_grid
        if stopped_early:
            break

        uplift = compute_uplift(len(u_hubs), len(last_u_union))
        if uplift <= config.COVERAGE_CONFIG.uplift_threshold or idx >= max_iter:
            break
        grid_size += 1

    return {
        "u_hubs": len(u_hubs),
        "u_grid_total": len(last_u_grid),
        "u_grid_new": len(last_u_grid - u_hubs),
        "u_union_total": len(last_u_union),
        "uplift": compute_uplift(len(u_hubs), len(last_u_union)),
        "grid_size": grid_size,
        "grid_points": grid_points,
        "stopped_early": stopped_early,
    }


def build_coverage_report(
    results_by_query: Dict[str, Set[str]],
    results_by_point: Dict[str, Set[str]],
) -> Dict[str, Any]:
    return {
        "unique_by_query": unique_contributions(results_by_query),
        "unique_by_point": unique_contributions(results_by_point),
        "jaccard_by_query": pairwise_jaccard(results_by_query),
    }


def render_summary(summary: Dict[str, Any]) -> List[str]:
    lines = []
    lines.append(f"Places requests (network): {summary['places_requests']}")
    lines.append(f"Routes requests (network): {summary['routes_requests']}")
    if "cache_hits_places" in summary:
        lines.append(f"Places cache hits: {summary.get('cache_hits_places', 0)}")
    if "cache_hits_routes" in summary:
        lines.append(f"Routes cache hits: {summary.get('cache_hits_routes', 0)}")
    if "dedup_skips_places" in summary or "dedup_skips_routes" in summary:
        lines.append("Request stats:")
        lines.append(
            "  Places: network={network}, cache_hits={cache_hits}, dedup_skips={dedup_skips}".format(
                network=summary.get("places_requests", 0),
                cache_hits=summary.get("cache_hits_places", 0),
                dedup_skips=summary.get("dedup_skips_places", 0),
            )
        )
        lines.append(
            "  Routes: network={network}, cache_hits={cache_hits}, dedup_skips={dedup_skips}".format(
                network=summary.get("routes_requests", 0),
                cache_hits=summary.get("cache_hits_routes", 0),
                dedup_skips=summary.get("dedup_skips_routes", 0),
            )
        )
    if summary.get("routes_skipped"):
        lines.append("Routes stage: skipped")
    cov = summary.get("coverage", {})
    if cov.get("mode") == "off":
        lines.append("Coverage: DISABLED")
    else:
        lines.append(
            "Coverage: U_hubs={u_hubs}, U_grid_total={u_grid_total}, uplift={uplift:.2%}".format(
                u_hubs=cov.get("u_hubs", 0),
                u_grid_total=cov.get("u_grid_total", 0),
                uplift=cov.get("uplift", 0.0),
            )
        )
        lines.append(
            "Coverage detail: U_grid_new={u_grid_new}, U_union_total={u_union_total}, grid_size={grid_size}".format(
                u_grid_new=cov.get("u_grid_new", 0),
                u_union_total=cov.get("u_union_total", 0),
                grid_size=cov.get("grid_size", 0),
            )
        )
    group_totals = summary.get("query_group_totals", {})
    if group_totals:
        group_uniques = summary.get("query_group_uniques", {})
        ortho_total = group_totals.get("ortho", 0)
        general_total = group_totals.get("general", 0)
        ortho_unique = group_uniques.get("ortho", 0)
        general_unique = group_uniques.get("general", 0)
        lines.append(
            "Query groups: ortho_total={ortho_total}, general_total={general_total}".format(
                ortho_total=ortho_total,
                general_total=general_total,
            )
        )
        lines.append(
            "Query group uniques: ortho={ortho_unique}, general={general_unique}".format(
                ortho_unique=ortho_unique,
                general_unique=general_unique,
            )
        )
    lines.append("Rejections:")
    for reason, count in sorted(summary.get("rejection_counts", {}).items()):
        lines.append(f"  - {reason}: {count}")
    lines.append("Top 10:")
    for item in summary.get("top10", []):
        lines.append(
            "  - {name} ({place_id}) quality={quality:.1f} transit={transit_min_minutes:.1f} final={final:.1f}".format(
                name=item.get("name") or "",
                place_id=item.get("place_id") or "",
                quality=item.get("quality") or 0.0,
                transit_min_minutes=item.get("transit_min_minutes") or 0.0,
                final=item.get("final") or 0.0,
            )
        )
    return lines


def render_list_mode_summary(summary: Dict[str, Any]) -> List[str]:
    lines = []
    total = summary.get("list_mode_total", 0)
    accepted = summary.get("list_mode_accepted", 0)
    rejected = summary.get("list_mode_rejected", 0)
    lines.append(f"Total: {total}")
    lines.append(f"Accepted: {accepted}")
    lines.append(f"Rejected: {rejected}")
    lines.append("Rejections:")
    for reason, count in sorted(summary.get("rejection_counts", {}).items()):
        lines.append(f"  - {reason}: {count}")
    lines.append("Top 20 by quality:")
    for item in summary.get("top20", []):
        lines.append(
            "  - {name} ({place_id}) quality={quality:.1f}".format(
                name=item.get("name") or "",
                place_id=item.get("place_id") or "",
                quality=item.get("quality") or 0.0,
            )
        )
    return lines


def render_radius_scan_summary(summary: Dict[str, Any]) -> List[str]:
    lines = []
    lines.append(f"Unique place ids: {summary.get('unique_place_ids', 0)}")
    lines.append(f"Accepted: {summary.get('accepted', 0)}")
    lines.append(f"Rejected: {summary.get('rejected', 0)}")
    lines.append(f"Budget exceeded: {summary.get('budget_exceeded', False)}")
    lines.append(
        "Request stats: places_network={places_network}, cache_hits={cache_hits}, dedup_skips={dedup_skips}".format(
            places_network=summary.get("places_requests", 0),
            cache_hits=summary.get("cache_hits_places", 0),
            dedup_skips=summary.get("dedup_skips_places", 0),
        )
    )
    lines.append("Rejections:")
    for reason, count in sorted(summary.get("rejection_counts", {}).items()):
        lines.append(f"  - {reason}: {count}")
    lines.append("Top 20 by quality:")
    for item in summary.get("top20", []):
        lines.append(
            "  - {name} ({place_id}) quality={quality:.1f}".format(
                name=item.get("name") or "",
                place_id=item.get("place_id") or "",
                quality=item.get("quality") or 0.0,
            )
        )
    return lines


def render_radius_scan_merged_summary(summary: Dict[str, Any]) -> List[str]:
    lines = []
    lines.append(f"Total unique place ids: {summary.get('total_unique_place_ids', 0)}")
    lines.append(f"Eligible: {summary.get('eligible_count', 0)}")
    lines.append(f"Rejected: {summary.get('rejected_count', 0)}")
    lines.append(f"Budget exceeded: {summary.get('budget_exceeded', False)}")
    lines.append(
        "Request stats: places_network={places_network}, cache_hits={cache_hits}, dedup_skips={dedup_skips}".format(
            places_network=summary.get("places_requests", 0),
            cache_hits=summary.get("cache_hits_places", 0),
            dedup_skips=summary.get("dedup_skips_places", 0),
        )
    )
    lines.append("Per-center unique counts:")
    for center_id, count in sorted(summary.get("per_center_unique_counts", {}).items()):
        lines.append(f"  - {center_id}: {count}")
    lines.append("Per-center budget exceeded:")
    for center_id, flag in sorted(summary.get("per_center_budget_exceeded", {}).items()):
        lines.append(f"  - {center_id}: {flag}")
    lines.append("Rejections:")
    for reason, count in sorted(summary.get("rejection_counts", {}).items()):
        lines.append(f"  - {reason}: {count}")
    lines.append("Top 50 preview:")
    for item in summary.get("top50", []):
        lines.append(
            "  - {name} ({place_id}) quality={quality:.1f} min_dist={min_dist:.2f} nearest={nearest}".format(
                name=item.get("name") or "",
                place_id=item.get("place_id") or "",
                quality=item.get("quality") or 0.0,
                min_dist=item.get("min_distance_km_to_any_center") or 0.0,
                nearest=item.get("nearest_center_id") or "",
            )
        )
    return lines


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _matches_any(text: str, needles: Iterable[str]) -> bool:
    if not text:
        return False
    for needle in needles:
        if needle and needle in text:
            return True
    return False


def compute_ortho_relevance(place: Dict[str, Any]) -> float:
    score = float(config.ORTHO_RELEVANCE_BASE)
    name = (place.get("name") or "").lower()
    found_by = place.get("found_by") or []
    queries: List[str] = []
    groups: Set[str] = set()
    ortho_query_set = set(config.ORTHO_QUERIES)
    general_query_set = set(config.GENERAL_QUERIES)
    for item in found_by:
        group = item.get("group")
        if isinstance(group, str) and group in {"ortho", "general"}:
            groups.add(group)
        query = item.get("query")
        if query:
            query_str = str(query)
            queries.append(query_str)
            if query_str in ortho_query_set:
                groups.add("ortho")
            elif query_str in general_query_set:
                groups.add("general")
    lower_queries = [q.lower() for q in queries]
    has_ortho_query = any(_matches_any(q, config.ORTHO_QUERY_HINTS) for q in lower_queries)
    if has_ortho_query:
        score += float(config.ORTHO_RELEVANCE_QUERY_BONUS)
    if _matches_any(name, config.ORTHO_NAME_HINTS):
        score += float(config.ORTHO_RELEVANCE_NAME_BONUS)
    if lower_queries:
        all_generic = all(_matches_any(q, config.ORTHO_GENERIC_QUERY_HINTS) for q in lower_queries)
        if all_generic and not has_ortho_query:
            score -= float(config.ORTHO_RELEVANCE_GENERIC_PENALTY)
    has_ortho_group = "ortho" in groups
    has_general_group = "general" in groups
    if has_general_group and not has_ortho_group:
        score -= float(config.ORTHO_GENERAL_ONLY_PENALTY)
    return max(0.0, min(100.0, score))


def compute_final_score(
    quality: float,
    transit_score: Optional[float],
    ortho_relevance: float,
) -> float:
    return (
        float(config.SCORE_WEIGHT_QUALITY) * safe_float(quality)
        + float(config.SCORE_WEIGHT_TRANSIT) * safe_float(transit_score)
        + float(config.SCORE_WEIGHT_ORTHO) * safe_float(ortho_relevance)
    )
