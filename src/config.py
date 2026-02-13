"""Project configuration.

Loads user-defined search parameters from search_config.json when available,
falling back to sensible defaults. Keep API request shapes centralized here.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

_REPO_ROOT = Path(__file__).resolve().parent.parent

# --- API endpoints ---

PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PLACES_NEARBY_SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"
ROUTES_COMPUTE_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"

# --- Field masks ---

PLACES_FIELD_MASK_MIN = (
    "places.id,places.displayName,places.rating,places.userRatingCount,"
    "places.location,places.types,places.businessStatus"
)
ROUTES_FIELD_MASK_MIN = "routes.duration"
ROUTES_FIELD_MASK_DEBUG = "routes.duration,routes.distanceMeters"

# --- Defaults (used when no search_config.json) ---

_DEFAULT_HUBS: Dict[str, Dict[str, Any]] = {}
_DEFAULT_BBOX: Dict[str, float] = {"lat_min": -90, "lat_max": 90, "lon_min": -180, "lon_max": 180}
_DEFAULT_PRIMARY_QUERIES: List[str] = []
_DEFAULT_SECONDARY_QUERIES: List[str] = []
_DEFAULT_TYPE_FILTERS: List[str] = []
_DEFAULT_ALLOWED_TYPES: Set[str] = set()
_DEFAULT_REJECTED_TYPES: Set[str] = {
    "restaurant", "bar", "cafe", "bakery", "store", "shopping_mall",
    "clothing_store", "gym", "lodging", "real_estate_agency", "car_repair",
}
_DEFAULT_REJECT_SUBSTRINGS: List[str] = []
_DEFAULT_MAX_DISTANCE_KM = 20.0
_DEFAULT_MIN_REVIEWS = 50

# --- Mutable config (populated by load_search_config or directly) ---

HUBS: Dict[str, Dict[str, Any]] = dict(_DEFAULT_HUBS)
SEARCH_BBOX: Dict[str, float] = dict(_DEFAULT_BBOX)

PRIMARY_QUERIES: List[str] = list(_DEFAULT_PRIMARY_QUERIES)
SECONDARY_QUERIES: List[str] = list(_DEFAULT_SECONDARY_QUERIES)
ORTHO_QUERIES: List[str] = []  # alias for backward compatibility
GENERAL_QUERIES: List[str] = []

PLACES_TYPE_FILTERS: List[str] = list(_DEFAULT_TYPE_FILTERS)
ALLOWED_TYPES: Set[str] = set(_DEFAULT_ALLOWED_TYPES)
NON_MEDICAL_TYPES: Set[str] = set(_DEFAULT_REJECTED_TYPES)
DOMAIN_REJECT_NAME_SUBSTRINGS: List[str] = list(_DEFAULT_REJECT_SUBSTRINGS)

# Legacy aliases
ALLOWED_MEDICAL_TYPES: Set[str] = ALLOWED_TYPES
QUERIES_PL: List[str] = []
QUERIES_EN: List[str] = []

# --- Scoring ---

BAYES_M = 200
TOP_N_QUALITY = 30
SCORE_WEIGHT_QUALITY = 0.85
SCORE_WEIGHT_TRANSIT = 0.10
SCORE_WEIGHT_RELEVANCE = 0.05
SCORE_WEIGHT_ORTHO = SCORE_WEIGHT_RELEVANCE

RELEVANCE_BASE = 50.0
RELEVANCE_QUERY_BONUS = 25.0
RELEVANCE_NAME_BONUS = 10.0
RELEVANCE_GENERIC_PENALTY = 15.0
RELEVANCE_QUERY_HINTS: List[str] = []
RELEVANCE_NAME_HINTS: List[str] = []
RELEVANCE_GENERIC_QUERY_HINTS: List[str] = []

# Legacy aliases for scoring
ORTHO_RELEVANCE_BASE = RELEVANCE_BASE
ORTHO_RELEVANCE_QUERY_BONUS = RELEVANCE_QUERY_BONUS
ORTHO_RELEVANCE_NAME_BONUS = RELEVANCE_NAME_BONUS
ORTHO_RELEVANCE_GENERIC_PENALTY = RELEVANCE_GENERIC_PENALTY
ORTHO_QUERY_HINTS = RELEVANCE_QUERY_HINTS
ORTHO_NAME_HINTS = RELEVANCE_NAME_HINTS
ORTHO_GENERIC_QUERY_HINTS = RELEVANCE_GENERIC_QUERY_HINTS

# --- Query controls ---

MIN_CANDIDATES = 150
GENERAL_MAX_PAGES_PER_QUERY = 1
ORTHO_GENERAL_ONLY_PENALTY = 5.0
HUBS_ONLY_QUERIES: List[str] = []
QUERY_MAX_PAGES_OVERRIDES: Dict[str, int] = {}

# Coverage
COVERAGE_QUERIES: List[str] = []
COVERAGE_MAX_PAGES_PER_QUERY = 1

# --- Places API request shape ---

PLACES_LOCATION_BIAS_RADIUS_M = 15000
PLACES_MAX_PAGES_PER_QUERY = 3
PLACES_SUPPORTS_TYPE_FILTER = True
PLACES_TEXT_SEARCH_BODY_EXTRA: Dict[str, Any] = {}
PLACES_NEARBY_BODY_EXTRA: Dict[str, Any] = {}

# --- Routes ---

TRANSIT_MODE = "TRANSIT"
TRANSIT_TIME_LIMIT_MIN = 75
TRANSIT_SCORE_K = 35.0
DEPARTURE_POLICY = "next_weekday_17_00"
WARSAW_TIMEZONE = "Europe/Warsaw"
DEPARTURE_TIME_RFC3339_OVERRIDE: Optional[str] = None
ROUTES_BODY_EXTRA: Dict[str, Any] = {}
ROUTES_CACHE_DEPARTURE_BUCKET_MINUTES = 15

# --- Filters ---

MIN_USER_RATING_COUNT = _DEFAULT_MIN_REVIEWS
MAX_DISTANCE_KM = _DEFAULT_MAX_DISTANCE_KM
BUSINESS_STATUS_OPERATIONAL = "OPERATIONAL"

# --- Budgets ---

MAX_PLACES_REQUESTS_PER_RUN = 200
MAX_ROUTES_REQUESTS_PER_RUN = 300
DRY_RUN_MAX_PLACES = 10
DRY_RUN_MAX_ROUTES = 0
DRY_RUN_TOP_N = 10

# --- HTTP ---

HTTP_TIMEOUT_SECONDS = 20
HTTP_RETRY_MAX = 5
HTTP_BACKOFF_BASE = 0.5
HTTP_BACKOFF_MAX = 8.0

# --- Cache and outputs ---

CACHE_DB_PATH = "cache.db"
OUTPUT_DIR = "out"
PROGRESS_LOG_EVERY = 50
PROGRESS_WRITE_INTERVAL_SECONDS = 5.0


@dataclass(frozen=True)
class CoverageConfig:
    grid_size_initial: int = 4
    grid_max_iterations: int = 2
    uplift_threshold: float = 0.10
    enable_nearby_on_poor_coverage: bool = False

COVERAGE_CONFIG = CoverageConfig()


def _compute_bbox(lat: float, lon: float, radius_km: float) -> Dict[str, float]:
    """Compute a bounding box around a center point."""
    delta_lat = radius_km / 111.0
    delta_lon = radius_km / (111.0 * max(0.01, __import__("math").cos(__import__("math").radians(lat))))
    return {
        "lat_min": lat - delta_lat,
        "lat_max": lat + delta_lat,
        "lon_min": lon - delta_lon,
        "lon_max": lon + delta_lon,
    }


def load_search_config(path: Optional[str] = None) -> bool:
    """Load search configuration from a JSON file.

    Updates module-level globals with values from the config file.
    Returns True if config was loaded, False if file not found.
    """
    if path is None:
        path = str(_REPO_ROOT / "search_config.json")

    config_path = Path(path)
    if not config_path.exists():
        return False

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    globals_ref = globals()

    center = data.get("center", {})
    center_lat = center.get("lat")
    center_lon = center.get("lon")
    center_name = center.get("name", "center")

    if center_lat is not None and center_lon is not None:
        hub_id = center_name.lower().replace(" ", "_").replace(",", "")[:30] or "center"
        globals_ref["HUBS"] = {
            hub_id: {"name": center_name, "lat": center_lat, "lon": center_lon}
        }

    max_dist = data.get("max_distance_km")
    if max_dist is not None:
        globals_ref["MAX_DISTANCE_KM"] = float(max_dist)

    if center_lat is not None and center_lon is not None:
        radius = float(max_dist or _DEFAULT_MAX_DISTANCE_KM)
        globals_ref["SEARCH_BBOX"] = _compute_bbox(center_lat, center_lon, radius)
        globals_ref["WARSAW_BBOX"] = globals_ref["SEARCH_BBOX"]

    queries = data.get("queries", {})
    primary = queries.get("primary", [])
    secondary = queries.get("secondary", [])

    if primary:
        globals_ref["PRIMARY_QUERIES"] = list(primary)
        globals_ref["ORTHO_QUERIES"] = list(primary)
        globals_ref["QUERIES_PL"] = list(primary)
        globals_ref["COVERAGE_QUERIES"] = list(primary[:2])
    if secondary:
        globals_ref["SECONDARY_QUERIES"] = list(secondary)
        globals_ref["GENERAL_QUERIES"] = list(secondary)

    type_filters = data.get("type_filters", [])
    if type_filters:
        globals_ref["PLACES_TYPE_FILTERS"] = list(type_filters)

    allowed = data.get("allowed_types", [])
    if allowed:
        globals_ref["ALLOWED_TYPES"] = set(allowed)
        globals_ref["ALLOWED_MEDICAL_TYPES"] = set(allowed)

    rejected = data.get("rejected_types", [])
    if rejected:
        globals_ref["NON_MEDICAL_TYPES"] = set(rejected)

    rejects = data.get("domain_reject_substrings", [])
    if rejects:
        globals_ref["DOMAIN_REJECT_NAME_SUBSTRINGS"] = list(rejects)

    min_rev = data.get("min_reviews")
    if min_rev is not None:
        globals_ref["MIN_USER_RATING_COUNT"] = int(min_rev)

    scoring = data.get("scoring", {})
    if "quality_weight" in scoring:
        globals_ref["SCORE_WEIGHT_QUALITY"] = float(scoring["quality_weight"])
    if "transit_weight" in scoring:
        globals_ref["SCORE_WEIGHT_TRANSIT"] = float(scoring["transit_weight"])
    if "relevance_weight" in scoring:
        globals_ref["SCORE_WEIGHT_RELEVANCE"] = float(scoring["relevance_weight"])
        globals_ref["SCORE_WEIGHT_ORTHO"] = float(scoring["relevance_weight"])

    return True


# Backward-compatibility alias
WARSAW_BBOX = SEARCH_BBOX
