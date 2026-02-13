#!/usr/bin/env python3
"""Enrich missing clinic website URLs using Google Places Details API (v1).

Safety notes:
- Uses only Places Details by place_id (no uncontrolled web search).
- Never sends emails or submits forms.
- Environment variables are the authoritative source of credentials.
"""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, unquote, urlparse

import requests

PLACES_DETAILS_URL_TEMPLATE = "https://places.googleapis.com/v1/places/{place_id}"
PLACES_DETAILS_FIELD_MASK = (
    "websiteUri,googleMapsUri,nationalPhoneNumber,displayName,formattedAddress"
)
DEFAULT_TOP_N = 100
DEFAULT_INPUT = (Path(__file__).resolve().parents[1] / "out" / "results.json").resolve()
DEFAULT_OUTPUT = Path("out/results_with_websites.json")

WEBSITE_COLUMNS: Sequence[str] = (
    "website",
    "website_url",
    "website_uri",
    "websiteUri",
    "websiteUrl",
    "site",
    "url",
)

HttpGet = Callable[[str, Dict[str, str], int], requests.Response]


@dataclass(frozen=True)
class EnrichSummary:
    processed: int
    updated_websites_count: int
    missing_place_id_count: int
    api_errors_count: int


def _extract_rows_from_json_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("results", "items", "data", "clinics"):
            val = payload.get(key)
            if isinstance(val, list):
                return [dict(item) for item in val if isinstance(item, dict)]
        if "name" in payload:
            return [dict(payload)]
    raise ValueError(
        "Unsupported JSON input shape; expected a list or a dict with a list under results/items/data/clinics."
    )


import csv

def _read_rows(path: Path) -> List[Dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _extract_rows_from_json_payload(payload)


def _place_id(row: Dict[str, Any]) -> str:
    return str(row.get("place_id") or row.get("placeId") or row.get("id") or "").strip()


def _has_website(row: Dict[str, Any]) -> bool:
    for key in WEBSITE_COLUMNS:
        val = row.get(key)
        if val is None:
            continue
        if str(val).strip():
            return True
    return False


def _default_http_get(session: requests.Session) -> HttpGet:
    def _http_get(url: str, headers: Dict[str, str], timeout: int) -> requests.Response:
        return session.get(url, headers=headers, timeout=timeout)

    return _http_get


def _details_url(place_id: str) -> str:
    encoded = quote(place_id, safe="")
    return PLACES_DETAILS_URL_TEMPLATE.format(place_id=encoded)


def _extract_place_id_from_url(url: str) -> str:
    path = urlparse(url).path
    place_part = path.rsplit("/", 1)[-1]
    return unquote(place_part)


def fetch_place_details(
    *,
    place_id: str,
    api_key: str,
    http_get: HttpGet,
    timeout: int = 20,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not place_id:
        return None, "missing_place_id"
    url = _details_url(place_id)
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": PLACES_DETAILS_FIELD_MASK,
    }
    try:
        resp = http_get(url, headers, timeout)
    except requests.RequestException as exc:  # pragma: no cover - network dependent
        return None, f"request_error:{exc}"
    if resp.status_code >= 400:
        return None, f"http_error:{resp.status_code}"
    try:
        data = resp.json()
    except ValueError as exc:
        return None, f"non_json_response:{exc}"
    if not isinstance(data, dict):
        return None, f"unexpected_payload:{type(data).__name__}"
    return data, None


def _apply_details(row: Dict[str, Any], details: Dict[str, Any]) -> bool:
    """Apply Places Details fields to the row. Returns True if website was updated."""
    updated_website = False
    website = str(details.get("websiteUri") or "").strip()
    if website:
        row["websiteUri"] = website
        # Fill common website columns only when they are empty.
        for key in ("website", "website_url", "website_uri", "websiteUrl"):
            if not str(row.get(key) or "").strip():
                row[key] = website
        if not _has_website(row):
            # Defensive: ensure at least one website field is populated.
            row["website"] = website
        updated_website = True

    google_maps_uri = str(details.get("googleMapsUri") or "").strip()
    if google_maps_uri:
        row["googleMapsUri"] = google_maps_uri

    phone = str(details.get("nationalPhoneNumber") or "").strip()
    if phone:
        row["nationalPhoneNumber"] = phone

    display_name = details.get("displayName")
    if display_name:
        row["displayName"] = display_name

    formatted_address = str(details.get("formattedAddress") or "").strip()
    if formatted_address:
        row["formattedAddress"] = formatted_address

    return updated_website


def enrich_rows(
    rows: List[Dict[str, Any]],
    *,
    top_n: int,
    api_key: str,
    http_get: Optional[HttpGet] = None,
    session: Optional[requests.Session] = None,
) -> EnrichSummary:
    session_obj = session or requests.Session()
    http_get_fn = http_get or _default_http_get(session_obj)

    processed = 0
    updated_websites_count = 0
    missing_place_id_count = 0
    api_errors_count = 0

    details_cache: Dict[str, Tuple[Optional[Dict[str, Any]], Optional[str]]] = {}

    limit = max(0, int(top_n))
    for idx, row in enumerate(rows):
        if idx >= limit:
            break
        processed += 1

        if _has_website(row):
            continue

        place_id = _place_id(row)
        if not place_id:
            missing_place_id_count += 1
            continue

        cached = details_cache.get(place_id)
        if cached is None:
            cached = fetch_place_details(place_id=place_id, api_key=api_key, http_get=http_get_fn)
            details_cache[place_id] = cached

        details, error = cached
        if error or not details:
            api_errors_count += 1
            continue

        if _apply_details(row, details):
            updated_websites_count += 1

    return EnrichSummary(
        processed=processed,
        updated_websites_count=updated_websites_count,
        missing_place_id_count=missing_place_id_count,
        api_errors_count=api_errors_count,
    )


def enrich_websites_from_places(
    *,
    input_path: Path,
    output_path: Path,
    top_n: int = DEFAULT_TOP_N,
    api_key: Optional[str] = None,
    http_get: Optional[HttpGet] = None,
    session: Optional[requests.Session] = None,
) -> EnrichSummary:
    api_key = (api_key or os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Missing GOOGLE_MAPS_API_KEY in environment")

    rows = _read_rows(input_path)
    summary = enrich_rows(rows, top_n=top_n, api_key=api_key, http_get=http_get, session=session)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich missing clinic websites using Places Details API")
    parser.add_argument("--in", dest="input_path", type=str, default=str(DEFAULT_INPUT))
    parser.add_argument("--out", dest="output_path", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--top-n", dest="top_n", type=int, default=DEFAULT_TOP_N)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    input_path = Path(args.input_path).expanduser().resolve()
    output_path = Path(args.output_path).expanduser().resolve()
    if not input_path.exists():
        print(f"Input JSON not found: {input_path}")
        return 1

    try:
        summary = enrich_websites_from_places(input_path=input_path, output_path=output_path, top_n=args.top_n)
    except Exception as exc:
        print(f"Enrichment error: {exc}")
        return 1

    print("Enrichment summary:")
    print(f"- processed: {summary.processed}")
    print(f"- updated_websites_count: {summary.updated_websites_count}")
    print(f"- missing_place_id_count: {summary.missing_place_id_count}")
    print(f"- api_errors_count: {summary.api_errors_count}")
    print(f"- output: {output_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
