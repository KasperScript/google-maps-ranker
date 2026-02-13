#!/usr/bin/env python3
"""
Calculate transit times from ALK, Centralny, and Galeria Północna to each clinic.

Uses Google Routes API to get public transit travel times.
Adds the times to outreach_results.json and regenerates price comparison.
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except ImportError:
    pass

# Hub coordinates — loaded from search_config.json center point
def _load_hubs() -> Dict[str, Dict[str, Any]]:
    """Load hub from search_config.json center, or fall back to env vars."""
    config_path = REPO_ROOT / "search_config.json"
    if config_path.exists():
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        center = cfg.get("center", {})
        lat, lon = center.get("lat"), center.get("lon")
        name = center.get("name", "center")
        if lat is not None and lon is not None:
            return {"center": {"name": name, "lat": float(lat), "lon": float(lon)}}
    # Fallback: use env vars
    lat = os.environ.get("CENTER_LAT")
    lon = os.environ.get("CENTER_LON")
    if lat and lon:
        return {"center": {"name": "center", "lat": float(lat), "lon": float(lon)}}
    return {}

HUBS = _load_hubs()

ROUTES_API_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"


def get_next_weekday_17(now: datetime) -> str:
    """Get next weekday at 17:00 Warsaw time as RFC3339."""
    # Find next weekday
    days_ahead = 0
    check = now
    while check.weekday() >= 5:  # Saturday=5, Sunday=6
        days_ahead += 1
        check = now + timedelta(days=days_ahead)
    
    # Set to 17:00
    departure = check.replace(hour=17, minute=0, second=0, microsecond=0)
    if departure <= now:
        departure += timedelta(days=1)
        while departure.weekday() >= 5:
            departure += timedelta(days=1)
    
    return departure.strftime("%Y-%m-%dT17:00:00+01:00")


def calculate_transit_time(
    api_key: str,
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    departure_time: str,
) -> Optional[int]:
    """Call Routes API to get transit duration in minutes."""
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "routes.duration",
    }
    
    body = {
        "origin": {
            "location": {
                "latLng": {"latitude": origin_lat, "longitude": origin_lon}
            }
        },
        "destination": {
            "location": {
                "latLng": {"latitude": dest_lat, "longitude": dest_lon}
            }
        },
        "travelMode": "TRANSIT",
        "departureTime": departure_time,
        "transitPreferences": {
            "routingPreference": "LESS_WALKING"
        }
    }
    
    try:
        resp = requests.post(ROUTES_API_URL, headers=headers, json=body, timeout=15)
        if resp.status_code != 200:
            return None
        
        data = resp.json()
        routes = data.get("routes", [])
        if not routes:
            return None
        
        duration_str = routes[0].get("duration", "")
        if duration_str.endswith("s"):
            seconds = int(duration_str[:-1])
            return round(seconds / 60)
        return None
    except Exception:
        return None


def get_latest_results() -> Tuple[Optional[Path], Optional[List[Dict]]]:
    """Load latest outreach results and merge coordinates from input."""
    pointer = REPO_ROOT / "out" / "outreach" / "latest_run_dir.txt"
    if not pointer.exists():
        return None, None
    
    run_dir = Path(pointer.read_text().strip())
    results_file = run_dir / "outreach_results.json"
    
    if not results_file.exists():
        return None, None
    
    results = json.loads(results_file.read_text(encoding="utf-8"))
    
    # Load coordinates from input file
    input_file = REPO_ROOT / "out" / "results_with_websites_deduped.json"
    if input_file.exists():
        input_data = json.loads(input_file.read_text(encoding="utf-8"))
        # Build lookup by place_id
        coords_by_place = {}
        for row in input_data:
            pid = row.get("place_id")
            if pid and row.get("lat") and row.get("lon"):
                coords_by_place[pid] = (float(row["lat"]), float(row["lon"]))
        
        # Merge coordinates into results
        for row in results:
            pid = row.get("place_id")
            if pid and pid in coords_by_place:
                row["lat"], row["lon"] = coords_by_place[pid]
    
    return results_file, results


def main():
    print("=" * 60)
    print("Transit Time Calculator")
    print("=" * 60)
    
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not api_key:
        print("❌ No GOOGLE_MAPS_API_KEY found")
        return 1
    
    results_file, results = get_latest_results()
    if not results:
        print("❌ No outreach results found")
        return 1
    
    print(f"Results file: {results_file}")
    print(f"Clinics: {len(results)}")
    print(f"Hubs: {', '.join(h['name'] for h in HUBS.values())}")
    
    # Get departure time
    now = datetime.now()
    departure = get_next_weekday_17(now)
    print(f"Departure time: {departure}")
    
    # Count how many need updates
    need_update = []
    for row in results:
        lat = row.get("lat")
        lon = row.get("lon")
        if lat and lon:
            need_update.append(row)
    
    if not need_update:
        print("❌ No clinics with coordinates found")
        return 1
    
    print(f"\n{len(need_update)} clinics with coordinates")
    total_calls = len(need_update) * len(HUBS)
    print(f"API calls needed: {total_calls}")
    
    print(f"\nProceed? [y/N] ", end="")
    if input().strip().lower() != "y":
        print("Cancelled")
        return 0
    
    print("\nCalculating transit times...")
    updated = 0
    
    for i, row in enumerate(need_update, 1):
        clinic_name = row.get("clinic_name", "Unknown")[:40]
        lat = row["lat"]
        lon = row["lon"]
        
        print(f"[{i}/{len(need_update)}] {clinic_name}... ", end="", flush=True)
        
        times = {}
        for hub_id, hub in HUBS.items():
            minutes = calculate_transit_time(
                api_key,
                hub["lat"], hub["lon"],
                lat, lon,
                departure
            )
            times[hub_id] = minutes
            time.sleep(0.1)  # Small delay between calls
        
        # Store in row
        if "transit_times" not in row:
            row["transit_times"] = {}
        row["transit_times"] = times
        
        # Format output
        time_strs = []
        for hub_id, mins in times.items():
            if mins:
                time_strs.append(f"{HUBS[hub_id]['name']}:{mins}'")
            else:
                time_strs.append(f"{HUBS[hub_id]['name']}:—")
        
        print(" | ".join(time_strs))
        updated += 1
    
    # Save results
    print(f"\n{'=' * 60}")
    print(f"Updated {updated} clinics")
    
    if updated:
        results_file.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"✓ Saved to {results_file}")
        print("\n💡 Run `python3 run.py --generate-price-list` to update the comparison")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
