from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import config  # noqa: E402


GRID_STEPS_KM = [3.0, 2.0, 1.2, 0.8]
SCAN_RADII_M = [2000, 1500, 1000]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run radius scan sweeps")
    parser.add_argument("--center", type=str, default=None, help="Hub id to use as scan center")
    parser.add_argument("--center-lat", type=float, default=None)
    parser.add_argument("--center-lon", type=float, default=None)
    parser.add_argument("--radius-km", type=float, default=None)
    parser.add_argument("--out", type=str, default="out/radius_scan_sweep")
    parser.add_argument("--max-pages", type=int, default=1)
    return parser.parse_args()


def resolve_center(args: argparse.Namespace) -> Tuple[float, float]:
    if args.center:
        hub = config.HUBS.get(args.center)
        if not hub:
            raise ValueError(f"Unknown hub id: {args.center}")
        return float(hub["lat"]), float(hub["lon"])
    if args.center_lat is not None and args.center_lon is not None:
        return float(args.center_lat), float(args.center_lon)
    if "centralny" in config.HUBS:
        hub = config.HUBS["centralny"]
        return float(hub["lat"]), float(hub["lon"])
    first = next(iter(config.HUBS.values()))
    return float(first["lat"]), float(first["lon"])


def parse_summary(summary_path: Path) -> Dict[str, float]:
    data: Dict[str, float] = {}
    for line in summary_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("Unique place ids:"):
            data["unique_place_ids"] = float(line.split(":", 1)[1].strip())
        elif line.startswith("Request stats:"):
            parts = line.split(":", 1)[1].strip().split(",")
            for part in parts:
                key, val = part.strip().split("=")
                data[key.strip()] = float(val)
    return data


def run_once(
    center_lat: float,
    center_lon: float,
    radius_km: float,
    grid_step_km: float,
    scan_radius_m: int,
    max_pages: int,
    out_dir: Path,
) -> Dict[str, float]:
    fd, cache_path = tempfile.mkstemp(prefix="ortho_radius_scan_", suffix=".db")
    os.close(fd)
    cmd = [
        sys.executable,
        str(ROOT / "run.py"),
        "--radius-scan",
        "--center-lat",
        str(center_lat),
        "--center-lon",
        str(center_lon),
        "--radius-km",
        str(radius_km),
        "--grid-step-km",
        str(grid_step_km),
        "--scan-radius-m",
        str(scan_radius_m),
        "--max-pages",
        str(max_pages),
        "--cache-path",
        cache_path,
        "--coverage-mode",
        "off",
        "--out",
        str(out_dir),
    ]
    subprocess.check_call(cmd, cwd=str(ROOT))
    summary_path = out_dir / "radius_scan_summary.txt"
    return parse_summary(summary_path)


def main() -> int:
    args = parse_args()
    center_lat, center_lon = resolve_center(args)
    radius_km = args.radius_km if args.radius_km is not None else config.MAX_DISTANCE_KM
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, float]] = []
    prev_unique = None
    for grid_step_km in GRID_STEPS_KM:
        for scan_radius_m in SCAN_RADII_M:
            out_dir = out_root / f"step{grid_step_km}_rad{scan_radius_m}"
            out_dir.mkdir(parents=True, exist_ok=True)
            metrics = run_once(
                center_lat,
                center_lon,
                radius_km,
                grid_step_km,
                scan_radius_m,
                args.max_pages,
                out_dir,
            )
            unique_places = int(metrics.get("unique_place_ids", 0))
            places_network = int(metrics.get("places_network", 0))
            delta_unique = None if prev_unique is None else unique_places - prev_unique
            delta_percent = None
            if prev_unique and delta_unique is not None:
                delta_percent = (delta_unique / prev_unique) * 100.0
            rows.append(
                {
                    "config": f"step={grid_step_km}, radius_m={scan_radius_m}",
                    "places_network": places_network,
                    "unique_places": unique_places,
                    "delta_unique": delta_unique,
                    "delta_percent": delta_percent,
                }
            )
            prev_unique = unique_places

    print("| config | places_network | unique_places | delta_unique_vs_prev | delta_percent |")
    print("|---|---:|---:|---:|---:|")
    for row in rows:
        delta_unique = "" if row["delta_unique"] is None else str(int(row["delta_unique"]))
        delta_percent = (
            ""
            if row["delta_percent"] is None
            else f"{row['delta_percent']:.2f}%"
        )
        print(
            f"| {row['config']} | {row['places_network']} | {row['unique_places']} | {delta_unique} | {delta_percent} |"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
