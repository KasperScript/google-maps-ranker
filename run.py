"""CLI entrypoint."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src import config
from src.cache import Cache
from src.http import HttpClient, RequestBudget, RequestMetrics
from src.outreach.pipeline_outreach import find_latest_merged_csv, run_outreach
from src.pipeline import run, validate_hubs
from src.places_client import PlacesClient

try:
    from dotenv import load_dotenv as _load_dotenv
except Exception:  # pragma: no cover - optional dependency in some environments
    _load_dotenv = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def load_env(path: str = ".env", root_dir: Optional[Path] = None) -> None:
    """Optionally load a repo-root .env file without overriding real env vars."""
    root = Path(root_dir) if root_dir else _repo_root()
    env_path = (root / path).resolve()
    if not env_path.exists():
        return
    if _load_dotenv is not None:
        _load_dotenv(dotenv_path=env_path, override=False)
        return
    # Fallback parser if python-dotenv is unavailable.
    with env_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())


def _env_len(name: str) -> int:
    return len((os.environ.get(name) or "").strip())


def _path_state(path: Path) -> str:
    exists = path.exists()
    readable = bool(exists and os.access(path, os.R_OK))
    return f"{path} (exists={exists}, readable={readable})"


def _outreach_preflight_summary(repo_root: Path) -> Tuple[Dict[str, Any], List[Path]]:
    prompt_price = (repo_root / "prompts" / "gemini_price_calc_v3.txt").resolve()
    prompt_outreach = (repo_root / "prompts" / "gemini_outreach_message_v2.txt").resolve()
    prompt_template = (repo_root / "prompts" / "email_template_no_pricing_pl.txt").resolve()

    try:
        from src.gmail_sender import gmail_client_token_paths

        gmail_client_path, gmail_token_path = gmail_client_token_paths()
    except Exception:
        gmail_client_path = (repo_root / "credentials.json").resolve()
        gmail_token_path = (repo_root / "token.json").resolve()

    preflight_info: Dict[str, Any] = {
        "gemini_api_key_len": _env_len("GEMINI_API_KEY"),
        "google_maps_api_key_len": _env_len("GOOGLE_MAPS_API_KEY"),
        "gmail_oauth_client_json": _path_state(gmail_client_path),
        "gmail_oauth_token_json": _path_state(gmail_token_path),
        "prompt_price_calc_exists": prompt_price.exists(),
        "prompt_outreach_exists": prompt_outreach.exists(),
        "prompt_email_template_exists": prompt_template.exists(),
    }

    print("Outreach preflight (redacted):")
    print(f"- GEMINI_API_KEY length: {preflight_info['gemini_api_key_len']}")
    print(f"- GOOGLE_MAPS_API_KEY length: {preflight_info['google_maps_api_key_len']}")
    print(f"- Gmail client: {preflight_info['gmail_oauth_client_json']}")
    print(f"- Gmail token: {preflight_info['gmail_oauth_token_json']}")
    print(f"- prompt gemini_price_calc_v3.txt exists: {preflight_info['prompt_price_calc_exists']}")
    print(f"- prompt gemini_outreach_message_v2.txt exists: {preflight_info['prompt_outreach_exists']}")
    print(f"- prompt email_template_no_pricing_pl.txt exists: {preflight_info['prompt_email_template_exists']}")

    missing_prompts = [p for p in (prompt_price, prompt_outreach, prompt_template) if not p.exists()]
    return preflight_info, missing_prompts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank places using Google Maps")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--preflight", action="store_true", help="Run offline checks only")
    group.add_argument(
        "--preflight-online",
        action="store_true",
        help="Run offline checks + one cheap Places call (uses cache if available)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Run with tiny request caps")
    parser.add_argument("--top", type=int, default=config.TOP_N_QUALITY, help="Top N by quality")
    parser.add_argument("--max-places", type=int, default=config.MAX_PLACES_REQUESTS_PER_RUN)
    parser.add_argument("--max-routes", type=int, default=config.MAX_ROUTES_REQUESTS_PER_RUN)
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--refresh-routes", action="store_true")
    parser.add_argument(
        "--coverage-mode",
        choices=["off", "light", "full"],
        default=None,
        help="Coverage mode: off, light, or full (default: light)",
    )
    parser.add_argument(
        "--coverage-budget-share",
        type=float,
        default=0.20,
        help="Max share of Places budget reserved for coverage (default: 0.20)",
    )
    parser.add_argument("--cache-path", type=str, default=config.CACHE_DB_PATH)
    parser.add_argument("--dedup-probe", action="store_true", help="Issue duplicate requests to prove in-run dedup")
    parser.add_argument("--list-mode", action="store_true", help="List all acceptable candidates near any hub")
    parser.add_argument("--radius-scan", action="store_true", help="Exhaustive radius scan mode")
    parser.add_argument("--center", type=str, default=None, help="Hub id to use as scan center")
    parser.add_argument("--centers", type=str, default=None, help="Comma-separated hub ids to scan")
    parser.add_argument("--center-lat", type=float, default=None)
    parser.add_argument("--center-lon", type=float, default=None)
    parser.add_argument("--radius-km", type=float, default=None)
    parser.add_argument("--grid-step-km", type=float, default=None)
    parser.add_argument("--scan-radius-m", type=int, default=None)
    parser.add_argument("--queries", type=str, default=None)
    parser.add_argument("--types", type=str, default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--extreme", action="store_true", help="Use extreme accuracy defaults for radius scan")
    parser.add_argument("--refresh-places", action="store_true", help="Bypass Places cache reads")
    parser.add_argument("--out", type=str, default=config.OUTPUT_DIR)

    # Places website enrichment flags
    parser.add_argument(
        "--enrich-websites",
        action="store_true",
        help="Enrich missing websites via Places Details API (by place_id) and exit",
    )
    parser.add_argument(
        "--enrich-top-n",
        type=int,
        default=100,
        help="Top N records to enrich (default: 100)",
    )
    parser.add_argument(
        "--enrich-input",
        type=str,
        default=str((_repo_root() / "out" / "results.json").resolve()),
        help="Input ranked JSON (default: out/results.json)",
    )
    parser.add_argument(
        "--enrich-output",
        type=str,
        default="out/results_with_websites.json",
        help="Output JSON path for enriched results (default: out/results_with_websites.json)",
    )

    # Outreach stage flags
    parser.add_argument("--outreach", action="store_true", help="Run outreach stage on merged CSV")
    parser.add_argument(
        "--outreach-input",
        type=str,
        default=None,
        help="Path to outreach input (CSV or JSON; default: latest merged CSV under out/ if available)",
    )
    parser.add_argument(
        "--outreach-top-n",
        type=int,
        default=30,
        help="Process top N clinics from merged CSV (default: 30)",
    )
    parser.add_argument(
        "--outreach-out",
        type=str,
        default="out/outreach",
        help="Outreach output directory (default: out/outreach)",
    )
    parser.add_argument(
        "--outreach-max-pages",
        type=int,
        default=30,
        help="Max pages to crawl per clinic (default: 30)",
    )
    parser.add_argument(
        "--outreach-playwright-assist",
        action="store_true",
        help="Enable Playwright assist mode (autofill only, no submit)",
    )
    parser.add_argument(
        "--outreach-playwright-headed",
        action="store_true",
        help="Run Playwright assist in headed mode (visible browser window)",
    )
    parser.add_argument(
        "--outreach-playwright-slowmo-ms",
        type=int,
        default=0,
        help="Playwright slow motion delay in milliseconds (default: 0)",
    )
    parser.add_argument(
        "--outreach-refresh-web",
        action="store_true",
        help="Bypass fetched-page cache for outreach crawling",
    )
    parser.add_argument(
        "--outreach-refresh-places",
        action="store_true",
        help="Bypass Places cache reads for outreach metadata (placeholder for parity)",
    )
    parser.add_argument(
        "--outreach-force",
        action="store_true",
        help="Force re-crawl and re-run Gemini even if evidence exists",
    )
    parser.add_argument(
        "--gmail-drafts",
        action="store_true",
        help="Create Gmail drafts for ready_to_email items (safe default path)",
    )
    parser.add_argument(
        "--gmail-sender",
        type=str,
        default="",
        help="Optional sender email address for Gmail drafts/sends",
    )
    parser.add_argument(
        "--gmail-max-drafts",
        type=int,
        default=5,
        help="Maximum number of Gmail drafts to create per run (default: 5)",
    )
    parser.add_argument(
        "--gmail-send",
        action="store_true",
        help="Enable Gmail sending (requires explicit acknowledgement flag)",
    )
    parser.add_argument(
        "--i-understand-this-will-send-email",
        dest="gmail_send_ack",
        action="store_true",
        help="Required acknowledgement flag before Gmail send will run",
    )
    parser.add_argument(
        "--gmail-send-dry-run",
        dest="gmail_send_dry_run",
        action="store_true",
        default=True,
        help="Dry run Gmail send mode (default: true; no emails sent)",
    )
    parser.add_argument(
        "--gmail-send-no-dry-run",
        dest="gmail_send_dry_run",
        action="store_false",
        help="Allow actual Gmail sends when acknowledgement flag is set",
    )
    parser.add_argument(
        "--gmail-daily-limit",
        type=int,
        default=10,
        help="Daily Gmail send limit enforced by local log (default: 10)",
    )
    parser.add_argument(
        "--allow-domains",
        type=str,
        default="",
        help="Optional comma-separated domain allowlist for Gmail sends",
    )
    parser.add_argument(
        "--gmail-send-log-path",
        type=str,
        default=None,
        help="Optional path for Gmail send dedupe/daily-limit log",
    )
    parser.add_argument(
        "--gmail-sync",
        action="store_true",
        help="Sync Gmail replies incrementally using last successful sync state (read-only)",
    )
    parser.add_argument(
        "--generate-price-list",
        action="store_true",
        help="Generate a human-readable price comparison table from outreach results",
    )
    parser.add_argument(
        "--gmail-sync-lookback-hours",
        type=int,
        default=72,
        help="Fallback lookback window in hours when no prior sync exists (default: 72)",
    )
    parser.add_argument(
        "--gmail-sync-grace-minutes",
        type=int,
        default=30,
        help="Grace overlap in minutes applied to the last successful sync timestamp (default: 30)",
    )
    parser.add_argument(
        "--gmail-sync-label",
        type=str,
        default="",
        help="Optional Gmail label to scope sync (e.g., OrthoRanker)",
    )
    parser.add_argument(
        "--gmail-sync-query",
        type=str,
        default="",
        help="Optional extra Gmail query filters to append",
    )
    parser.add_argument(
        "--outreach-send-gmail",
        action="store_true",
        help="Legacy Gmail send flag (prefer --gmail-drafts / --gmail-send)",
    )
    parser.add_argument(
        "--outreach-send-confirm",
        type=str,
        default="",
        help='Legacy safety confirmation string; must equal "SEND" to send',
    )
    parser.add_argument(
        "--outreach-send-max",
        type=int,
        default=5,
        help="Legacy Gmail send max (mapped to gmail daily limit when used)",
    )
    parser.add_argument(
        "--outreach-send-dry-run",
        dest="outreach_send_dry_run",
        action="store_true",
        default=True,
        help="Dry run Gmail sending (default: true; no emails sent)",
    )
    parser.add_argument(
        "--outreach-send-no-dry-run",
        dest="outreach_send_dry_run",
        action="store_false",
        help="Allow actual Gmail sends when confirm string is SEND",
    )
    return parser.parse_args()


def apply_extreme_defaults(args: argparse.Namespace) -> None:
    if not args.extreme:
        return
    if args.radius_km is None:
        args.radius_km = 20.0
    if args.grid_step_km is None:
        args.grid_step_km = 0.5
    if args.scan_radius_m is None:
        args.scan_radius_m = 800
    if args.max_pages is None:
        args.max_pages = 3


def run_preflight(api_key: Optional[str], online: bool, cache_path: str) -> int:
    ok = True
    hubs_ok = True

    if api_key:
        print("API key: OK")
    else:
        print("API key: MISSING")
        ok = False

    try:
        validate_hubs(config.HUBS)
        print("Hubs: OK")
    except ValueError as exc:
        print(f"Hubs: FAIL ({exc})")
        ok = False
        hubs_ok = False

    print(
        "Request caps: max_places={max_places}, max_routes={max_routes}".format(
            max_places=config.MAX_PLACES_REQUESTS_PER_RUN,
            max_routes=config.MAX_ROUTES_REQUESTS_PER_RUN,
        )
    )

    if online:
        if not api_key:
            print("Online Places call: FAIL (missing API key)")
            ok = False
        elif not hubs_ok:
            print("Online Places call: SKIPPED (invalid hubs)")
        else:
            try:
                budget = RequestBudget(max_places=1, max_routes=0)
                cache = Cache(cache_path)
                try:
                    http_client = HttpClient(
                        api_key,
                        timeout=config.HTTP_TIMEOUT_SECONDS,
                        retry_max=config.HTTP_RETRY_MAX,
                        backoff_base=config.HTTP_BACKOFF_BASE,
                        backoff_max=config.HTTP_BACKOFF_MAX,
                    )
                    places_client = PlacesClient(http_client, cache, budget, no_cache=False)
                    hub_id = sorted(config.HUBS.keys())[0]
                    hub = config.HUBS[hub_id]
                    point = {"id": hub_id, "name": hub["name"], "lat": hub["lat"], "lon": hub["lon"]}
                    queries = list(config.PRIMARY_QUERIES)
                    query = queries[0] if queries else "place"
                    places_client.search_text(query, point, type_filter=None)
                    print("Online Places call: OK")
                finally:
                    cache.close()
            except Exception as exc:
                print(f"Online Places call: FAIL ({exc})")
                ok = False

    print("Preflight: PASS" if ok else "Preflight: FAIL")
    return 0 if ok else 1


def main() -> int:
    load_env()
    config.load_search_config()
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    if args.preflight or args.preflight_online:
        api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
        return run_preflight(api_key, online=args.preflight_online, cache_path=args.cache_path)

    if args.gmail_sync:
        try:
            from src.gmail_sender import GmailSender
            from src.gmail_sync import sync_gmail_replies

            gmail_client = GmailSender()
            summary = sync_gmail_replies(
                gmail_client=gmail_client,
                lookback_hours=args.gmail_sync_lookback_hours,
                grace_minutes=args.gmail_sync_grace_minutes,
                label=args.gmail_sync_label,
                extra_query=args.gmail_sync_query,
            )
        except Exception as exc:
            print(f"Gmail sync error: {exc}", file=sys.stderr)
            return 1

        print("Gmail sync summary:")
        print(f"- success: {summary.success}")
        print(f"- start_time_utc: {summary.start_time_utc}")
        print(f"- end_time_utc: {summary.end_time_utc}")
        print(f"- used_last_sync: {summary.used_last_sync}")
        print(f"- gmail_query: {summary.gmail_query}")
        print(f"- thread_map_count: {summary.thread_map_count}")
        print(f"- fetched_count: {summary.fetched_count}")
        print(f"- new_replies_count: {summary.new_replies_count}")
        print(f"- updated_results_count: {summary.updated_results_count}")
        print(f"- updated_queue_count: {summary.updated_queue_count}")
        print(f"- state_path: {summary.state_path}")
        print(f"- replies_path: {summary.replies_path}")
        print(f"- report_path: {summary.report_path}")
        if summary.last_run_dir:
            print(f"- last_run_dir: {summary.last_run_dir}")
        return 0 if summary.success else 1

    if args.generate_price_list:
        try:
            from scripts.generate_price_comparison import main as generate_prices
            return generate_prices()
        except Exception as exc:
            print(f"Price list generation error: {exc}", file=sys.stderr)
            return 1

    if args.enrich_websites:
        api_key = (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
        if not api_key:
            print("Missing GOOGLE_MAPS_API_KEY in environment", file=sys.stderr)
            return 1
        input_path = Path(args.enrich_input).expanduser().resolve()
        output_path = Path(args.enrich_output).expanduser().resolve()
        if not input_path.exists():
            print(f"Enrich input missing: {input_path}", file=sys.stderr)
            return 1
        try:
            from scripts.enrich_websites_from_places import enrich_websites_from_places

            summary = enrich_websites_from_places(
                input_path=input_path,
                output_path=output_path,
                top_n=args.enrich_top_n,
                api_key=api_key,
            )
        except Exception as exc:
            print(f"Enrichment error: {exc}", file=sys.stderr)
            return 1

        print("Enrichment summary:")
        print(f"- processed: {summary.processed}")
        print(f"- updated_websites_count: {summary.updated_websites_count}")
        print(f"- missing_place_id_count: {summary.missing_place_id_count}")
        print(f"- api_errors_count: {summary.api_errors_count}")
        print(f"- output: {output_path}")
        return 0

    if args.outreach:
        repo_root = _repo_root()
        preflight_info, missing_prompts = _outreach_preflight_summary(repo_root)
        if missing_prompts:
            print("Outreach blocked: missing required prompt/template files:", file=sys.stderr)
            for path in missing_prompts:
                print(f"- {path}", file=sys.stderr)
            return 1

        outreach_input = args.outreach_input
        if outreach_input is None:
            latest = find_latest_merged_csv()
            if latest:
                outreach_input = str(latest)
        if not outreach_input:
            print(
                "Outreach input not found. Provide --outreach-input or run a radius scan first.",
                file=sys.stderr,
            )
            return 1
        outreach_input_path = Path(outreach_input).expanduser().resolve()
        if not outreach_input_path.exists():
            print(f"Outreach input missing: {outreach_input_path}", file=sys.stderr)
            return 1
        try:
            result = run_outreach(
                input_csv_path=str(outreach_input_path),
                out_dir=args.outreach_out,
                top_n=args.outreach_top_n,
                max_pages=args.outreach_max_pages,
                refresh_web=args.outreach_refresh_web or args.outreach_force,
                refresh_places=args.outreach_refresh_places or args.refresh_places or args.outreach_force,
                playwright_assist=args.outreach_playwright_assist,
                playwright_headed=args.outreach_playwright_headed,
                playwright_slowmo_ms=args.outreach_playwright_slowmo_ms,
                outreach_force=args.outreach_force,
                gmail_drafts=args.gmail_drafts,
                gmail_sender_email=args.gmail_sender,
                gmail_max_drafts=args.gmail_max_drafts,
                gmail_send=args.gmail_send,
                gmail_send_ack=args.gmail_send_ack,
                gmail_send_dry_run=args.gmail_send_dry_run,
                gmail_daily_limit=args.gmail_daily_limit,
                gmail_allow_domains=args.allow_domains,
                gmail_send_log_path=args.gmail_send_log_path,
                preflight_info=preflight_info,
                outreach_send_gmail=args.outreach_send_gmail,
                outreach_send_confirm=args.outreach_send_confirm,
                outreach_send_max=args.outreach_send_max,
                outreach_send_dry_run=args.outreach_send_dry_run,
            )
        except Exception as exc:
            print(f"Outreach error: {exc}", file=sys.stderr)
            return 1

        print(f"Outreach run_id: {result.run_id}")
        print(f"Outreach run_dir: {result.run_dir}")
        print(f"Outreach QA report: {result.qa_report_path}")
        if result.gmail_report_path:
            print(f"Outreach Gmail report: {result.gmail_report_path}")
        print(f"Outreach results: {result.results_path}")
        print(f"Outreach queue: {result.queue_path}")
        print(f"Outreach summary: {result.summary_path}")
        return 0

    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("Missing GOOGLE_MAPS_API_KEY in environment", file=sys.stderr)
        return 1

    try:
        max_places = args.max_places
        max_routes = args.max_routes
        top_n = args.top
        skip_routes = False
        coverage_mode = args.coverage_mode
        if coverage_mode is None:
            coverage_mode = "off" if args.list_mode or args.radius_scan else "light"
        apply_extreme_defaults(args)
        if args.dry_run:
            max_places = config.DRY_RUN_MAX_PLACES
            max_routes = config.DRY_RUN_MAX_ROUTES
            top_n = min(top_n, config.DRY_RUN_TOP_N)
            skip_routes = True
        if args.list_mode:
            skip_routes = True
        if args.radius_scan:
            skip_routes = True
            max_routes = 0

        scan_queries = None
        if args.queries is not None:
            scan_queries = [q.strip() for q in args.queries.split(",") if q.strip()]
        scan_types = None
        if args.types is not None:
            scan_types = [t.strip() for t in args.types.split(",") if t.strip()]

        centers_list = None
        if args.centers is not None:
            centers_list = [c.strip() for c in args.centers.split(",") if c.strip()]

        center_lat = args.center_lat
        center_lon = args.center_lon
        if args.radius_scan:
            if centers_list:
                pass
            elif args.center:
                hub = config.HUBS.get(args.center)
                if not hub:
                    print(f"Unknown hub id for --center: {args.center}", file=sys.stderr)
                    return 1
                center_lat = hub["lat"]
                center_lon = hub["lon"]
            if not centers_list and (center_lat is None or center_lon is None):
                print("radius-scan requires --center or --center-lat/--center-lon", file=sys.stderr)
                return 1

        metrics = RequestMetrics()
        run(
            api_key=api_key,
            cache_db_path=args.cache_path,
            max_places=max_places,
            max_routes=max_routes,
            no_cache=args.no_cache,
            refresh_routes=args.refresh_routes,
            top_n=top_n,
            output_dir=args.out,
            write_outputs=True,
            skip_routes=skip_routes,
            coverage_mode=coverage_mode,
            coverage_budget_share=args.coverage_budget_share,
            metrics=metrics,
            dedup_probe=args.dedup_probe,
            list_mode=args.list_mode,
            radius_scan=args.radius_scan,
            radius_scan_center_lat=center_lat,
            radius_scan_center_lon=center_lon,
            radius_scan_center_id=args.center,
            radius_scan_radius_km=args.radius_km,
            radius_scan_grid_step_km=args.grid_step_km,
            radius_scan_scan_radius_m=args.scan_radius_m,
            radius_scan_queries=scan_queries,
            radius_scan_types=scan_types,
            radius_scan_max_pages=args.max_pages,
            radius_scan_centers=centers_list,
            refresh_places=args.refresh_places,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(
            f"Dry-run complete. Results written to {args.out}/results.csv and {args.out}/results.json"
        )
    else:
        print(f"Done. Results written to {args.out}/results.csv and {args.out}/results.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
