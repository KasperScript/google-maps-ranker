"""Setup wizard server.

Serves the setup UI and provides API endpoints for Gemini search term
generation, geocoding, and saving configuration files.
"""
from __future__ import annotations

import json
import os
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

import requests

REPO_ROOT = Path(__file__).resolve().parent
SETUP_DIR = REPO_ROOT / "setup"
DEFAULT_PORT = 8000


class SetupHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(SETUP_DIR), **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/geocode":
            self._handle_geocode(parsed.query)
        else:
            super().do_GET()

    def do_POST(self) -> None:
        if self.path == "/api/generate-searches":
            self._handle_generate_searches()
        elif self.path == "/api/save-config":
            self._handle_save_config()
        else:
            self.send_error(404)

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        return json.loads(raw) if raw else {}

    def _send_json(self, data: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_generate_searches(self) -> None:
        payload = self._read_json_body()
        description = (payload.get("description") or "").strip()
        api_key = (payload.get("gemini_api_key") or "").strip()

        if not description:
            self._send_json({"error": "Description is required."}, 400)
            return
        if not api_key:
            self._send_json({"error": "Gemini API key is required."}, 400)
            return

        prompt = _build_search_prompt(description)

        try:
            result = _call_gemini(api_key, prompt)
            self._send_json(result)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 500)

    def _handle_geocode(self, query_string: str) -> None:
        params = parse_qs(query_string)
        query = (params.get("q") or [""])[0].strip()
        api_key = (params.get("key") or [""])[0].strip()

        if not query:
            self._send_json({"error": "Query parameter 'q' is required."}, 400)
            return

        try:
            results = _geocode(query, api_key)
            self._send_json({"results": results})
        except Exception as exc:
            self._send_json({"error": str(exc)}, 500)

    def _handle_save_config(self) -> None:
        payload = self._read_json_body()
        config_data = payload.get("config", {})
        env_data = payload.get("env", {})

        try:
            config_path = REPO_ROOT / "search_config.json"
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)

            if env_data:
                _write_env(env_data)

            self._send_json({"ok": True, "config_path": str(config_path)})
        except Exception as exc:
            self._send_json({"error": str(exc)}, 500)

    def log_message(self, fmt: str, *args: Any) -> None:
        if "/api/" in (args[0] if args else ""):
            super().log_message(fmt, *args)


def _build_search_prompt(description: str) -> str:
    return f"""You are helping configure a Google Maps place ranking tool.

The user wants to find: "{description}"

Generate a JSON object with exactly these keys:
- "primary_queries": list of 4-8 text search queries (the main terms to search for)
- "secondary_queries": list of 2-4 broader fallback queries
- "type_filters": list of Google Maps place types to filter by (use official types like "restaurant", "dentist", "hair_care", etc.)
- "reject_substrings": list of name substrings to exclude irrelevant results (e.g. if searching for barbershops, reject "pet", "animal")
- "min_reviews": recommended minimum review count (integer, typically 20-100)

Return ONLY valid JSON, nothing else."""


GEMINI_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-2.0-flash",
]


def _call_gemini(api_key: str, prompt: str) -> Dict[str, Any]:
    last_error = None

    for model in GEMINI_MODELS:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        body = {"contents": [{"parts": [{"text": prompt}]}]}

        try:
            resp = requests.post(url, json=body, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                text = (
                    data.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                )
                return _parse_gemini_json(text)
            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except requests.RequestException as exc:
            last_error = str(exc)

    raise RuntimeError(f"All Gemini models failed. Last error: {last_error}")


def _parse_gemini_json(text: str) -> Dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(cleaned[start:end])
        raise ValueError("Could not parse Gemini response as JSON.")


def _geocode(query: str, api_key: str) -> list:
    if api_key:
        return _geocode_google(query, api_key)
    return _geocode_nominatim(query)


def _geocode_google(query: str, api_key: str) -> list:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": query, "key": api_key}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    results = []
    for item in data.get("results", [])[:5]:
        loc = item.get("geometry", {}).get("location", {})
        results.append({
            "name": item.get("formatted_address", query),
            "lat": loc.get("lat"),
            "lon": loc.get("lng"),
        })
    return results


def _geocode_nominatim(query: str) -> list:
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": query, "format": "json", "limit": 5}
    headers = {"User-Agent": "PlaceRankerSetup/1.0"}
    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    results = []
    for item in data[:5]:
        results.append({
            "name": item.get("display_name", query),
            "lat": float(item.get("lat", 0)),
            "lon": float(item.get("lon", 0)),
        })
    return results


def _write_env(env_data: Dict[str, str]) -> None:
    env_path = REPO_ROOT / ".env"
    existing: Dict[str, str] = {}

    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    existing[key.strip()] = val.strip()

    for key, val in env_data.items():
        if val:
            existing[key] = val

    with open(env_path, "w", encoding="utf-8") as f:
        for key, val in existing.items():
            f.write(f"{key}={val}\n")


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT

    server = HTTPServer(("", port), SetupHandler)
    print(f"Setup wizard running at http://localhost:{port}")
    print("Press Ctrl+C to stop.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
