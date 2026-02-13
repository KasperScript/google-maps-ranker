"""Gemini doctor: minimal end-to-end API probe.

Usage:
  python3 scripts/gemini_doctor.py
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests


MODEL = "gemini-3-pro-preview"
API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _redact_key(key: str) -> str:
    if not key:
        return ""
    if len(key) <= 8:
        return f"{key[:2]}...{key[-2:]}"
    return f"{key[:4]}...{key[-4:]}"


def _redact_text(text: str, key: str) -> str:
    if not text:
        return text
    redacted = text.replace(key, "[REDACTED]")
    redacted = redacted.replace(f"key={key}", "key=[REDACTED]")
    return redacted


def _write_report(
    *,
    report_path: Path,
    api_key: str,
    http_status: Optional[int],
    error: str,
    response_snippet: str,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "Gemini Doctor Report",
        f"generated_at: {_utc_now_iso()}",
        f"model: {MODEL}",
        f"api_key_preview: {_redact_key(api_key)}",
        f"http_status: {http_status if http_status is not None else ''}",
        f"error: {error}",
        "response_snippet:",
        response_snippet,
    ]
    report_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    report_path = repo_root / "out" / "gemini_doctor_report.txt"

    api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    if not api_key:
        msg = "Missing GEMINI_API_KEY in environment."
        print(msg)
        _write_report(
            report_path=report_path,
            api_key="",
            http_status=None,
            error=msg,
            response_snippet="",
        )
        return 1

    payload = {
        "contents": [{"parts": [{"text": 'Return ONLY valid JSON. No markdown. {"ok":true}'}]}],
        "generationConfig": {"temperature": 0.0},
    }

    http_status: Optional[int] = None
    error = ""
    response_snippet = ""

    try:
        resp = requests.post(f"{API_URL}?key={api_key}", json=payload, timeout=30)
        http_status = resp.status_code
        raw_text = resp.text or ""
        redacted_text = _redact_text(raw_text, api_key)
        response_snippet = redacted_text[:300]
        if resp.status_code == 200:
            try:
                data = resp.json()
                candidates = data.get("candidates") or []
                if candidates:
                    print(f"Gemini doctor OK ({MODEL}). Response snippet: {response_snippet[:300]}")
                else:
                    error = "no_candidates"
            except Exception as exc:
                error = f"json_parse_error: {exc}"
        else:
            error = f"http_error: {resp.status_code}"
    except Exception as exc:
        error = f"request_error: {exc}"

    _write_report(
        report_path=report_path,
        api_key=api_key,
        http_status=http_status,
        error=error,
        response_snippet=response_snippet,
    )

    if error:
        print(f"Gemini doctor FAILED: {error}")
        print(f"Report written to: {report_path}")
        return 1

    print(f"Report written to: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
