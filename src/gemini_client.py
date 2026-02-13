"""Gemini client with a safe no-op fallback.

This module never hard-fails when GEMINI_API_KEY is missing. Instead, callers
receive a structured skipped status that can be surfaced in outreach outputs.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import requests

GEMINI_API_URL_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
DEFAULT_GEMINI_MODEL = "gemini-3-pro-preview"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MODEL_CHAIN = [
    "gemini-3-pro-preview",
    "gemini-3-flash-preview",
    "gemini-2.5-pro",
    "gemini-2.5-flash",
]

Validator = Callable[[Dict[str, Any]], None]


@dataclass(frozen=True)
class GeminiCallResult:
    status: str
    raw_text: str
    data: Optional[Dict[str, Any]]
    model: str
    prompt_name: str
    prompt_hash: str
    error: Optional[str] = None


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _strip_code_fences(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    stripped = re.sub(r"^```(?:json)?", "", stripped, flags=re.IGNORECASE).strip()
    stripped = re.sub(r"```$", "", stripped).strip()
    return stripped


def _extract_json_candidate(text: str) -> str:
    """Best-effort extraction of a JSON object or array from model output."""
    candidate = _strip_code_fences(text)
    if candidate.startswith("{") or candidate.startswith("["):
        return candidate
    obj_start = candidate.find("{")
    obj_end = candidate.rfind("}")
    if obj_start != -1 and obj_end != -1 and obj_end > obj_start:
        return candidate[obj_start : obj_end + 1]
    arr_start = candidate.find("[")
    arr_end = candidate.rfind("]")
    if arr_start != -1 and arr_end != -1 and arr_end > arr_start:
        return candidate[arr_start : arr_end + 1]
    return candidate


def _parse_json_loose(text: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    candidate = _extract_json_candidate(text)
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError as exc:
        return None, f"json_decode_error: {exc}"
    if not isinstance(parsed, dict):
        return None, f"json_not_object: {type(parsed).__name__}"
    return parsed, None


class BaseGeminiClient:
    def generate_json(
        self,
        prompt_name: str,
        prompt_text: str,
        prompt_hash: str,
        validator: Optional[Validator] = None,
    ) -> GeminiCallResult:
        raise NotImplementedError


class NoopGeminiClient(BaseGeminiClient):
    def __init__(self, reason: str = "skipped_no_api_key") -> None:
        self.reason = reason

    def generate_json(
        self,
        prompt_name: str,
        prompt_text: str,
        prompt_hash: str,
        validator: Optional[Validator] = None,
    ) -> GeminiCallResult:
        return GeminiCallResult(
            status=self.reason,
            raw_text="",
            data={"status": self.reason},
            model="noop",
            prompt_name=prompt_name,
            prompt_hash=prompt_hash,
            error=None,
        )


class GeminiClient(BaseGeminiClient):
    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_GEMINI_MODEL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    @classmethod
    def from_env(cls) -> BaseGeminiClient:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            return NoopGeminiClient("skipped_no_api_key")
        model = os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)
        return cls(api_key=api_key, model=model)

    def _redact(self, text: str) -> str:
        if not text:
            return text
        redacted = text.replace(self.api_key, "[REDACTED]")
        redacted = re.sub(r"(key=)[^&\\s()]+", r"\1[REDACTED]", redacted)
        return redacted

    def _call_api(self, prompt_text: str, model: str) -> tuple[str, Optional[str], Optional[str]]:
        url = f"{GEMINI_API_URL_TEMPLATE.format(model=model)}?key={self.api_key}"
        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt_text}],
                }
            ],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
        }
        try:
            resp = self.session.post(url, json=payload, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            return "", "request_error", f"request_error: {exc}"
        if resp.status_code >= 400:
            return resp.text, "http_error", f"http_error: {resp.status_code}"
        try:
            data = resp.json()
        except ValueError as exc:
            return resp.text, "invalid_json", f"non_json_response: {exc}"
        candidates = data.get("candidates") or []
        if not candidates:
            return json.dumps(data, ensure_ascii=False), "invalid_json", "no_candidates"
        content = candidates[0].get("content") or {}
        parts = content.get("parts") or []
        if not parts:
            return json.dumps(data, ensure_ascii=False), "invalid_json", "no_parts"
        text = parts[0].get("text")
        if not isinstance(text, str):
            return json.dumps(data, ensure_ascii=False), "invalid_json", "missing_text_part"
        return text, None, None

    def _model_chain(self) -> list[str]:
        primary = (self.model or "").strip()
        chain: list[str] = []
        if primary:
            chain.append(primary)
        for model in DEFAULT_MODEL_CHAIN:
            if model not in chain:
                chain.append(model)
        return chain

    def generate_json(
        self,
        prompt_name: str,
        prompt_text: str,
        prompt_hash: str,
        validator: Optional[Validator] = None,
    ) -> GeminiCallResult:
        last_raw_text = ""
        last_error_type: Optional[str] = None
        last_error_detail: Optional[str] = None
        used_model = self.model

        for model in self._model_chain():
            used_model = model
            raw_text, error_type, error_detail = self._call_api(prompt_text, model=model)
            if error_type in {"http_error", "request_error"}:
                last_raw_text = raw_text
                last_error_type = error_type
                last_error_detail = error_detail
                continue
            if error_type:
                safe_raw_text = self._redact(raw_text)
                safe_error = self._redact(error_detail or error_type)
                return GeminiCallResult(
                    status=error_type,
                    raw_text=safe_raw_text,
                    data=None,
                    model=model,
                    prompt_name=prompt_name,
                    prompt_hash=prompt_hash,
                    error=safe_error,
                )

            last_raw_text = raw_text
            last_error_type = None
            last_error_detail = None
            break

        if last_error_type in {"http_error", "request_error"}:
            safe_error = self._redact(last_error_detail or last_error_type)
            safe_raw_text = self._redact(last_raw_text)
            return GeminiCallResult(
                status=last_error_type,
                raw_text=safe_raw_text,
                data=None,
                model=used_model,
                prompt_name=prompt_name,
                prompt_hash=prompt_hash,
                error=safe_error,
            )

        raw_text = self._redact(last_raw_text)
        parsed, parse_error = _parse_json_loose(raw_text)
        if parse_error:
            return GeminiCallResult(
                status="invalid_json",
                raw_text=raw_text,
                data=None,
                model=self.model,
                prompt_name=prompt_name,
                prompt_hash=prompt_hash,
                error=parse_error,
            )
        if validator is not None:
            try:
                validator(parsed)
            except Exception as exc:  # defensive: validators are project code
                return GeminiCallResult(
                    status="invalid_json",
                    raw_text=raw_text,
                    data=None,
                    model=self.model,
                    prompt_name=prompt_name,
                    prompt_hash=prompt_hash,
                    error=f"validation_error: {exc}",
                )
        return GeminiCallResult(
            status="ok",
            raw_text=raw_text,
            data=parsed,
            model=used_model,
            prompt_name=prompt_name,
            prompt_hash=prompt_hash,
            error=None,
        )
