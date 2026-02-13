"""Optional Gmail sender for outreach emails.

This module is intentionally safe by default:
- It requires explicit opt-in flags in the outreach pipeline.
- It supports dry-run mode and a confirmation string gate.
- Google API imports are performed lazily so tests do not require OAuth.
"""
from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Iterable, Optional

GMAIL_SEND_SCOPE = "https://www.googleapis.com/auth/gmail.send"
GMAIL_MODIFY_SCOPE = "https://www.googleapis.com/auth/gmail.modify"
DEFAULT_CLIENT_JSON = "credentials.json"
DEFAULT_TOKEN_JSON = "token.json"
DEFAULT_SCOPES = (GMAIL_SEND_SCOPE, GMAIL_MODIFY_SCOPE)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _env_path(var_name: str, default_name: str) -> Path:
    raw = (os.environ.get(var_name) or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (_repo_root() / default_name).resolve()


def gmail_client_token_paths() -> tuple[Path, Path]:
    """Return configured Gmail OAuth client and token paths without network calls."""
    client_path = _env_path("GMAIL_OAUTH_CLIENT_JSON", DEFAULT_CLIENT_JSON)
    token_path = _env_path("GMAIL_OAUTH_TOKEN_JSON", DEFAULT_TOKEN_JSON)
    return client_path, token_path


def _import_google_clients():
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError as exc:  # pragma: no cover - depends on optional deps
        raise RuntimeError(
            "Missing Google Gmail dependencies. Install requirements and ensure google-auth-oauthlib is available."
        ) from exc
    return Request, Credentials, InstalledAppFlow, build


@dataclass(frozen=True)
class GmailSendResult:
    status: str
    message_id: str = ""
    error: str = ""


class GmailSender:
    def __init__(
        self,
        client_json_path: Optional[Path] = None,
        token_json_path: Optional[Path] = None,
        scopes: Optional[Iterable[str]] = None,
    ) -> None:
        self.client_json_path = (client_json_path or _env_path("GMAIL_OAUTH_CLIENT_JSON", DEFAULT_CLIENT_JSON)).resolve()
        self.token_json_path = (token_json_path or _env_path("GMAIL_OAUTH_TOKEN_JSON", DEFAULT_TOKEN_JSON)).resolve()
        scopes_list = list(scopes) if scopes is not None else list(DEFAULT_SCOPES)
        self.scopes = scopes_list
        self._service: Any = None

    def _load_credentials(self):
        Request, Credentials, InstalledAppFlow, _build = _import_google_clients()

        creds = None
        if self.token_json_path.exists():
            creds = Credentials.from_authorized_user_file(str(self.token_json_path), self.scopes)
        if creds and creds.valid:
            return creds
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            self._write_token(creds)
            return creds
        if not self.client_json_path.exists():
            raise FileNotFoundError(
                f"Gmail OAuth client JSON not found: {self.client_json_path}. Set GMAIL_OAUTH_CLIENT_JSON or add credentials.json at repo root."
            )
        flow = InstalledAppFlow.from_client_secrets_file(str(self.client_json_path), self.scopes)
        creds = flow.run_local_server(port=0)
        self._write_token(creds)
        return creds

    def _write_token(self, creds: Any) -> None:
        self.token_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.token_json_path.write_text(creds.to_json(), encoding="utf-8")

    def _get_service(self):
        if self._service is not None:
            return self._service
        _Request, _Credentials, _InstalledAppFlow, build = _import_google_clients()
        creds = self._load_credentials()
        self._service = build("gmail", "v1", credentials=creds)
        return self._service

    def get_profile_email(self) -> str:
        service = self._get_service()
        try:
            resp = service.users().getProfile(userId="me").execute()
        except Exception:  # pragma: no cover - depends on external API
            return ""
        return str(resp.get("emailAddress") or "")

    def create_draft(
        self,
        *,
        to_email: str,
        subject: str,
        body: str,
        sender_email: str = "",
        label_name: str = "",
    ) -> dict[str, Any]:
        to_email = (to_email or "").strip()
        subject = (subject or "").strip()
        body = body or ""
        sender_email = (sender_email or "").strip()
        label_name = (label_name or "").strip()
        if not to_email or not subject or not body:
            return {
                "status": "blocked_missing_fields",
                "draft_id": "",
                "message_id": "",
                "thread_id": "",
                "label_name": label_name,
                "label_id": "",
                "label_applied": False,
            }

        service = self._get_service()
        message = MIMEText(body, _subtype="plain", _charset="utf-8")
        message["to"] = to_email
        message["subject"] = subject
        if sender_email:
            message["from"] = sender_email
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

        try:
            resp = (
                service.users()
                .drafts()
                .create(userId="me", body={"message": {"raw": raw}})
                .execute()
            )
        except Exception as exc:  # pragma: no cover - depends on external API
            return {
                "status": "error",
                "draft_id": "",
                "message_id": "",
                "thread_id": "",
                "label_name": label_name,
                "label_id": "",
                "label_applied": False,
                "error": str(exc),
            }

        draft_id = str(resp.get("id") or "")
        message_payload = resp.get("message") or {}
        message_id = str(message_payload.get("id") or "")
        thread_id = str(message_payload.get("threadId") or "")
        label_id = ""
        label_applied = False
        label_error = ""
        if label_name and message_id:
            label_id, label_error = self.ensure_label(label_name)
            if label_id:
                label_applied, apply_error = self.apply_label(message_id=message_id, label_id=label_id)
                if apply_error and not label_error:
                    label_error = apply_error
        status = "drafted" if draft_id else "error"
        result: dict[str, Any] = {
            "status": status,
            "draft_id": draft_id,
            "message_id": message_id,
            "thread_id": thread_id,
            "label_name": label_name,
            "label_id": label_id,
            "label_applied": label_applied,
        }
        if label_error:
            result["label_error"] = label_error
        return result

    def send_email(
        self,
        *,
        to_email: str,
        subject: str,
        body: str,
        dry_run: bool = False,
        label_name: str = "",
    ) -> dict[str, Any]:
        to_email = (to_email or "").strip()
        subject = (subject or "").strip()
        body = body or ""
        label_name = (label_name or "").strip()
        if not to_email or not subject or not body:
            return {
                "status": "blocked_missing_fields",
                "message_id": "",
                "thread_id": "",
                "label_name": label_name,
                "label_id": "",
                "label_applied": False,
            }
        if dry_run:
            return {
                "status": "dry_run",
                "message_id": "",
                "thread_id": "",
                "label_name": label_name,
                "label_id": "",
                "label_applied": False,
            }

        service = self._get_service()
        message = MIMEText(body, _subtype="plain", _charset="utf-8")
        message["to"] = to_email
        message["subject"] = subject
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

        try:
            resp = (
                service.users()
                .messages()
                .send(userId="me", body={"raw": raw})
                .execute()
            )
        except Exception as exc:  # pragma: no cover - depends on external API
            return {
                "status": "error",
                "message_id": "",
                "thread_id": "",
                "label_name": label_name,
                "label_id": "",
                "label_applied": False,
                "error": str(exc),
            }

        message_id = str(resp.get("id") or "")
        thread_id = str(resp.get("threadId") or "")
        label_id = ""
        label_applied = False
        label_error = ""
        if label_name and message_id:
            label_id, label_error = self.ensure_label(label_name)
            if label_id:
                label_applied, apply_error = self.apply_label(message_id=message_id, label_id=label_id)
                if apply_error and not label_error:
                    label_error = apply_error
        result: dict[str, Any] = {
            "status": "sent" if message_id else "error",
            "message_id": message_id,
            "thread_id": thread_id,
            "label_name": label_name,
            "label_id": label_id,
            "label_applied": label_applied,
        }
        if label_error:
            result["label_error"] = label_error
        return result

    def ensure_label(self, label_name: str) -> tuple[str, str]:
        label_name = (label_name or "").strip()
        if not label_name:
            return "", ""
        service = self._get_service()
        try:
            resp = service.users().labels().list(userId="me").execute()
            labels = resp.get("labels") or []
            for label in labels:
                if str(label.get("name") or "") == label_name:
                    return str(label.get("id") or ""), ""
        except Exception as exc:  # pragma: no cover - depends on external API
            return "", f"label_list_error:{exc}"

        try:
            created = (
                service.users()
                .labels()
                .create(
                    userId="me",
                    body={
                        "name": label_name,
                        "labelListVisibility": "labelShow",
                        "messageListVisibility": "show",
                    },
                )
                .execute()
            )
            return str(created.get("id") or ""), ""
        except Exception as exc:  # pragma: no cover - depends on external API
            return "", f"label_create_error:{exc}"

    def apply_label(self, *, message_id: str, label_id: str) -> tuple[bool, str]:
        message_id = (message_id or "").strip()
        label_id = (label_id or "").strip()
        if not message_id or not label_id:
            return False, ""
        service = self._get_service()
        try:
            service.users().messages().modify(
                userId="me",
                id=message_id,
                body={"addLabelIds": [label_id], "removeLabelIds": []},
            ).execute()
            return True, ""
        except Exception as exc:  # pragma: no cover - depends on external API
            return False, f"label_apply_error:{exc}"

    def list_messages(self, *, query: str, max_results: int = 500) -> list[dict[str, Any]]:
        service = self._get_service()
        max_results = max(1, int(max_results))
        results: list[dict[str, Any]] = []
        page_token: Optional[str] = None
        while len(results) < max_results:
            batch_size = min(500, max_results - len(results))
            resp = (
                service.users()
                .messages()
                .list(userId="me", q=query, maxResults=batch_size, pageToken=page_token)
                .execute()
            )
            messages = resp.get("messages") or []
            for msg in messages:
                if isinstance(msg, dict):
                    results.append(msg)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return results[:max_results]

    def get_message(self, *, message_id: str) -> dict[str, Any]:
        message_id = (message_id or "").strip()
        if not message_id:
            return {}
        service = self._get_service()
        resp = (
            service.users()
            .messages()
            .get(
                userId="me",
                id=message_id,
                format="metadata",
                metadataHeaders=["From", "Subject", "Date"],
            )
            .execute()
        )
        return resp if isinstance(resp, dict) else {}
