"""Incremental Gmail reply sync for outreach threads.

This module is read-only with respect to Gmail:
- It never sends emails.
- It syncs replies based on the last successful sync timestamp with a grace overlap.
- It writes minimal structured data locally for manual review.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from src.reporting import atomic_write_text, atomic_writer

logger = logging.getLogger(__name__)

STATE_PATH = Path("out/gmail_sync_state.json")
REPLIES_PATH = Path("out/outreach_replies.jsonl")
REPORT_PATH = Path("out/outreach_gmail_sync_report.txt")
OUTREACH_RUNS_PARENT = Path("out/outreach_runs")

DEFAULT_LOOKBACK_HOURS = 72
DEFAULT_GRACE_MINUTES = 30
MAX_SEEN_MESSAGE_IDS = 10_000


@dataclass(frozen=True)
class GmailSyncSummary:
    success: bool
    start_time_utc: str
    end_time_utc: str
    used_last_sync: bool
    gmail_query: str
    thread_map_count: int
    fetched_count: int
    new_replies_count: int
    filtered_old_count: int
    filtered_self_count: int
    filtered_not_thread_count: int
    dedup_skipped_count: int
    updated_results_count: int
    updated_queue_count: int
    state_path: Path
    replies_path: Path
    report_path: Path
    last_run_dir: Optional[Path]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _format_utc_z(dt: datetime) -> str:
    dt_utc = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc_z(value: str) -> datetime:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("empty timestamp")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def load_sync_state(path: Path = STATE_PATH) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to parse Gmail sync state at %s", path)
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _save_sync_state(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_writer(str(path), mode="w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def compute_time_window(
    *,
    now_utc: datetime,
    state: Dict[str, Any],
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    grace_minutes: int = DEFAULT_GRACE_MINUTES,
) -> Tuple[datetime, datetime, bool]:
    lookback_hours = max(1, int(lookback_hours))
    grace_minutes = max(0, int(grace_minutes))

    last_sync_raw = str(state.get("last_successful_sync_utc") or "").strip()
    if last_sync_raw:
        try:
            last_sync = _parse_utc_z(last_sync_raw)
            start = last_sync - timedelta(minutes=grace_minutes)
            return start, now_utc, True
        except Exception:
            logger.warning("Invalid last_successful_sync_utc value: %s", last_sync_raw)

    start = now_utc - timedelta(hours=lookback_hours)
    return start, now_utc, False


def build_gmail_query(
    *,
    start_time_utc: datetime,
    label: str = "",
    extra_query: str = "",
) -> str:
    # Gmail query language is date-only for after:, so we widen by 1 day
    # and apply precise filtering in code using internalDate.
    after_date = (start_time_utc - timedelta(days=1)).date().strftime("%Y/%m/%d")
    parts: List[str] = [f"after:{after_date}"]
    label = (label or "").strip()
    extra_query = (extra_query or "").strip()
    if label:
        parts.append(f"label:{label}")
    if extra_query:
        parts.append(extra_query)
    return " ".join(parts).strip()


def _iter_run_dirs(out_parent: Path) -> Iterable[Path]:
    if not out_parent.exists():
        return []
    run_dirs = [p for p in out_parent.iterdir() if p.is_dir()]
    return sorted(run_dirs, key=lambda p: p.name, reverse=True)


def find_latest_run_dir(out_parent: Path = OUTREACH_RUNS_PARENT) -> Optional[Path]:
    for run_dir in _iter_run_dirs(out_parent):
        if (run_dir / "outreach_results.json").exists():
            return run_dir
    return None


def _thread_ids_from_row(row: Dict[str, Any]) -> Sequence[str]:
    thread_ids: List[str] = []
    for key in ("gmail_draft", "gmail_send"):
        payload = row.get(key) or {}
        if not isinstance(payload, dict):
            continue
        thread_id = str(payload.get("thread_id") or payload.get("threadId") or "").strip()
        if thread_id:
            thread_ids.append(thread_id)
    return thread_ids


def collect_thread_mapping(out_parent: Path = OUTREACH_RUNS_PARENT) -> Tuple[Dict[str, str], str]:
    mapping: Dict[str, str] = {}
    last_run_id = ""
    for run_dir in _iter_run_dirs(out_parent):
        results_path = run_dir / "outreach_results.json"
        if not results_path.exists():
            continue
        try:
            rows = json.loads(results_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        if not last_run_id:
            last_run_id = run_dir.name
        for row in rows:
            if not isinstance(row, dict):
                continue
            clinic_name = str(row.get("clinic_name") or row.get("name") or "").strip()
            if not clinic_name:
                continue
            for thread_id in _thread_ids_from_row(row):
                mapping.setdefault(thread_id, clinic_name)
    return mapping, last_run_id


def _load_seen_message_ids(
    *,
    replies_path: Path,
    state_seen_ids: Sequence[str],
) -> set[str]:
    seen: set[str] = {str(v) for v in state_seen_ids if str(v)}
    if not replies_path.exists():
        return seen
    for line in replies_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        msg_id = str(payload.get("message_id") or "").strip()
        if msg_id:
            seen.add(msg_id)
    return seen


def _header_value(message: Dict[str, Any], name: str) -> str:
    payload = message.get("payload") or {}
    headers = payload.get("headers") or []
    for header in headers:
        if not isinstance(header, dict):
            continue
        if str(header.get("name") or "").lower() == name.lower():
            return str(header.get("value") or "")
    return ""


def _internal_date_ms(message: Dict[str, Any]) -> int:
    raw = str(message.get("internalDate") or "0").strip()
    try:
        return int(raw)
    except Exception:
        return 0


def _from_is_self(from_header: str, own_email: str) -> bool:
    own = (own_email or "").strip().lower()
    if not own:
        return False
    return own in (from_header or "").lower()


def _latest_reply_by_thread(replies: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for reply in replies:
        thread_id = str(reply.get("thread_id") or "").strip()
        if not thread_id:
            continue
        prev = latest.get(thread_id)
        if not prev:
            latest[thread_id] = reply
            continue
        prev_ts = str(prev.get("received_at_utc") or "")
        cur_ts = str(reply.get("received_at_utc") or "")
        if cur_ts > prev_ts:
            latest[thread_id] = reply
    return latest


def _attach_latest_reply_to_results(run_dir: Path, replies: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    results_path = run_dir / "outreach_results.json"
    queue_path = run_dir / "outreach_queue.jsonl"
    if not results_path.exists():
        return 0, 0

    latest_by_thread = _latest_reply_by_thread(replies)
    if not latest_by_thread:
        return 0, 0

    updated_results = 0
    try:
        rows = json.loads(results_path.read_text(encoding="utf-8"))
    except Exception:
        rows = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            thread_id = ""
            for key in ("gmail_draft", "gmail_send"):
                payload = row.get(key) or {}
                if not isinstance(payload, dict):
                    continue
                thread_id = str(payload.get("thread_id") or payload.get("threadId") or "").strip()
                if thread_id:
                    break
            if not thread_id or thread_id not in latest_by_thread:
                continue
            latest = latest_by_thread[thread_id]
            row["latest_reply"] = {
                "received_at_utc": latest.get("received_at_utc"),
                "from": latest.get("from"),
                "subject": latest.get("subject"),
                "snippet": latest.get("snippet"),
                "message_id": latest.get("message_id"),
                "thread_id": thread_id,
            }
            updated_results += 1
        with atomic_writer(str(results_path), mode="w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)

    updated_queue = 0
    if queue_path.exists():
        queue_rows: List[Dict[str, Any]] = []
        for line in queue_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            thread_id = ""
            for key in ("gmail_draft", "gmail_send"):
                sub = payload.get(key) or {}
                if not isinstance(sub, dict):
                    continue
                thread_id = str(sub.get("thread_id") or sub.get("threadId") or "").strip()
                if thread_id:
                    break
            if thread_id and thread_id in latest_by_thread:
                latest = latest_by_thread[thread_id]
                payload["latest_reply"] = {
                    "received_at_utc": latest.get("received_at_utc"),
                    "from": latest.get("from"),
                    "subject": latest.get("subject"),
                    "snippet": latest.get("snippet"),
                    "message_id": latest.get("message_id"),
                    "thread_id": thread_id,
                }
                updated_queue += 1
            queue_rows.append(payload)
        with atomic_writer(str(queue_path), mode="w", encoding="utf-8") as f:
            for row in queue_rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return updated_results, updated_queue


def _update_seen_ids(state_seen_ids: Sequence[str], new_ids: Sequence[str]) -> List[str]:
    ordered = list(state_seen_ids) + [msg_id for msg_id in new_ids if msg_id]
    deduped = list(dict.fromkeys(ordered))
    if len(deduped) > MAX_SEEN_MESSAGE_IDS:
        deduped = deduped[-MAX_SEEN_MESSAGE_IDS:]
    return deduped


def _write_report(path: Path, lines: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(str(path), "\n".join(lines).strip() + "\n")


def sync_gmail_replies(
    *,
    gmail_client: Any,
    state_path: Path = STATE_PATH,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    grace_minutes: int = DEFAULT_GRACE_MINUTES,
    label: str = "",
    extra_query: str = "",
    out_replies_path: Path = REPLIES_PATH,
    out_report_path: Path = REPORT_PATH,
    out_runs_parent: Path = OUTREACH_RUNS_PARENT,
) -> GmailSyncSummary:
    """Sync Gmail replies incrementally and attach the latest reply to the latest run."""
    state = load_sync_state(state_path)
    now_utc = _utc_now()
    start_utc, end_utc, used_last_sync = compute_time_window(
        now_utc=now_utc,
        state=state,
        lookback_hours=lookback_hours,
        grace_minutes=grace_minutes,
    )
    gmail_query = build_gmail_query(start_time_utc=start_utc, label=label, extra_query=extra_query)

    thread_map, last_run_id = collect_thread_mapping(out_runs_parent)
    thread_map_count = len(thread_map)

    start_ms = int(start_utc.timestamp() * 1000)
    state_seen_ids = list(state.get("seen_message_ids") or [])
    seen_ids = _load_seen_message_ids(replies_path=out_replies_path, state_seen_ids=state_seen_ids)

    fetched_count = 0
    new_replies: List[Dict[str, Any]] = []
    filtered_old_count = 0
    filtered_self_count = 0
    filtered_not_thread_count = 0
    dedup_skipped_count = 0
    own_email = ""
    last_run_dir = find_latest_run_dir(out_runs_parent)

    try:
        own_email = str(gmail_client.get_profile_email() or "").strip()
        messages = list(gmail_client.list_messages(query=gmail_query, max_results=500) or [])
        fetched_count = len(messages)

        for item in messages:
            if not isinstance(item, dict):
                continue
            message_id = str(item.get("id") or "").strip()
            thread_id = str(item.get("threadId") or item.get("thread_id") or "").strip()
            if not message_id:
                continue
            if message_id in seen_ids:
                dedup_skipped_count += 1
                continue
            if thread_map_count > 0 and thread_id and thread_id not in thread_map:
                filtered_not_thread_count += 1
                continue
            message = gmail_client.get_message(message_id=message_id) or {}
            if not isinstance(message, dict):
                continue
            internal_ms = _internal_date_ms(message)
            if internal_ms < start_ms:
                filtered_old_count += 1
                continue
            from_header = _header_value(message, "From")
            if _from_is_self(from_header, own_email):
                filtered_self_count += 1
                continue
            subject = _header_value(message, "Subject")
            received_at = datetime.fromtimestamp(internal_ms / 1000, tz=timezone.utc)
            record = {
                "received_at_utc": _format_utc_z(received_at),
                "thread_id": thread_id or str(message.get("threadId") or ""),
                "message_id": message_id,
                "from": from_header,
                "subject": subject,
                "snippet": str(message.get("snippet") or ""),
                "clinic_name_guess": thread_map.get(thread_id or "", ""),
                "attachments": [],
            }
            new_replies.append(record)
            seen_ids.add(message_id)

        # Append new replies idempotently.
        out_replies_path.parent.mkdir(parents=True, exist_ok=True)
        if new_replies:
            with out_replies_path.open("a", encoding="utf-8") as f:
                for reply in new_replies:
                    f.write(json.dumps(reply, ensure_ascii=False) + "\n")

        updated_results_count = 0
        updated_queue_count = 0
        if last_run_dir and new_replies:
            updated_results_count, updated_queue_count = _attach_latest_reply_to_results(
                last_run_dir, new_replies
            )

        # Update state only on success.
        new_ids = [str(r.get("message_id") or "") for r in new_replies if r.get("message_id")]
        state_payload = {
            "last_successful_sync_utc": _format_utc_z(end_utc),
            "last_run_id": last_run_id or (last_run_dir.name if last_run_dir else ""),
            "last_error": "",
            "seen_message_ids": _update_seen_ids(state_seen_ids, new_ids),
        }
        _save_sync_state(state_path, state_payload)

        lines: List[str] = []
        lines.append("Outreach Gmail Sync Report")
        lines.append(f"generated_at_utc: {_format_utc_z(now_utc)}")
        lines.append(f"start_time_utc: {_format_utc_z(start_utc)}")
        lines.append(f"end_time_utc: {_format_utc_z(end_utc)}")
        lines.append(f"used_last_sync: {used_last_sync}")
        lines.append(f"lookback_hours: {int(lookback_hours)}")
        lines.append(f"grace_minutes: {int(grace_minutes)}")
        lines.append(f"gmail_query: {gmail_query}")
        lines.append(f"thread_map_count: {thread_map_count}")
        lines.append(f"fetched_count: {fetched_count}")
        lines.append(f"new_replies_count: {len(new_replies)}")
        lines.append(f"filtered_old_count: {filtered_old_count}")
        lines.append(f"filtered_self_count: {filtered_self_count}")
        lines.append(f"filtered_not_thread_count: {filtered_not_thread_count}")
        lines.append(f"dedup_skipped_count: {dedup_skipped_count}")
        lines.append(f"updated_results_count: {updated_results_count}")
        lines.append(f"updated_queue_count: {updated_queue_count}")
        lines.append(f"state_path: {state_path}")
        lines.append(f"replies_path: {out_replies_path}")
        lines.append(f"last_run_dir: {last_run_dir or ''}")
        _write_report(out_report_path, lines)

        return GmailSyncSummary(
            success=True,
            start_time_utc=_format_utc_z(start_utc),
            end_time_utc=_format_utc_z(end_utc),
            used_last_sync=used_last_sync,
            gmail_query=gmail_query,
            thread_map_count=thread_map_count,
            fetched_count=fetched_count,
            new_replies_count=len(new_replies),
            filtered_old_count=filtered_old_count,
            filtered_self_count=filtered_self_count,
            filtered_not_thread_count=filtered_not_thread_count,
            dedup_skipped_count=dedup_skipped_count,
            updated_results_count=updated_results_count,
            updated_queue_count=updated_queue_count,
            state_path=state_path,
            replies_path=out_replies_path,
            report_path=out_report_path,
            last_run_dir=last_run_dir,
        )
    except Exception as exc:  # pragma: no cover - depends on external API/network
        logger.exception("Gmail sync failed: %s", exc)
        error_payload = dict(state)
        error_payload["last_error"] = str(exc)
        # Do not update last_successful_sync_utc on failure.
        _save_sync_state(state_path, error_payload)
        lines = [
            "Outreach Gmail Sync Report",
            f"generated_at_utc: {_format_utc_z(now_utc)}",
            f"start_time_utc: {_format_utc_z(start_utc)}",
            f"end_time_utc: {_format_utc_z(end_utc)}",
            f"used_last_sync: {used_last_sync}",
            f"gmail_query: {gmail_query}",
            f"success: False",
            f"error: {exc}",
            f"state_path: {state_path}",
            f"replies_path: {out_replies_path}",
        ]
        _write_report(out_report_path, lines)
        return GmailSyncSummary(
            success=False,
            start_time_utc=_format_utc_z(start_utc),
            end_time_utc=_format_utc_z(end_utc),
            used_last_sync=used_last_sync,
            gmail_query=gmail_query,
            thread_map_count=thread_map_count,
            fetched_count=fetched_count,
            new_replies_count=0,
            filtered_old_count=filtered_old_count,
            filtered_self_count=filtered_self_count,
            filtered_not_thread_count=filtered_not_thread_count,
            dedup_skipped_count=dedup_skipped_count,
            updated_results_count=0,
            updated_queue_count=0,
            state_path=state_path,
            replies_path=out_replies_path,
            report_path=out_report_path,
            last_run_dir=last_run_dir,
        )

