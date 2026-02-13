import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from src.gmail_sync import compute_time_window, load_sync_state, sync_gmail_replies


class FakeGmailClient:
    def __init__(
        self,
        *,
        messages: Iterable[Dict[str, Any]],
        messages_by_id: Dict[str, Dict[str, Any]],
        profile_email: str = "me@example.com",
        raise_on_list: bool = False,
    ) -> None:
        self._messages = list(messages)
        self._messages_by_id = dict(messages_by_id)
        self._profile_email = profile_email
        self._raise_on_list = raise_on_list
        self.list_calls: List[Dict[str, Any]] = []
        self.get_calls: List[str] = []

    def get_profile_email(self) -> str:
        return self._profile_email

    def list_messages(self, *, query: str, max_results: int = 500):
        self.list_calls.append({"query": query, "max_results": str(max_results)})
        if self._raise_on_list:
            raise RuntimeError("list_failed")
        return self._messages[:max_results]

    def get_message(self, *, message_id: str):
        self.get_calls.append(message_id)
        return self._messages_by_id.get(message_id) or {}


def _ms(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1000))


def _headers(from_val: str, subject: str) -> List[Dict[str, str]]:
    return [
        {"name": "From", "value": from_val},
        {"name": "Subject", "value": subject},
        {"name": "Date", "value": "Tue, 27 Jan 2026 15:00:00 +0000"},
    ]


def test_compute_time_window_uses_last_sync_with_grace() -> None:
    now = datetime(2026, 1, 27, 15, 0, tzinfo=timezone.utc)
    state = {"last_successful_sync_utc": "2026-01-27T14:00:00Z"}
    start, end, used_last_sync = compute_time_window(
        now_utc=now,
        state=state,
        lookback_hours=72,
        grace_minutes=30,
    )
    assert used_last_sync is True
    assert start == datetime(2026, 1, 27, 13, 30, tzinfo=timezone.utc)
    assert end == now


def test_compute_time_window_falls_back_to_lookback_hours() -> None:
    now = datetime(2026, 1, 27, 15, 0, tzinfo=timezone.utc)
    start, end, used_last_sync = compute_time_window(
        now_utc=now,
        state={},
        lookback_hours=24,
        grace_minutes=30,
    )
    assert used_last_sync is False
    assert start == datetime(2026, 1, 26, 15, 0, tzinfo=timezone.utc)
    assert end == now


def test_sync_filters_old_messages_and_updates_state_and_latest_reply(tmp_path: Path, monkeypatch) -> None:
    from src import gmail_sync as gmail_sync_module

    fixed_now = datetime(2026, 1, 27, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(gmail_sync_module, "_utc_now", lambda: fixed_now)

    out_runs_parent = tmp_path / "outreach_runs"
    run_dir = out_runs_parent / "20260127_140000"
    run_dir.mkdir(parents=True, exist_ok=True)

    results_path = run_dir / "outreach_results.json"
    results_path.write_text(
        json.dumps(
            [
                {
                    "clinic_name": "Clinic A",
                    "gmail_draft": {"thread_id": "t-1"},
                }
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    queue_path = run_dir / "outreach_queue.jsonl"
    queue_path.write_text(
        json.dumps({"clinic_name": "Clinic A", "gmail_draft": {"thread_id": "t-1"}}, ensure_ascii=False)
        + "\n",
        encoding="utf-8",
    )

    # Start time will be fixed_now - 1 hour (no state).
    old_dt = fixed_now - timedelta(hours=2)
    new_dt = fixed_now - timedelta(minutes=10)

    messages = [
        {"id": "m-old", "threadId": "t-1"},
        {"id": "m-new", "threadId": "t-1"},
    ]
    messages_by_id = {
        "m-old": {
            "id": "m-old",
            "threadId": "t-1",
            "internalDate": _ms(old_dt),
            "snippet": "old reply",
            "payload": {"headers": _headers("reply@clinic.test", "Re: old")},
        },
        "m-new": {
            "id": "m-new",
            "threadId": "t-1",
            "internalDate": _ms(new_dt),
            "snippet": "new reply",
            "payload": {"headers": _headers("reply@clinic.test", "Re: new")},
        },
    }
    fake_client = FakeGmailClient(messages=messages, messages_by_id=messages_by_id)

    state_path = tmp_path / "gmail_sync_state.json"
    replies_path = tmp_path / "outreach_replies.jsonl"
    report_path = tmp_path / "outreach_gmail_sync_report.txt"

    summary = sync_gmail_replies(
        gmail_client=fake_client,
        state_path=state_path,
        lookback_hours=1,
        grace_minutes=30,
        out_replies_path=replies_path,
        out_report_path=report_path,
        out_runs_parent=out_runs_parent,
    )

    assert summary.success is True
    assert summary.new_replies_count == 1
    assert summary.filtered_old_count == 1
    assert replies_path.exists()
    reply_lines = [line for line in replies_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(reply_lines) == 1
    reply_payload = json.loads(reply_lines[0])
    assert reply_payload["message_id"] == "m-new"

    state_payload = load_sync_state(state_path)
    assert state_payload.get("last_successful_sync_utc") == "2026-01-27T15:00:00Z"
    assert "m-new" in (state_payload.get("seen_message_ids") or [])

    updated_results = json.loads(results_path.read_text(encoding="utf-8"))
    assert updated_results[0].get("latest_reply", {}).get("message_id") == "m-new"


def test_state_updates_only_on_success(tmp_path: Path, monkeypatch) -> None:
    from src import gmail_sync as gmail_sync_module

    fixed_now = datetime(2026, 1, 27, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(gmail_sync_module, "_utc_now", lambda: fixed_now)

    state_path = tmp_path / "gmail_sync_state.json"
    state_path.write_text(
        json.dumps({"last_successful_sync_utc": "2026-01-27T14:00:00Z"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    fake_client = FakeGmailClient(messages=[], messages_by_id={}, raise_on_list=True)
    summary = sync_gmail_replies(
        gmail_client=fake_client,
        state_path=state_path,
        lookback_hours=1,
        grace_minutes=30,
        out_replies_path=tmp_path / "outreach_replies.jsonl",
        out_report_path=tmp_path / "outreach_gmail_sync_report.txt",
        out_runs_parent=tmp_path / "outreach_runs",
    )

    assert summary.success is False
    state_payload = load_sync_state(state_path)
    # last_successful_sync_utc should be unchanged on failure.
    assert state_payload.get("last_successful_sync_utc") == "2026-01-27T14:00:00Z"
    assert state_payload.get("last_error") == "list_failed"

