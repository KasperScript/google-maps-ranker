import json
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from src.gemini_client import BaseGeminiClient, GeminiCallResult
from src.outreach.crawler import FetchResult
from src.outreach.pipeline_outreach import run_outreach


def _fixture_path(name: str) -> Path:
    return Path(__file__).parent / "fixtures" / "outreach" / name


def _load_fixture(name: str) -> str:
    return _fixture_path(name).read_text(encoding="utf-8")


def _fake_site_mapping() -> Dict[str, str]:
    return {
        "/": _load_fixture("site_root.html"),
        "/cennik": _load_fixture("cennik.html"),
        "/kontakt": _load_fixture("kontakt.html"),
        "/kontakt/wyslij": _load_fixture("kontakt.html"),
        "/about": "<html><body>O nas</body></html>",
    }


def _fake_fetcher(url: str) -> FetchResult:
    mapping = _fake_site_mapping()
    parsed = urlparse(url)
    path = parsed.path or "/"
    html = mapping.get(path)
    if html is None:
        return FetchResult(
            url=url,
            final_url=url,
            status_code=404,
            content_type="text/html",
            text="",
            error="not_found",
            from_cache=False,
        )
    return FetchResult(
        url=url,
        final_url=url,
        status_code=200,
        content_type="text/html",
        text=html,
        error=None,
        from_cache=False,
    )


class FakeGeminiClient(BaseGeminiClient):
    def generate_json(self, prompt_name, prompt_text, prompt_hash, validator=None):
        if prompt_name == "gemini_price_calc_v3.txt":
            data = {
                "clinic_name": "Clinic Test",
                "currency": "PLN",
                "evidence_level": "strong",
                "extracted_prices": [
                    {
                        "key": "bonding_metal_1_arch",
                        "label": "Aparat stały metalowy 1 łuk",
                        "amount": 2800,
                        "source": "explicit",
                        "notes": "z cennika",
                    }
                ],
                "variants": {
                    "A": {
                        "total": 7000,
                        "breakdown": {
                            "start": 450,
                            "bonding": 2800,
                            "controls": 3080,
                            "debonding": 0,
                            "retention": 670,
                        },
                        "missing": ["debond 1 arch"],
                        "missing_items_count": 1,
                        "fallback_items_count": 2,
                        "fallback_total_pln": 1120,
                        "fallback_share_pct": 16,
                        "confidence": "medium",
                        "assumptions": ["fallback debonding"],
                    },
                    "B": {
                        "total": 14000,
                        "breakdown": {
                            "start": 450,
                            "bonding": 5600,
                            "controls": 6300,
                            "debonding": 0,
                            "retention": 1650,
                        },
                        "missing": ["debond 1 arch", "fixed retainer 1 arch"],
                        "missing_items_count": 2,
                        "fallback_items_count": 3,
                        "fallback_total_pln": 1950,
                        "fallback_share_pct": 14,
                        "confidence": "medium",
                        "assumptions": [],
                    },
                    "C": {
                        "total": 15000,
                        "breakdown": {
                            "start": 450,
                            "bonding": 5600,
                            "controls": 7000,
                            "debonding": 0,
                            "retention": 1950,
                        },
                        "missing": ["debond 1 arch"],
                        "missing_items_count": 1,
                        "fallback_items_count": 3,
                        "fallback_total_pln": 1950,
                        "fallback_share_pct": 13,
                        "confidence": "medium",
                        "assumptions": [],
                    },
                },
                "notes": ["deterministic fake"],
            }
        else:
            data = {
                "clinic_name": "Clinic Test",
                "subject": "Prośba o brakujące informacje cenowe",
                "body": "Dzień dobry, proszę o brakujące ceny.",
                "questions_missing_prices": ["debond 1 arch"],
                "template_preservation_check": {
                    "preserved_phrase_1": True,
                    "preserved_phrase_2": True,
                },
            }

        if validator is not None:
            validator(data)
        return GeminiCallResult(
            status="ok",
            raw_text=json.dumps(data, ensure_ascii=False),
            data=data,
            model="fake",
            prompt_name=prompt_name,
            prompt_hash=prompt_hash,
            error=None,
        )


class FakeGmailSender:
    def __init__(self) -> None:
        self.draft_calls: list[dict[str, str]] = []
        self.send_calls: list[dict[str, str]] = []
        self.profile_email = "sender@unit.test"

    def get_profile_email(self) -> str:
        return self.profile_email

    def create_draft(
        self,
        *,
        to_email: str,
        subject: str,
        body: str,
        sender_email: str = "",
        label_name: str = "",
    ):
        self.draft_calls.append(
            {
                "to": to_email,
                "subject": subject,
                "body": body,
                "sender": sender_email or self.profile_email,
                "label_name": label_name,
            }
        )
        idx = len(self.draft_calls)
        return {
            "status": "drafted",
            "draft_id": f"draft-{idx}",
            "message_id": f"draft-msg-{idx}",
            "thread_id": f"thread-{idx}",
            "label_name": label_name,
            "label_id": "label-1",
            "label_applied": True,
        }

    def send_email(
        self,
        *,
        to_email: str,
        subject: str,
        body: str,
        dry_run: bool = False,
        label_name: str = "",
    ):
        self.send_calls.append(
            {
                "to": to_email,
                "subject": subject,
                "body": body,
                "dry_run": str(dry_run),
                "label_name": label_name,
            }
        )
        idx = len(self.send_calls)
        return {
            "status": "sent",
            "message_id": f"fake-{idx}",
            "thread_id": f"thread-{idx}",
            "label_name": label_name,
            "label_id": "label-1",
            "label_applied": True,
        }


def _template_path() -> Path:
    return Path("prompts/email_template_no_pricing_pl.txt").resolve()


def _write_csv(path: Path) -> None:
    rows = [
        ("id-1", "Clinic One", "100"),
        ("id-2", "DeClinic", "99"),
        ("id-3", "Clinic Two", "98"),
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("place_id,name,quality,rating,user_rating_count,website\n")
        for place_id, name, quality in rows:
            f.write(
                ",".join([place_id, name, quality, "5.0", "200", "http://clinic.test/"])
                + "\n"
            )


def test_gmail_send_disabled_by_default(tmp_path: Path):
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)
    template_path = _template_path()
    assert template_path.exists()

    fake_gmail = FakeGmailSender()
    out_dir = tmp_path / "outreach_default"
    result = run_outreach(
        input_csv_path=str(csv_path),
        out_dir=out_dir,
        top_n=3,
        max_pages=10,
        refresh_web=True,
        fetcher=_fake_fetcher,
        gemini_client=FakeGeminiClient(),
        template_path=template_path,
        gmail_sender=fake_gmail,
    )

    assert fake_gmail.draft_calls == []
    assert fake_gmail.send_calls == []
    assert not (result.run_dir / "outreach_gmail_report.txt").exists()


def test_gmail_drafts_mode_creates_drafts_and_respects_limit(tmp_path: Path):
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)
    template_path = _template_path()
    assert template_path.exists()

    fake_gmail = FakeGmailSender()
    out_dir = tmp_path / "outreach_drafts"
    result = run_outreach(
        input_csv_path=str(csv_path),
        out_dir=out_dir,
        top_n=3,
        max_pages=10,
        refresh_web=True,
        fetcher=_fake_fetcher,
        gemini_client=FakeGeminiClient(),
        template_path=template_path,
        gmail_drafts=True,
        gmail_max_drafts=1,
        gmail_sender=fake_gmail,
        gmail_send_log_path=tmp_path / "send_log.jsonl",
    )

    assert len(fake_gmail.draft_calls) == 1
    assert fake_gmail.send_calls == []
    report_path = result.run_dir / "outreach_gmail_report.txt"
    assert report_path.exists()

    results_payload = json.loads(result.results_path.read_text(encoding="utf-8"))
    by_name = {row.get("clinic_name"): row for row in results_payload}
    assert by_name["Clinic One"]["gmail_draft"]["status"] == "drafted"
    assert by_name["Clinic Two"]["gmail_draft"]["status"] == "blocked_max_drafts"


def test_gmail_send_requires_ack_and_non_dry_run(tmp_path: Path):
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)
    template_path = _template_path()
    assert template_path.exists()

    fake_gmail = FakeGmailSender()
    out_dir = tmp_path / "outreach_send_blocked"
    result = run_outreach(
        input_csv_path=str(csv_path),
        out_dir=out_dir,
        top_n=3,
        max_pages=10,
        fetcher=_fake_fetcher,
        gemini_client=FakeGeminiClient(),
        template_path=template_path,
        gmail_send=True,
        gmail_send_ack=False,
        gmail_send_dry_run=False,
        gmail_daily_limit=5,
        gmail_allow_domains="clinic.test",
        gmail_sender=fake_gmail,
        gmail_send_log_path=tmp_path / "send_log.jsonl",
    )

    assert fake_gmail.send_calls == []

    results_payload = json.loads(result.results_path.read_text(encoding="utf-8"))
    gmail_statuses = [row.get("gmail_send", {}).get("status") for row in results_payload if row.get("suggested_action")]
    assert "blocked_ack_required" in gmail_statuses


def test_gmail_send_calls_sender_when_allowed_and_respects_dedupe_and_limits(tmp_path: Path):
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)
    template_path = _template_path()
    assert template_path.exists()

    send_log_path = tmp_path / "send_log.jsonl"
    send_log_path.write_text(
        json.dumps(
            {
                "clinic_name": "Clinic One",
                "to": "kontakt@clinic.test",
                "date": "2099-01-01",
                "status": "sent",
                "message_id": "old-message",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    fake_gmail = FakeGmailSender()
    out_dir = tmp_path / "outreach_send_ok"
    result = run_outreach(
        input_csv_path=str(csv_path),
        out_dir=out_dir,
        top_n=3,
        max_pages=10,
        outreach_force=True,
        fetcher=_fake_fetcher,
        gemini_client=FakeGeminiClient(),
        template_path=template_path,
        gmail_send=True,
        gmail_send_ack=True,
        gmail_send_dry_run=False,
        gmail_daily_limit=1,
        gmail_allow_domains="clinic.test",
        gmail_sender=fake_gmail,
        gmail_send_log_path=send_log_path,
    )

    assert len(fake_gmail.send_calls) == 1
    assert fake_gmail.send_calls[0]["to"].endswith("@clinic.test")

    results_payload = json.loads(result.results_path.read_text(encoding="utf-8"))
    by_name = {row.get("clinic_name"): row for row in results_payload}

    clinic_one = by_name["Clinic One"]
    assert clinic_one["gmail_send"]["status"] == "blocked_dedupe"

    clinic_two = by_name["Clinic Two"]
    assert clinic_two["gmail_send"]["status"] == "sent"
    assert clinic_two["gmail_send"]["message_id"].startswith("fake-")

    gmail_attempt_dir = Path(clinic_two["gmail_send"]["attempt_dir"])
    assert gmail_attempt_dir.exists()
    assert (gmail_attempt_dir / "status.json").exists()

    log_lines = [line for line in send_log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(log_lines) >= 2
