import json
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from src.gemini_client import BaseGeminiClient, GeminiCallResult
from src.outreach.crawler import FetchResult
from src.outreach.pipeline_outreach import run_outreach
from src.outreach.playwright_assist import run_playwright_assist


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


class CountingFakeGeminiClient(BaseGeminiClient):
    def __init__(self) -> None:
        self.call_count = 0

    def generate_json(self, prompt_name, prompt_text, prompt_hash, validator=None):
        self.call_count += 1
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


def _write_csv(path: Path) -> None:
    rows = [
        {
            "place_id": "id-1",
            "name": "Clinic Test",
            "quality": "100",
            "rating": "5.0",
            "user_rating_count": "200",
            "website": "http://clinic.test/",
        }
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(
            "place_id,name,quality,rating,user_rating_count,website\n"
            + "\n".join(
                ",".join(
                    [
                        row["place_id"],
                        row["name"],
                        row["quality"],
                        row["rating"],
                        row["user_rating_count"],
                        row["website"],
                    ]
                )
                for row in rows
            )
        )


def _write_template(path: Path) -> None:
    path.write_text(
        """Szanowni Państwo,

rozważam leczenie aparatem stałym (metalowym lub ICONIX). Opis na stronie jest bardzo zachęcający i mam poczucie, że podchodzą Państwo do leczenia profesjonalnie, dlatego chciałbym poprosić o orientacyjną wycenę i komplet informacji o kosztach, żebym mógł spokojnie zaplanować budżet.

Na koniec, czy są jeszcze jakieś typowe koszty dodatkowe, które warto uwzględnić w budżecie (np. awarie/naprawy elementów, dodatkowe badania)?
""",
        encoding="utf-8",
    )


def _attempt_dirs(gemini_root: Path) -> list[Path]:
    if not gemini_root.exists():
        return []
    return sorted([p for p in gemini_root.iterdir() if p.is_dir() and p.name.startswith("attempt_")])


def _gemini_root_from_result(result) -> Path:
    payload = json.loads(result.results_path.read_text(encoding="utf-8"))
    return Path(payload[0]["evidence"]["gemini_dir"])


def _latest_meta_status(gemini_root: Path, prompt_name: str) -> str:
    for attempt_dir in sorted(_attempt_dirs(gemini_root), key=lambda p: p.name, reverse=True):
        meta_path = attempt_dir / f"{prompt_name}.meta.json"
        if not meta_path.exists():
            continue
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return str(meta.get("status") or "")
    return ""


def test_outreach_force_reruns_gemini_and_attempts_are_non_destructive(tmp_path: Path):
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)
    template_path = tmp_path / "email_template_no_pricing_pl.txt"
    _write_template(template_path)

    out_dir = tmp_path / "outreach"

    first_client = CountingFakeGeminiClient()
    first_run = run_outreach(
        input_csv_path=str(csv_path),
        out_dir=out_dir,
        top_n=1,
        max_pages=10,
        refresh_web=True,
        fetcher=_fake_fetcher,
        gemini_client=first_client,
        template_path=template_path,
    )
    assert first_client.call_count > 0

    gemini_root_first = _gemini_root_from_result(first_run)
    attempts_after_first = _attempt_dirs(gemini_root_first)
    assert attempts_after_first
    assert _latest_meta_status(gemini_root_first, "gemini_price_calc_v3.txt") == "ok"

    second_client = CountingFakeGeminiClient()
    second_run = run_outreach(
        input_csv_path=str(csv_path),
        out_dir=out_dir,
        top_n=1,
        max_pages=10,
        refresh_web=False,
        fetcher=_fake_fetcher,
        gemini_client=second_client,
        template_path=template_path,
    )
    assert second_client.call_count == 0
    gemini_root_second = _gemini_root_from_result(second_run)
    assert _latest_meta_status(gemini_root_second, "gemini_price_calc_v3.txt") == "ok_cached"

    third_client = CountingFakeGeminiClient()
    third_run = run_outreach(
        input_csv_path=str(csv_path),
        out_dir=out_dir,
        top_n=1,
        max_pages=10,
        outreach_force=True,
        fetcher=_fake_fetcher,
        gemini_client=third_client,
        template_path=template_path,
    )
    assert third_client.call_count > 0

    gemini_root_third = _gemini_root_from_result(third_run)
    assert _latest_meta_status(gemini_root_third, "gemini_price_calc_v3.txt") == "ok"
    attempts_after_third = _attempt_dirs(gemini_root_third)
    assert attempts_after_third

    latest_pointer = gemini_root_third / "latest_attempt.txt"
    assert latest_pointer.exists()
    latest_name = latest_pointer.read_text(encoding="utf-8").strip()
    assert latest_name
    assert (gemini_root_third / latest_name).exists()

    third_results = json.loads(third_run.results_path.read_text(encoding="utf-8"))
    assert third_results[0]["outreach_force"] is True


class _FakeLocator:
    def __init__(self, count: int = 1) -> None:
        self._count = int(count)
        self.filled_values: list[str] = []

    def count(self) -> int:
        return self._count

    @property
    def first(self):
        return self

    def fill(self, value: str) -> None:
        self.filled_values.append(value)


class _FakePage:
    def __init__(self) -> None:
        self._html = "<html><body><form></form></body></html>"

    def goto(self, url: str, wait_until: str, timeout: int) -> None:
        self.last_goto = (url, wait_until, timeout)

    def content(self) -> str:
        return self._html

    def locator(self, selector: str):
        lowered = selector.lower()
        if "recaptcha" in lowered or "hcaptcha" in lowered:
            return _FakeLocator(count=0)
        return _FakeLocator(count=1)

    def screenshot(self, path: str, full_page: bool) -> None:
        Path(path).write_bytes(b"fake-screenshot")


class _FakeBrowser:
    def __init__(self, page: _FakePage) -> None:
        self._page = page

    def new_page(self) -> _FakePage:
        return self._page

    def close(self) -> None:
        return None


class _FakeChromium:
    def __init__(self, browser: _FakeBrowser) -> None:
        self.browser = browser
        self.launch_kwargs: dict[str, object] = {}

    def launch(self, **kwargs):
        self.launch_kwargs = dict(kwargs)
        return self.browser


class _FakePlaywright:
    def __init__(self, chromium: _FakeChromium) -> None:
        self.chromium = chromium


class _FakeSyncPlaywright:
    def __init__(self, playwright: _FakePlaywright) -> None:
        self.playwright = playwright

    def __enter__(self):
        return self.playwright

    def __exit__(self, exc_type, exc, tb):
        return False


def test_playwright_headed_toggles_headless_and_slowmo(tmp_path: Path):
    screenshots_dir = tmp_path / "screenshots"
    page = _FakePage()
    browser = _FakeBrowser(page)
    chromium = _FakeChromium(browser)
    playwright = _FakePlaywright(chromium)

    result = run_playwright_assist(
        clinic_name="Clinic Test",
        form_url="http://clinic.test/kontakt",
        message_body="Dzień dobry",
        evidence_screenshots_dir=screenshots_dir,
        headed=True,
        slowmo_ms=250,
        sync_playwright_factory=lambda: _FakeSyncPlaywright(playwright),
    )

    assert chromium.launch_kwargs.get("headless") is False
    assert chromium.launch_kwargs.get("slow_mo") == 250
    assert result.status == "ok"
    assert any(screenshots_dir.glob("*.png"))
