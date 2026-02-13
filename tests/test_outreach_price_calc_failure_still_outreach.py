import json
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from src.gemini_client import BaseGeminiClient, GeminiCallResult
from src.outreach.crawler import FetchResult
from src.outreach.pipeline_outreach import run_outreach


class PriceCalcErrorGeminiClient(BaseGeminiClient):
    def __init__(self) -> None:
        self.price_calc_calls = 0
        self.outreach_calls = 0

    def generate_json(self, prompt_name, prompt_text, prompt_hash, validator=None):
        if prompt_name == "gemini_price_calc_v3.txt":
            self.price_calc_calls += 1
            return GeminiCallResult(
                status="http_error",
                raw_text="",
                data=None,
                model="fake",
                prompt_name=prompt_name,
                prompt_hash=prompt_hash,
                error="http_error: 403",
            )

        self.outreach_calls += 1
        return GeminiCallResult(
            status="http_error",
            raw_text="",
            data=None,
            model="fake",
            prompt_name=prompt_name,
            prompt_hash=prompt_hash,
            error="http_error: 403",
        )


def _site_mapping() -> Dict[str, str]:
    return {
        "/": """
            <html><body>
              <a href="/cennik">Cennik</a>
              <a href="/kontakt">Kontakt</a>
            </body></html>
        """,
        "/cennik": """
            <html><body>
              <h1>Cennik ortodoncji</h1>
              Konsultacja 200 zł
              Aparat stały 1 łuk 2800 zł
            </body></html>
        """,
        "/kontakt": """
            <html><body>
              <a href="mailto:kontakt@clinic.test">Napisz</a>
              <form action="/kontakt/wyslij"></form>
            </body></html>
        """,
        "/kontakt/wyslij": "<html><body><form></form></body></html>",
    }


def _fake_fetcher(url: str) -> FetchResult:
    mapping = _site_mapping()
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


def _template_path() -> Path:
    return Path("prompts/email_template_no_pricing_pl.txt").resolve()


def _write_input(path: Path) -> None:
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
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def test_price_calc_error_still_generates_outreach_message(tmp_path: Path) -> None:
    input_path = tmp_path / "results.json"
    _write_input(input_path)

    template_path = _template_path()
    assert template_path.exists()

    gemini = PriceCalcErrorGeminiClient()
    result = run_outreach(
        input_csv_path=str(input_path),
        out_dir=tmp_path / "outreach_runs",
        top_n=1,
        max_pages=5,
        refresh_web=True,
        fetcher=_fake_fetcher,
        gemini_client=gemini,
        template_path=template_path,
    )

    assert gemini.price_calc_calls == 1
    assert gemini.outreach_calls == 0

    payload = json.loads(result.results_path.read_text(encoding="utf-8"))
    row = payload[0]
    assert row["gemini_status"]["price_calc"] == "http_error"
    assert row["gemini_status"]["outreach"] == "template"
    assert row["suggested_action"]["status"] == "ready_to_email"
