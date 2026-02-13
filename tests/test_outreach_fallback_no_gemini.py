import json
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from src.gemini_client import BaseGeminiClient, GeminiCallResult
from src.outreach.crawler import FetchResult
from src.outreach.pipeline_outreach import run_outreach


def _fixture_html() -> str:
    return """<html>
    <body>
      <a href="mailto:kontakt@clinic.test">kontakt@clinic.test</a>
      <a href="/kontakt">Kontakt</a>
    </body>
    </html>
    """


def _fake_fetcher(url: str) -> FetchResult:
    parsed = urlparse(url)
    path = parsed.path or "/"
    if path in {"/", "/kontakt"}:
        html = _fixture_html()
        return FetchResult(
            url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            text=html,
            error=None,
            from_cache=False,
        )
    return FetchResult(
        url=url,
        final_url=url,
        status_code=404,
        content_type="text/html",
        text="",
        error="not_found",
        from_cache=False,
    )


class FailingGeminiClient(BaseGeminiClient):
    def generate_json(self, prompt_name, prompt_text, prompt_hash, validator=None):
        return GeminiCallResult(
            status="http_error",
            raw_text="",
            data=None,
            model="fail",
            prompt_name=prompt_name,
            prompt_hash=prompt_hash,
            error="http_error: 404",
        )


def _template_path() -> Path:
    return Path("prompts/email_template_no_pricing_pl.txt").resolve()


def _write_csv(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("place_id,name,quality,rating,user_rating_count,website\n")
        f.write("id-1,Clinic One,100,5.0,200,http://clinic.test/\n")


def test_fallback_outreach_message_when_gemini_fails(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)
    template_path = _template_path()
    assert template_path.exists()

    result = run_outreach(
        input_csv_path=str(csv_path),
        out_dir=tmp_path / "outreach_fallback",
        top_n=1,
        max_pages=5,
        refresh_web=True,
        fetcher=_fake_fetcher,
        gemini_client=FailingGeminiClient(),
        template_path=template_path,
    )

    payload = json.loads(result.results_path.read_text(encoding="utf-8"))
    row = payload[0]
    outreach_message = (row.get("gemini") or {}).get("outreach_message") or {}
    assert outreach_message.get("status") == "template"
    body = outreach_message.get("body") or ""
    assert "aparatem stałym" in body
    assert (
        "rozważam leczenie aparatem stałym (metalowym lub ICONIX). Opis na stronie jest bardzo zachęcający i mam poczucie, że podchodzą Państwo do leczenia profesjonalnie, dlatego chciałbym poprosić o orientacyjną wycenę i komplet informacji o kosztach, żebym mógł spokojnie zaplanować budżet."
        in body
    )
    assert (
        "Na koniec, czy są jeszcze jakieś typowe koszty dodatkowe, które warto uwzględnić w budżecie (np. awarie/naprawy elementów, dodatkowe badania)?"
        in body
    )
    assert row.get("suggested_action", {}).get("status") == "ready_to_email"
    assert row.get("gemini_status", {}).get("outreach") == "template"
