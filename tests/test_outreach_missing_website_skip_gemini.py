import json
from pathlib import Path

from src.gemini_client import BaseGeminiClient, GeminiCallResult
from src.outreach.pipeline_outreach import run_outreach


class CountingGeminiClient(BaseGeminiClient):
    def __init__(self) -> None:
        self.call_count = 0

    def generate_json(self, prompt_name, prompt_text, prompt_hash, validator=None):
        self.call_count += 1
        data = {"status": "should_not_run"}
        if validator is not None:
            try:
                validator(data)
            except Exception:
                # We should never get here in this test because Gemini must not run.
                pass
        return GeminiCallResult(
            status="ok",
            raw_text=json.dumps(data, ensure_ascii=False),
            data=data,
            model="fake",
            prompt_name=prompt_name,
            prompt_hash=prompt_hash,
            error=None,
        )


def _write_template(path: Path) -> None:
    path.write_text(
        """Szanowni Państwo,\n\nrozważam leczenie aparatem stałym (metalowym lub ICONIX). Opis na stronie jest bardzo zachęcający i mam poczucie, że podchodzą Państwo do leczenia profesjonalnie, dlatego chciałbym poprosić o orientacyjną wycenę i komplet informacji o kosztach, żebym mógł spokojnie zaplanować budżet.\n\nNa koniec, czy są jeszcze jakieś typowe koszty dodatkowe, które warto uwzględnić w budżecie (np. awarie/naprawy elementów, dodatkowe badania)?\n""",
        encoding="utf-8",
    )


def test_missing_website_skips_gemini_calls(tmp_path: Path) -> None:
    input_path = tmp_path / "results.json"
    input_rows = [
        {
            "name": "No Website Clinic",
            "place_id": "place-1",
            "website": "",
            "quality": "100",
            "rating": "5.0",
            "user_rating_count": "200",
        }
    ]
    input_path.write_text(json.dumps(input_rows, ensure_ascii=False, indent=2), encoding="utf-8")

    template_path = tmp_path / "email_template_no_pricing_pl.txt"
    _write_template(template_path)

    gemini = CountingGeminiClient()
    out_parent = tmp_path / "outreach_runs"
    result = run_outreach(
        input_csv_path=str(input_path),
        out_dir=out_parent,
        top_n=1,
        max_pages=5,
        gemini_client=gemini,
        template_path=template_path,
    )

    assert gemini.call_count == 0

    payload = json.loads(result.results_path.read_text(encoding="utf-8"))
    row = payload[0]
    assert row["needs_manual_search"] is True
    assert row["gemini_status"]["price_calc"] == "skipped_missing_website"
    assert row["gemini_status"]["outreach"] == "skipped_missing_website"
