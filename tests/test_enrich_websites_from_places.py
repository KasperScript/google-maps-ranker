import json
from pathlib import Path
from typing import Dict

from scripts.enrich_websites_from_places import (
    EnrichSummary,
    _extract_place_id_from_url,
    enrich_websites_from_places,
)


class _FakeResponse:
    def __init__(self, payload: Dict[str, object], status_code: int = 200) -> None:
        self._payload = dict(payload)
        self.status_code = int(status_code)

    def json(self) -> Dict[str, object]:
        return dict(self._payload)



def _write_input(path: Path) -> None:
    rows = [
        {"name": "Clinic A", "place_id": "p1", "website": ""},
        {"name": "Clinic B", "place_id": "p2"},
        {"name": "Clinic A Duplicate", "place_id": "p1"},
    ]
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def test_enrich_websites_fills_missing_and_preserves_order(tmp_path: Path) -> None:
    input_path = tmp_path / "results.json"
    output_path = tmp_path / "results_with_websites.json"
    _write_input(input_path)

    details_by_id = {
        "p1": {
            "websiteUri": "https://clinic-a.example",
            "googleMapsUri": "https://maps.example/p1",
            "nationalPhoneNumber": "+48 123 456 789",
        },
        "p2": {
            "websiteUri": "https://clinic-b.example",
        },
    }

    calls = {"count": 0, "field_mask": "", "api_key": ""}

    def fake_http_get(url: str, headers: Dict[str, str], timeout: int):
        calls["count"] += 1
        calls["field_mask"] = headers.get("X-Goog-FieldMask", "")
        calls["api_key"] = headers.get("X-Goog-Api-Key", "")
        place_id = _extract_place_id_from_url(url)
        payload = details_by_id.get(place_id)
        assert payload is not None, f"unexpected place_id: {place_id}"
        return _FakeResponse(payload, status_code=200)

    summary = enrich_websites_from_places(
        input_path=input_path,
        output_path=output_path,
        top_n=3,
        api_key="test-key",
        http_get=fake_http_get,
    )

    assert isinstance(summary, EnrichSummary)
    assert summary.processed == 3
    assert summary.updated_websites_count == 3
    assert summary.missing_place_id_count == 0
    assert summary.api_errors_count == 0

    # Two unique place_ids should result in two HTTP calls (in-run dedup).
    assert calls["count"] == 2
    assert calls["api_key"] == "test-key"
    assert "websiteUri" in calls["field_mask"]

    rows = json.loads(output_path.read_text(encoding="utf-8"))
    names = [row.get("name") for row in rows]
    assert names == ["Clinic A", "Clinic B", "Clinic A Duplicate"]

    by_name = {row["name"]: row for row in rows}
    assert by_name["Clinic A"]["websiteUri"] == "https://clinic-a.example"
    assert by_name["Clinic A"]["website"] == "https://clinic-a.example"
    assert by_name["Clinic B"]["websiteUri"] == "https://clinic-b.example"
    assert by_name["Clinic A Duplicate"]["website"] == "https://clinic-a.example"


def test_missing_place_id_does_not_call_api(tmp_path: Path) -> None:
    input_path = tmp_path / "results.json"
    output_path = tmp_path / "results_with_websites.json"
    input_path.write_text(json.dumps([{"name": "No Place Id"}], indent=2), encoding="utf-8")

    calls = {"count": 0}

    def fake_http_get(url: str, headers: Dict[str, str], timeout: int):
        calls["count"] += 1
        raise AssertionError("HTTP should not be called when place_id is missing")

    summary = enrich_websites_from_places(
        input_path=input_path,
        output_path=output_path,
        top_n=1,
        api_key="test-key",
        http_get=fake_http_get,
    )

    assert summary.processed == 1
    assert summary.updated_websites_count == 0
    assert summary.missing_place_id_count == 1
    assert summary.api_errors_count == 0
    assert calls["count"] == 0
