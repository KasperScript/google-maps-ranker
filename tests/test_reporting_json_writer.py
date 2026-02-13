import json

from src.reporting import write_json_object


def test_write_json_object_nested_atomic(tmp_path):
    path = tmp_path / "coverage.json"
    payload = {
        "unique_by_query": {"ortho": 3},
        "nested": {"list": [1, 2, 3], "word": "Z\u00f3\u0142\u0107"},
    }

    write_json_object(str(path), payload)

    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    assert data == payload
    assert "\u00f3" in text

    leftovers = [p for p in tmp_path.iterdir() if p.name != "coverage.json"]
    assert not leftovers
