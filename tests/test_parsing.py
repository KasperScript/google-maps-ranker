from src.places_client import parse_places_response
from src.routes_client import parse_routes_duration


def test_parse_places_missing_fields():
    response = {
        "places": [
            {"id": "p1"},
            {"id": "p2", "displayName": "Name"},
            {"placeId": "p3", "location": {"lat": 1.0, "lng": 2.0}},
            {"displayName": {"text": "no-id"}},
        ]
    }

    parsed = parse_places_response(response)
    assert [p["place_id"] for p in parsed] == ["p1", "p2", "p3"]
    assert parsed[0]["name"] is None
    assert parsed[0]["rating"] is None
    assert parsed[0]["lat"] is None
    assert parsed[0]["lon"] is None
    assert parsed[2]["lat"] == 1.0
    assert parsed[2]["lon"] == 2.0


def test_parse_routes_duration_variants():
    assert parse_routes_duration({"routes": [{"duration": "123s"}]}) == 123
    assert parse_routes_duration({"routes": [{"duration": 456}]}) == 456
    assert parse_routes_duration({"routes": []}) is None
