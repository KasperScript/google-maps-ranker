from src import config
from src.pipeline import apply_filters


def _place(
    place_id: str,
    name: str,
    *,
    rating: float = 4.8,
    user_rating_count: int = 120,
    lat: float = 52.23,
    lon: float = 21.01,
    types=None,
    business_status: str | None = "OPERATIONAL",
):
    return {
        "place_id": place_id,
        "name": name,
        "rating": rating,
        "user_rating_count": user_rating_count,
        "lat": lat,
        "lon": lon,
        "types": types or ["dentist", "health"],
        "business_status": business_status,
    }


def test_domain_filter_rejects_veterinary_dentist():
    original = list(config.DOMAIN_REJECT_NAME_SUBSTRINGS)
    config.DOMAIN_REJECT_NAME_SUBSTRINGS = ["weteryn", "veterinary", "zwierz", "animal", "vet."]
    try:
        hubs = [{"lat": 52.23, "lon": 21.01}]
        places_by_id = {
            "vet1": _place("vet1", "Veterinary dentist - vet. Emilia Klim"),
        }

        filtered, rejection_counts = apply_filters(places_by_id, hubs)

        assert filtered == []
        assert rejection_counts.get("irrelevant_domain") == 1
        assert places_by_id["vet1"]["rejected_reason"] == "irrelevant_domain"
    finally:
        config.DOMAIN_REJECT_NAME_SUBSTRINGS = original


def test_domain_filter_keeps_relevant_dental_clinics():
    hubs = [{"lat": 52.23, "lon": 21.01}]
    places_by_id = {
        "d1": _place("d1", "Avenue Dental Dental Clinic"),
        "o1": _place("o1", "Orthodontic Excellence Center"),
    }

    filtered, rejection_counts = apply_filters(places_by_id, hubs)
    filtered_ids = {p["place_id"] for p in filtered}

    assert filtered_ids == {"d1", "o1"}
    assert "irrelevant_domain" not in rejection_counts
    assert all("rejected_reason" not in places_by_id[p] for p in places_by_id)
