import pytest

from src.pipeline import validate_hubs


def test_validate_hubs_rejects_zero_coords():
    hubs = {
        "bad": {"name": "Bad Hub", "lat": 0.0, "lon": 21.0},
    }
    with pytest.raises(ValueError):
        validate_hubs(hubs)
