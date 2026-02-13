from src.coverage import compute_uplift, unique_contributions


def test_compute_uplift_and_threshold():
    u_hubs = 10
    u_union_total = 12
    uplift = compute_uplift(u_hubs, u_union_total)
    assert uplift == 0.2
    assert uplift > 0.10


def test_unique_contributions():
    sets_by_key = {
        "q1": {"a", "b", "c"},
        "q2": {"b", "c", "d"},
        "q3": {"e"},
    }
    contributions = unique_contributions(sets_by_key)
    assert contributions["q1"] == 1  # only "a"
    assert contributions["q2"] == 1  # only "d"
    assert contributions["q3"] == 1  # only "e"
