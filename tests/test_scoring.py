from src.scoring import quality_score


def test_quality_increases_with_rating():
    c = 4.2
    m = 200
    s1 = quality_score(4.0, 200, c, m)["quality"]
    s2 = quality_score(4.5, 200, c, m)["quality"]
    assert s2 > s1


def test_quality_non_decreasing_with_votes():
    c = 4.2
    m = 200
    s1 = quality_score(4.7, 100, c, m)["quality"]
    s2 = quality_score(4.7, 500, c, m)["quality"]
    assert s2 >= s1


def test_quality_prefers_large_sample_over_perfect_small():
    c = 4.2
    m = 200
    s1 = quality_score(4.7, 2000, c, m)["quality"]
    s2 = quality_score(5.0, 50, c, m)["quality"]
    assert s1 > s2
