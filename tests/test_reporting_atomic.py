from src.reporting import atomic_write_text


def test_atomic_write_text(tmp_path):
    path = tmp_path / "atomic.txt"

    atomic_write_text(str(path), "first")
    assert path.read_text(encoding="utf-8") == "first"

    atomic_write_text(str(path), "second")
    assert path.read_text(encoding="utf-8") == "second"

    leftovers = [p for p in tmp_path.iterdir() if p.name != "atomic.txt"]
    assert not leftovers
