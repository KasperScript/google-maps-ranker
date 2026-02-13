import os
from pathlib import Path

import run


def _parse_env_file(env_path: Path, *, override: bool = False) -> None:
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        if override or key not in os.environ:
            os.environ[key] = val


def test_load_env_is_optional_and_does_not_override_env(tmp_path: Path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text("GEMINI_API_KEY=from-dotenv\n", encoding="utf-8")

    called: dict[str, Path] = {}

    def fake_load_dotenv(*, dotenv_path, override=False):
        called["dotenv_path"] = Path(dotenv_path).resolve()
        _parse_env_file(Path(dotenv_path), override=bool(override))
        return True

    monkeypatch.setattr(run, "_load_dotenv", fake_load_dotenv)
    monkeypatch.setenv("GEMINI_API_KEY", "from-env")

    run.load_env(root_dir=tmp_path)

    assert called["dotenv_path"] == env_path.resolve()
    assert os.environ.get("GEMINI_API_KEY") == "from-env"
