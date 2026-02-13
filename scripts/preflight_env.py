#!/usr/bin/env python3
"""Quick preflight: print secret env var lengths without exposing values."""
from __future__ import annotations

import os


def _len(name: str) -> int:
    return len((os.getenv(name) or "").strip())


if __name__ == "__main__":
    print("GEMINI_API_KEY", _len("GEMINI_API_KEY"))
    print("GOOGLE_MAPS_API_KEY", _len("GOOGLE_MAPS_API_KEY"))
