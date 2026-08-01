"""Pytest path setup for tools.passive_dart_capture tests."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
for path in (ROOT, SRC):
    p = str(path)
    if p not in sys.path:
        sys.path.insert(0, p)
