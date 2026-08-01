"""Allow ``python -m tools.passive_dart_capture``."""

from __future__ import annotations

from tools.passive_dart_capture.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
