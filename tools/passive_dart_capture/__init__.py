"""Strictly passive Wayne DART RS-485 capture and offline analysis.

LISTEN_ONLY: this package never transmits bytes, asserts RTS/DE, or imports
active controller command paths. See REQUIREMENTS.md and README.md.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
