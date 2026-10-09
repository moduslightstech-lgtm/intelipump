"""Once-per-morning OPENING meter capture when the Pi boots.

Station attendants power the Pi at open; we capture cumulative totals then
without requiring a dashboard Read now / nozzle re-seat ritual.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


DEFAULT_MARKER_NAME = "meter-startup-capture.json"


def marker_path() -> Path:
    base = Path(os.environ.get("INTELIPUMP_METER_READ_REQUEST_DIR", "/var/lib/intelipump"))
    return base / DEFAULT_MARKER_NAME


def business_date_today(*, timezone: str = "Africa/Lagos") -> str:
    tz = ZoneInfo(timezone or "Africa/Lagos")
    return datetime.now(tz).date().isoformat()


def load_marker(path: Path | None = None) -> dict[str, Any]:
    p = path or marker_path()
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def save_marker(data: dict[str, Any], path: Path | None = None) -> None:
    p = path or marker_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(p)


def captured_addresses_for_today(
    *,
    timezone: str = "Africa/Lagos",
    path: Path | None = None,
) -> set[int]:
    data = load_marker(path)
    if str(data.get("businessDate") or "") != business_date_today(timezone=timezone):
        return set()
    captured = data.get("captured") or {}
    if not isinstance(captured, dict):
        return set()
    out: set[int] = set()
    for key in captured:
        try:
            out.add(int(key))
        except (TypeError, ValueError):
            continue
    return out


def mark_address_captured(
    *,
    address: int,
    correlation_id: str,
    timezone: str = "Africa/Lagos",
    path: Path | None = None,
) -> None:
    today = business_date_today(timezone=timezone)
    data = load_marker(path)
    if str(data.get("businessDate") or "") != today:
        data = {"businessDate": today, "captured": {}}
    captured = data.setdefault("captured", {})
    if not isinstance(captured, dict):
        captured = {}
        data["captured"] = captured
    captured[str(int(address))] = {
        "correlationId": correlation_id,
        "capturedAt": datetime.now(ZoneInfo(timezone or "Africa/Lagos")).isoformat(),
    }
    data["businessDate"] = today
    save_marker(data, path)
