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


def _int_keys(section: Any) -> set[int]:
    if not isinstance(section, dict):
        return set()
    out: set[int] = set()
    for key in section:
        try:
            out.add(int(key))
        except (TypeError, ValueError):
            continue
    return out


def _today_marker(
    *,
    timezone: str = "Africa/Lagos",
    path: Path | None = None,
) -> dict[str, Any]:
    today = business_date_today(timezone=timezone)
    data = load_marker(path)
    if str(data.get("businessDate") or "") != today:
        return {"businessDate": today, "captured": {}, "failed": {}}
    if not isinstance(data.get("captured"), dict):
        data["captured"] = {}
    if not isinstance(data.get("failed"), dict):
        data["failed"] = {}
    data["businessDate"] = today
    return data


def captured_addresses_for_today(
    *,
    timezone: str = "Africa/Lagos",
    path: Path | None = None,
) -> set[int]:
    data = load_marker(path)
    if str(data.get("businessDate") or "") != business_date_today(timezone=timezone):
        return set()
    return _int_keys(data.get("captured"))


def finished_addresses_for_today(
    *,
    timezone: str = "Africa/Lagos",
    path: Path | None = None,
) -> set[int]:
    """Addresses that must not be re-queued today (CAPTURED or terminal fail)."""
    data = load_marker(path)
    if str(data.get("businessDate") or "") != business_date_today(timezone=timezone):
        return set()
    return _int_keys(data.get("captured")) | _int_keys(data.get("failed"))


def mark_address_captured(
    *,
    address: int,
    correlation_id: str,
    timezone: str = "Africa/Lagos",
    path: Path | None = None,
) -> None:
    today = business_date_today(timezone=timezone)
    data = _today_marker(timezone=timezone, path=path)
    captured = data.setdefault("captured", {})
    if not isinstance(captured, dict):
        captured = {}
        data["captured"] = captured
    # Success wins over a prior fail entry for the same day.
    failed = data.get("failed")
    if isinstance(failed, dict):
        failed.pop(str(int(address)), None)
    captured[str(int(address))] = {
        "correlationId": correlation_id,
        "capturedAt": datetime.now(ZoneInfo(timezone or "Africa/Lagos")).isoformat(),
    }
    data["businessDate"] = today
    save_marker(data, path)


def mark_address_failed(
    *,
    address: int,
    correlation_id: str,
    status: str,
    error_code: str | None = None,
    timezone: str = "Africa/Lagos",
    path: Path | None = None,
) -> None:
    """Stop retrying this address for the local business day after a terminal fail.

    Does not overwrite an existing CAPTURED entry.
    """
    today = business_date_today(timezone=timezone)
    data = _today_marker(timezone=timezone, path=path)
    captured = data.get("captured")
    if isinstance(captured, dict) and str(int(address)) in captured:
        return
    failed = data.setdefault("failed", {})
    if not isinstance(failed, dict):
        failed = {}
        data["failed"] = failed
    failed[str(int(address))] = {
        "correlationId": correlation_id,
        "status": str(status or "UNSUPPORTED").upper(),
        "errorCode": error_code,
        "failedAt": datetime.now(ZoneInfo(timezone or "Africa/Lagos")).isoformat(),
    }
    data["businessDate"] = today
    save_marker(data, path)
