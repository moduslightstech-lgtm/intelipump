"""Pump meter readings — read-only cumulative reconciliation helpers.

Default path reports UNSUPPORTED (never invents zero). Optional auto-CD101 is
gated and only enqueues via the existing outbound/poll serial path.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from intelipump_fdc.core.config import MeterReadingSettings
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.cd101 import build_cd101_request

SOFTWARE_VERSION = "intelipump-fdc-meter-reading-1"

# Process-local rate limit (survives only until restart; DB pending count also bounds).
_last_read_monotonic: dict[str, float] = {}


def rate_limit_key(station_id: str, pump_id: str, nozzle_id: str) -> str:
    return f"{station_id}:{pump_id}:{nozzle_id}"


def is_rate_limited(
    *,
    station_id: str,
    pump_id: str,
    nozzle_id: str,
    min_interval_seconds: float,
    now_mono: float | None = None,
) -> bool:
    key = rate_limit_key(station_id, pump_id, nozzle_id)
    last = _last_read_monotonic.get(key)
    if last is None:
        return False
    mono = now_mono if now_mono is not None else time.monotonic()
    return (mono - last) < float(min_interval_seconds)


def mark_read_attempt(
    *,
    station_id: str,
    pump_id: str,
    nozzle_id: str,
    now_mono: float | None = None,
) -> None:
    key = rate_limit_key(station_id, pump_id, nozzle_id)
    _last_read_monotonic[key] = now_mono if now_mono is not None else time.monotonic()


def clear_rate_limits() -> None:
    """Test helper."""
    _last_read_monotonic.clear()


def nozzle_from_payload(payload: dict[str, Any] | None, *, default: str = "nozzle-1") -> str:
    if not payload:
        return default
    raw = (
        payload.get("nozzleId")
        or payload.get("nozzle_id")
        or payload.get("nozzle")
        or default
    )
    text = str(raw).strip()
    if text.isdigit():
        return f"nozzle-{text}"
    return text or default


def dispensing_blocks_read(state: PumpState | str | None) -> bool:
    if state is None:
        return False
    value = state.value if isinstance(state, PumpState) else str(state)
    return value in {
        PumpState.FILLING.value,
        PumpState.AUTHORIZED.value,
        PumpState.SUSPENDED.value,
        PumpState.FILLING_COMPLETE.value,
        "NOZZLE_UP",
    }


def build_unsupported_payload(
    *,
    station_id: str,
    device_id: str,
    pump_id: str,
    nozzle_id: str,
    dart_address: int | None,
    correlation_id: str,
    requested_at: datetime | None = None,
    reason: str,
    error_code: str = "METER_READ_UNSUPPORTED",
    flags: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = requested_at or datetime.now(UTC)
    return {
        "stationId": station_id,
        "deviceId": device_id,
        "pumpId": pump_id,
        "nozzleId": nozzle_id,
        "dartAddress": dart_address,
        "status": "UNSUPPORTED",
        "source": "READ_NOW",
        "slot": "AD_HOC",
        "requestedAt": now.isoformat(),
        "capturedAt": None,
        "cumulativeVolumeRaw": None,
        "volumeLiters": None,
        "volumeDecimals": 2,
        "units": "liters",
        "correlationId": correlation_id,
        "errorCode": error_code,
        "errorMessage": reason,
        "flags": flags or {"automatic_cd101": "unverified"},
        "softwareVersion": SOFTWARE_VERSION,
        "rawEvidence": {
            "kind": "unsupported",
            "detail": reason,
            "spec_ref": "Pump Interface Rev 2.11 CD101/DC101",
            "note": "Do not treat last-sale DC2 volume as a totalizer.",
        },
    }


def build_cd101_outbound_payload(*, counter_select: int = 1) -> bytes:
    """Application payload only — controller wraps via existing outbound path."""
    return build_cd101_request(counter_select=counter_select).application_payload


def decide_read_meter(
    *,
    settings: MeterReadingSettings,
    current_state: PumpState | str | None,
    pending_count: int,
    rate_limited: bool,
) -> tuple[str, str, str]:
    """Return (execution_status, error_code, message).

    Never invents a cumulative value. Auto-CD101 only when explicitly gated.
    """
    if rate_limited:
        return (
            "RATE_LIMITED",
            "METER_READ_RATE_LIMITED",
            "Meter read suppressed to protect critical polling / sale persistence.",
        )
    if pending_count >= int(settings.max_pending):
        return (
            "RATE_LIMITED",
            "METER_READ_PENDING_CAP",
            "Too many pending meter reads for this pump; refusing additional request.",
        )
    if settings.block_during_dispensing and dispensing_blocks_read(current_state):
        return (
            "DEFERRED",
            "METER_READ_DEFERRED_DISPENSING",
            "Meter read deferred while pump appears to be dispensing.",
        )
    if not settings.auto_cd101:
        return (
            "UNSUPPORTED",
            "METER_READ_UNSUPPORTED",
            "Automatic CD101 meter capture is unverified on this controller; "
            "use manual cumulative readings. Never invents a zero.",
        )
    return (
        "PENDING_CONTROLLER",
        "METER_READ_QUEUED",
        "Experimental auto-CD101 queued on existing outbound path "
        "(physical validation still required).",
    )
