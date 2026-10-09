"""Pump meter readings — read-only cumulative reconciliation helpers.

Default path reports UNSUPPORTED (never invents zero). Optional auto-CD101 is
gated and only enqueues via the existing outbound/poll serial path.

CAPTURED means a DC101 reply was captured and correlated — not that field-to-
nozzle mapping or litre scale has been physically verified at the site.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from intelipump_fdc.controller.session_models import NozzlePosition
from intelipump_fdc.core.config import MeterReadingSettings
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.cd101 import build_cd101_request

SOFTWARE_VERSION = "intelipump-fdc-meter-reading-2"

# Process-local rate limit (survives only until restart; DB pending count also bounds).
_last_read_monotonic: dict[str, float] = {}

# COUN 0x01–0x09 are volume-class counters per Pump Interface Rev 2.11 p.19/25.
VOLUME_COUN_MIN = 0x01
VOLUME_COUN_MAX = 0x09

_BUSY_SALE_LIFECYCLES = frozenset(
    {
        "NOZZLE_LIFTED",
        "AUTHORIZED",
        "FILLING",
        "FILLING_COMPLETED",
    }
)


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
    """True when CD101 must not run — protect live sale / nozzle-lift path."""
    if state is None:
        return False
    value = state.value if isinstance(state, PumpState) else str(state)
    return value in {
        PumpState.FILLING.value,
        PumpState.AUTHORIZED.value,
        PumpState.SUSPENDED.value,
        PumpState.FILLING_COMPLETE.value,
        PumpState.LIMIT_REACHED.value,
        PumpState.NOZZLE_UP.value,
        "NOZZLE_UP",
    }


def is_volume_counter_select(counter_select: int) -> bool:
    return VOLUME_COUN_MIN <= int(counter_select) <= VOLUME_COUN_MAX


def liters_from_raw_scaled(
    raw: int | None,
    decimals: int | None,
    *,
    counter_select: int | None = None,
) -> float | None:
    """Return litres only when COUN is volume-class and decimals are configured.

    Never invents 0.0 on missing/unknown scale.
    """
    if raw is None or decimals is None:
        return None
    if counter_select is not None and not is_volume_counter_select(int(counter_select)):
        return None
    return int(raw) / (10 ** int(decimals))


def evaluate_meter_tx_eligibility(
    *,
    current_state: PumpState | str | None,
    nozzle_position: NozzlePosition | str | None,
    last_nozio_mono: float | None,
    now_mono: float,
    nozzle_in_max_age_s: float,
    sale_lifecycle: str | None = None,
    active_transaction_id: str | None = None,
    held_completion: bool = False,
    pending_exchange: bool = False,
    block_during_dispensing: bool = True,
) -> tuple[bool, str, str]:
    """Return (allowed, error_code, message) for CD101 TX / enqueue.

    Unknown or stale nozzle-IN evidence refuses the read.
    """
    if block_during_dispensing and dispensing_blocks_read(current_state):
        return (
            False,
            "METER_READ_DEFERRED_DISPENSING",
            "pump busy dispensing; retry when idle",
        )
    if isinstance(nozzle_position, NozzlePosition):
        pos = nozzle_position
    elif nozzle_position is None:
        pos = NozzlePosition.UNKNOWN
    else:
        try:
            pos = NozzlePosition(str(nozzle_position))
        except ValueError:
            pos = NozzlePosition.UNKNOWN
    if pos is NozzlePosition.UNKNOWN:
        return (
            False,
            "METER_READ_REFUSED_NOZZLE_UNKNOWN",
            "nozzle position unknown; wait for verified NOZIO IN",
        )
    if pos is NozzlePosition.OUT:
        return (
            False,
            "METER_READ_DEFERRED_NOZZLE_OUT",
            "nozzle is OUT; hang up and retry when idle",
        )
    if last_nozio_mono is None:
        return (
            False,
            "METER_READ_REFUSED_NOZZLE_UNVERIFIED",
            "no verified nozzle-IN observation yet this session",
        )
    age = float(now_mono) - float(last_nozio_mono)
    if age > float(nozzle_in_max_age_s):
        return (
            False,
            "METER_READ_REFUSED_NOZZLE_STALE",
            f"nozzle-IN evidence stale ({age:.1f}s > {nozzle_in_max_age_s}s); "
            "re-seat nozzle or wait for a fresh DC3/NOZIO",
        )
    life = str(sale_lifecycle or "")
    if life in _BUSY_SALE_LIFECYCLES:
        return (
            False,
            "METER_READ_DEFERRED_SALE_LIFECYCLE",
            f"sale lifecycle {life} blocks meter read",
        )
    if active_transaction_id:
        return (
            False,
            "METER_READ_DEFERRED_ACTIVE_SALE",
            "active sale identity present; retry when idle",
        )
    if held_completion:
        return (
            False,
            "METER_READ_DEFERRED_COMPLETION_HOLD",
            "completion hold pending; retry when idle",
        )
    if pending_exchange:
        return (
            False,
            "METER_READ_DEFERRED_PENDING_EXCHANGE",
            "command exchange in flight; retry shortly",
        )
    return (True, "", "")


def dc101_matches_pending(
    *,
    decoded: dict[str, Any] | None,
    observed_at_mono: float | None,
    expected_address: int,
    observed_address: int,
    expected_coun: int,
    queued_at_mono: float,
    tx_started_at_mono: float | None,
) -> tuple[bool, str]:
    """Correlate DC101 to a pending CD101 request.

    Protocol limitation (documented): Wayne DC101 does not echo a request UUID.
    Correlation is (DART address + requested COUN + observation after our TX).
    Unsolicited / late / wrong-address / wrong-COUN replies must not complete a
    newer request.
    """
    if observed_address != int(expected_address):
        return False, "wrong_address"
    if not isinstance(decoded, dict):
        return False, "missing_decoded"
    if observed_at_mono is None:
        return False, "missing_timestamp"
    # Prefer post-TX window; fall back to post-queue only if TX stamp missing.
    floor = (
        float(tx_started_at_mono)
        if tx_started_at_mono is not None
        else float(queued_at_mono)
    )
    if float(observed_at_mono) < floor:
        return False, "before_request_window"
    coun = decoded.get("counter_select")
    if not isinstance(coun, int):
        return False, "missing_coun"
    if int(coun) != int(expected_coun):
        return False, "wrong_coun"
    return True, "matched"


def build_capture_flags(
    *,
    counter_select: int,
    volume_decimals: int | None,
    liters: float | None,
) -> dict[str, Any]:
    return {
        "scaleVerified": bool(
            liters is not None
            and volume_decimals is not None
            and is_volume_counter_select(counter_select)
        ),
        "nozzleMappingVerified": False,
        "counterSelect": int(counter_select),
        "volumeDecimalsConfigured": volume_decimals,
        "note": (
            "CAPTURED = correlated DC101 reply retained. "
            "Field-to-nozzle mapping and face scale require attended verification."
        ),
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


def dart_address_for_nozzle(
    channel_mappings: dict[int, Any] | None,
    nozzle_id: str,
) -> int | None:
    """Resolve DART logical address from channel map nozzle_id."""
    if not channel_mappings:
        return None
    want = (nozzle_id or "").strip()
    for addr, meta in channel_mappings.items():
        nozzle = None
        if hasattr(meta, "nozzle_id"):
            nozzle = getattr(meta, "nozzle_id")
        elif isinstance(meta, dict):
            nozzle = meta.get("nozzle_id") or meta.get("nozzleId")
        if str(nozzle or "").strip() == want:
            return int(addr)
    return None


def decide_read_meter(
    *,
    settings: MeterReadingSettings,
    current_state: PumpState | str | None,
    pending_count: int,
    rate_limited: bool,
) -> tuple[str, str, str]:
    """Return (execution_status, error_code, message).

    Never invents a cumulative value. Hardware CD101 (file bridge) or LAB auto.
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
    if settings.hardware_cd101:
        return (
            "PENDING_CONTROLLER",
            "METER_READ_QUEUED_HARDWARE",
            "Hardware CD101 queued via sole-controller file bridge (read-only).",
        )
    if not settings.auto_cd101:
        return (
            "UNSUPPORTED",
            "METER_READ_UNSUPPORTED",
            "Automatic CD101 meter capture is not enabled on this controller; "
            "use manual cumulative readings or enable HARDWARE_CD101. "
            "Never invents a zero.",
        )
    return (
        "PENDING_CONTROLLER",
        "METER_READ_QUEUED",
        "Experimental LAB auto-CD101 queued on existing outbound path.",
    )
