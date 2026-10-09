"""File bridge: one-shot CD101 meter read via the sole controller outbound path.

Mirrors SET_PRICE: a separate process never opens RS-485. The controller
(sole bus master) applies CD101 when idle and gated, then writes a result
JSON. Read-only — no RESET / SET_PRICE / AUTHORIZE.

Concurrency:
- Request file is created with O_EXCL (atomic ownership).
- Controller claims by renaming request → inflight.
- Results are written as meter-read-result.<correlationId>.json (and a
  latest pointer) so concurrent CLIs never consume each other's reply.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


DEFAULT_REQUEST_NAME = "meter-read-request.json"
DEFAULT_INFLIGHT_NAME = "meter-read-inflight.json"
DEFAULT_RESULT_NAME = "meter-read-result.json"
DEFAULT_REQUEST_MAX_AGE_S = 120.0


def request_dir() -> Path:
    return Path(os.environ.get("INTELIPUMP_METER_READ_REQUEST_DIR", "/var/lib/intelipump"))


def request_path() -> Path:
    return request_dir() / DEFAULT_REQUEST_NAME


def inflight_path() -> Path:
    return request_dir() / DEFAULT_INFLIGHT_NAME


def result_path() -> Path:
    """Latest pointer (diagnostics / cloud-sync publisher)."""
    return request_dir() / DEFAULT_RESULT_NAME


def result_path_for(correlation_id: str) -> Path:
    safe = "".join(c for c in correlation_id if c.isalnum() or c in "-_")
    if not safe:
        raise ValueError("correlation_id required for result path")
    return request_dir() / f"meter-read-result.{safe}.json"


@dataclass(frozen=True, slots=True)
class MeterReadRequest:
    correlation_id: str
    dart_address: int
    counter_select: int = 1
    requested_by: str | None = None
    requested_at: str | None = None
    nozzle_hint: str | None = None
    pump_id: str | None = None
    notes: str | None = None
    slot: str | None = None  # OPENING | CLOSING | AD_HOC
    startup_opening: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlationId": self.correlation_id,
            "dartAddress": self.dart_address,
            "counterSelect": self.counter_select,
            "requestedBy": self.requested_by,
            "requestedAt": self.requested_at or datetime.now(UTC).isoformat(),
            "nozzleHint": self.nozzle_hint,
            "pumpId": self.pump_id,
            "notes": self.notes,
            "slot": self.slot,
            "startupOpening": bool(self.startup_opening),
            "readOnly": True,
            "schemaVersion": "1.1",
        }


def _parse_request_dict(raw: dict[str, Any]) -> MeterReadRequest | None:
    try:
        addr = int(
            raw.get("dartAddress")
            if raw.get("dartAddress") is not None
            else raw.get("dart_address")
        )
        coun = int(
            raw.get("counterSelect")
            if raw.get("counterSelect") is not None
            else raw.get("counter_select")
            or 1
        )
        corr = (
            str(raw.get("correlationId") or raw.get("correlation_id") or "").strip()
            or str(uuid4())
        )
    except (TypeError, ValueError):
        return None
    nozzle = raw.get("nozzleHint") or raw.get("nozzle_hint")
    pump = raw.get("pumpId") or raw.get("pump_id")
    slot = raw.get("slot")
    startup = raw.get("startupOpening")
    if startup is None:
        startup = raw.get("startup_opening")
    return MeterReadRequest(
        correlation_id=corr,
        dart_address=addr,
        counter_select=coun,
        requested_by=(str(raw["requestedBy"]) if raw.get("requestedBy") else None),
        requested_at=(str(raw["requestedAt"]) if raw.get("requestedAt") else None),
        nozzle_hint=(str(nozzle) if nozzle else None),
        pump_id=(str(pump) if pump else None),
        notes=(str(raw["notes"]) if raw.get("notes") else None),
        slot=(str(slot) if slot else None),
        startup_opening=bool(startup),
    )


def _load_request_file(path: Path) -> MeterReadRequest | None:
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    return _parse_request_dict(raw)


def request_busy(*, max_age_s: float = DEFAULT_REQUEST_MAX_AGE_S) -> bool:
    """True if an exclusive request or inflight claim currently owns the bridge."""
    _expire_stale_request(max_age_s=max_age_s)
    return request_path().is_file() or inflight_path().is_file()


def _expire_stale_request(*, max_age_s: float) -> None:
    path = request_path()
    if not path.is_file():
        return
    try:
        age = time_monotonic_file_age_s(path)
    except OSError:
        return
    if age is not None and age > float(max_age_s):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def time_monotonic_file_age_s(path: Path) -> float | None:
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    return max(0.0, __import__("time").time() - mtime)


def write_meter_read_request(req: MeterReadRequest) -> Path:
    """Atomically create the sole request file (O_EXCL). Raises FileExistsError if busy."""
    _expire_stale_request(max_age_s=DEFAULT_REQUEST_MAX_AGE_S)
    path = request_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if inflight_path().is_file() or path.is_file():
        raise FileExistsError(
            f"meter-read bridge busy (request or inflight present at {path.parent})"
        )
    payload = json.dumps(req.to_dict(), separators=(",", ":")).encode("utf-8")
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open(path, flags, 0o644)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    return path


def read_meter_read_request() -> MeterReadRequest | None:
    """Peek pending request (not yet claimed). Prefer claim_meter_read_request()."""
    return _load_request_file(request_path())


def claim_meter_read_request() -> MeterReadRequest | None:
    """Atomically move request → inflight and return it. None if nothing to claim."""
    src = request_path()
    dst = inflight_path()
    if not src.is_file():
        return None
    if dst.is_file():
        # Prior claim still in flight — do not steal.
        return None
    try:
        os.rename(src, dst)
    except FileNotFoundError:
        return None
    except OSError:
        return None
    req = _load_request_file(dst)
    if req is None:
        try:
            dst.unlink(missing_ok=True)
        except OSError:
            pass
    return req


def clear_meter_read_request() -> None:
    """Clear pending request and/or inflight claim."""
    for path in (request_path(), inflight_path()):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def write_meter_read_result(payload: dict[str, Any]) -> Path:
    """Write correlation-scoped result + latest pointer (atomic replace)."""
    path_latest = result_path()
    path_latest.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body.setdefault("writtenAt", datetime.now(UTC).isoformat())
    corr = str(body.get("correlationId") or "").strip()
    data = json.dumps(body, indent=2, sort_keys=True)

    def _atomic_write(target: Path) -> None:
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(data, encoding="utf-8")
        tmp.replace(target)

    _atomic_write(path_latest)
    if corr:
        _atomic_write(result_path_for(corr))
    return path_latest


def read_meter_read_result(*, correlation_id: str | None = None) -> dict[str, Any] | None:
    path = result_path_for(correlation_id) if correlation_id else result_path()
    if not path.is_file():
        # Fall back to latest pointer only when not asking for a specific id.
        if correlation_id:
            return None
        path = result_path()
        if not path.is_file():
            return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def new_correlation_id() -> str:
    return str(uuid4())
