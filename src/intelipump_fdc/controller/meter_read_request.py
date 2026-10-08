"""File bridge: one-shot CD101 meter read via the sole controller outbound path.

Mirrors SET_PRICE: a separate process never opens RS-485. The controller
(sole bus master) applies CD101 when idle and gated, then writes a result
JSON. Read-only — no RESET / SET_PRICE / AUTHORIZE.
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
DEFAULT_RESULT_NAME = "meter-read-result.json"


def request_dir() -> Path:
    return Path(os.environ.get("INTELIPUMP_METER_READ_REQUEST_DIR", "/var/lib/intelipump"))


def request_path() -> Path:
    return request_dir() / DEFAULT_REQUEST_NAME


def result_path() -> Path:
    return request_dir() / DEFAULT_RESULT_NAME


@dataclass(frozen=True, slots=True)
class MeterReadRequest:
    correlation_id: str
    dart_address: int
    counter_select: int = 1
    requested_by: str | None = None
    requested_at: str | None = None
    nozzle_hint: str | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlationId": self.correlation_id,
            "dartAddress": self.dart_address,
            "counterSelect": self.counter_select,
            "requestedBy": self.requested_by,
            "requestedAt": self.requested_at or datetime.now(UTC).isoformat(),
            "nozzleHint": self.nozzle_hint,
            "notes": self.notes,
            "readOnly": True,
            "schemaVersion": "1.0",
        }


def write_meter_read_request(req: MeterReadRequest) -> Path:
    path = request_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(req.to_dict(), separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)
    return path


def read_meter_read_request() -> MeterReadRequest | None:
    path = request_path()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        addr = int(raw.get("dartAddress") if raw.get("dartAddress") is not None else raw.get("dart_address"))
        coun = int(raw.get("counterSelect") if raw.get("counterSelect") is not None else raw.get("counter_select") or 1)
        corr = str(raw.get("correlationId") or raw.get("correlation_id") or "").strip() or str(uuid4())
    except (TypeError, ValueError):
        return None
    return MeterReadRequest(
        correlation_id=corr,
        dart_address=addr,
        counter_select=coun,
        requested_by=(str(raw["requestedBy"]) if raw.get("requestedBy") else None),
        requested_at=(str(raw["requestedAt"]) if raw.get("requestedAt") else None),
        nozzle_hint=(str(raw["nozzleHint"]) if raw.get("nozzleHint") else None),
        notes=(str(raw["notes"]) if raw.get("notes") else None),
    )


def clear_meter_read_request() -> None:
    path = request_path()
    if path.is_file():
        path.unlink()


def write_meter_read_result(payload: dict[str, Any]) -> Path:
    path = result_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body.setdefault("writtenAt", datetime.now(UTC).isoformat())
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)
    return path


def read_meter_read_result() -> dict[str, Any] | None:
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
