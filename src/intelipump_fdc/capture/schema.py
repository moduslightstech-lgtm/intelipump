"""JSONL capture record schema (passive Wayne lab)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from intelipump_fdc.capture import SCHEMA_VERSION


class RecordType(StrEnum):
    RX_CHUNK = "rx_chunk"
    EVENT = "event"


class CaptureEvent(StrEnum):
    CAPTURE_STARTED = "capture_started"
    SERIAL_DISCONNECTED = "serial_disconnected"
    SERIAL_RECONNECTED = "serial_reconnected"
    CAPTURE_STOPPED = "capture_stopped"
    ERROR = "error"


class SerialCaptureState(StrEnum):
    OPEN = "open"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"
    FAULTED = "faulted"


def new_capture_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S") + "-" + uuid4().hex[:8]


def bytes_to_raw_hex(data: bytes) -> str:
    """Preserve exact byte order as space-separated lowercase hex."""
    return data.hex(" ")


@dataclass(frozen=True, slots=True)
class CaptureRecord:
    """One JSONL line. Raw RX bytes are never modified or interpreted here."""

    schemaVersion: int
    captureId: str
    recordType: str
    timestampUtc: str
    monotonicNs: int
    port: str
    baud: int
    direction: str | None
    rawHex: str | None
    byteCount: int | None
    idleGapMs: float | None
    chunkSequence: int | None
    serialState: str
    notes: str | None
    event: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=True)


def make_event_record(
    *,
    capture_id: str,
    port: str,
    baud: int,
    event: CaptureEvent,
    monotonic_ns: int,
    serial_state: SerialCaptureState,
    notes: str | None = None,
    timestamp_utc: datetime | None = None,
) -> CaptureRecord:
    ts = timestamp_utc or datetime.now(UTC)
    return CaptureRecord(
        schemaVersion=SCHEMA_VERSION,
        captureId=capture_id,
        recordType=RecordType.EVENT.value,
        timestampUtc=ts.isoformat(),
        monotonicNs=monotonic_ns,
        port=port,
        baud=baud,
        direction=None,
        rawHex=None,
        byteCount=None,
        idleGapMs=None,
        chunkSequence=None,
        serialState=serial_state.value,
        notes=notes,
        event=event.value,
    )


def make_rx_record(
    *,
    capture_id: str,
    port: str,
    baud: int,
    raw: bytes,
    monotonic_ns: int,
    chunk_sequence: int,
    idle_gap_ms: float | None,
    serial_state: SerialCaptureState,
    notes: str | None = None,
    timestamp_utc: datetime | None = None,
) -> CaptureRecord:
    ts = timestamp_utc or datetime.now(UTC)
    return CaptureRecord(
        schemaVersion=SCHEMA_VERSION,
        captureId=capture_id,
        recordType=RecordType.RX_CHUNK.value,
        timestampUtc=ts.isoformat(),
        monotonicNs=monotonic_ns,
        port=port,
        baud=baud,
        direction="RX",
        rawHex=bytes_to_raw_hex(raw),
        byteCount=len(raw),
        idleGapMs=idle_gap_ms,
        chunkSequence=chunk_sequence,
        serialState=serial_state.value,
        notes=notes,
        event=None,
    )
