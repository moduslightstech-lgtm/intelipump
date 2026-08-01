"""Operator markers for passive capture sessions."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from tools.passive_dart_capture.evidence_writer import EvidenceWriter, session_evidence_path


class OperatorMarker(StrEnum):
    STARTUP = "STARTUP"
    EPUMP_CONNECTED = "EPUMP_CONNECTED"
    NOZZLE_LIFTED = "NOZZLE_LIFTED"
    NOZZLE_RETURNED = "NOZZLE_RETURNED"
    DISPLAY_CHANGED = "DISPLAY_CHANGED"
    RESET_OBSERVED = "RESET_OBSERVED"
    AUTHORIZED_OBSERVED = "AUTHORIZED_OBSERVED"
    FILLING_OBSERVED = "FILLING_OBSERVED"
    TRANSACTION_COMPLETED = "TRANSACTION_COMPLETED"
    END_CAPTURE = "END_CAPTURE"


ALLOWED_MARKERS: frozenset[str] = frozenset(m.value for m in OperatorMarker)


def parse_marker_name(name: str) -> OperatorMarker:
    key = name.strip().upper()
    if key not in ALLOWED_MARKERS:
        allowed = ", ".join(sorted(ALLOWED_MARKERS))
        raise ValueError(f"unknown marker {name!r}; allowed: {allowed}")
    return OperatorMarker(key)


def make_marker_record(
    *,
    session_id: str,
    marker: OperatorMarker | str,
    note: str | None = None,
    timestamp_utc: datetime | None = None,
    monotonic_ns: int | None = None,
) -> dict[str, Any]:
    m = parse_marker_name(str(marker))
    ts = timestamp_utc or datetime.now(UTC)
    mono = monotonic_ns if monotonic_ns is not None else time.monotonic_ns()
    return {
        "recordType": "operator_marker",
        "sessionId": session_id,
        "marker": m.value,
        "captureTimestampUtc": ts.isoformat(),
        "monotonicTimestampNs": mono,
        "note": note,
        "source": "EPUMP_PASSIVE_CAPTURE",
    }


def append_marker(
    session_id: str,
    marker: str,
    *,
    evidence_dir=None,
    note: str | None = None,
) -> dict[str, Any]:
    """Append a marker to an existing (or new) session JSONL file."""
    path = session_evidence_path(session_id, evidence_dir)
    record = make_marker_record(session_id=session_id, marker=marker, note=note)
    with EvidenceWriter(path) as writer:
        writer.write_record(record)
    return record
