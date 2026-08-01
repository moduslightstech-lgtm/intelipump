"""JSONL evidence writer for passive DART capture sessions."""

from __future__ import annotations

import base64
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

# Default under the tool package so captures stay owned by this tool.
PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_EVIDENCE_DIR = PACKAGE_ROOT / "evidence"
DEFAULT_REPORTS_DIR = PACKAGE_ROOT / "reports"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


class EvidenceWriter:
    """Append-only JSONL writer (file I/O only — not serial)."""

    def __init__(self, path: Path) -> None:
        ensure_dir(path.parent)
        self._path = path
        self._lock = threading.Lock()
        self._fh = path.open("a", encoding="utf-8")

    @property
    def path(self) -> Path:
        return self._path

    def write_record(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=True)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                self._fh.close()

    def __enter__(self) -> EvidenceWriter:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def session_evidence_path(session_id: str, evidence_dir: Path | None = None) -> Path:
    root = evidence_dir or DEFAULT_EVIDENCE_DIR
    return ensure_dir(root) / f"{session_id}.jsonl"


def make_serial_chunk_record(
    *,
    session_id: str,
    chunk_sequence: int,
    data: bytes,
    capture_timestamp_utc: datetime,
    monotonic_timestamp_ns: int,
    serial_device: str,
    baud: int,
    parity: str,
    stop_bits: int,
) -> dict[str, Any]:
    return {
        "recordType": "serial_chunk",
        "sessionId": session_id,
        "captureTimestampUtc": capture_timestamp_utc.isoformat(),
        "monotonicTimestampNs": monotonic_timestamp_ns,
        "serialDevice": serial_device,
        "baud": baud,
        "parity": parity,
        "stopBits": stop_bits,
        "chunkSequence": chunk_sequence,
        "byteCount": len(data),
        "rawHex": data.hex(" ").upper(),
        "rawBase64": base64.b64encode(data).decode("ascii"),
        "direction": "MERGED_BUS",
        "source": "EPUMP_PASSIVE_CAPTURE",
    }
