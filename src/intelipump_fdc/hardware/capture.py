"""Append-safe JSONL raw frame capture for RS-485 bench evidence."""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

_SECRET_KEYS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "credential",
        "private_key",
        "client_key",
    }
)


@dataclass(frozen=True, slots=True)
class CaptureRecord:
    utc_timestamp: str
    monotonic_ns: int
    direction: str  # TX | RX
    role: str  # controller | simulator
    port: str
    adapter_stable_id: str | None
    raw_frame_hex: str
    parsed_frame_type: str | None
    dart_address: int | None
    sequence: int | None
    crc_valid: bool | None
    latency_ms: float | None
    environment: str = "LAB"
    simulated: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def sanitize_mapping(data: dict[str, Any]) -> dict[str, Any]:
    """Drop credential-like keys from a mapping (shallow)."""
    out: dict[str, Any] = {}
    for key, value in data.items():
        if key.lower() in _SECRET_KEYS or any(s in key.lower() for s in _SECRET_KEYS):
            continue
        if isinstance(value, dict):
            out[key] = sanitize_mapping(value)
        else:
            out[key] = value
    return out


class JsonlCaptureWriter:
    """Append-safe JSONL capture with flush + atomic finalize."""

    def __init__(
        self,
        path: Path | str,
        *,
        write_sanitized_copy: bool = True,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._tmp = self.path.with_suffix(self.path.suffix + ".partial")
        self._sanitized_path = (
            self.path.with_name(self.path.stem + ".sanitized" + self.path.suffix)
            if write_sanitized_copy
            else None
        )
        self._fh: TextIO = self._tmp.open("a", encoding="utf-8")
        self._sanitized_fh: TextIO | None = None
        if self._sanitized_path is not None:
            self._sanitized_fh = self._sanitized_path.with_suffix(
                self._sanitized_path.suffix + ".partial"
            ).open("a", encoding="utf-8")
        self._count = 0
        self._closed = False

    @property
    def record_count(self) -> int:
        return self._count

    def append(self, record: CaptureRecord) -> None:
        if self._closed:
            raise RuntimeError("capture writer is closed")
        payload = record.to_dict()
        line = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        self._fh.write(line + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())
        if self._sanitized_fh is not None:
            sanitized = sanitize_mapping(payload)
            self._sanitized_fh.write(
                json.dumps(sanitized, separators=(",", ":"), sort_keys=True) + "\n"
            )
            self._sanitized_fh.flush()
            os.fsync(self._sanitized_fh.fileno())
        self._count += 1

    def record_bytes(
        self,
        *,
        direction: str,
        role: str,
        port: str,
        adapter_stable_id: str | None,
        raw: bytes,
        parsed_frame_type: str | None = None,
        dart_address: int | None = None,
        sequence: int | None = None,
        crc_valid: bool | None = None,
        latency_ms: float | None = None,
        environment: str = "LAB",
        simulated: bool = True,
    ) -> CaptureRecord:
        rec = CaptureRecord(
            utc_timestamp=datetime.now(UTC).isoformat(),
            monotonic_ns=time.monotonic_ns(),
            direction=direction,
            role=role,
            port=port,
            adapter_stable_id=adapter_stable_id,
            raw_frame_hex=raw.hex(" "),
            parsed_frame_type=parsed_frame_type,
            dart_address=dart_address,
            sequence=sequence,
            crc_valid=crc_valid,
            latency_ms=latency_ms,
            environment=environment,
            simulated=simulated,
        )
        self.append(rec)
        return rec

    def finalize(self) -> Path:
        """Flush, close, and atomically move partial → final path."""
        if self._closed:
            return self.path
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        if self._sanitized_fh is not None and self._sanitized_path is not None:
            self._sanitized_fh.flush()
            os.fsync(self._sanitized_fh.fileno())
            partial_s = Path(self._sanitized_fh.name)
            self._sanitized_fh.close()
            os.replace(partial_s, self._sanitized_path)
        os.replace(self._tmp, self.path)
        self._closed = True
        return self.path

    def close(self) -> Path:
        return self.finalize()

    def __enter__(self) -> JsonlCaptureWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.finalize()
