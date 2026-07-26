"""Evidence JSONL + Markdown for intelipump-poll-bench."""

from __future__ import annotations

import json
import statistics
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import IO, Any
from uuid import uuid4

from intelipump_fdc.bench_poll import SCHEMA_VERSION
from intelipump_fdc.bench_poll.guards import software_commit
from intelipump_fdc.capture.schema import bytes_to_raw_hex


class RecordType(StrEnum):
    EVENT = "event"
    FRAME = "frame"


class BenchEvent(StrEnum):
    BENCH_STARTED = "bench_started"
    POLL_SENT = "poll_sent"
    RESPONSE_RECEIVED = "response_received"
    RESPONSE_TIMEOUT = "response_timeout"
    PROTOCOL_ERROR = "protocol_error"
    BENCH_STOPPED = "bench_stopped"
    SAFETY_REFUSED = "safety_refused"


class BenchResult(StrEnum):
    PASS = "POLL_BENCH_PASS"
    INCONCLUSIVE = "POLL_BENCH_INCONCLUSIVE"
    FAIL = "POLL_BENCH_FAIL"


def new_session_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S") + "-" + uuid4().hex[:8]


@dataclass
class EvidenceRecord:
    schemaVersion: int
    sessionId: str
    timestampUtc: str
    monotonicNs: int
    recordType: str
    direction: str | None
    rawHex: str | None
    byteCount: int | None
    pumpAddress: int | None
    logicalAddress: int | None = None
    wireAddress: int | None = None
    pollSequence: int | None = None
    timeoutMs: int | None = None
    responseClassification: str | None = None
    crcValid: bool | None = None
    notes: str | None = None
    event: str | None = None
    targetType: str | None = None
    simulatorValidation: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=True)


@dataclass
class BenchSessionStats:
    polls_sent: int = 0
    valid_responses: int = 0
    timeouts: int = 0
    crc_errors: int = 0
    protocol_errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    last_tx_hex: str | None = None
    last_rx_hex: str | None = None


class EvidenceWriter:
    def __init__(
        self,
        path: Path,
        session_id: str,
        *,
        target_type: str,
        simulator_validation: bool,
    ) -> None:
        self.path = path
        self.session_id = session_id
        self.target_type = target_type
        self.simulator_validation = simulator_validation
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp: IO[str] | None = self.path.open("w", encoding="utf-8")
        self.records = 0

    def close(self) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def write(self, record: EvidenceRecord) -> None:
        assert self._fp is not None
        self._fp.write(record.to_json_line() + "\n")
        self._fp.flush()
        self.records += 1

    def emit_event(
        self,
        event: BenchEvent,
        *,
        monotonic_ns: int,
        pump_address: int | None = None,
        logical_address: int | None = None,
        wire_address: int | None = None,
        poll_sequence: int | None = None,
        timeout_ms: int | None = None,
        notes: str | None = None,
        classification: str | None = None,
        crc_valid: bool | None = None,
    ) -> None:
        logical = logical_address if logical_address is not None else pump_address
        self.write(
            EvidenceRecord(
                schemaVersion=SCHEMA_VERSION,
                sessionId=self.session_id,
                timestampUtc=datetime.now(UTC).isoformat(),
                monotonicNs=monotonic_ns,
                recordType=RecordType.EVENT.value,
                direction=None,
                rawHex=None,
                byteCount=None,
                pumpAddress=logical,
                logicalAddress=logical,
                wireAddress=wire_address,
                pollSequence=poll_sequence,
                timeoutMs=timeout_ms,
                responseClassification=classification,
                crcValid=crc_valid,
                notes=notes,
                event=event.value,
                targetType=self.target_type,
                simulatorValidation=self.simulator_validation,
            )
        )

    def emit_frame(
        self,
        *,
        direction: str,
        raw: bytes,
        monotonic_ns: int,
        pump_address: int,
        poll_sequence: int,
        timeout_ms: int | None,
        classification: str | None,
        crc_valid: bool | None,
        notes: str | None = None,
        event: BenchEvent | None = None,
        logical_address: int | None = None,
        wire_address: int | None = None,
    ) -> None:
        logical = logical_address if logical_address is not None else pump_address
        self.write(
            EvidenceRecord(
                schemaVersion=SCHEMA_VERSION,
                sessionId=self.session_id,
                timestampUtc=datetime.now(UTC).isoformat(),
                monotonicNs=monotonic_ns,
                recordType=RecordType.FRAME.value,
                direction=direction,
                rawHex=bytes_to_raw_hex(raw),
                byteCount=len(raw),
                pumpAddress=logical,
                logicalAddress=logical,
                wireAddress=wire_address,
                pollSequence=poll_sequence,
                timeoutMs=timeout_ms,
                responseClassification=classification,
                crcValid=crc_valid,
                notes=notes,
                event=event.value if event else None,
                targetType=self.target_type,
                simulatorValidation=self.simulator_validation,
            )
        )


def suggest_result(stats: BenchSessionStats, *, max_polls: int) -> BenchResult:
    if stats.crc_errors > 0 or stats.protocol_errors > 0:
        return BenchResult.FAIL
    if stats.polls_sent == 0:
        return BenchResult.FAIL
    if stats.valid_responses >= 1 and stats.timeouts == 0:
        return BenchResult.PASS
    if stats.valid_responses >= 1:
        return BenchResult.INCONCLUSIVE
    if stats.timeouts >= max_polls:
        return BenchResult.INCONCLUSIVE
    return BenchResult.INCONCLUSIVE


def write_markdown_summary(
    path: Path,
    *,
    session_id: str,
    port: str,
    baud: int,
    address: int,
    max_polls: int,
    stats: BenchSessionStats,
    result: BenchResult,
    target_type: str,
    simulator_validation: bool,
    dispenser_observations: str = "(operator to fill)",
) -> None:
    lat = stats.latencies_ms
    if lat:
        lat_min = f"{min(lat):.2f}"
        lat_mean = f"{statistics.mean(lat):.2f}"
        lat_max = f"{max(lat):.2f}"
    else:
        lat_min = lat_mean = lat_max = "n/a"
    lines = [
        "# Real-pump poll bench summary",
        "",
        f"**Result:** `{result.value}`",
        "",
        "## Session",
        "",
        f"- Session ID: `{session_id}`",
        f"- Software commit: `{software_commit()}`",
        f"- Target type: `{target_type}`",
        f"- Simulator validation: `{simulator_validation}`",
        f"- Serial port: `{port}`",
        f"- Baud: {baud}",
        f"- Address: {address}",
        f"- Max polls: {max_polls}",
        "",
        "## Counts",
        "",
        f"- Polls sent: {stats.polls_sent}",
        f"- Valid responses: {stats.valid_responses}",
        f"- Timeouts: {stats.timeouts}",
        f"- CRC errors: {stats.crc_errors}",
        f"- Protocol errors: {stats.protocol_errors}",
        "",
        "## Latency (ms)",
        "",
        f"- Min: {lat_min}",
        f"- Mean: {lat_mean}",
        f"- Max: {lat_max}",
        "",
        "## Exact frames",
        "",
        f"- TX: `{stats.last_tx_hex or '(none)'}`",
        f"- RX: `{stats.last_rx_hex or '(none)'}`",
        "",
        "## Dispenser observations",
        "",
        dispenser_observations,
        "",
        f"**Final result:** `{result.value}`",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
