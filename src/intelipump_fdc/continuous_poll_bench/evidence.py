"""Evidence JSONL + Markdown for intelipump-continuous-poll-bench."""

from __future__ import annotations

import json
import statistics
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import IO, Any
from uuid import uuid4

from intelipump_fdc.bench_poll.guards import software_commit
from intelipump_fdc.capture.schema import bytes_to_raw_hex
from intelipump_fdc.continuous_poll_bench import SCHEMA_VERSION


class RecordType(StrEnum):
    EVENT = "event"
    FRAME = "frame"


class ContinuousBenchEvent(StrEnum):
    BENCH_STARTED = "bench_started"
    POLL_SENT = "poll_sent"
    RESPONSE_RECEIVED = "response_received"
    RESPONSE_TIMEOUT = "response_timeout"
    SCHEDULE_LAG = "schedule_lag"
    PROTOCOL_ERROR = "protocol_error"
    SERIAL_DISCONNECT = "serial_disconnect"
    SERIAL_READ_CHUNK = "serial_read_chunk"
    STALE_INPUT_DRAINED = "stale_input_drained"
    TRANSIENT_EMPTY_READ = "transient_empty_read"
    CONTROL_RESPONSE = "control_response"
    DATA_RESPONSE = "data_response"
    CONTROL_ONLY = "control_only"
    BENCH_STOPPED = "bench_stopped"
    SAFETY_REFUSED = "safety_refused"


class ContinuousBenchResult(StrEnum):
    PASS = "CONTINUOUS_POLL_BENCH_PASS"
    INCONCLUSIVE = "CONTINUOUS_POLL_BENCH_INCONCLUSIVE"
    FAIL = "CONTINUOUS_POLL_BENCH_FAIL"


class StopReason(StrEnum):
    DURATION_EXPIRED = "duration_expired"
    OPERATOR_INTERRUPT = "operator_interrupt"
    ADDRESS_MISMATCH = "address_mismatch"
    UNEXPECTED_FRAME = "unexpected_frame"
    CRC_ERROR = "crc_error"
    PROTOCOL_ERROR = "protocol_error"
    MALFORMED_THRESHOLD = "malformed_threshold"
    SERIAL_DISCONNECT = "serial_disconnect"
    MAX_WRITES = "max_writes"
    SAFETY_FAULT = "safety_fault"


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
    scheduleLagMs: float | None = None
    latencyMs: float | None = None
    stopReason: str | None = None
    softwareCommit: str | None = None
    source: str | None = None
    pollCycleOutcome: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json_line(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=True)


@dataclass
class ContinuousSessionStats:
    polls_sent: int = 0
    # Any complete recognized protocol frame (SHORT_CONTROL_70 or DATA_FRAME).
    # valid_responses / validResponses is a documented alias of this counter.
    protocol_frames_received: int = 0
    control_responses: int = 0
    data_responses: int = 0
    control_only_cycles: int = 0
    timeouts: int = 0
    crc_errors: int = 0
    protocol_errors: int = 0
    unexpected_frames: int = 0
    malformed_count: int = 0
    schedule_lag_events: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    schedule_lags_ms: list[float] = field(default_factory=list)
    last_tx_hex: str | None = None
    last_rx_hex: str | None = None
    stop_reason: StopReason | None = None
    requested_duration_s: float = 0.0
    actual_duration_s: float = 0.0

    @property
    def valid_responses(self) -> int:
        """Alias of protocol_frames_received (not status-data completions)."""
        return self.protocol_frames_received


class EvidenceWriter:
    def __init__(
        self,
        path: Path,
        session_id: str,
        *,
        target_type: str,
        simulator_validation: bool,
        commit: str | None = None,
    ) -> None:
        self.path = path
        self.session_id = session_id
        self.target_type = target_type
        self.simulator_validation = simulator_validation
        self.commit = commit if commit is not None else software_commit()
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
        event: ContinuousBenchEvent,
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
        schedule_lag_ms: float | None = None,
        latency_ms: float | None = None,
        stop_reason: str | None = None,
        source: str | None = None,
        raw: bytes | None = None,
        poll_cycle_outcome: str | None = None,
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
                rawHex=bytes_to_raw_hex(raw) if raw is not None else None,
                byteCount=len(raw) if raw is not None else None,
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
                scheduleLagMs=schedule_lag_ms,
                latencyMs=latency_ms,
                stopReason=stop_reason,
                softwareCommit=self.commit,
                source=source,
                pollCycleOutcome=poll_cycle_outcome,
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
        event: ContinuousBenchEvent | None = None,
        schedule_lag_ms: float | None = None,
        latency_ms: float | None = None,
        logical_address: int | None = None,
        wire_address: int | None = None,
        source: str | None = None,
        poll_cycle_outcome: str | None = None,
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
                scheduleLagMs=schedule_lag_ms,
                latencyMs=latency_ms,
                stopReason=None,
                softwareCommit=self.commit,
                source=source,
                pollCycleOutcome=poll_cycle_outcome,
            )
        )


def suggest_result(
    stats: ContinuousSessionStats,
    *,
    fault: bool,
) -> ContinuousBenchResult:
    if fault or stats.stop_reason in {
        StopReason.ADDRESS_MISMATCH,
        StopReason.UNEXPECTED_FRAME,
        StopReason.CRC_ERROR,
        StopReason.PROTOCOL_ERROR,
        StopReason.MALFORMED_THRESHOLD,
        StopReason.SERIAL_DISCONNECT,
        StopReason.MAX_WRITES,
        StopReason.SAFETY_FAULT,
        StopReason.OPERATOR_INTERRUPT,
    }:
        return ContinuousBenchResult.FAIL
    if stats.polls_sent == 0:
        return ContinuousBenchResult.FAIL
    # Status-data collection PASS requires at least one DATA_FRAME cycle.
    if (
        stats.data_responses >= 1
        and stats.crc_errors == 0
        and stats.protocol_errors == 0
        and stats.unexpected_frames == 0
    ):
        return ContinuousBenchResult.PASS
    if stats.data_responses == 0 and (
        stats.timeouts >= 1 or stats.control_only_cycles >= 1
    ):
        return ContinuousBenchResult.INCONCLUSIVE
    return ContinuousBenchResult.INCONCLUSIVE


def write_markdown_summary(
    path: Path,
    *,
    session_id: str,
    port: str,
    canonical: str,
    baud: int,
    address: int,
    poll_interval_ms: int,
    response_timeout_ms: int,
    stats: ContinuousSessionStats,
    result: ContinuousBenchResult,
    target_type: str,
    simulator_validation: bool,
    command_queue_created: bool,
    authorization_objects_created: int,
    write_count: int,
    commit: str,
) -> None:
    avg_lat = (
        statistics.mean(stats.latencies_ms) if stats.latencies_ms else None
    )
    avg_lag = (
        statistics.mean(stats.schedule_lags_ms) if stats.schedule_lags_ms else None
    )
    lines = [
        "# Continuous poll bench summary",
        "",
        f"- Software commit: `{commit}`",
        f"- Session ID: `{session_id}`",
        f"- Target type: `{target_type}`",
        f"- Simulator validation: `{simulator_validation}`",
        f"- Serial port: `{port}`",
        f"- Canonical device: `{canonical}`",
        f"- Baud / data / parity / stop: `{baud}` / 8 / ODD / 1",
        f"- Address: `{address}`",
        f"- Requested duration (s): `{stats.requested_duration_s}`",
        f"- Actual duration (s): `{stats.actual_duration_s:.3f}`",
        f"- Poll interval (ms): `{poll_interval_ms}`",
        f"- Response timeout (ms): `{response_timeout_ms}`",
        f"- Polls sent / write count: `{stats.polls_sent}` / `{write_count}`",
        (
            f"- Protocol frames received (validResponses alias): "
            f"`{stats.protocol_frames_received}`"
        ),
        f"- Control responses (SHORT_CONTROL_70): `{stats.control_responses}`",
        f"- Data responses (DATA_FRAME): `{stats.data_responses}`",
        f"- Control-only cycles: `{stats.control_only_cycles}`",
        f"- Timeouts (no protocol frame): `{stats.timeouts}`",
        f"- CRC errors: `{stats.crc_errors}`",
        f"- Protocol errors: `{stats.protocol_errors}`",
        f"- Unexpected frames: `{stats.unexpected_frames}`",
        f"- Schedule lag events: `{stats.schedule_lag_events}`",
        (
            f"- Avg latency (ms): `{avg_lat:.2f}`"
            if avg_lat is not None
            else "- Avg latency (ms): `n/a`"
        ),
        (
            f"- Avg schedule lag (ms): `{avg_lag:.2f}`"
            if avg_lag is not None
            else "- Avg schedule lag (ms): `n/a`"
        ),
        f"- Last TX: `{stats.last_tx_hex or 'n/a'}`",
        f"- Last RX: `{stats.last_rx_hex or 'n/a'}`",
        f"- commandQueueCreated: `{command_queue_created}`",
        f"- authorizationObjectsCreated: `{authorization_objects_created}`",
        f"- Stop reason: `{stats.stop_reason.value if stats.stop_reason else 'n/a'}`",
        f"- Result: `{result.value}`",
        "",
        (
            "Note: PASS requires dataResponses > 0. "
            "validResponses/protocolFramesReceived counts any recognized "
            "protocol frame including interim SHORT_CONTROL_70."
        ),
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
