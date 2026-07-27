"""Bounded continuous status-poll session (verified DART POLL frames only)."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from intelipump_fdc.bench_poll.guards import TARGET_OWNED_LAB_WAYNE
from intelipump_fdc.bench_poll.poll_io import (
    ObservedFrame,
    StatusPollOutcome,
    send_status_poll_and_read_response,
)
from intelipump_fdc.bench_poll.transport import (
    BenchByteTransport,
    format_serial_config,
)
from intelipump_fdc.continuous_poll_bench.evidence import (
    ContinuousBenchEvent,
    ContinuousBenchResult,
    ContinuousSessionStats,
    EvidenceWriter,
    StopReason,
    new_session_id,
    suggest_result,
    write_markdown_summary,
)
from intelipump_fdc.continuous_poll_bench.guards import (
    MALFORMED_THRESHOLD,
    REAL_WAYNE_MAX_WRITES,
    software_commit,
)
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.captured_classify import CapturedFrameClass
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError

logger = logging.getLogger(__name__)

_VALID_DATA_CLASSES = frozenset({CapturedFrameClass.DATA_FRAME})
_INTERIM_CONTROL_CLASSES = frozenset({CapturedFrameClass.SHORT_CONTROL_70})


@dataclass(frozen=True, slots=True)
class ContinuousPollSessionConfig:
    port: str
    address: int
    baud: int
    duration_seconds: float
    poll_interval_ms: int
    response_timeout_ms: int
    evidence_jsonl: Path
    evidence_md: Path
    max_writes: int
    read_size: int = 256
    target_type: str = TARGET_OWNED_LAB_WAYNE
    simulator_validation: bool = False
    canonical: str | None = None


class ContinuousPollSession:
    """Sends only shared status polls on a monotonic schedule; no command queue/auth."""

    def __init__(
        self,
        transport: BenchByteTransport,
        config: ContinuousPollSessionConfig,
    ) -> None:
        if config.duration_seconds < 1:
            raise ValueError("duration_seconds must be >= 1")
        if not (50 <= config.poll_interval_ms <= 1000):
            raise ValueError("poll_interval_ms must be 50-1000")
        if config.response_timeout_ms >= config.poll_interval_ms:
            raise ValueError("response_timeout_ms must be < poll_interval_ms")
        if config.max_writes < 1:
            raise ValueError("max_writes must be >= 1")
        self.transport = transport
        self.config = config
        self.logical_address = config.address
        self.wire_address = encode_wire_address(config.address)
        self.session_id = new_session_id()
        self.stats = ContinuousSessionStats(
            requested_duration_s=config.duration_seconds
        )
        self._stop = asyncio.Event()
        self.command_queue_created = False
        self.authorization_objects_created = 0
        self.result: ContinuousBenchResult | None = None
        self._fault = False
        self._commit = software_commit()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> dict[str, object]:
        jsonl_path = self.config.evidence_jsonl
        md_path = self.config.evidence_md
        writer = EvidenceWriter(
            jsonl_path,
            self.session_id,
            target_type=self.config.target_type,
            simulator_validation=self.config.simulator_validation,
            commit=self._commit,
        )
        t_start = time.monotonic()
        try:
            if not self.transport.is_open:
                await self.transport.open()
            serial_notes = ""
            snapshot = getattr(self.transport, "serial_config_snapshot", None)
            if callable(snapshot):
                cfg = snapshot()
                serial_notes = " serial={" + format_serial_config(cfg) + "}"
                logger.info(
                    "continuous-poll-bench serial config: %s",
                    format_serial_config(cfg),
                )
            writer.emit_event(
                ContinuousBenchEvent.BENCH_STARTED,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.config.address,
                notes=(
                    f"CONTINUOUS_POLL_BENCH port={self.config.port} "
                    f"baud={self.config.baud} "
                    f"duration_s={self.config.duration_seconds} "
                    f"interval_ms={self.config.poll_interval_ms} "
                    f"timeout_ms={self.config.response_timeout_ms} "
                    f"max_writes={self.config.max_writes} "
                    f"targetType={self.config.target_type} "
                    f"simulatorValidation={self.config.simulator_validation}"
                    f"{serial_notes}"
                ),
            )
            await self._run_scheduler(writer, t_start)
        except (OSError, TransportNotOpenError) as exc:
            self._fault = True
            self.stats.stop_reason = StopReason.SERIAL_DISCONNECT
            writer.emit_event(
                ContinuousBenchEvent.SERIAL_DISCONNECT,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.config.address,
                notes=str(exc),
                stop_reason=StopReason.SERIAL_DISCONNECT.value,
            )
        finally:
            self.stats.actual_duration_s = time.monotonic() - t_start
            if self._stop.is_set() and self.stats.stop_reason is None:
                self._fault = True
                self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
            if self.stats.stop_reason is None:
                self.stats.stop_reason = StopReason.DURATION_EXPIRED
            if (
                self.command_queue_created
                or self.authorization_objects_created != 0
            ):
                self._fault = True
                self.stats.stop_reason = StopReason.SAFETY_FAULT
            self.result = suggest_result(self.stats, fault=self._fault)
            writer.emit_event(
                ContinuousBenchEvent.BENCH_STOPPED,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.config.address,
                stop_reason=(
                    self.stats.stop_reason.value if self.stats.stop_reason else None
                ),
                notes=(
                    f"result={self.result.value} "
                    f"polls={self.stats.polls_sent} "
                    f"valid={self.stats.valid_responses} "
                    f"timeouts={self.stats.timeouts} "
                    f"stop={self.stats.stop_reason.value if self.stats.stop_reason else 'n/a'}"
                ),
            )
            writer.close()
            try:
                if self.transport.is_open:
                    await self.transport.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("serial close failed: %s", exc)
            write_markdown_summary(
                md_path,
                session_id=self.session_id,
                port=self.config.port,
                canonical=self.config.canonical or self.config.port,
                baud=self.config.baud,
                address=self.config.address,
                poll_interval_ms=self.config.poll_interval_ms,
                response_timeout_ms=self.config.response_timeout_ms,
                stats=self.stats,
                result=self.result,
                target_type=self.config.target_type,
                simulator_validation=self.config.simulator_validation,
                command_queue_created=self.command_queue_created,
                authorization_objects_created=self.authorization_objects_created,
                write_count=getattr(
                    self.transport, "write_count", self.stats.polls_sent
                ),
                commit=self._commit,
            )

        return {
            "sessionId": self.session_id,
            "result": self.result.value if self.result else None,
            "pollsSent": self.stats.polls_sent,
            "protocolFramesReceived": self.stats.protocol_frames_received,
            # validResponses is a documented alias of protocolFramesReceived.
            "validResponses": self.stats.valid_responses,
            "controlResponses": self.stats.control_responses,
            "dataResponses": self.stats.data_responses,
            "controlOnlyCycles": self.stats.control_only_cycles,
            "timeouts": self.stats.timeouts,
            "crcErrors": self.stats.crc_errors,
            "protocolErrors": self.stats.protocol_errors,
            "unexpectedFrames": self.stats.unexpected_frames,
            "scheduleLagEvents": self.stats.schedule_lag_events,
            "commandQueueCreated": self.command_queue_created,
            "authorizationObjectsCreated": self.authorization_objects_created,
            "targetType": self.config.target_type,
            "simulatorValidation": self.config.simulator_validation,
            "stopReason": (
                self.stats.stop_reason.value if self.stats.stop_reason else None
            ),
            "requestedDurationS": self.stats.requested_duration_s,
            "actualDurationS": self.stats.actual_duration_s,
            "evidenceJsonl": str(jsonl_path),
            "evidenceMd": str(md_path),
            "writeCount": getattr(
                self.transport, "write_count", self.stats.polls_sent
            ),
            "softwareCommit": self._commit,
            "logicalAddress": self.logical_address,
            "wireAddress": self.wire_address,
        }

    async def _run_scheduler(
        self, writer: EvidenceWriter, t_start: float
    ) -> None:
        interval_s = self.config.poll_interval_ms / 1000.0
        end = t_start + self.config.duration_seconds
        next_deadline = t_start
        seq = 0

        while True:
            if self._stop.is_set():
                self._fault = True
                self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
                return

            now = time.monotonic()
            if now >= end:
                self.stats.stop_reason = StopReason.DURATION_EXPIRED
                return

            # Skip missed slots — never burst catch-up polls.
            while next_deadline + interval_s <= now:
                next_deadline += interval_s

            if next_deadline >= end:
                self.stats.stop_reason = StopReason.DURATION_EXPIRED
                return

            if now < next_deadline:
                wait = min(next_deadline - now, end - now)
                if wait > 0:
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=wait)
                    except TimeoutError:
                        pass
                    else:
                        self._fault = True
                        self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
                        return
                now = time.monotonic()
                if now >= end:
                    self.stats.stop_reason = StopReason.DURATION_EXPIRED
                    return

            while next_deadline + interval_s <= now:
                next_deadline += interval_s
            if next_deadline >= end:
                self.stats.stop_reason = StopReason.DURATION_EXPIRED
                return

            lag_ms = max(0.0, (now - next_deadline) * 1000.0)
            if lag_ms > 0.5:
                self.stats.schedule_lag_events += 1
                self.stats.schedule_lags_ms.append(lag_ms)
                writer.emit_event(
                    ContinuousBenchEvent.SCHEDULE_LAG,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.config.address,
                    poll_sequence=seq + 1,
                    schedule_lag_ms=lag_ms,
                    notes=f"schedule_lag_ms={lag_ms:.2f}",
                )

            write_count = getattr(self.transport, "write_count", self.stats.polls_sent)
            max_writes = self.config.max_writes
            if not self.config.simulator_validation:
                max_writes = min(max_writes, REAL_WAYNE_MAX_WRITES)
            if write_count >= max_writes or self.stats.polls_sent >= max_writes:
                self._fault = True
                self.stats.stop_reason = StopReason.MAX_WRITES
                writer.emit_event(
                    ContinuousBenchEvent.PROTOCOL_ERROR,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.config.address,
                    notes=f"max_writes={max_writes}",
                    stop_reason=StopReason.MAX_WRITES.value,
                )
                return

            seq += 1
            stop_early = await self._one_poll(writer, seq, lag_ms=lag_ms)
            next_deadline += interval_s
            if stop_early:
                return

    async def _one_poll(
        self,
        writer: EvidenceWriter,
        seq: int,
        *,
        lag_ms: float,
    ) -> bool:
        """One poll cycle via the shared status-poll receive path."""

        def _on_chunk(chunk: bytes) -> None:
            writer.emit_frame(
                direction="RX",
                raw=chunk,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                timeout_ms=self.config.response_timeout_ms,
                classification=None,
                crc_valid=None,
                event=ContinuousBenchEvent.SERIAL_READ_CHUNK,
                source="serial_read_chunk",
                notes="raw_serial_read_chunk",
            )

        def _on_observed(observed: ObservedFrame) -> None:
            self.stats.protocol_frames_received += 1
            classification = observed.classification
            raw = observed.frame.raw_frame
            if observed.is_short_control_70:
                self.stats.control_responses += 1
                writer.emit_frame(
                    direction="RX",
                    raw=raw,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.logical_address,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    poll_sequence=seq,
                    timeout_ms=self.config.response_timeout_ms,
                    classification=classification,
                    crc_valid=observed.frame.crc_valid,
                    event=ContinuousBenchEvent.CONTROL_RESPONSE,
                    latency_ms=observed.latency_ms,
                    notes=(
                        f"interim_short_control_70 latency_ms="
                        f"{observed.latency_ms:.2f}"
                    ),
                )
                writer.emit_event(
                    ContinuousBenchEvent.CONTROL_RESPONSE,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.logical_address,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    poll_sequence=seq,
                    classification=classification,
                    latency_ms=observed.latency_ms,
                    notes="interim_continue_response_window",
                )
                return
            # DATA or unexpected — final handling occurs after collector returns.

        def _on_transient(exc: BaseException, count: int) -> None:
            writer.emit_event(
                ContinuousBenchEvent.TRANSIENT_EMPTY_READ,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                notes=f"transient_empty_read count={count}: {exc}",
            )

        try:
            response = await send_status_poll_and_read_response(
                self.transport,
                self.logical_address,
                self.config.response_timeout_ms,
                read_size=self.config.read_size,
                stop_event=self._stop,
                on_chunk=_on_chunk,
                on_observed_frame=_on_observed,
                on_transient_empty=_on_transient,
            )
        except (OSError, TransportNotOpenError) as exc:
            self._fault = True
            self.stats.stop_reason = StopReason.SERIAL_DISCONNECT
            writer.emit_event(
                ContinuousBenchEvent.SERIAL_DISCONNECT,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                notes=str(exc),
                stop_reason=StopReason.SERIAL_DISCONNECT.value,
                poll_cycle_outcome=StatusPollOutcome.DISCONNECT.value,
            )
            return True

        self.stats.polls_sent += 1
        self.stats.last_tx_hex = response.poll_tx.hex(" ")
        writer.emit_frame(
            direction="TX",
            raw=response.poll_tx,
            monotonic_ns=response.mono_tx_ns,
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            timeout_ms=self.config.response_timeout_ms,
            classification="POLL",
            crc_valid=None,
            event=ContinuousBenchEvent.POLL_SENT,
            schedule_lag_ms=lag_ms if lag_ms > 0.5 else None,
            notes="verified_build_poll_only",
        )
        writer.emit_event(
            ContinuousBenchEvent.POLL_SENT,
            monotonic_ns=response.mono_tx_ns,
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            timeout_ms=self.config.response_timeout_ms,
            schedule_lag_ms=lag_ms if lag_ms > 0.5 else None,
        )

        if response.outcome is StatusPollOutcome.DISCONNECT:
            self._fault = True
            self.stats.stop_reason = StopReason.SERIAL_DISCONNECT
            writer.emit_event(
                ContinuousBenchEvent.SERIAL_DISCONNECT,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                notes=response.message or "serial_disconnect",
                stop_reason=StopReason.SERIAL_DISCONNECT.value,
                poll_cycle_outcome=StatusPollOutcome.DISCONNECT.value,
            )
            return True

        if response.outcome is StatusPollOutcome.STOPPED:
            self._fault = True
            self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
            return True

        if response.outcome is StatusPollOutcome.PROTOCOL_ERROR:
            event = response.terminal_event
            classification = (
                response.captured.classification.value
                if response.captured is not None
                else "PROTOCOL_ERROR"
            )
            if event is not None and event.kind.value == "REJECTED":
                self.stats.malformed_count += 1
                writer.emit_frame(
                    direction="RX",
                    raw=event.raw,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.config.address,
                    poll_sequence=seq,
                    timeout_ms=self.config.response_timeout_ms,
                    classification="REJECTED",
                    crc_valid=None,
                    event=ContinuousBenchEvent.PROTOCOL_ERROR,
                    notes=response.message,
                    poll_cycle_outcome=StatusPollOutcome.PROTOCOL_ERROR.value,
                )
                if self.stats.malformed_count > MALFORMED_THRESHOLD:
                    self._fault = True
                    self.stats.protocol_errors += 1
                    self.stats.stop_reason = StopReason.MALFORMED_THRESHOLD
                    return True
                return False

            # Unexpected complete frame or overflow.
            if response.frame is not None:
                if response.frame.address != self.wire_address:
                    self._fault = True
                    self.stats.protocol_errors += 1
                    self.stats.stop_reason = StopReason.ADDRESS_MISMATCH
                    writer.emit_event(
                        ContinuousBenchEvent.PROTOCOL_ERROR,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        notes=response.message or "address_mismatch",
                        classification="ADDRESS_MISMATCH",
                        stop_reason=StopReason.ADDRESS_MISMATCH.value,
                        poll_cycle_outcome=StatusPollOutcome.PROTOCOL_ERROR.value,
                    )
                    return True
                captured_class = (
                    response.captured.classification
                    if response.captured is not None
                    else None
                )
                if (
                    captured_class is not None
                    and captured_class not in _VALID_DATA_CLASSES
                    and captured_class not in _INTERIM_CONTROL_CLASSES
                ):
                    self._fault = True
                    self.stats.unexpected_frames += 1
                    self.stats.protocol_errors += 1
                    self.stats.stop_reason = StopReason.UNEXPECTED_FRAME
                    writer.emit_event(
                        ContinuousBenchEvent.PROTOCOL_ERROR,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        classification=classification,
                        notes=response.message or "unexpected_frame",
                        stop_reason=StopReason.UNEXPECTED_FRAME.value,
                        poll_cycle_outcome=StatusPollOutcome.PROTOCOL_ERROR.value,
                    )
                    return True

            self._fault = True
            self.stats.protocol_errors += 1
            self.stats.stop_reason = StopReason.PROTOCOL_ERROR
            writer.emit_event(
                ContinuousBenchEvent.PROTOCOL_ERROR,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.config.address,
                poll_sequence=seq,
                notes=response.message or "protocol_error",
                classification=classification,
                stop_reason=StopReason.PROTOCOL_ERROR.value,
                poll_cycle_outcome=StatusPollOutcome.PROTOCOL_ERROR.value,
            )
            return True

        if response.outcome is StatusPollOutcome.TIMEOUT_NO_RESPONSE:
            for event in response.partial_events:
                writer.emit_frame(
                    direction="RX",
                    raw=event.raw,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.logical_address,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    poll_sequence=seq,
                    timeout_ms=self.config.response_timeout_ms,
                    classification="PARTIAL_OR_NOISE",
                    crc_valid=None,
                    event=ContinuousBenchEvent.RESPONSE_TIMEOUT,
                    notes=event.message or "partial_frame_timeout",
                )
            if not response.partial_events and response.chunks:
                writer.emit_frame(
                    direction="RX",
                    raw=b"".join(response.chunks),
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.logical_address,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    poll_sequence=seq,
                    timeout_ms=self.config.response_timeout_ms,
                    classification="PARTIAL_OR_NOISE",
                    crc_valid=None,
                    event=ContinuousBenchEvent.RESPONSE_TIMEOUT,
                    notes="bytes_before_timeout",
                )
            self.stats.timeouts += 1
            writer.emit_event(
                ContinuousBenchEvent.RESPONSE_TIMEOUT,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                timeout_ms=self.config.response_timeout_ms,
                notes="no_frame_before_deadline",
                poll_cycle_outcome=StatusPollOutcome.TIMEOUT_NO_RESPONSE.value,
            )
            return False

        if response.outcome is StatusPollOutcome.CONTROL_ONLY:
            self.stats.control_only_cycles += 1
            if response.frame is not None:
                self.stats.last_rx_hex = response.frame.raw_frame.hex(" ")
            writer.emit_event(
                ContinuousBenchEvent.CONTROL_ONLY,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                timeout_ms=self.config.response_timeout_ms,
                classification=CapturedFrameClass.SHORT_CONTROL_70.value,
                latency_ms=response.latency_ms,
                notes=response.message or "control_only_no_data_frame",
                poll_cycle_outcome=StatusPollOutcome.CONTROL_ONLY.value,
            )
            return False

        # DATA_RESPONSE
        assert response.outcome is StatusPollOutcome.DATA_RESPONSE
        frame = response.frame
        assert frame is not None
        captured = response.captured
        latency_ms = response.latency_ms or 0.0
        self.stats.latencies_ms.append(latency_ms)
        self.stats.last_rx_hex = frame.raw_frame.hex(" ")
        classification = (
            captured.classification.value
            if captured is not None
            else frame.control_type.value
        )
        crc_valid = frame.crc_valid

        # Count DATA as a protocol frame if on_observed did not already
        # (on_observed increments for every complete frame including DATA).
        # on_observed already counted it via protocol_frames_received.

        if (
            (
                captured is not None
                and captured.classification is CapturedFrameClass.DATA_FRAME
            )
            or frame.control_type is ControlType.DATA
        ) and crc_valid is False:
            self._fault = True
            self.stats.crc_errors += 1
            self.stats.stop_reason = StopReason.CRC_ERROR
            writer.emit_frame(
                direction="RX",
                raw=frame.raw_frame,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                timeout_ms=self.config.response_timeout_ms,
                classification=classification,
                crc_valid=False,
                event=ContinuousBenchEvent.PROTOCOL_ERROR,
                latency_ms=latency_ms,
                notes="crc_invalid_stop",
                poll_cycle_outcome=StatusPollOutcome.PROTOCOL_ERROR.value,
            )
            return True

        if frame.address != self.wire_address:
            self._fault = True
            self.stats.protocol_errors += 1
            self.stats.stop_reason = StopReason.ADDRESS_MISMATCH
            writer.emit_event(
                ContinuousBenchEvent.PROTOCOL_ERROR,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                notes=(
                    f"address_mismatch got_wire=0x{frame.address:02X} "
                    f"expected_wire=0x{self.wire_address:02X}"
                ),
                classification="ADDRESS_MISMATCH",
                latency_ms=latency_ms,
                stop_reason=StopReason.ADDRESS_MISMATCH.value,
                poll_cycle_outcome=StatusPollOutcome.PROTOCOL_ERROR.value,
            )
            return True

        self.stats.data_responses += 1
        writer.emit_frame(
            direction="RX",
            raw=frame.raw_frame,
            monotonic_ns=time.monotonic_ns(),
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            timeout_ms=self.config.response_timeout_ms,
            classification=classification,
            crc_valid=crc_valid,
            event=ContinuousBenchEvent.DATA_RESPONSE,
            latency_ms=latency_ms,
            notes=f"latency_ms={latency_ms:.2f}",
            poll_cycle_outcome=StatusPollOutcome.DATA_RESPONSE.value,
        )
        writer.emit_event(
            ContinuousBenchEvent.DATA_RESPONSE,
            monotonic_ns=time.monotonic_ns(),
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            classification=classification,
            crc_valid=crc_valid,
            latency_ms=latency_ms,
            notes=f"latency_ms={latency_ms:.2f}",
            poll_cycle_outcome=StatusPollOutcome.DATA_RESPONSE.value,
        )
        writer.emit_event(
            ContinuousBenchEvent.RESPONSE_RECEIVED,
            monotonic_ns=time.monotonic_ns(),
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            classification=classification,
            crc_valid=crc_valid,
            latency_ms=latency_ms,
            notes=f"data_response latency_ms={latency_ms:.2f}",
            poll_cycle_outcome=StatusPollOutcome.DATA_RESPONSE.value,
        )
        return False
