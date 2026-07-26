"""Bounded continuous status-poll session (verified DART POLL frames only)."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from intelipump_fdc.bench_poll.guards import TARGET_OWNED_LAB_WAYNE
from intelipump_fdc.bench_poll.transport import BenchByteTransport
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
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError

logger = logging.getLogger(__name__)

_VALID_RESPONSE_CLASSES = frozenset(
    {
        CapturedFrameClass.SHORT_CONTROL_70,
        CapturedFrameClass.DATA_FRAME,
    }
)

# Bounded quiet window for optional stale-input drain (startup / post-timeout only).
_DRAIN_QUIET_MS = 25
_TRANSIENT_EMPTY_GRACE_MS = 50
_MAX_CONSECUTIVE_TRANSIENT_EMPTY = 3
_TRANSIENT_EMPTY_MARKER = "device reports readiness to read but returned no data"


def _is_transient_empty_read(exc: BaseException) -> bool:
    return _TRANSIENT_EMPTY_MARKER in str(exc).lower()


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
    """Sends only ``build_poll`` on a monotonic schedule; no command queue/auth."""

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
        self._assembler = LegacyIgemStreamAssembler()
        self.command_queue_created = False
        self.authorization_objects_created = 0
        self.result: ContinuousBenchResult | None = None
        self._fault = False
        self._commit = software_commit()
        self._consecutive_transient_empty = 0
        self._drain_before_next_poll = True  # session startup quiet drain

    def request_stop(self) -> None:
        self._stop.set()

    def _device_still_usable(self) -> bool:
        if not self.transport.is_open:
            return False
        checker = getattr(self.transport, "device_path_exists", None)
        if callable(checker):
            return bool(checker())
        device = getattr(self.transport, "device", None) or self.config.port
        if str(device).startswith(("/tmp", "memory", "pty:")):
            return True
        return Path(str(device)).exists()

    async def _quiet_drain_stale(
        self,
        writer: EvidenceWriter,
        *,
        poll_sequence: int,
        reason: str,
    ) -> None:
        """Drain only at startup or after timeout — never immediately before every TX.

        Uses ``drain_available`` (in_waiting only) so a blocking read cannot
        consume the leading byte of a soon-to-arrive valid response.
        """
        drain_available = getattr(self.transport, "drain_available", None)
        if not callable(drain_available):
            return

        deadline = time.monotonic() + (_DRAIN_QUIET_MS / 1000.0)
        drained = bytearray()
        while time.monotonic() < deadline and not self._stop.is_set():
            try:
                chunk = await drain_available()
            except (OSError, TransportNotOpenError) as exc:
                if _is_transient_empty_read(exc) and self._device_still_usable():
                    writer.emit_event(
                        ContinuousBenchEvent.TRANSIENT_EMPTY_READ,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=poll_sequence,
                        notes=str(exc),
                    )
                    await asyncio.sleep(0.005)
                    continue
                raise
            if chunk:
                drained.extend(chunk)
            else:
                await asyncio.sleep(0.002)

        if not drained:
            return
        writer.emit_frame(
            direction="RX",
            raw=bytes(drained),
            monotonic_ns=time.monotonic_ns(),
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=poll_sequence,
            timeout_ms=self.config.response_timeout_ms,
            classification="PARTIAL_OR_NOISE",
            crc_valid=None,
            event=ContinuousBenchEvent.STALE_INPUT_DRAINED,
            notes=reason,
            source="stale_input_drained",
        )
        logger.info(
            "drained %d stale serial byte(s) (%s) poll=%s",
            len(drained),
            reason,
            poll_sequence,
        )

    def _ensure_assembler_clean(
        self, writer: EvidenceWriter, *, poll_sequence: int
    ) -> None:
        """Clear leftover assembler state without serial drain (poll-cycle ownership)."""
        if self._assembler.pending_size <= 0:
            return
        stale = self._assembler.reset()
        if not stale:
            return
        writer.emit_frame(
            direction="RX",
            raw=stale,
            monotonic_ns=time.monotonic_ns(),
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=poll_sequence,
            timeout_ms=self.config.response_timeout_ms,
            classification="PARTIAL_OR_NOISE",
            crc_valid=None,
            event=ContinuousBenchEvent.RESPONSE_TIMEOUT,
            notes="stale_assembler_bytes_cleared_before_poll",
        )

    def _record_timeout_partial(
        self,
        writer: EvidenceWriter,
        *,
        seq: int,
        rx_bytes: bytes,
    ) -> None:
        """Record timed-out partial bytes and clear the response assembler."""
        partial = self._assembler.pending_raw or rx_bytes
        emitted = False
        for event in self._assembler.expire_partial():
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
            emitted = True

        self._assembler.reset()

        if not emitted and partial:
            writer.emit_frame(
                direction="RX",
                raw=partial,
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

    async def _read_response_chunk(
        self,
        writer: EvidenceWriter,
        *,
        seq: int,
        remaining_s: float,
    ) -> bytes | None:
        """Read one chunk; ``None`` means fail-closed serial disconnect."""
        if remaining_s <= 0:
            return b""
        grace_deadline = time.monotonic() + min(
            remaining_s, _TRANSIENT_EMPTY_GRACE_MS / 1000.0
        )
        while True:
            try:
                chunk = await asyncio.wait_for(
                    self.transport.read(self.config.read_size),
                    timeout=min(remaining_s, 0.05),
                )
                self._consecutive_transient_empty = 0
                return chunk
            except TimeoutError:
                return b""
            except TransportNotOpenError as exc:
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
                )
                return None
            except OSError as exc:
                if _is_transient_empty_read(exc):
                    if not self._device_still_usable():
                        self._fault = True
                        self.stats.stop_reason = StopReason.SERIAL_DISCONNECT
                        writer.emit_event(
                            ContinuousBenchEvent.SERIAL_DISCONNECT,
                            monotonic_ns=time.monotonic_ns(),
                            pump_address=self.logical_address,
                            logical_address=self.logical_address,
                            wire_address=self.wire_address,
                            poll_sequence=seq,
                            notes=f"device_missing_after_transient_empty: {exc}",
                            stop_reason=StopReason.SERIAL_DISCONNECT.value,
                        )
                        return None
                    self._consecutive_transient_empty += 1
                    writer.emit_event(
                        ContinuousBenchEvent.TRANSIENT_EMPTY_READ,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        notes=(
                            f"transient_empty_read count="
                            f"{self._consecutive_transient_empty}: {exc}"
                        ),
                    )
                    if (
                        self._consecutive_transient_empty
                        >= _MAX_CONSECUTIVE_TRANSIENT_EMPTY
                    ):
                        self._fault = True
                        self.stats.stop_reason = StopReason.SERIAL_DISCONNECT
                        writer.emit_event(
                            ContinuousBenchEvent.SERIAL_DISCONNECT,
                            monotonic_ns=time.monotonic_ns(),
                            pump_address=self.logical_address,
                            logical_address=self.logical_address,
                            wire_address=self.wire_address,
                            poll_sequence=seq,
                            notes=(
                                "repeated_transient_empty_read: "
                                f"{self._consecutive_transient_empty}"
                            ),
                            stop_reason=StopReason.SERIAL_DISCONNECT.value,
                        )
                        return None
                    if time.monotonic() >= grace_deadline:
                        # Grace expired: treat as empty read, keep polling.
                        return b""
                    await asyncio.sleep(0.005)
                    remaining_s = max(0.0, grace_deadline - time.monotonic())
                    continue
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
                )
                return None

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
            "validResponses": self.stats.valid_responses,
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

            # Re-skip if wait overshot into later slots.
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
        """Return True to stop the bench early."""
        # Drain only at session startup or after a timed-out poll (never every TX).
        if self._drain_before_next_poll:
            self._drain_before_next_poll = False
            await self._quiet_drain_stale(
                writer,
                poll_sequence=seq,
                reason=(
                    "session_startup_drain"
                    if seq == 1
                    else "post_timeout_quiet_drain"
                ),
            )
        self._ensure_assembler_clean(writer, poll_sequence=seq)

        poll = build_poll(self.config.address)
        t0 = time.monotonic()
        mono_tx = time.monotonic_ns()
        try:
            await self.transport.write(poll)
        except (OSError, TransportNotOpenError) as exc:
            self._fault = True
            self.stats.stop_reason = StopReason.SERIAL_DISCONNECT
            writer.emit_event(
                ContinuousBenchEvent.SERIAL_DISCONNECT,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.config.address,
                poll_sequence=seq,
                notes=str(exc),
                stop_reason=StopReason.SERIAL_DISCONNECT.value,
            )
            return True

        self.stats.polls_sent += 1
        self.stats.last_tx_hex = poll.hex(" ")
        writer.emit_frame(
            direction="TX",
            raw=poll,
            monotonic_ns=mono_tx,
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
            monotonic_ns=mono_tx,
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            timeout_ms=self.config.response_timeout_ms,
            schedule_lag_ms=lag_ms if lag_ms > 0.5 else None,
        )

        deadline = t0 + (self.config.response_timeout_ms / 1000.0)
        rx_bytes = bytearray()
        while time.monotonic() < deadline and not self._stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            chunk = await self._read_response_chunk(
                writer, seq=seq, remaining_s=remaining
            )
            if chunk is None:
                return True
            if not chunk:
                await asyncio.sleep(0.001)
                continue
            rx_bytes.extend(chunk)
            # Raw chunk evidence before assembler/parser processing.
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
            for event in self._assembler.feed(chunk):
                if event.kind is AssemblerEventKind.NOISE:
                    self.stats.malformed_count += 1
                    if self.stats.malformed_count > MALFORMED_THRESHOLD:
                        self._fault = True
                        self.stats.protocol_errors += 1
                        self.stats.stop_reason = StopReason.MALFORMED_THRESHOLD
                        writer.emit_event(
                            ContinuousBenchEvent.PROTOCOL_ERROR,
                            monotonic_ns=time.monotonic_ns(),
                            pump_address=self.config.address,
                            poll_sequence=seq,
                            notes="malformed_threshold_exceeded",
                            classification="MALFORMED",
                            stop_reason=StopReason.MALFORMED_THRESHOLD.value,
                        )
                        return True
                    continue
                if event.kind is AssemblerEventKind.OVERFLOW:
                    self._fault = True
                    self.stats.protocol_errors += 1
                    self.stats.stop_reason = StopReason.PROTOCOL_ERROR
                    writer.emit_event(
                        ContinuousBenchEvent.PROTOCOL_ERROR,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.config.address,
                        poll_sequence=seq,
                        notes=event.message or "overflow",
                        classification="OVERFLOW",
                        stop_reason=StopReason.PROTOCOL_ERROR.value,
                    )
                    return True
                if event.kind is AssemblerEventKind.REJECTED:
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
                        notes=event.message,
                    )
                    if self.stats.malformed_count > MALFORMED_THRESHOLD:
                        self._fault = True
                        self.stats.protocol_errors += 1
                        self.stats.stop_reason = StopReason.MALFORMED_THRESHOLD
                        writer.emit_event(
                            ContinuousBenchEvent.PROTOCOL_ERROR,
                            monotonic_ns=time.monotonic_ns(),
                            pump_address=self.config.address,
                            poll_sequence=seq,
                            notes="malformed_threshold_exceeded",
                            classification="MALFORMED",
                            stop_reason=StopReason.MALFORMED_THRESHOLD.value,
                        )
                        return True
                    continue


                frame = event.frame
                assert frame is not None
                captured = event.captured
                latency_ms = (time.monotonic() - t0) * 1000.0
                self.stats.latencies_ms.append(latency_ms)
                self.stats.last_rx_hex = frame.raw_frame.hex(" ")
                classification = (
                    captured.classification.value
                    if captured is not None
                    else frame.control_type.value
                )
                crc_valid = frame.crc_valid

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
                    )
                    return True

                captured_class = (
                    captured.classification if captured is not None else None
                )
                if captured_class is not None and captured_class not in _VALID_RESPONSE_CLASSES:
                    self._fault = True
                    self.stats.unexpected_frames += 1
                    self.stats.protocol_errors += 1
                    self.stats.stop_reason = StopReason.UNEXPECTED_FRAME
                    note = "unsupported_or_unexpected_frame"
                    if (
                        captured is not None
                        and captured_class is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
                    ):
                        note = (
                            f"{note}; sequenceNibble={captured.sequence_nibble}; "
                            "possibleAcknowledgement=true; not nozzle_lift"
                        )
                    writer.emit_event(
                        ContinuousBenchEvent.PROTOCOL_ERROR,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        classification=classification,
                        notes=note,
                        stop_reason=StopReason.UNEXPECTED_FRAME.value,
                    )
                    return True

                self.stats.valid_responses += 1
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
                    event=ContinuousBenchEvent.RESPONSE_RECEIVED,
                    latency_ms=latency_ms,
                    notes=f"latency_ms={latency_ms:.2f}",
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
                    notes=f"latency_ms={latency_ms:.2f}",
                )
                return False

        if self._stop.is_set():
            self._fault = True
            self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
            self._assembler.reset()
            return True

        # Timeout: evidence the incomplete response, then clear assembler so
        # the next poll cannot concatenate a stale prefix with a new frame.
        self._record_timeout_partial(writer, seq=seq, rx_bytes=bytes(rx_bytes))
        # Bounded quiet drain only after timeout (assembler already reset).
        self._drain_before_next_poll = True

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
        )
        return False
