"""Bounded continuous status-poll session (verified DART POLL frames only)."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from intelipump_fdc.bench_poll.guards import TARGET_OWNED_LAB_WAYNE
from intelipump_fdc.bench_poll.poll_io import (
    ChunkOwnership,
    ObservedFrame,
    StatusPollOutcome,
    send_status_poll_and_read_response,
    wait_for_quiet_gap,
)
from intelipump_fdc.bench_poll.serial_reader import SerialChunk
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
    software_commit,
)
from intelipump_fdc.controller.price_safety import (
    ActiveFrameKind,
    RealWayneActiveCommandRefusedError,
)
from intelipump_fdc.protocol.cd1 import build_cd1_candidate_frame, build_cd1_command
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.captured_classify import CapturedFrameClass
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError
from intelipump_fdc.real_wayne_price.session_helpers import (
    next_sequence_nibble,
    wait_for_ack_frame,
)

logger = logging.getLogger(__name__)

_VALID_DATA_CLASSES = frozenset({CapturedFrameClass.DATA_FRAME})
_INTERIM_CONTROL_CLASSES = frozenset({CapturedFrameClass.SHORT_CONTROL_70})


@dataclass(frozen=True, slots=True)
class ContinuousPollSessionConfig:
    port: str
    addresses: tuple[int, ...]
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
    return_status_cadence: bool = False
    return_status_every_n_polls: int = 2
    return_status_sequence: int = 0
    ack_timeout_ms: int = 200
    until_ctrl_c: bool = False

    @property
    def address(self) -> int:
        return self.addresses[0]


class ContinuousPollSession:
    """Sends status polls on a monotonic schedule; optional gated RETURN_STATUS.

    One or two logical addresses on a single adapter (round-robin).
    Never builds RESET/AUTHORIZE. No controller command queue.
    """

    def __init__(
        self,
        transport: BenchByteTransport,
        config: ContinuousPollSessionConfig,
    ) -> None:
        if not config.addresses:
            raise ValueError("addresses must be non-empty")
        if len(config.addresses) > 2:
            raise ValueError("at most two addresses")
        if len(set(config.addresses)) != len(config.addresses):
            raise ValueError("duplicate addresses")
        for addr in config.addresses:
            if addr not in {1, 2}:
                raise ValueError(f"address must be 1 or 2, got {addr}")
        if config.until_ctrl_c:
            pass
        elif config.duration_seconds < 1:
            raise ValueError("duration_seconds must be >= 1")
        if not (50 <= config.poll_interval_ms <= 1000):
            raise ValueError("poll_interval_ms must be 50-1000")
        if config.response_timeout_ms >= config.poll_interval_ms:
            raise ValueError("response_timeout_ms must be < poll_interval_ms")
        if config.max_writes < 0:
            raise ValueError("max_writes must be >= 0 (0=unlimited)")
        if not config.until_ctrl_c and config.max_writes < 1:
            raise ValueError("max_writes must be >= 1 unless until_ctrl_c")
        if config.return_status_cadence:
            if not (1 <= config.return_status_every_n_polls <= 20):
                raise ValueError("return_status_every_n_polls must be 1-20")
            if not (0 <= config.return_status_sequence <= 15):
                raise ValueError("return_status_sequence must be 0-15")
        self.transport = transport
        self.config = config
        self.addresses = config.addresses
        self.logical_address = config.addresses[0]
        self.wire_address = encode_wire_address(self.logical_address)
        self.session_id = new_session_id()
        self.stats = ContinuousSessionStats(
            requested_duration_s=(
                0.0 if config.until_ctrl_c else config.duration_seconds
            )
        )
        self._stop = asyncio.Event()
        self.command_queue_created = False
        self.authorization_objects_created = 0
        self.result: ContinuousBenchResult | None = None
        self._fault = False
        self._commit = software_commit()
        start_seq = int(config.return_status_sequence) & 0x0F
        self._rs_sequence: dict[int, int] = {
            addr: start_seq for addr in config.addresses
        }
        self._rr_index = 0

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
                    f"return_status_cadence={self.config.return_status_cadence} "
                    f"return_status_every_n={self.config.return_status_every_n_polls} "
                    f"until_ctrl_c={self.config.until_ctrl_c} "
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
                self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
                # Intentional stop for until-ctrl-c; fault only for bounded early stop.
                if not self.config.until_ctrl_c:
                    self._fault = True
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
                address=",".join(str(a) for a in self.addresses),
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
                records=writer.record_list,
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
            "timeoutNoResponse": self.stats.timeout_no_response,
            "lateChunks": self.stats.late_chunks,
            "staleChunks": self.stats.stale_chunks,
            "unownedFrames": self.stats.unowned_frames,
            "crcErrors": self.stats.crc_errors,
            "protocolErrors": self.stats.protocol_errors,
            "unexpectedFrames": self.stats.unexpected_frames,
            "scheduleLagEvents": self.stats.schedule_lag_events,
            "skippedSlotsTotal": self.stats.skipped_slots_total,
            "returnStatusSent": self.stats.return_status_sent,
            "returnStatusAckMatch": self.stats.return_status_ack_match,
            "returnStatusAckTimeout": self.stats.return_status_ack_timeout,
            "returnStatusCadence": self.config.return_status_cadence,
            "untilCtrlC": self.config.until_ctrl_c,
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
            "logicalAddresses": list(self.addresses),
            "wireAddress": self.wire_address,
            "wireAddresses": [encode_wire_address(a) for a in self.addresses],
        }

    async def _run_scheduler(
        self, writer: EvidenceWriter, t_start: float
    ) -> None:
        """Monotonic schedule: never TX sooner than previous_tx + poll_interval.

        Ideal cadence slots are skipped when late — never compressed catch-up
        bursts. Spacing is always measured from the previous actual TX time.
        """
        interval_s = self.config.poll_interval_ms / 1000.0
        end: float | None = (
            None
            if self.config.until_ctrl_c
            else t_start + self.config.duration_seconds
        )
        next_ideal = t_start
        previous_tx: float | None = None
        seq = 0

        while True:
            if self._stop.is_set():
                self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
                if not self.config.until_ctrl_c:
                    self._fault = True
                return

            now = time.monotonic()
            if end is not None and now >= end:
                self.stats.stop_reason = StopReason.DURATION_EXPIRED
                return

            min_next = (
                previous_tx + interval_s if previous_tx is not None else t_start
            )

            # Skip missed ideal slots; never schedule a catch-up burst.
            # Advance only by whole intervals past `now` / before `min_next`.
            # Do NOT use `next_ideal < min_next` — a tiny TX overshoot would
            # skip an extra full interval and double the effective cadence.
            skipped = 0
            while next_ideal + interval_s <= now:
                next_ideal += interval_s
                skipped += 1
            while next_ideal + interval_s <= min_next:
                next_ideal += interval_s
                skipped += 1

            next_tx = max(next_ideal, min_next)
            if end is not None and next_tx >= end:
                self.stats.stop_reason = StopReason.DURATION_EXPIRED
                return

            if now < next_tx:
                wait = next_tx - now
                if end is not None:
                    wait = min(wait, end - now)
                if wait > 0:
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=wait)
                    except TimeoutError:
                        pass
                    else:
                        self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
                        if not self.config.until_ctrl_c:
                            self._fault = True
                        return
                # Recompute with fresh monotonic time after the wait.
                continue

            lag_ms = max(0.0, (now - next_ideal) * 1000.0)
            if lag_ms > 0.5 or skipped > 0:
                self.stats.schedule_lag_events += 1
                self.stats.schedule_lags_ms.append(lag_ms)
                self.stats.skipped_slots_total += skipped
                writer.emit_event(
                    ContinuousBenchEvent.SCHEDULE_LAG,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.config.address,
                    poll_sequence=seq + 1,
                    schedule_lag_ms=lag_ms,
                    notes=(
                        f"schedule_lag_ms={lag_ms:.2f}; "
                        f"skipped_slots={skipped}"
                    ),
                )

            write_count = getattr(self.transport, "write_count", self.stats.polls_sent)
            max_writes = self.config.max_writes
            if max_writes > 0 and (
                write_count >= max_writes or self.stats.polls_sent >= max_writes
            ):
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
            logical = self.addresses[self._rr_index % len(self.addresses)]
            self._rr_index += 1
            self.logical_address = logical
            self.wire_address = encode_wire_address(logical)
            stop_early, tx_monotonic = await self._one_poll(
                writer, seq, lag_ms=lag_ms, logical_address=logical
            )
            previous_tx = (
                tx_monotonic if tx_monotonic is not None else time.monotonic()
            )
            next_ideal += interval_s
            if stop_early:
                return

            if (
                self.config.return_status_cadence
                and seq % self.config.return_status_every_n_polls == 0
            ):
                stop_rs, rs_tx = await self._one_return_status(
                    writer, seq, logical_address=logical
                )
                if rs_tx is not None:
                    previous_tx = rs_tx
                if stop_rs:
                    return

    async def _one_return_status(
        self,
        writer: EvidenceWriter,
        poll_seq: int,
        *,
        logical_address: int,
    ) -> tuple[bool, float | None]:
        """Transmit one gated CD1 RETURN_STATUS; never RESET/AUTHORIZE."""
        self.logical_address = logical_address
        self.wire_address = encode_wire_address(logical_address)
        authorize = getattr(self.transport, "authorize_single_active_write", None)
        if not callable(authorize):
            self._fault = True
            self.stats.stop_reason = StopReason.SAFETY_FAULT
            writer.emit_event(
                ContinuousBenchEvent.SAFETY_REFUSED,
                monotonic_ns=time.monotonic_ns(),
                pump_address=logical_address,
                notes="transport_missing_active_authorization",
                stop_reason=StopReason.SAFETY_FAULT.value,
            )
            return True, None

        write_count = getattr(self.transport, "write_count", self.stats.polls_sent)
        if self.config.max_writes > 0 and write_count >= self.config.max_writes:
            self._fault = True
            self.stats.stop_reason = StopReason.MAX_WRITES
            return True, None

        cd1 = build_cd1_command(PumpControlCommand.RETURN_STATUS)
        if cd1.command is not PumpControlCommand.RETURN_STATUS:
            self._fault = True
            self.stats.stop_reason = StopReason.SAFETY_FAULT
            return True, None
        rs_seq = self._rs_sequence[logical_address]
        frame, _crc, expected_ack = build_cd1_candidate_frame(
            logical_address=logical_address,
            sequence=rs_seq,
            cd1=cd1,
        )
        try:
            authorize(frame, kind=ActiveFrameKind.CD1_RETURN_STATUS)
            tx_mono = time.monotonic()
            mono_ns = time.monotonic_ns()
            await self.transport.write(frame)
            flush = getattr(self.transport, "flush", None)
            if callable(flush):
                await flush()
        except (RealWayneActiveCommandRefusedError, OSError, TransportNotOpenError) as exc:
            self._fault = True
            self.stats.stop_reason = (
                StopReason.SERIAL_DISCONNECT
                if isinstance(exc, (OSError, TransportNotOpenError))
                else StopReason.SAFETY_FAULT
            )
            writer.emit_event(
                ContinuousBenchEvent.SAFETY_REFUSED,
                monotonic_ns=time.monotonic_ns(),
                pump_address=logical_address,
                notes=str(exc),
                stop_reason=self.stats.stop_reason.value,
            )
            return True, None

        self.stats.return_status_sent += 1
        self.stats.last_tx_hex = frame.hex(" ")
        writer.emit_frame(
            direction="TX",
            raw=frame,
            monotonic_ns=mono_ns,
            pump_address=logical_address,
            logical_address=logical_address,
            wire_address=self.wire_address,
            poll_sequence=poll_seq,
            timeout_ms=self.config.ack_timeout_ms,
            classification=None,
            crc_valid=None,
            event=ContinuousBenchEvent.RETURN_STATUS_SENT,
            notes=(
                f"CD1_RETURN_STATUS address={logical_address} "
                f"sequence={rs_seq} (no RESET/AUTHORIZE)"
            ),
        )

        matched, ack_outcome, observed = await wait_for_ack_frame(
            self.transport,
            expected_ack=expected_ack,
            timeout_ms=self.config.ack_timeout_ms,
        )
        if matched:
            self.stats.return_status_ack_match += 1
        else:
            self.stats.return_status_ack_timeout += 1
        writer.emit_event(
            ContinuousBenchEvent.RETURN_STATUS_ACK,
            monotonic_ns=time.monotonic_ns(),
            pump_address=logical_address,
            logical_address=logical_address,
            wire_address=self.wire_address,
            poll_sequence=poll_seq,
            notes=f"ack={ack_outcome}; observed={observed[:3]}",
        )
        self._rs_sequence[logical_address] = next_sequence_nibble(rs_seq)
        # ACK timeout is non-fatal (mirrors office path continuing after RS).
        return False, tx_mono

    async def _wait_quiet_gap(
        self,
        writer: EvidenceWriter,
        seq: int,
        *,
        t0: float,
    ) -> None:
        """After timeout, wait for a quiet capture gap before the next TX."""
        writer.emit_event(
            ContinuousBenchEvent.QUIET_GAP_WAIT,
            monotonic_ns=time.monotonic_ns(),
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            notes="waiting_quiet_gap_after_timeout",
        )

        def _on_chunk(chunk: SerialChunk, ownership: ChunkOwnership) -> None:
            writer.emit_frame(
                direction="RX",
                raw=chunk.raw,
                monotonic_ns=chunk.monotonic_ns,
                timestamp_utc=chunk.timestamp_utc,
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=None,
                timeout_ms=self.config.response_timeout_ms,
                classification=None,
                crc_valid=None,
                event=ContinuousBenchEvent.LATE_CHUNK,
                source="quiet_gap",
                notes=f"quiet_gap_{ownership.value}",
            )

        def _on_observed(observed: ObservedFrame) -> None:
            writer.emit_frame(
                direction="RX",
                raw=observed.frame.raw_frame,
                monotonic_ns=observed.capture_monotonic_ns or time.monotonic_ns(),
                timestamp_utc=observed.capture_timestamp_utc,
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=None,
                timeout_ms=self.config.response_timeout_ms,
                classification=observed.classification,
                crc_valid=observed.frame.crc_valid,
                event=ContinuousBenchEvent.UNOWNED_FRAME,
                latency_ms=observed.latency_ms,
                notes="quiet_gap_unowned_frame",
            )

        late, unowned = await wait_for_quiet_gap(
            self.transport,
            on_chunk=_on_chunk,
            on_observed_frame=_on_observed,
            t0_reference=t0,
        )
        self.stats.late_chunks += late
        self.stats.unowned_frames += unowned

    async def _one_poll(
        self,
        writer: EvidenceWriter,
        seq: int,
        *,
        lag_ms: float,
        logical_address: int,
    ) -> tuple[bool, float | None]:
        """Return ``(stop_early, tx_monotonic)`` for schedule spacing."""
        self.logical_address = logical_address
        self.wire_address = encode_wire_address(logical_address)

        def _on_chunk(chunk: SerialChunk, ownership: ChunkOwnership) -> None:
            if ownership is ChunkOwnership.STALE:
                event = ContinuousBenchEvent.STALE_CHUNK
                notes = "stale_unowned_before_tx"
            elif ownership is ChunkOwnership.LATE:
                event = ContinuousBenchEvent.LATE_CHUNK
                notes = "late_unowned_after_deadline"
            else:
                event = ContinuousBenchEvent.SERIAL_READ_CHUNK
                notes = "owned_serial_read_chunk"
            # pollSequence is only set for owned chunks; stale/late stay unowned.
            writer.emit_frame(
                direction="RX",
                raw=chunk.raw,
                monotonic_ns=chunk.monotonic_ns,
                timestamp_utc=chunk.timestamp_utc,
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=(
                    seq if ownership is ChunkOwnership.OWNED else None
                ),
                timeout_ms=self.config.response_timeout_ms,
                classification=None,
                crc_valid=None,
                event=event,
                source="permanent_serial_reader",
                notes=notes,
            )

        def _on_observed(observed: ObservedFrame) -> None:
            classification = observed.classification
            raw = observed.frame.raw_frame
            mono = observed.capture_monotonic_ns or time.monotonic_ns()
            utc = observed.capture_timestamp_utc
            if observed.ownership is not ChunkOwnership.OWNED:
                writer.emit_frame(
                    direction="RX",
                    raw=raw,
                    monotonic_ns=mono,
                    timestamp_utc=utc,
                    pump_address=self.logical_address,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    poll_sequence=None,
                    timeout_ms=self.config.response_timeout_ms,
                    classification=classification,
                    crc_valid=observed.frame.crc_valid,
                    event=ContinuousBenchEvent.UNOWNED_FRAME,
                    latency_ms=observed.latency_ms,
                    notes=f"unowned_{observed.ownership.value}",
                )
                return
            self.stats.protocol_frames_received += 1
            if observed.is_short_control_70:
                self.stats.control_responses += 1
                writer.emit_frame(
                    direction="RX",
                    raw=raw,
                    monotonic_ns=mono,
                    timestamp_utc=utc,
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
                    monotonic_ns=mono,
                    timestamp_utc=utc,
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

        try:
            response = await send_status_poll_and_read_response(
                self.transport,
                self.logical_address,
                self.config.response_timeout_ms,
                read_size=self.config.read_size,
                stop_event=self._stop,
                on_chunk=_on_chunk,
                on_observed_frame=_on_observed,
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
            return True, None

        tx_monotonic = response.t0
        self.stats.polls_sent += 1
        self.stats.last_tx_hex = response.poll_tx.hex(" ")
        self.stats.stale_chunks += response.stale_chunks
        self.stats.late_chunks += response.late_chunks
        self.stats.unowned_frames += response.unowned_frames
        writer.emit_frame(
            direction="TX",
            raw=response.poll_tx,
            monotonic_ns=response.mono_tx_ns,
            timestamp_utc=response.timestamp_utc_tx,
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
            timestamp_utc=response.timestamp_utc_tx,
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
            return True, tx_monotonic
        if response.outcome is StatusPollOutcome.STOPPED:
            self._fault = True
            self.stats.stop_reason = StopReason.OPERATOR_INTERRUPT
            return True, tx_monotonic
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
                    return True, tx_monotonic
                return False, tx_monotonic
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
                    return True, tx_monotonic
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
                    return True, tx_monotonic
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
            return True, tx_monotonic
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
            self.stats.timeout_no_response += 1
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
            await self._wait_quiet_gap(writer, seq, t0=response.t0)
            return False, tx_monotonic
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
            return False, tx_monotonic
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
            return True, tx_monotonic
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
            return True, tx_monotonic
        self.stats.data_responses += 1
        data_mono = (
            response.data_frame.capture_monotonic_ns
            if response.data_frame is not None
            and response.data_frame.capture_monotonic_ns is not None
            else time.monotonic_ns()
        )
        data_utc = (
            response.data_frame.capture_timestamp_utc
            if response.data_frame is not None
            else None
        )
        writer.emit_frame(
            direction="RX",
            raw=frame.raw_frame,
            monotonic_ns=data_mono,
            timestamp_utc=data_utc,
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
            monotonic_ns=data_mono,
            timestamp_utc=data_utc,
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
            monotonic_ns=data_mono,
            timestamp_utc=data_utc,
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
        return False, tx_monotonic