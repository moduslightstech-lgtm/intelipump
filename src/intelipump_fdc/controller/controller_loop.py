"""Async polling scheduler and controller loop."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from intelipump_fdc.controller.comm_health import (
    HealthThresholds,
    HealthTransitionLog,
    SerialHealthMonitor,
    pump_health_counts,
    pump_health_summary,
)
from intelipump_fdc.controller.exchange_result import (
    ExchangeResult,
    ExchangeResultStatus,
)
from intelipump_fdc.controller.feature_flags import WayneFeatureFlags
from intelipump_fdc.controller.outbound import OutboundQueue
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.rx_demux import AddressFrameDemux
from intelipump_fdc.controller.safety import (
    ControllerSafetyContext,
    evaluate_polling_allowed,
)
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.controller.session_models import (
    IdempotencyClass,
    ObservedStatus,
    OutboundDataItem,
)
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.core.liveness import LivenessSnapshot, LivenessTracker
from intelipump_fdc.core.systemd_notify import Notifier, NullNotifier
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_ack, build_data_frame
from intelipump_fdc.protocol.dart.transport.base import ByteTransport
from intelipump_fdc.simulator.config import next_sequence
from intelipump_fdc.simulator.encoding import encode_cd1_command


@dataclass
class ControllerTotals:
    poll_count: int = 0
    eot_count: int = 0
    data_count: int = 0
    crc_errors: int = 0
    timeouts: int = 0
    nak_count: int = 0
    duplicate_count: int = 0
    stale_frame_count: int = 0
    malformed_count: int = 0


@dataclass
class ControllerRuntime:
    transport: ByteTransport
    safety: ControllerSafetyContext
    config: PollSchedulerConfig = field(default_factory=PollSchedulerConfig)
    events: EventBus = field(default_factory=EventBus)
    outbound: OutboundQueue = field(default_factory=OutboundQueue)
    log_frames: bool = False
    log_dart_timing: bool = False
    feature_flags: WayneFeatureFlags = field(default_factory=WayneFeatureFlags)
    liveness: LivenessTracker = field(default_factory=LivenessTracker)
    notifier: Notifier = field(default_factory=NullNotifier)
    status_interval_s: float = 15.0
    serial_health: SerialHealthMonitor | None = None
    health_transitions: HealthTransitionLog = field(default_factory=HealthTransitionLog)


class ControllerLoop:
    """Round-robin DART polling over an abstract transport."""

    def __init__(self, runtime: ControllerRuntime) -> None:
        self.runtime = runtime
        thresholds = HealthThresholds(
            degraded_after_timeouts=runtime.config.degraded_after_timeouts,
            disconnected_after_timeouts=runtime.config.max_consecutive_timeouts,
            faulted_after_protocol_errors=runtime.config.faulted_after_protocol_errors,
            reconnect_min_delay_s=runtime.config.reconnect_min_delay_s,
            reconnect_max_delay_s=runtime.config.reconnect_max_delay_s,
            reconnect_jitter=runtime.config.reconnect_jitter,
        )
        self.thresholds = thresholds
        meta = runtime.transport.metadata
        path = meta.device or ""
        virtual = meta.is_virtual_or_memory
        if runtime.serial_health is None:
            runtime.serial_health = SerialHealthMonitor(
                configured_path=path,
                thresholds=thresholds,
                transitions=runtime.health_transitions,
                virtual=virtual,
            )
        self.serial_health = runtime.serial_health
        self.sessions: dict[int, PumpSession] = {
            addr: PumpSession(
                address=addr,
                pump_id=f"pump-{addr}",
                events=runtime.events,
                sequence_policy=runtime.config.sequence_policy,
                thresholds=thresholds,
                transitions=runtime.health_transitions,
                awaiting_filling_complete_timeout=timedelta(
                    seconds=runtime.config.awaiting_filling_complete_timeout_s
                ),
                dc2_stability_window=timedelta(
                    seconds=runtime.config.dc2_stability_window_s
                ),
                soft_rx_sequence=runtime.config.soft_rx_sequence,
            )
            for addr in runtime.config.addresses
        }
        wires = tuple(encode_wire_address(a) for a in runtime.config.addresses)
        self.demux = AddressFrameDemux(
            wire_addresses=wires,
            quiet_gap_timeout_s=runtime.config.quiet_gap_timeout_s,
        )
        self.totals = ControllerTotals()
        self._stop = asyncio.Event()
        self._last_status_mono: float | None = None
        self._ever_opened = False
        self._rx_task: asyncio.Task[None] | None = None
        self.runtime.liveness.controller_mode = runtime.safety.mode.value
        self.runtime.liveness.watchdog_enabled = runtime.notifier.enabled
        self.runtime.liveness.notify_socket_present = (
            runtime.notifier.notify_socket_present
        )

    def request_stop(self) -> None:
        self._stop.set()

    def liveness_snapshot(self) -> LivenessSnapshot:
        self._refresh_totals()
        self._sync_serial_status()
        states = {a: s.state.communication for a, s in self.sessions.items()}
        counts = pump_health_counts(states)
        self.runtime.liveness.reconnect_attempts = (
            self.serial_health.state.reconnect_attempt_count
        )
        self.runtime.liveness.pump_health_summary = pump_health_summary(states)
        self.runtime.liveness.crc_errors = self.totals.crc_errors
        self.runtime.liveness.disconnected_pump_count = counts["disconnected"]
        self.runtime.liveness.faulted_pump_count = counts["faulted"]
        open_mono = self.serial_health.state.last_open_ok_mono
        if open_mono is None:
            self.runtime.liveness.last_serial_open_age_s = None
        else:
            self.runtime.liveness.last_serial_open_age_s = max(
                0.0, time.monotonic() - open_mono
            )
        return self.runtime.liveness.snapshot(
            total_polls=self.totals.poll_count,
            total_valid_responses=self.totals.eot_count + self.totals.data_count,
            total_timeouts=self.totals.timeouts,
        )

    def health_diagnostic_snapshot(self) -> dict[str, object]:
        """Read-only in-process health view for a future health CLI."""
        snap = self.liveness_snapshot()
        return {
            "overall": {
                "mode": self.runtime.safety.mode.value,
                "active_commands_enabled": (
                    self.runtime.safety.active_commands_enabled
                ),
                "listen_only": (
                    self.runtime.safety.mode.value == ControllerMode.LISTEN_ONLY.value
                ),
                "environment": self.runtime.safety.environment,
                "poll_and_observe": self.runtime.feature_flags.poll_and_observe,
                "feature_flags": {
                    "automatic_startup_price_programming": (
                        self.runtime.feature_flags.automatic_startup_price_programming
                    ),
                    "automatic_reset": self.runtime.feature_flags.automatic_reset,
                    "automatic_authorization": (
                        self.runtime.feature_flags.automatic_authorization
                    ),
                    "automatic_transaction_publishing": (
                        self.runtime.feature_flags.automatic_transaction_publishing
                    ),
                },
            },
            "liveness": snap.to_dict(),
            "serial": self.serial_health.state.to_dict(),
            "demux": {
                "malformed_count": self.demux.malformed_count,
                "stale_frame_count": self.demux.stale_frame_count,
                "queue_overflow_count": self.demux.queue_overflow_count,
            },
            "pumps": {
                str(addr): {
                    "communication": s.state.communication.value,
                    "communication_online": s.state.communication_online,
                    "state_synchronized": s.state.state_synchronized,
                    "observed_status": s.state.observed_status.value,
                    "nozzle_position": s.state.nozzle_position.value,
                    "sale_lifecycle": s.state.sale_lifecycle.value,
                    "last_poll_mono": s.state.last_poll_mono,
                    "last_valid_eot_mono": s.state.last_valid_eot_mono,
                    "last_valid_data_mono": s.state.last_valid_data_mono,
                    "last_valid_response_at": (
                        s.state.last_response_at.isoformat()
                        if s.state.last_response_at
                        else None
                    ),
                    "consecutive_timeouts": s.state.consecutive_timeouts,
                    "cumulative_timeouts": s.state.stats.timeout_count,
                    "crc_errors": s.state.stats.crc_error_count,
                    "nak_count": s.state.stats.nak_count,
                    "duplicate_count": s.state.stats.duplicate_count,
                    "address_mismatch_count": s.state.stats.address_mismatch_count,
                    "sequence_mismatch_count": s.state.stats.sequence_error_count,
                    "missed_bus_responses": s.state.missed_bus_responses,
                    "transient_error": s.state.last_transient_error,
                    "persistent_fault": s.state.last_persistent_fault,
                }
                for addr, s in self.sessions.items()
            },
            "thresholds": {
                "degraded_after_timeouts": self.thresholds.degraded_after_timeouts,
                "disconnected_after_timeouts": (
                    self.thresholds.disconnected_after_timeouts
                ),
                "faulted_after_protocol_errors": (
                    self.thresholds.faulted_after_protocol_errors
                ),
                "reconnect_min_delay_s": self.thresholds.reconnect_min_delay_s,
                "reconnect_max_delay_s": self.thresholds.reconnect_max_delay_s,
                "response_timeout_ms": self.runtime.config.response_timeout_ms,
            },
        }

    def _sync_serial_status(self) -> None:
        transport = self.runtime.transport
        self.serial_health.sync_from_transport(is_open=transport.is_open)
        self.runtime.liveness.serial_device_status = self.serial_health.state.status

    def _mark_all_pumps_disconnected(self) -> None:
        for session in self.sessions.values():
            session.mark_serial_lost()

    def _on_loop_progress(self) -> None:
        self.runtime.liveness.mark_loop_progress()
        self.runtime.notifier.watchdog()
        self._maybe_status()

    def _maybe_status(self) -> None:
        interval = self.runtime.status_interval_s
        now = time.monotonic()
        if self._last_status_mono is not None and (now - self._last_status_mono) < interval:
            return
        self._last_status_mono = now
        snap = self.liveness_snapshot()
        self.runtime.notifier.status(snap.status_line())

    def _bus_delays_enabled(self) -> bool:
        kind = self.runtime.transport.metadata.kind
        if kind == "serial_physical":
            return True
        return self.runtime.config.apply_bus_delays_on_virtual

    async def run(self, *, duration_s: float | None = None) -> None:
        """Run the poll loop until stop, optional deadline, or transport close."""
        if duration_s is not None:
            if duration_s < 0:
                raise ValueError("duration_s must not be negative")
            if duration_s == 0:
                raise ValueError(
                    "duration_s must be > 0, or None to run continuously"
                )
        decision = evaluate_polling_allowed(self.runtime.safety)
        if not decision.allowed:
            raise RuntimeError(f"polling not allowed: {decision.reasons}")

        try:
            await self.runtime.transport.open()
            self.serial_health.observe_open_success(is_reconnect=False)
            self._ever_opened = True
            self._start_rx_task()
        except Exception as exc:
            self.serial_health.observe_open_failure(f"{type(exc).__name__}:{exc}")
            self._mark_all_pumps_disconnected()
        self._sync_serial_status()
        deadline = (
            None
            if duration_s is None
            else asyncio.get_running_loop().time() + duration_s
        )
        try:
            while not self._stop.is_set():
                if (
                    deadline is not None
                    and asyncio.get_running_loop().time() >= deadline
                ):
                    break
                if not self.runtime.transport.is_open:
                    self._sync_serial_status()
                    await self._stop_rx_task()
                    await self._reconnect()
                    self._on_loop_progress()
                    continue
                if self._rx_task is None or self._rx_task.done():
                    self._start_rx_task()
                for address in self.runtime.config.addresses:
                    if self._stop.is_set():
                        break
                    try:
                        await self._poll_one(address)
                    except Exception as exc:
                        session = self.sessions[address]
                        session.state.last_error = f"poll_error:{type(exc).__name__}:{exc}"
                        if not self.runtime.transport.is_open:
                            self.serial_health.observe_closed(
                                reason=f"{type(exc).__name__}:{exc}"
                            )
                            self._mark_all_pumps_disconnected()
                            break
                        if self.runtime.log_frames:
                            print(f"pump {address} error: {exc}")
                    await asyncio.sleep(self.runtime.config.inter_poll_delay_ms / 1000)
                self._on_loop_progress()
                await asyncio.sleep(self.runtime.config.idle_sleep_ms / 1000)
        finally:
            await self._stop_rx_task()
            await self.runtime.transport.close()
            self.serial_health.observe_closed(reason="shutdown")
            self._sync_serial_status()

    def _start_rx_task(self) -> None:
        if self._rx_task is not None and not self._rx_task.done():
            return
        self._rx_task = asyncio.create_task(self._rx_forever(), name="controller-rx")

    async def _stop_rx_task(self) -> None:
        task = self._rx_task
        self._rx_task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _rx_forever(self) -> None:
        """Sole continuous reader: feed demux; never discard by address here."""
        chunk_size = self.runtime.config.read_chunk_size
        while not self._stop.is_set() and self.runtime.transport.is_open:
            try:
                chunk = await self.runtime.transport.read(chunk_size)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.runtime.log_frames:
                    print(f"RX error: {exc}")
                await asyncio.sleep(0.01)
                continue
            now = time.monotonic()
            if chunk:
                if self.runtime.log_frames:
                    print(f"RX raw {chunk.hex(' ')}")
                for diag in self.demux.feed(chunk, capture_mono=now):
                    self.runtime.events.publish(
                        ControllerEvent(
                            type=ControllerEventType.FRAME_REJECTED,
                            timestamp=datetime.now(UTC),
                            detail=diag.kind.value,
                            payload={
                                "raw_hex": diag.raw.hex(" "),
                                "message": diag.message,
                            },
                        )
                    )
                    if self.runtime.log_dart_timing:
                        print(
                            f"RX diag {diag.kind.value} {diag.message} "
                            f"{diag.raw.hex(' ')}"
                        )
            else:
                for diag in self.demux.maybe_expire_partial(now=now):
                    self.runtime.events.publish(
                        ControllerEvent(
                            type=ControllerEventType.FRAME_REJECTED,
                            timestamp=datetime.now(UTC),
                            detail=diag.kind.value,
                            payload={
                                "raw_hex": diag.raw.hex(" "),
                                "message": diag.message,
                            },
                        )
                    )
                await asyncio.sleep(0)

    async def _reconnect(self) -> None:
        """Bounded exponential reconnect; never issues dispenser commands."""
        assert self.runtime.safety.active_commands_enabled is False
        delay = self.serial_health.state.current_reconnect_delay_s
        if delay <= 0:
            delay = self.serial_health.observe_open_failure("port_not_open")
        await asyncio.sleep(delay)
        try:
            await self.runtime.transport.open()
        except Exception as exc:
            self.serial_health.observe_open_failure(f"{type(exc).__name__}:{exc}")
            self._mark_all_pumps_disconnected()
            return
        self.serial_health.observe_open_success(is_reconnect=self._ever_opened)
        self._ever_opened = True
        self.demux.reset()
        self._start_rx_task()

    async def _poll_one(self, address: int) -> None:
        session = self.sessions[address]
        await self._maybe_send_outbound(session)

        write_complete = await self._write_frame(
            session.build_poll(), address=address, note="POLL"
        )
        outcome = await self._read_poll_session(
            session, not_before_mono=write_complete
        )
        if outcome == "timeout":
            await self._handle_timeout_with_retries(session)
        session.tick_awaiting_completion()
        if outcome != "timeout":
            self.runtime.liveness.mark_successful_poll()
        self._refresh_totals()

    async def _read_poll_session(
        self,
        session: PumpSession,
        *,
        not_before_mono: float,
    ) -> str:
        """Wait through empty queue reads until EOT, deadline, or no response.

        Processes all correlated DATA for the address until EOT or timeout.
        Temporary emptiness does not end the exchange.
        """
        wire = session.wire_address
        timeout_ms = self.runtime.config.response_timeout_ms
        deadline = not_before_mono + (timeout_ms / 1000.0)
        got_response = False
        got_data = False
        first_byte_marked = False

        while time.monotonic() < deadline and not self._stop.is_set():
            remaining = deadline - time.monotonic()
            stamped = await self.demux.get(
                wire, timeout_s=min(0.01, max(0.0, remaining))
            )
            if stamped is None:
                # Temporary empty ≠ end of exchange.
                continue
            if stamped.first_byte_time < not_before_mono:
                self.demux.stale_frame_count += 1
                session.state.stats.stale_frame_count += 1
                if self.runtime.log_dart_timing:
                    print(
                        f"RX stale addr={session.address} "
                        f"first={stamped.first_byte_time:.6f} "
                        f"tx={not_before_mono:.6f} {stamped.raw.hex(' ')}"
                    )
                continue

            if not first_byte_marked:
                first_byte_marked = True
                latency_ms = (stamped.first_byte_time - not_before_mono) * 1000.0
                self.runtime.events.publish(
                    ControllerEvent(
                        type=ControllerEventType.FIRST_RESPONSE_BYTE,
                        address=session.address,
                        timestamp=datetime.now(UTC),
                        payload={
                            "first_byte_monotonic_s": stamped.first_byte_time,
                            "last_byte_monotonic_s": stamped.last_byte_time,
                            "latency_ms": latency_ms,
                            "raw_hex": stamped.raw.hex(" "),
                        },
                    )
                )
                if self.runtime.log_dart_timing:
                    print(
                        f"RX first-byte addr={session.address} "
                        f"latency_ms={latency_ms:.1f}"
                    )

            got_response = True
            frame = stamped.frame
            if self.runtime.log_frames:
                print(f"RX frame {frame.control_type} {stamped.raw.hex(' ')}")

            if frame.control_type is ControlType.EOT:
                session.handle_response_frame(
                    frame, capture_mono=stamped.first_byte_time
                )
                return "eot"

            if frame.control_type is ControlType.DATA:
                got_data = True
                ack = session.handle_response_frame(
                    frame, capture_mono=stamped.first_byte_time
                )
                if ack is not None:
                    await self._write_frame(ack, address=session.address, note="ACK")
                continue

            # Recognized short bus response — keep online, continue until EOT/deadline.
            session.handle_response_frame(
                frame, capture_mono=stamped.first_byte_time
            )

        if not got_response:
            return "timeout"
        if got_data:
            return "data"
        # Short bus activity without EOT still counts as a response (not offline).
        return "short_bus"

    async def _handle_timeout_with_retries(self, session: PumpSession) -> None:
        session.note_missed_bus_response()
        for _ in range(self.runtime.config.max_retries):
            if self._stop.is_set():
                return
            write_complete = await self._write_frame(
                session.build_poll(), address=session.address, note="POLL_RETRY"
            )
            outcome = await self._read_poll_session(
                session, not_before_mono=write_complete
            )
            if outcome != "timeout":
                self.runtime.liveness.mark_successful_poll()
                return
            session.note_missed_bus_response()

    async def _maybe_send_outbound(self, session: PumpSession) -> ExchangeResult | None:
        # Feature flags: never auto-issue actives from poll-and-observe defaults.
        flags = self.runtime.feature_flags
        if flags.poll_and_observe and not flags.any_automatic_command_enabled():
            # Still allow explicitly queued simulator/lab items via outbound API.
            pass

        item = self.runtime.outbound.pop_for_address(session.address)
        if item is None:
            return None

        # Skip redundant RESET when already RESET / application-confirmed.
        if (
            item.expect_status_after_tx == ObservedStatus.RESET.value
            and session.should_skip_reset()
        ):
            return ExchangeResult(
                status=ExchangeResultStatus.APPLICATION_CONFIRMED,
                address=session.address,
                detail="skip_reset_already_reset",
            )

        seq = item.sequence if item.sequence is not None else session.state.tx_sequence
        attempts = item.attempts
        result = await self._send_outbound_once(session, item, seq=seq)
        result.attempts = attempts + 1

        if result.status is ExchangeResultStatus.LINK_ACKNOWLEDGED:
            session.state.tx_sequence = next_sequence(
                seq, self.runtime.config.sequence_policy
            )
            if item.expect_status_after_tx:
                confirmed = await self._confirm_application(
                    session,
                    expected=ObservedStatus(item.expect_status_after_tx),
                    not_before_mono=result.command_tx_mono or time.monotonic(),
                )
                if confirmed:
                    result.status = ExchangeResultStatus.APPLICATION_CONFIRMED
                    result.application_confirm_mono = session.state.last_status_time
            return result

        if result.status is ExchangeResultStatus.TIMED_OUT and attempts + 1 <= item.max_retries:
            # Retries reuse the same sequence; do not advance.
            session.state.stats.retry_count += 1
            retry = item.with_attempt(sequence=seq, attempts=attempts + 1)
            with contextlib.suppress(Exception):
                self.runtime.outbound.enqueue(retry, self.runtime.safety)
        return result

    async def _send_outbound_once(
        self,
        session: PumpSession,
        item: OutboundDataItem,
        *,
        seq: int,
    ) -> ExchangeResult:
        wire = build_data_frame(session.wire_address, seq, item.application_payload)
        write_complete = await self._write_frame(
            wire, address=session.address, note="DATA_OUT"
        )
        session.note_command_tx(tx_mono=write_complete)
        expected_ack = build_ack(session.wire_address, seq)
        deadline = write_complete + (self.runtime.config.response_timeout_ms / 1000.0)
        preserved = 0

        while time.monotonic() < deadline and not self._stop.is_set():
            remaining = deadline - time.monotonic()
            stamped = await self.demux.get(
                session.wire_address, timeout_s=min(0.01, max(0.0, remaining))
            )
            if stamped is None:
                continue
            if stamped.first_byte_time < write_complete:
                self.demux.stale_frame_count += 1
                session.state.stats.stale_frame_count += 1
                continue

            frame = stamped.frame
            if (
                frame.control_type is ControlType.ACK
                and frame.sequence == seq
                and (
                    stamped.raw == expected_ack
                    or frame.address == session.wire_address
                )
            ):
                session.note_link_ack(ack_mono=stamped.first_byte_time)
                return ExchangeResult(
                    status=ExchangeResultStatus.LINK_ACKNOWLEDGED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    command_tx_mono=write_complete,
                    link_ack_mono=stamped.first_byte_time,
                    preserved_event_count=preserved,
                )

            if frame.control_type is ControlType.NAK and frame.sequence == seq:
                session.state.stats.nak_count += 1
                session.state.pending_exchange = False
                return ExchangeResult(
                    status=ExchangeResultStatus.REJECTED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    command_tx_mono=write_complete,
                    detail="nak",
                    preserved_event_count=preserved,
                )

            # Do not silently discard non-ACK: process/ACK valid DATA, keep waiting.
            if frame.control_type is ControlType.DATA:
                ack = session.handle_response_frame(
                    frame, capture_mono=stamped.first_byte_time
                )
                preserved += 1
                if ack is not None:
                    await self._write_frame(ack, address=session.address, note="ACK")
                continue

            if frame.control_type is ControlType.EOT:
                session.handle_response_frame(
                    frame, capture_mono=stamped.first_byte_time
                )
                continue

        session.state.stats.timeout_count += 1
        session.state.pending_exchange = False
        return ExchangeResult(
            status=ExchangeResultStatus.TIMED_OUT,
            address=session.address,
            sequence=seq,
            correlation_id=item.correlation_id,
            command_tx_mono=write_complete,
            detail="ack_timeout",
            preserved_event_count=preserved,
        )

    async def _confirm_application(
        self,
        session: PumpSession,
        *,
        expected: ObservedStatus,
        not_before_mono: float,
    ) -> bool:
        """Poll until expected DC1 is observed strictly after command TX time."""
        max_polls = self.runtime.config.application_confirm_max_polls
        for _ in range(max_polls):
            if session.status_observed_after(expected, not_before_mono=not_before_mono):
                return True
            write_complete = await self._write_frame(
                session.build_poll(), address=session.address, note="POLL_CONFIRM"
            )
            await self._read_poll_session(session, not_before_mono=write_complete)
            if session.status_observed_after(expected, not_before_mono=not_before_mono):
                return True
        return False

    async def _write_frame(self, data: bytes, *, address: int, note: str) -> float:
        """Write frame; return write-complete monotonic timestamp."""
        if self._bus_delays_enabled():
            if note == "ACK":
                delay_ms = self.runtime.config.ack_delay_ms
            else:
                delay_ms = self.runtime.config.tx_delay_ms
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
        write_start_s = time.monotonic()
        await self.runtime.transport.write(data)
        await self.runtime.transport.drain()
        write_complete_s = time.monotonic()
        self.runtime.events.publish(
            ControllerEvent(
                type=ControllerEventType.FRAME_SENT,
                address=address,
                timestamp=datetime.now(UTC),
                detail=note,
                payload={
                    "raw_hex": data.hex(" "),
                    "write_start_monotonic_s": write_start_s,
                    "write_complete_monotonic_s": write_complete_s,
                },
            )
        )
        if self.runtime.log_frames:
            print(f"TX [{note}] addr={address} {data.hex(' ')}")
        if self.runtime.log_dart_timing:
            print(
                f"TX timing [{note}] addr={address} "
                f"start={write_start_s:.6f} complete={write_complete_s:.6f}"
            )
        return write_complete_s

    def _refresh_totals(self) -> None:
        self.totals = ControllerTotals(
            poll_count=sum(s.state.stats.poll_count for s in self.sessions.values()),
            eot_count=sum(s.state.stats.eot_count for s in self.sessions.values()),
            data_count=sum(s.state.stats.data_count for s in self.sessions.values()),
            crc_errors=sum(
                s.state.stats.crc_error_count for s in self.sessions.values()
            ),
            timeouts=sum(s.state.stats.timeout_count for s in self.sessions.values()),
            nak_count=sum(s.state.stats.nak_count for s in self.sessions.values()),
            duplicate_count=sum(
                s.state.stats.duplicate_count for s in self.sessions.values()
            ),
            stale_frame_count=self.demux.stale_frame_count,
            malformed_count=self.demux.malformed_count,
        )

    def enqueue_status_request(self, address: int) -> None:
        item = OutboundDataItem.create(
            address=address,
            application_payload=encode_cd1_command(PumpControlCommand.RETURN_STATUS),
            command_type=PumpCommand.READ_STATUS,
            simulator_only=True,
            idempotency=IdempotencyClass.IDEMPOTENT,
        )
        self.runtime.outbound.enqueue(item, self.runtime.safety)

    def summary(self) -> dict[str, object]:
        self._refresh_totals()
        return {
            "totals": {
                "poll_count": self.totals.poll_count,
                "eot_count": self.totals.eot_count,
                "data_count": self.totals.data_count,
                "crc_errors": self.totals.crc_errors,
                "timeouts": self.totals.timeouts,
                "nak_count": self.totals.nak_count,
                "duplicate_count": self.totals.duplicate_count,
                "stale_frame_count": self.totals.stale_frame_count,
                "malformed_count": self.totals.malformed_count,
            },
            "feature_flags": {
                "poll_and_observe": self.runtime.feature_flags.poll_and_observe,
                "automatic_startup_price_programming": (
                    self.runtime.feature_flags.automatic_startup_price_programming
                ),
                "automatic_reset": self.runtime.feature_flags.automatic_reset,
                "automatic_authorization": (
                    self.runtime.feature_flags.automatic_authorization
                ),
                "automatic_transaction_publishing": (
                    self.runtime.feature_flags.automatic_transaction_publishing
                ),
            },
            "pumps": {
                str(addr): {
                    "state": s.state.last_state.value,
                    "communication": s.state.communication.value,
                    "communication_online": s.state.communication_online,
                    "state_synchronized": s.state.state_synchronized,
                    "observed_status": s.state.observed_status.value,
                    "nozzle_position": s.state.nozzle_position.value,
                    "sale_lifecycle": s.state.sale_lifecycle.value,
                    "stats": {
                        "poll": s.state.stats.poll_count,
                        "eot": s.state.stats.eot_count,
                        "data": s.state.stats.data_count,
                        "timeout": s.state.stats.timeout_count,
                        "crc": s.state.stats.crc_error_count,
                        "nak": s.state.stats.nak_count,
                        "dup": s.state.stats.duplicate_count,
                    },
                    "last_error": s.state.last_error,
                }
                for addr, s in self.sessions.items()
            },
        }


def default_lab_safety() -> ControllerSafetyContext:
    return ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.LISTEN_ONLY,
        active_commands_enabled=False,
        require_physical_control_enable=True,
        physical_enable_present=False,
        allow_virtual_polling=True,
    )
