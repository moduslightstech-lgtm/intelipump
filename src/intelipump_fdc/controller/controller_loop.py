"""Async polling scheduler and controller loop."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog

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
from intelipump_fdc.controller.outbound import OutboundQueue, OutboundRejectedError
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.rx_demux import AddressFrameDemux, TimestampedFrame
from intelipump_fdc.controller.safety import (
    ControllerSafetyContext,
    evaluate_outbound_safety,
    evaluate_polling_allowed,
)
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.controller.sale_lifecycle import SaleLifecycle
from intelipump_fdc.controller.session_models import (
    IdempotencyClass,
    NozzlePosition,
    ObservedStatus,
    OutboundDataItem,
)
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.core.liveness import LivenessSnapshot, LivenessTracker
from intelipump_fdc.core.systemd_notify import Notifier, NullNotifier
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.protocol.cd2 import build_cd2_allowed_nozzles
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_ack, build_data_frame
from intelipump_fdc.protocol.dart.transport.base import ByteTransport
from intelipump_fdc.simulator.config import next_sequence
from intelipump_fdc.simulator.encoding import encode_cd1_command, encode_cd5_price_update

logger = structlog.get_logger(__name__)


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


def format_raw_as_2dp(raw: int) -> str:
    """Display-only 2-decimal view of a packed-BCD integer (DC7 decimals unknown)."""
    sign = "-" if raw < 0 else ""
    magnitude = abs(raw)
    return f"{sign}{magnitude // 100}.{magnitude % 100:02d}"


@dataclass
class ControllerRuntime:
    transport: ByteTransport
    safety: ControllerSafetyContext
    config: PollSchedulerConfig = field(default_factory=PollSchedulerConfig)
    events: EventBus = field(default_factory=EventBus)
    outbound: OutboundQueue = field(default_factory=OutboundQueue)
    log_frames: bool = False
    log_dart_timing: bool = False
    log_all_frames: bool = False
    feature_flags: WayneFeatureFlags = field(default_factory=WayneFeatureFlags)
    liveness: LivenessTracker = field(default_factory=LivenessTracker)
    notifier: Notifier = field(default_factory=NullNotifier)
    status_interval_s: float = 15.0
    serial_health: SerialHealthMonitor | None = None
    health_transitions: HealthTransitionLog = field(default_factory=HealthTransitionLog)
    startup_unit_price: int | None = None
    logical_nozzle_count: int = 1


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
        self._last_nozzle: dict[int, NozzlePosition] = {}
        self._last_status: dict[int, ObservedStatus] = {}
        self._last_dc2: dict[int, tuple[int, int]] = {}
        self._last_sale: dict[int, SaleLifecycle] = {}
        self._last_unit_price: dict[int, int] = {}
        self._last_completed_sale: dict[int, dict[str, int | None]] = {}
        self._sale_display_held: set[int] = set()
        self._startup_price_attempted: set[int] = set()
        self._startup_reset_attempted: set[int] = set()
        self._bus_silent_warned: set[int] = set()
        self._price_programmed: set[int] = set()
        self._startup_reset_done: set[int] = set()
        self._auth_this_lift: set[int] = set()
        self._auth_deferred_logged: set[int] = set()
        self._rs_poll_counter: dict[int, int] = {}
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

    def _log_all_bus(self) -> bool:
        return self.runtime.log_all_frames

    def _log_tx_note(self, note: str) -> bool:
        if self._log_all_bus():
            return True
        return note not in {"POLL", "POLL_RETRY", "POLL_CONFIRM"}

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
                if self.runtime.log_all_frames:
                    print(f"RX error: {exc}")
                await asyncio.sleep(0.01)
                continue
            now = time.monotonic()
            if chunk:
                if self.runtime.log_all_frames:
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
                    if self.runtime.log_all_frames:
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
        """Bounded exponential reconnect; reopens serial only, no dispenser TX."""
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

        _write_start, _write_complete = await self._write_frame(
            session.build_poll(), address=address, note="POLL"
        )
        outcome = await self._read_poll_session(
            session, not_before_mono=_write_start
        )
        if outcome == "timeout":
            recovered = await self._handle_timeout_with_retries(session)
            if recovered:
                outcome = "data"
        session.tick_awaiting_completion()
        if outcome != "timeout":
            self.runtime.liveness.mark_successful_poll()
        self._report_observed_changes(session)
        unknown = (
            session.state.observed_status is ObservedStatus.UNKNOWN
            or session.state.nozzle_position is NozzlePosition.UNKNOWN
        )
        silent = (
            outcome == "timeout"
            and unknown
            and session.state.nozzle_position is not NozzlePosition.OUT
        )
        if unknown and outcome != "eot":
            print(f"[BUS addr={address}] poll={outcome}")
        if silent:
            if address not in self._bus_silent_warned:
                self._bus_silent_warned.add(address)
                print(
                    f"[BUS addr={address}] no response (check port, dialout, "
                    "one master only); skipping commands until poll succeeds"
                )
        else:
            self._bus_silent_warned.discard(address)
            await self._owned_lab_tick(session)
        self._refresh_totals()

    async def _read_poll_session(
        self,
        session: PumpSession,
        *,
        not_before_mono: float,
    ) -> str:
        """Wait through empty queue reads until EOT, deadline, or no response.

        Processes all correlated DATA for the address until EOT or timeout.
        Temporary emptiness does not end the exchange. Leftover/stale CRC-valid
        DATA is still applied and ACKed so the pump is not left repeating.
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
                continue
            terminal, saw_data, first_byte_marked = await self._ingest_poll_frame(
                session,
                stamped,
                not_before_mono=not_before_mono,
                first_byte_marked=first_byte_marked,
            )
            if saw_data:
                got_data = True
            if terminal == "skip":
                continue
            got_response = True
            if terminal == "eot":
                return "eot"

        # Grace drain: frame finished assembling after the software deadline.
        while True:
            extra = self.demux.get_nowait(wire)
            if extra is None:
                break
            terminal, saw_data, first_byte_marked = await self._ingest_poll_frame(
                session,
                extra,
                not_before_mono=not_before_mono,
                first_byte_marked=first_byte_marked,
            )
            if saw_data:
                got_data = True
            if terminal == "skip":
                continue
            got_response = True
            if terminal == "eot":
                return "eot"

        if got_data:
            return "data"
        if not got_response:
            return "timeout"
        return "short_bus"

    async def _ingest_poll_frame(
        self,
        session: PumpSession,
        stamped: TimestampedFrame,
        *,
        not_before_mono: float,
        first_byte_marked: bool,
    ) -> tuple[str, bool, bool]:
        """Apply one demuxed frame. Returns (terminal, saw_data, first_byte_marked).

        terminal: ``eot`` | ``data`` | ``short`` | ``skip``
        """
        stale = stamped.last_byte_time < not_before_mono
        frame = stamped.frame
        if stale:
            self.demux.stale_frame_count += 1
            session.state.stats.stale_frame_count += 1
            if frame.control_type is ControlType.DATA:
                ack = session.handle_response_frame(
                    frame, capture_mono=stamped.first_byte_time
                )
                if ack is not None:
                    await self._write_frame(
                        ack, address=session.address, note="ACK_STALE"
                    )
                print(
                    f"RX DATA addr={session.address} stale-applied "
                    f"{stamped.raw.hex(' ')}"
                )
                return ("more", True, first_byte_marked)
            if self._log_all_bus():
                print(
                    f"RX stale addr={session.address} "
                    f"first={stamped.first_byte_time:.6f} "
                    f"tx={not_before_mono:.6f} {stamped.raw.hex(' ')}"
                )
            return ("skip", False, first_byte_marked)

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
            latency_for_print = latency_ms
        else:
            latency_for_print = None

        if frame.control_type is ControlType.DATA:
            extra = ""
            if latency_for_print is not None and latency_for_print >= 0:
                extra = f" latency_ms={latency_for_print:.1f}"
            print(
                f"RX DATA addr={session.address}{extra} {stamped.raw.hex(' ')}"
            )
        elif self._log_all_bus():
            if latency_for_print is not None:
                print(
                    f"RX first-byte addr={session.address} "
                    f"latency_ms={latency_for_print:.1f}"
                )
            print(f"RX frame {frame.control_type} {stamped.raw.hex(' ')}")

        if frame.control_type is ControlType.EOT:
            session.handle_response_frame(
                frame, capture_mono=stamped.first_byte_time
            )
            return ("eot", False, first_byte_marked)

        if frame.control_type is ControlType.DATA:
            ack = session.handle_response_frame(
                frame, capture_mono=stamped.first_byte_time
            )
            if ack is not None:
                await self._write_frame(ack, address=session.address, note="ACK")
            return ("more", True, first_byte_marked)

        session.handle_response_frame(frame, capture_mono=stamped.first_byte_time)
        return ("short", False, first_byte_marked)

    async def _handle_timeout_with_retries(self, session: PumpSession) -> bool:
        """True if leftover DATA or a retry recovered the poll."""
        leftover = await self._drain_pending_data(session)
        if leftover:
            self.runtime.liveness.mark_successful_poll()
            return True
        session.note_missed_bus_response()
        for _ in range(self.runtime.config.max_retries):
            if self._stop.is_set():
                return False
            _write_start, _write_complete = await self._write_frame(
                session.build_poll(), address=session.address, note="POLL_RETRY"
            )
            outcome = await self._read_poll_session(
                session, not_before_mono=_write_start
            )
            if outcome != "timeout":
                self.runtime.liveness.mark_successful_poll()
                return True
            leftover = await self._drain_pending_data(session)
            if leftover:
                self.runtime.liveness.mark_successful_poll()
                return True
            session.note_missed_bus_response()
        return False

    async def _drain_pending_data(self, session: PumpSession) -> bool:
        """ACK/apply any queued DATA for this address. True if DATA was applied."""
        applied = False
        while True:
            stamped = self.demux.get_nowait(session.wire_address)
            if stamped is None:
                return applied
            terminal, saw_data, _ = await self._ingest_poll_frame(
                session,
                stamped,
                not_before_mono=0.0,
                first_byte_marked=True,
            )
            if saw_data or terminal in {"eot", "data"}:
                applied = True
        return applied

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
        exchange_start: float | None = None
        while True:
            result = await self._send_outbound_once(
                session, item, seq=seq, ack_not_before=exchange_start
            )
            result.attempts = attempts + 1
            if exchange_start is None:
                exchange_start = result.write_start_mono
            if result.status in {
                ExchangeResultStatus.LINK_ACKNOWLEDGED,
                ExchangeResultStatus.APPLICATION_CONFIRMED,
            }:
                self._advance_tx_sequence(session, seq)
                if (
                    result.status is ExchangeResultStatus.LINK_ACKNOWLEDGED
                    and item.expect_status_after_tx
                ):
                    confirmed = await self._confirm_application(
                        session,
                        expected=ObservedStatus(item.expect_status_after_tx),
                        not_before_mono=exchange_start or result.command_tx_mono or time.monotonic(),
                    )
                    if confirmed:
                        result.status = ExchangeResultStatus.APPLICATION_CONFIRMED
                        result.application_confirm_mono = session.state.last_status_time
                return result

            if result.status is ExchangeResultStatus.TIMED_OUT:
                recovered = await self._recover_command_after_timeout(
                    session,
                    item,
                    seq=seq,
                    not_before=exchange_start or result.write_start_mono or time.monotonic(),
                )
                if recovered is not None:
                    recovered.attempts = result.attempts
                    self._advance_tx_sequence(session, seq)
                    return recovered
                if attempts + 1 <= item.max_retries:
                    attempts += 1
                    session.state.stats.retry_count += 1
                    await asyncio.sleep(0.08)
                    continue
                self._advance_tx_sequence(session, seq)
            return result

    def _advance_tx_sequence(self, session: PumpSession, seq: int) -> None:
        session.state.tx_sequence = next_sequence(
            seq, self.runtime.config.sequence_policy
        )

    def _command_ack_seen(
        self, session: PumpSession, seq: int, *, not_before: float
    ) -> bool:
        if session.state.last_rx_ack_sequence != seq:
            return False
        ack_at = session.state.last_ack_time
        return ack_at is not None and ack_at >= not_before

    async def _recover_command_after_timeout(
        self,
        session: PumpSession,
        item: OutboundDataItem,
        *,
        seq: int,
        not_before: float,
    ) -> ExchangeResult | None:
        """Wayne often answers DATA commands on the next POLL, not with an ACK.

        Poll after ACK timeout to pick up DC1 / a late matching ACK.
        """
        print(
            f"[OWNED-LAB addr={session.address}] polling after "
            f"{item.command_type.value} (no link ACK yet)"
        )
        await self._drain_pending_data(session)
        expected = item.expect_status_after_tx
        if expected is not None:
            confirmed = await self._confirm_application(
                session,
                expected=ObservedStatus(expected),
                not_before_mono=not_before,
            )
            if confirmed or session.state.observed_status.value == expected:
                print(
                    f"[OWNED-LAB addr={session.address}] "
                    f"{item.command_type.value} confirmed by poll DC1"
                )
                return ExchangeResult(
                    status=ExchangeResultStatus.APPLICATION_CONFIRMED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    detail="confirmed_by_poll",
                    write_start_mono=not_before,
                )
        else:
            for _ in range(self.runtime.config.application_confirm_max_polls):
                if self._observation_satisfies_command(session, item):
                    print(
                        f"[OWNED-LAB addr={session.address}] "
                        f"{item.command_type.value} confirmed by poll DATA"
                    )
                    return ExchangeResult(
                        status=ExchangeResultStatus.APPLICATION_CONFIRMED,
                        address=session.address,
                        sequence=seq,
                        correlation_id=item.correlation_id,
                        detail="confirmed_by_poll",
                        write_start_mono=not_before,
                    )
                if self._command_ack_seen(session, seq, not_before=not_before):
                    print(f"RX ACK addr={session.address} seq={seq} (late)")
                    return ExchangeResult(
                        status=ExchangeResultStatus.LINK_ACKNOWLEDGED,
                        address=session.address,
                        sequence=seq,
                        correlation_id=item.correlation_id,
                        detail="ack_on_poll",
                        write_start_mono=not_before,
                    )
                write_start, _complete = await self._write_frame(
                    session.build_poll(),
                    address=session.address,
                    note="POLL_CONFIRM",
                )
                await self._read_poll_session(session, not_before_mono=write_start)
            if self._observation_satisfies_command(session, item):
                print(
                    f"[OWNED-LAB addr={session.address}] "
                    f"{item.command_type.value} confirmed by poll DATA"
                )
                return ExchangeResult(
                    status=ExchangeResultStatus.APPLICATION_CONFIRMED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    detail="confirmed_by_poll",
                    write_start_mono=not_before,
                )
            if self._command_ack_seen(session, seq, not_before=not_before):
                print(f"RX ACK addr={session.address} seq={seq} (late)")
                return ExchangeResult(
                    status=ExchangeResultStatus.LINK_ACKNOWLEDGED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    detail="ack_on_poll",
                    write_start_mono=not_before,
                )
            if self._is_cd2_payload(item.application_payload):
                # This head often skips CD2 ACK; a quiet poll after TX is enough.
                return ExchangeResult(
                    status=ExchangeResultStatus.LINK_ACKNOWLEDGED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    detail="cd2_assumed_after_poll",
                    write_start_mono=not_before,
                )
        return None

    async def _send_outbound_once(
        self,
        session: PumpSession,
        item: OutboundDataItem,
        *,
        seq: int,
        ack_not_before: float | None = None,
    ) -> ExchangeResult:
        wire = build_data_frame(session.wire_address, seq, item.application_payload)
        write_start, write_complete = await self._write_frame(
            wire, address=session.address, note="DATA_OUT"
        )
        session.note_command_tx(tx_mono=write_complete)
        expected_ack = build_ack(session.wire_address, seq)
        ack_floor = ack_not_before if ack_not_before is not None else write_start
        timeout_ms = self.runtime.config.response_timeout_ms
        deadline = write_complete + (timeout_ms / 1000.0)
        preserved = 0

        async def _handle(stamped: TimestampedFrame) -> ExchangeResult | None:
            nonlocal preserved
            frame = stamped.frame
            matching_ack = (
                frame.control_type is ControlType.ACK
                and frame.sequence == seq
                and (
                    stamped.raw == expected_ack
                    or frame.address == session.wire_address
                )
            )
            if matching_ack and stamped.last_byte_time >= ack_floor:
                print(f"RX ACK addr={session.address} seq={seq}")
                session.note_link_ack(ack_mono=stamped.first_byte_time, sequence=seq)
                return ExchangeResult(
                    status=ExchangeResultStatus.LINK_ACKNOWLEDGED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    command_tx_mono=write_complete,
                    write_start_mono=write_start,
                    link_ack_mono=stamped.first_byte_time,
                    preserved_event_count=preserved,
                )
            if matching_ack:
                return None

            stale_by_last_byte = stamped.last_byte_time < write_complete
            if stale_by_last_byte:
                self.demux.stale_frame_count += 1
                session.state.stats.stale_frame_count += 1
                if frame.control_type is ControlType.DATA:
                    print(
                        f"RX DATA addr={session.address} stale-applied "
                        f"{stamped.raw.hex(' ')}"
                    )
                    ack = session.handle_response_frame(
                        frame, capture_mono=stamped.last_byte_time
                    )
                    preserved += 1
                    if ack is not None:
                        await self._write_frame(
                            ack, address=session.address, note="ACK_STALE"
                        )
                    return self._command_status_met(
                        session,
                        item,
                        command_tx_mono=write_complete,
                        write_start_mono=write_start,
                        sequence=seq,
                        preserved=preserved,
                        frame_is_stale=True,
                    )
                return None

            if frame.control_type is ControlType.NAK and frame.sequence == seq:
                session.state.stats.nak_count += 1
                session.state.pending_exchange = False
                return ExchangeResult(
                    status=ExchangeResultStatus.REJECTED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    command_tx_mono=write_complete,
                    write_start_mono=write_start,
                    detail="nak",
                    preserved_event_count=preserved,
                )

            if frame.control_type is ControlType.DATA:
                print(f"RX DATA addr={session.address} {stamped.raw.hex(' ')}")
                ack = session.handle_response_frame(
                    frame, capture_mono=stamped.last_byte_time
                )
                preserved += 1
                if ack is not None:
                    await self._write_frame(ack, address=session.address, note="ACK")
                return self._command_status_met(
                    session,
                    item,
                    command_tx_mono=write_complete,
                    write_start_mono=write_start,
                    sequence=seq,
                    preserved=preserved,
                )

            if frame.control_type is ControlType.EOT:
                session.handle_response_frame(
                    frame, capture_mono=stamped.last_byte_time
                )
            return None

        while time.monotonic() < deadline and not self._stop.is_set():
            remaining = deadline - time.monotonic()
            stamped = await self.demux.get(
                session.wire_address, timeout_s=min(0.02, max(0.0, remaining))
            )
            if stamped is None:
                continue
            handled = await _handle(stamped)
            if handled is not None:
                return handled

        grace_end = time.monotonic() + 0.08
        while time.monotonic() < grace_end and not self._stop.is_set():
            stamped = await self.demux.get(session.wire_address, timeout_s=0.02)
            if stamped is None:
                continue
            handled = await _handle(stamped)
            if handled is not None:
                return handled

        await self._drain_pending_data(session)
        late = self._command_status_met(
            session,
            item,
            command_tx_mono=write_complete,
            write_start_mono=write_start,
            sequence=seq,
            preserved=preserved,
        )
        if late is not None:
            late.detail = "status_after_ack_timeout"
            return late
        session.state.stats.timeout_count += 1
        session.state.pending_exchange = False
        return ExchangeResult(
            status=ExchangeResultStatus.TIMED_OUT,
            address=session.address,
            sequence=seq,
            correlation_id=item.correlation_id,
            command_tx_mono=write_complete,
            write_start_mono=write_start,
            detail="ack_timeout",
            preserved_event_count=preserved,
        )

    def _is_cd2_payload(self, payload: bytes) -> bool:
        return len(payload) >= 2 and payload[0] == 0x02

    def _observation_satisfies_command(
        self, session: PumpSession, item: OutboundDataItem
    ) -> bool:
        expected = item.expect_status_after_tx
        if expected is not None:
            return session.state.observed_status.value == expected
        if item.command_type is PumpCommand.READ_STATUS:
            return (
                session.state.observed_status is not ObservedStatus.UNKNOWN
                or session.state.nozzle_position is not NozzlePosition.UNKNOWN
            )
        return False

    def _command_status_met(
        self,
        session: PumpSession,
        item: OutboundDataItem,
        *,
        command_tx_mono: float | None = None,
        write_start_mono: float | None = None,
        sequence: int | None = None,
        preserved: int = 0,
        frame_is_stale: bool = False,
    ) -> ExchangeResult | None:
        if not self._observation_satisfies_command(session, item):
            return None
        # Status-expecting commands must see DC1 observed after this TX.
        # Stale DATA must not APPLICATION_CONFIRM AUTHORIZE/RESET.
        if item.expect_status_after_tx is not None:
            floor = command_tx_mono if command_tx_mono is not None else write_start_mono
            if floor is None or not session.status_observed_after(
                ObservedStatus(item.expect_status_after_tx),
                not_before_mono=floor,
            ):
                logger.info(
                    "stale_response_rejected",
                    address=session.address,
                    command=item.command_type.value,
                    expected=item.expect_status_after_tx,
                    observed=session.state.observed_status.value,
                    frame_is_stale=frame_is_stale,
                    last_status_time=session.state.last_status_time,
                    command_tx_mono=command_tx_mono,
                    reason="status_not_fresh_after_command",
                )
                return None
        session.note_link_ack(ack_mono=time.monotonic(), sequence=sequence)
        return ExchangeResult(
            status=ExchangeResultStatus.APPLICATION_CONFIRMED,
            address=session.address,
            sequence=sequence,
            correlation_id=item.correlation_id,
            command_tx_mono=command_tx_mono,
            write_start_mono=write_start_mono,
            application_confirm_mono=session.state.last_status_time,
            preserved_event_count=preserved,
            detail="status_in_command_window",
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
            _write_start, _write_complete = await self._write_frame(
                session.build_poll(), address=session.address, note="POLL_CONFIRM"
            )
            await self._read_poll_session(session, not_before_mono=_write_start)
            if session.status_observed_after(expected, not_before_mono=not_before_mono):
                return True
        return False

    def _report_observed_changes(self, session: PumpSession) -> None:
        addr = session.address
        pos = session.state.nozzle_position
        status = session.state.observed_status
        prev_pos = self._last_nozzle.get(addr)
        prev_status = self._last_status.get(addr)
        if prev_pos is not pos:
            if prev_pos is not None or pos is not NozzlePosition.UNKNOWN:
                print(
                    f"[NOZIO addr={addr}] {prev_pos.value if prev_pos else 'UNKNOWN'} "
                    f"-> {pos.value}"
                )
            if prev_pos is NozzlePosition.OUT and pos is NozzlePosition.IN:
                self._auth_this_lift.discard(addr)
                self._auth_deferred_logged.discard(addr)
            self._last_nozzle[addr] = pos
        if prev_status is not status:
            if prev_status is not None or status is not ObservedStatus.UNKNOWN:
                print(
                    f"[DC1 addr={addr}] "
                    f"{prev_status.value if prev_status else 'UNKNOWN'} -> {status.value}"
                )
            self._last_status[addr] = status
        price = session.state.unit_price_raw
        if price is not None and self._last_unit_price.get(addr) != price:
            print(f"[DC3 addr={addr}] unit_price_raw={price}")
            self._last_unit_price[addr] = price
        vol = session.state.filled_volume_raw
        amt = session.state.filled_amount_raw
        prev_dc2 = self._last_dc2.get(addr)
        if prev_dc2 != (vol, amt) and (vol > 0 or amt > 0 or prev_dc2 is not None):
            print(
                f"[DC2 addr={addr}] volume_raw={vol} amount_raw={amt} "
                f"volume={format_raw_as_2dp(vol)} amount={format_raw_as_2dp(amt)}"
            )
            self._last_dc2[addr] = (vol, amt)
        life = session.state.sale_lifecycle
        prev_life = self._last_sale.get(addr)
        if life is not prev_life:
            if life in {
                SaleLifecycle.FILLING_COMPLETED,
                SaleLifecycle.CLOSED,
                SaleLifecycle.ABORTED_NO_DELIVERY,
                SaleLifecycle.ABORTED,
            }:
                ev = session.state.sale_evidence
                peak_vol = ev.peak_volume_raw
                peak_amt = ev.peak_amount_raw
                print(
                    f"[SALE addr={addr}] {life.value} "
                    f"volume_raw={peak_vol} amount_raw={peak_amt} "
                    f"volume={format_raw_as_2dp(peak_vol)} "
                    f"amount={format_raw_as_2dp(peak_amt)} "
                    f"unit_price_raw={price}"
                )
                if life in {SaleLifecycle.FILLING_COMPLETED, SaleLifecycle.CLOSED}:
                    self._last_completed_sale[addr] = {
                        "volume_raw": peak_vol,
                        "amount_raw": peak_amt,
                        "unit_price_raw": price,
                    }
            self._last_sale[addr] = life

    def _should_hold_sale_display(self, session: PumpSession) -> bool:
        """Keep FILLING_COMPLETED totals on the pump until the next lift.

        Normal Wayne / ePump sequence: hang-up shows volume and amount; RESET
        (which clears the display) runs only when the nozzle is lifted again.
        Do not copy the working-controller immediate RESET on completion.
        Do not hold after a zero-delivery hang-up (ABORTED_NO_DELIVERY).
        """
        if session.state.nozzle_position is not NozzlePosition.IN:
            return False
        if session.state.sale_lifecycle in {
            SaleLifecycle.ABORTED_NO_DELIVERY,
            SaleLifecycle.ABORTED,
            SaleLifecycle.NOZZLE_LIFTED,
            SaleLifecycle.AUTHORIZED,
            SaleLifecycle.FILLING,
        }:
            return False
        if session.state.sale_lifecycle not in {
            SaleLifecycle.FILLING_COMPLETED,
            SaleLifecycle.CLOSED,
        }:
            return False
        if session.state.observed_status not in {
            ObservedStatus.FILLING_COMPLETED,
            ObservedStatus.MAX_AMOUNT_VOLUME_REACHED,
        }:
            return False
        return session.state.sale_evidence.has_positive_delivery

    async def _owned_lab_tick(self, session: PumpSession) -> None:
        flags = self.runtime.feature_flags
        if not self.runtime.safety.owned_lab_active_session:
            return
        addr = session.address
        unknown = (
            session.state.observed_status is ObservedStatus.UNKNOWN
            or session.state.nozzle_position is NozzlePosition.UNKNOWN
        )
        if unknown:
            n = self._rs_poll_counter.get(addr, 0) + 1
            self._rs_poll_counter[addr] = n
            if n % 5 == 1:
                result = await self._run_owned_command(
                    session,
                    encode_cd1_command(PumpControlCommand.RETURN_STATUS),
                    PumpCommand.READ_STATUS,
                    idempotency=IdempotencyClass.IDEMPOTENT,
                )
                print(
                    f"[OWNED-LAB addr={addr}] RETURN_STATUS result={result.status.value}"
                    f"{':' + result.detail if result.detail else ''} "
                    f"dc1={session.state.observed_status.value} "
                    f"nozio={session.state.nozzle_position.value}"
                )
            still_blank = (
                session.state.observed_status is ObservedStatus.UNKNOWN
                and session.state.nozzle_position is NozzlePosition.UNKNOWN
            )
            if still_blank and n < 6:
                return
            # Idle Wayne is EOT-only; after a few RS attempts still send CD5/RESET
            # so a later DC1 can confirm. Do not block forever in RETURN_STATUS.

        if self._should_hold_sale_display(session):
            self._startup_reset_done.add(addr)
            if addr not in self._sale_display_held:
                self._sale_display_held.add(addr)
                print(
                    f"[OWNED-LAB addr={addr}] holding pump display until next lift "
                    "(RESET deferred)"
                )
            return
        self._sale_display_held.discard(session.address)

        if (
            flags.automatic_startup_price_programming
            and addr not in self._price_programmed
            and addr not in self._startup_price_attempted
            and self.runtime.startup_unit_price is not None
        ):
            self._startup_price_attempted.add(addr)
            payload = encode_cd5_price_update(
                prices_raw=[self.runtime.startup_unit_price]
                * self.runtime.logical_nozzle_count
            )
            result = await self._run_owned_command(
                session,
                payload,
                PumpCommand.SET_PRICE,
                idempotency=IdempotencyClass.NON_IDEMPOTENT,
            )
            print(
                f"[OWNED-LAB addr={addr}] CD5 price "
                f"{self.runtime.startup_unit_price} result={result.status.value}"
            )
            if result.status in {
                ExchangeResultStatus.LINK_ACKNOWLEDGED,
                ExchangeResultStatus.APPLICATION_CONFIRMED,
            }:
                self._price_programmed.add(addr)

        if flags.automatic_reset and addr not in self._startup_reset_done:
            if session.should_skip_reset():
                self._startup_reset_done.add(addr)
                print(f"[OWNED-LAB addr={addr}] RESET skipped (already RESET)")
            elif addr not in self._startup_reset_attempted:
                self._startup_reset_attempted.add(addr)
                result = await self._run_owned_command(
                    session,
                    encode_cd1_command(PumpControlCommand.RESET),
                    PumpCommand.RESET,
                    expect_status=ObservedStatus.RESET,
                    idempotency=IdempotencyClass.NON_IDEMPOTENT,
                )
                print(f"[OWNED-LAB addr={addr}] RESET result={result.status.value}")
                if result.status in {
                    ExchangeResultStatus.LINK_ACKNOWLEDGED,
                    ExchangeResultStatus.APPLICATION_CONFIRMED,
                }:
                    self._startup_reset_done.add(addr)

        if (
            session.state.nozzle_position is NozzlePosition.OUT
            and addr not in self._auth_this_lift
        ):
            # EXPERIMENT 2026-09-11: auto-AUTHORIZE on lift is opt-in via
            # --authorize-on-nozzle-lift. Default owned-lab waits for a manual
            # request file so lift alone cannot enable phantom delivery.
            # REVERT: enable automatic_authorization (CLI flag) again.
            manual = self._consume_manual_authorize_request(addr)
            if flags.automatic_authorization or manual:
                if manual and not flags.automatic_authorization:
                    logger.info(
                        "owned_lab_manual_authorize_requested",
                        address=addr,
                        source="authorize_request_file",
                    )
                    print(
                        f"[OWNED-LAB addr={addr}] manual AUTHORIZE request "
                        f"(authorize-{addr} file)"
                    )
                await self._owned_lab_authorize(session)
            elif addr not in self._auth_deferred_logged:
                self._auth_deferred_logged.add(addr)
                logger.info(
                    "owned_lab_authorize_deferred_no_auto_lift",
                    address=addr,
                    nozzleState=session.state.nozzle_position.value,
                    controllerState=session.state.observed_status.value,
                    detail=(
                        "Nozzle OUT; AUTHORIZE not sent (auto-lift disabled). "
                        f"Touch /var/lib/intelipump/authorize-{addr} to enable, "
                        "or restart with --authorize-on-nozzle-lift to revert."
                    ),
                )
                print(
                    f"[OWNED-LAB addr={addr}] nozzle OUT — AUTHORIZE deferred "
                    f"(no auto-lift; touch /var/lib/intelipump/authorize-{addr} "
                    "to enable delivery)"
                )

    def _consume_manual_authorize_request(self, address: int) -> bool:
        """True once if operator requested AUTHORIZE via request file.

        EXPERIMENT helper while auto-AUTHORIZE-on-lift is disabled. File is
        removed on consume so each touch is a single authorize attempt.
        """
        base = Path(
            os.environ.get("INTELIPUMP_AUTHORIZE_REQUEST_DIR", "/var/lib/intelipump")
        )
        path = base / f"authorize-{address}"
        if not path.is_file():
            return False
        try:
            path.unlink()
        except OSError as exc:
            logger.warning(
                "owned_lab_authorize_request_unlink_failed",
                address=address,
                path=str(path),
                error=str(exc),
            )
            return False
        return True

    async def _owned_lab_authorize(self, session: PumpSession) -> None:
        addr = session.address
        before_status = session.state.observed_status.value
        before_noz = session.state.nozzle_position.value
        await self._drain_pending_data(session)
        if self._bus_delays_enabled():
            await asyncio.sleep(0.08)
        latest = self._last_dc2.get(addr)
        face_nonzero = bool(latest and (latest[0] > 0 or latest[1] > 0))
        if session.state.observed_status is not ObservedStatus.RESET or face_nonzero:
            reset = await self._run_owned_command(
                session,
                encode_cd1_command(PumpControlCommand.RESET),
                PumpCommand.RESET,
                expect_status=ObservedStatus.RESET,
                idempotency=IdempotencyClass.NON_IDEMPOTENT,
                command_label="CD1_RESET_PRE_AUTH",
            )
            print(
                f"[OWNED-LAB addr={addr}] pre-auth RESET "
                f"result={reset.status.value}"
                f"{' (clear retained face)' if face_nonzero else ''}"
            )
            if reset.status not in {
                ExchangeResultStatus.LINK_ACKNOWLEDGED,
                ExchangeResultStatus.APPLICATION_CONFIRMED,
            }:
                # Do not retry endlessly on this lift.
                self._auth_this_lift.add(addr)
                return
            # Invalidate retained-sale DC2 cache. Wayne often keeps LCD totals
            # after RESET without immediately emitting a zero DC2; stale cache
            # must not permanently block AUTHORIZE.
            self._last_dc2.pop(addr, None)
            session.state.filled_volume_raw = 0
            session.state.filled_amount_raw = 0
            session.state.sale_evidence.reset_attempt()
        # Require that any *fresh* DC2 after RESET is zero. Missing DC2 after
        # RESET is normal (hold-display) and must not withhold AUTHORIZE.
        if not await self._verify_zero_meter_before_auth(session):
            logger.warning(
                "pre_auth_nonzero_meter",
                address=addr,
                volumeMinorUnits=(self._last_dc2.get(addr) or (0, 0))[0],
                amountMinorUnits=(self._last_dc2.get(addr) or (0, 0))[1],
                observedStatus=session.state.observed_status.value,
                nozzle=session.state.nozzle_position.value,
                detail="Fresh non-zero DC2 after RESET; AUTHORIZE withheld",
            )
            print(
                f"[OWNED-LAB addr={addr}] AUTHORIZE withheld — "
                f"fresh non-zero meter after RESET "
                f"(last_dc2={self._last_dc2.get(addr)})"
            )
            self._auth_this_lift.add(addr)
            return
        nozzle = session.state.logical_nozzle or 1
        cd2 = build_cd2_allowed_nozzles([nozzle])
        cd2_res = await self._run_owned_command(
            session,
            cd2.application_payload,
            PumpCommand.AUTHORIZE,
            idempotency=IdempotencyClass.NON_IDEMPOTENT,
            command_label="CD2_ALLOWED_NOZZLES",
        )
        print(f"[OWNED-LAB addr={addr}] CD2 result={cd2_res.status.value}")
        if not cd2_res.link_acknowledged:
            self._auth_this_lift.add(addr)
            return
        auth = await self._run_owned_command(
            session,
            encode_cd1_command(PumpControlCommand.AUTHORIZE),
            PumpCommand.AUTHORIZE,
            expect_status=ObservedStatus.AUTHORIZED,
            idempotency=IdempotencyClass.NON_IDEMPOTENT,
            command_label="CD1_AUTHORIZE",
        )
        print(f"[OWNED-LAB addr={addr}] AUTHORIZE result={auth.status.value}")
        logger.info(
            "owned_lab_authorize_finished",
            address=addr,
            beforeStatus=before_status,
            afterStatus=session.state.observed_status.value,
            beforeNozzle=before_noz,
            afterNozzle=session.state.nozzle_position.value,
            authResult=auth.status.value,
            volumeMinorUnits=session.state.filled_volume_raw,
            amountMinorUnits=session.state.filled_amount_raw,
        )
        # Record attempt for this lift whether or not APPLICATION_CONFIRMED —
        # avoids RESET/AUTHORIZE spam on every owned-lab tick.
        self._auth_this_lift.add(addr)

    async def _verify_zero_meter_before_auth(self, session: PumpSession) -> bool:
        """After RESET, only block AUTHORIZE on a *fresh* non-zero DC2.

        Retained LCD totals from the previous sale often remain in ``_last_dc2``
        and on the face without a new zero DC2. Stale cache must not freeze
        the hose. Fail closed only when a new DC2 frame after RESET reports
        positive volume/amount.
        """
        addr = session.address
        saw_fresh_nonzero = False
        for attempt in range(4):
            before = self._last_dc2.get(addr)
            _ws, _wc = await self._write_frame(
                session.build_poll(), address=addr, note="POLL_PRE_AUTH_ZERO"
            )
            await self._read_poll_session(session, not_before_mono=_ws)
            self._report_observed_changes(session)
            after = self._last_dc2.get(addr)
            if after is not None and after != before:
                vol, amt = after
                if vol <= 0 and amt <= 0:
                    session.state.filled_volume_raw = 0
                    session.state.filled_amount_raw = 0
                    session.state.sale_evidence.reset_attempt()
                    logger.info(
                        "pre_auth_zero_baseline_confirmed",
                        address=addr,
                        attempt=attempt,
                        volumeMinorUnits=vol,
                        amountMinorUnits=amt,
                        reason="fresh_zero_dc2",
                    )
                    return True
                if vol > 0 or amt > 0:
                    saw_fresh_nonzero = True
                    logger.info(
                        "pre_auth_waiting_zero_baseline",
                        address=addr,
                        attempt=attempt,
                        volumeMinorUnits=vol,
                        amountMinorUnits=amt,
                        reason="fresh_nonzero_dc2",
                    )
            else:
                logger.info(
                    "pre_auth_waiting_zero_baseline",
                    address=addr,
                    attempt=attempt,
                    volumeMinorUnits=(after or (None, None))[0],
                    amountMinorUnits=(after or (None, None))[1],
                    reason="no_fresh_dc2_after_reset",
                )
            if self._bus_delays_enabled():
                await asyncio.sleep(0.05)
        if saw_fresh_nonzero:
            return False
        session.state.filled_volume_raw = 0
        session.state.filled_amount_raw = 0
        session.state.sale_evidence.reset_attempt()
        logger.info(
            "pre_auth_zero_baseline_confirmed",
            address=addr,
            volumeMinorUnits=0,
            amountMinorUnits=0,
            reason="reset_without_fresh_nonzero_dc2",
        )
        return True

    async def _run_owned_command(
        self,
        session: PumpSession,
        payload: bytes,
        command_type: PumpCommand,
        *,
        expect_status: ObservedStatus | None = None,
        idempotency: IdempotencyClass,
        command_label: str | None = None,
    ) -> ExchangeResult:
        label = command_label or command_type.value
        item = OutboundDataItem.create(
            address=session.address,
            application_payload=payload,
            command_type=command_type,
            simulator_only=False,
            idempotency=idempotency,
            ttl_ms=30_000,
            max_retries=0,
            expect_status_after_tx=(
                expect_status.value if expect_status is not None else None
            ),
        )
        decision = evaluate_outbound_safety(item, self.runtime.safety)
        if not decision.allowed:
            print(
                f"[OWNED-LAB addr={session.address}] blocked {command_type.value}: "
                f"{','.join(decision.reasons)}"
            )
            return ExchangeResult(
                status=ExchangeResultStatus.REJECTED,
                address=session.address,
                detail=",".join(decision.reasons),
            )
        self.runtime.outbound.drop_for_address(session.address)
        logger.info(
            "command_sent",
            address=session.address,
            command=label,
            commandType=command_type.value,
            rawHex=payload.hex(" "),
            correlationId=item.correlation_id,
            expectStatus=expect_status.value if expect_status else None,
            controllerState=session.state.observed_status.value,
            nozzleState=session.state.nozzle_position.value,
            mono=time.monotonic(),
        )
        try:
            self.runtime.outbound.enqueue(item, self.runtime.safety)
        except OutboundRejectedError as exc:
            return ExchangeResult(
                status=ExchangeResultStatus.REJECTED,
                address=session.address,
                detail=",".join(exc.reasons),
            )
        result = await self._maybe_send_outbound(session)
        if result is None:
            logger.warning(
                "command_timed_out",
                address=session.address,
                command=label,
                correlationId=item.correlation_id,
            )
            return ExchangeResult(
                status=ExchangeResultStatus.REJECTED,
                address=session.address,
                detail="outbound_not_sent",
            )
        if result.status is ExchangeResultStatus.LINK_ACKNOWLEDGED:
            logger.info(
                "command_link_acknowledged",
                address=session.address,
                command=label,
                correlationId=item.correlation_id,
                sequence=result.sequence,
            )
        elif result.status is ExchangeResultStatus.APPLICATION_CONFIRMED:
            logger.info(
                "command_application_confirmed",
                address=session.address,
                command=label,
                correlationId=item.correlation_id,
                sequence=result.sequence,
                controllerState=session.state.observed_status.value,
            )
        elif result.status in {
            ExchangeResultStatus.TIMED_OUT,
            ExchangeResultStatus.REJECTED,
        }:
            logger.warning(
                "command_timed_out" if result.status is ExchangeResultStatus.TIMED_OUT else "command_rejected",
                address=session.address,
                command=label,
                correlationId=item.correlation_id,
                detail=result.detail,
                status=result.status.value,
            )
        return result

    async def _write_frame(self, data: bytes, *, address: int, note: str) -> tuple[float, float]:
        """Write frame; return (write_start, write_complete) monotonic timestamps."""
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
        if self._log_tx_note(note):
            print(f"TX [{note}] addr={address} {data.hex(' ')}")
        if self._log_all_bus() and self.runtime.log_dart_timing:
            print(
                f"TX timing [{note}] addr={address} "
                f"start={write_start_s:.6f} complete={write_complete_s:.6f}"
            )
        return write_start_s, write_complete_s

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
                    "filled_volume_raw": s.state.filled_volume_raw,
                    "filled_amount_raw": s.state.filled_amount_raw,
                    "unit_price_raw": s.state.unit_price_raw,
                    "last_completed_sale": self._last_completed_sale.get(addr),
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
