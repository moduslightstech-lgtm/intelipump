"""Async polling scheduler and controller loop."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable

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
    # After a completed sale with nozzle hung up, keep totals on the face.
    # Negative = until next lift (default for SAO / POS-like display hold).
    # Non-negative = hold N seconds then RESET while hung (faster next-lift
    # AUTHORIZE, but clears the face before the next lift).
    sale_display_hold_seconds: float = -1.0
    # Attended hardware meter canary (from MeterReadingSettings). Default off.
    meter_hardware_cd101: bool = False
    meter_counter_select: int = 1
    meter_volume_decimals: int | None = None
    meter_response_timeout_s: float = 8.0
    meter_min_interval_s: float = 60.0
    meter_nozzle_in_max_age_s: float = 30.0
    meter_post_timeout_quarantine_s: float = 8.0
    meter_channel_map: dict | None = None
    meter_startup_capture_enabled: bool = False
    meter_startup_capture_timezone: str = "Africa/Lagos"
    meter_startup_capture_settle_s: float = 20.0
    meter_startup_capture_window_s: float = 1800.0


class ControllerLoop:
    """Round-robin DART polling over an abstract transport."""

    def __init__(self, runtime: ControllerRuntime) -> None:
        self.runtime = runtime
        self._meter_pending: dict | None = None
        self._meter_last_attempt_mono: dict[int, float] = {}
        self._meter_loop_started_mono: float | None = None
        self._meter_startup_window_closed: bool = False
        # After DEFERRED / quarantine, wait before re-queuing the same OPENING addr.
        self._meter_startup_next_try_mono: dict[int, float] = {}
        self._meter_startup_defer_backoff_s: float = 15.0
        # METER_DC101_TIMEOUT often means a pre-TX stale DC101 was ignored; retry.
        self._meter_startup_timeout_attempts: dict[int, int] = {}
        self._meter_startup_timeout_max_attempts: int = 3
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
        # Persist bridge binds hose UUID → keep live SM aligned for hang-up.
        runtime.events.add_subscriber(self._on_sale_identity_event)
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
        self._sale_display_hold_since: dict[int, float] = {}
        # Addresses whose completed-sale durable handoff is still in flight.
        self._sale_handoff_pending: set[int] = set()
        # addr → persist identity currently gating RESET (per-sale association).
        self._sale_handoff_identity: dict[int, str] = {}
        # Optional: PersistenceWorker.is_handoff_pending by identity, set by CLI.
        self._sale_handoff_blocker: Callable[[int], bool] | None = None
        self._startup_price_attempted: set[int] = set()
        self._startup_reset_attempted: set[int] = set()
        self._bus_silent_warned: set[int] = set()
        self._price_programmed: set[int] = set()
        # Per-address startup CD5 restore (durable unit-price.json target).
        # States: pending | deferred | awaiting_verify | verified | failed
        self._startup_price_state: dict[int, str] = {}
        self._startup_price_fail_count: dict[int, int] = {}
        self._startup_price_next_try: dict[int, float] = {}
        self._startup_price_dc3_baseline: dict[int, int] = {}
        self._startup_price_verify_deadline: dict[int, float] = {}
        self._startup_price_max_attempts: int = 6
        self._startup_price_dc3_timeout_s: float = 30.0
        self._startup_reset_done: set[int] = set()
        self._auth_this_lift: set[int] = set()
        self._auth_deferred_logged: set[int] = set()
        self._armed_for_lift: set[int] = set()
        self._rs_poll_counter: dict[int, int] = {}
        # correlationId -> dart addresses that already got this cloud CD5
        self._cloud_set_price_applied: dict[str, set[int]] = {}
        self._cloud_set_price_gave_up: dict[str, set[int]] = {}
        # LINK_ACK sent; waiting for matching DC3 (idle DC3 may report 0).
        self._cloud_set_price_awaiting_dc3: dict[str, set[int]] = {}
        # LINK_ACK received but DC3 never matched within the bound.
        self._cloud_set_price_unverified: dict[str, set[int]] = {}
        # Addresses where LINK_ACK/APP_CONFIRMED seeded provisional face (idle DC3=0).
        self._cloud_set_price_face_seeded: dict[str, set[int]] = {}
        # corr:addr -> unit_price_obs_gen at LINK_ACK (confirm only when gen increases)
        self._cloud_set_price_dc3_baseline: dict[str, int] = {}
        # corr:addr -> monotonic deadline to stop awaiting DC3
        self._cloud_set_price_dc3_deadline: dict[str, float] = {}
        # corr:addr -> read-only verification poll attempts after LINK_ACK
        self._cloud_set_price_verify_reads: dict[str, int] = {}
        self._cloud_set_price_fail_count: dict[str, int] = {}
        self._cloud_set_price_next_try: dict[str, float] = {}
        self._set_price_defer_log_at: dict[str, float] = {}
        self._set_price_defer_last_reason: dict[str, str] = {}
        self._cloud_set_price_retry_log_at: float = 0.0
        self._cloud_set_price_zero_dc3_log_at: dict[str, float] = {}
        # Bound wait for fresh DC3 after LINK_ACK (read-only; no CD5 resend on 0).
        self._cloud_set_price_dc3_timeout_s: float = 30.0
        self._cloud_set_price_verify_max_reads: int = 8
        # First monotonic sighting of a correlationId (TTL uses requestedAt too).
        self._cloud_set_price_seen_at: dict[str, float] = {}
        # Rate-limit RETURN_STATUS refresh per dart address.
        self._set_price_refresh_at: dict[int, float] = {}
        self._set_price_refresh_min_interval_s: float = 2.0
        # NOZIO/status observation older than this is stale safety evidence.
        self._set_price_nozio_fresh_s: float = 15.0
        self._set_price_status_fresh_s: float = 15.0
        # Matches DigitalTwin MQTT_COMMAND_TTL_SECONDS default.
        self._set_price_defer_timeout_s: float = float(
            os.environ.get("INTELIPUMP_SET_PRICE_DEFER_TIMEOUT_S") or 120.0
        )
        # corr -> unit_price_raw successfully written to unit-price.json.
        # Waiting for sibling addresses or cloud outcome ACK must not rewrite.
        self._cloud_set_price_unit_persisted: dict[str, int] = {}
        self._cloud_set_price_persist_fail_count: dict[str, int] = {}
        self._cloud_set_price_persist_next_try: dict[str, float] = {}
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
                await self._apply_pending_cloud_set_price()
                await self._maybe_queue_startup_meter_capture()
                await self._apply_pending_meter_read()
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
            # last_byte matches command-response path; first_byte can predate
            # CD101 TX when demux was mid-frame and break meter correlation.
            ack = session.handle_response_frame(
                frame, capture_mono=stamped.last_byte_time
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

        # Meter CD101: re-check eligibility immediately before TX. If the nozzle
        # lifted (or sale woke up) while queued, cancel without transmitting.
        if item.command_type is PumpCommand.READ_METER:
            if not self._meter_tx_still_allowed(session, item):
                return ExchangeResult(
                    status=ExchangeResultStatus.REJECTED,
                    address=session.address,
                    correlation_id=item.correlation_id,
                    detail="meter_read_cancelled_before_tx",
                )

        seq = item.sequence if item.sequence is not None else session.state.tx_sequence
        attempts = item.attempts
        exchange_start: float | None = None
        while True:
            if item.command_type is PumpCommand.READ_METER:
                self._meter_mark_tx_started(item, write_mono=time.monotonic())
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
            f"[CMD addr={session.address}] polling after "
            f"{item.command_type.value} (no link ACK yet)"
        )
        await self._drain_pending_data(session)
        expected = item.expect_status_after_tx
        if expected is not None:
            # After display-hold RESET/AUTHORIZE, Wayne often skips link ACK but
            # already shows the expected DC1 in the demux (sometimes stamped
            # just before write_complete). Accept that immediately — do not burn
            # application_confirm_max_polls (~2s) failing a freshness check first.
            if session.state.observed_status.value == expected:
                print(
                    f"[OWNED-LAB addr={session.address}] "
                    f"{item.command_type.value} confirmed by poll DC1"
                )
                return ExchangeResult(
                    status=ExchangeResultStatus.APPLICATION_CONFIRMED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    detail="confirmed_by_observed_status",
                    write_start_mono=not_before,
                )
            # One forced poll before the confirm budget — soft-stale RESET/AUTH
            # frames often land in the demux right as ACK wait ends.
            write_start, _complete = await self._write_frame(
                session.build_poll(),
                address=session.address,
                note="POLL_CONFIRM",
            )
            await self._read_poll_session(session, not_before_mono=write_start)
            await self._drain_pending_data(session)
            if session.state.observed_status.value == expected:
                print(
                    f"[OWNED-LAB addr={session.address}] "
                    f"{item.command_type.value} confirmed by poll DC1"
                )
                return ExchangeResult(
                    status=ExchangeResultStatus.APPLICATION_CONFIRMED,
                    address=session.address,
                    sequence=seq,
                    correlation_id=item.correlation_id,
                    detail="confirmed_by_observed_status",
                    write_start_mono=not_before,
                )
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
            return None

        # CD2 / CD5: this head often skips ACK and answers on the next POLL.
        # One quiet poll is enough — do not burn application_confirm_max_polls
        # (and falsely TIMED_OUT cloud SET_PRICE) waiting for a link ACK.
        if self._is_cd2_payload(item.application_payload) or self._is_set_price_item(
            item
        ):
            assumed_detail = (
                "cd2_assumed_after_poll"
                if self._is_cd2_payload(item.application_payload)
                else "cd5_assumed_after_poll"
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
            return ExchangeResult(
                status=ExchangeResultStatus.LINK_ACKNOWLEDGED,
                address=session.address,
                sequence=seq,
                correlation_id=item.correlation_id,
                detail=assumed_detail,
                write_start_mono=not_before,
            )

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
        # DATA commands need the longer ACK window; poll EOT uses response_timeout_ms.
        timeout_ms = int(
            getattr(
                self.runtime.config,
                "command_response_timeout_ms",
                self.runtime.config.response_timeout_ms,
            )
        )
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
                met = self._command_status_met(
                    session,
                    item,
                    command_tx_mono=write_complete,
                    write_start_mono=write_start,
                    sequence=seq,
                    preserved=preserved,
                )
                if met is not None:
                    return met
                # Wayne often answers CD5 with DATA (no link ACK). Treat fresh
                # post-TX DATA as link acceptance so cloud SET_PRICE does not
                # false-TIMED_OUT while the pump already applied the price.
                if self._is_set_price_item(item):
                    return ExchangeResult(
                        status=ExchangeResultStatus.LINK_ACKNOWLEDGED,
                        address=session.address,
                        sequence=seq,
                        correlation_id=item.correlation_id,
                        command_tx_mono=write_complete,
                        write_start_mono=write_start,
                        detail="data_after_set_price",
                        preserved_event_count=preserved,
                    )
                return None

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

    def _is_set_price_item(self, item: OutboundDataItem) -> bool:
        if item.command_type is PumpCommand.SET_PRICE:
            return True
        payload = item.application_payload
        return len(payload) >= 1 and payload[0] == 0x05

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
        # Status-expecting commands must see DC1 observed after this TX started.
        # Prefer write_start over write_complete: Wayne often overlaps the reply
        # with our TX (last_byte < write_complete) — that is still a fresh reply
        # to this command, not a pre-command stale face.
        if item.expect_status_after_tx is not None:
            floor = (
                write_start_mono
                if write_start_mono is not None
                else command_tx_mono
            )
            if floor is None or not session.status_observed_after(
                ObservedStatus(item.expect_status_after_tx),
                not_before_mono=floor,
            ):
                # Soft-stale but already showing expected status: accept when the
                # demux status matches (display-hold RESET/AUTHORIZE path).
                if not (
                    frame_is_stale
                    and session.state.observed_status.value
                    == item.expect_status_after_tx
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
                        write_start_mono=write_start_mono,
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
        """Poll until expected DC1 is observed strictly after command TX time.

        Also accept a matching ``observed_status`` without the freshness floor:
        after display-hold RESET/AUTHORIZE, Wayne often replies with a frame
        whose last-byte time slightly precedes write_complete (soft-stale). The
        status is already correct — waiting out max_polls (~2s) only adds lag.
        """
        max_polls = self.runtime.config.application_confirm_max_polls
        for _ in range(max_polls):
            if session.status_observed_after(expected, not_before_mono=not_before_mono):
                return True
            if session.state.observed_status is expected:
                return True
            _write_start, _write_complete = await self._write_frame(
                session.build_poll(), address=session.address, note="POLL_CONFIRM"
            )
            await self._read_poll_session(session, not_before_mono=_write_start)
            if session.status_observed_after(expected, not_before_mono=not_before_mono):
                return True
            if session.state.observed_status is expected:
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
                # Keep arm across hang-up so operator can arm while nozzle IN,
                # then lift to AUTHORIZE. Arm clears on successful authorize.
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
                    self._capture_completed_sale_snapshot(session)
                    # Hold RESET until durable persist handoff completes.
                    self._sale_handoff_pending.add(addr)
            self._last_sale[addr] = life

    def set_sale_handoff_blocker(self, blocker: Callable[[int], bool] | None) -> None:
        """Block RESET while ``blocker(addr)`` is True (durable handoff pending)."""
        self._sale_handoff_blocker = blocker

    def _on_sale_identity_event(self, event: ControllerEvent) -> None:
        """Adopt / clear durable sale UUID on the live session (bridge-driven)."""
        if event.address is None:
            return
        session = self.sessions.get(event.address)
        if session is None:
            return
        payload = event.payload or {}
        if event.type is ControllerEventType.SALE_IDENTITY_BOUND:
            tx = payload.get("transaction_uuid") or event.detail
            if isinstance(tx, str) and tx.strip():
                session.bind_durable_sale_identity(tx.strip())
        elif event.type is ControllerEventType.SALE_IDENTITY_CLEARED:
            expected = payload.get("transaction_uuid")
            session.clear_durable_sale_identity(
                expected=str(expected) if isinstance(expected, str) else None
            )

    def note_sale_handoff_identity(self, addr: int, identity_key: str) -> None:
        """Bind the persist identity that currently gates RESET for ``addr``."""
        self._sale_handoff_pending.add(addr)
        self._sale_handoff_identity[addr] = identity_key

    def mark_sale_handoff_durable(
        self, addr: int, *, identity_key: str | None = None
    ) -> None:
        """Release RESET gate for ``addr``.

        When ``identity_key`` is provided, only release if it matches the bound
        sale (or no identity is bound yet). A different sale must not clear
        another sale's gate.
        """
        if identity_key is not None:
            bound = self._sale_handoff_identity.get(addr)
            if bound is not None and bound != identity_key:
                return
            self._sale_handoff_identity[addr] = identity_key
        self._sale_handoff_pending.discard(addr)
        self._sale_handoff_identity.pop(addr, None)

    def _sale_reset_blocked_by_handoff(self, addr: int) -> bool:
        if self._sale_handoff_blocker is not None and self._sale_handoff_blocker(addr):
            return True
        return addr in self._sale_handoff_pending

    def _should_hold_sale_display(self, session: PumpSession) -> bool:
        """Keep FILLING_COMPLETED totals on the pump after hang-up.

        Default (negative ``sale_display_hold_seconds``): hold until the next
        lift; RESET runs as part of AUTHORIZE-on-lift (face stays until lift).
        The lift path confirms RESET/AUTHORIZE from observed DC1 immediately
        when Wayne skips link ACK, so the motor still comes up quickly.

        Non-negative: timed hold, then RESET while hung for a faster next lift
        (face clears before lift). Do not hold after zero-delivery hang-up.
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
        if not session.state.sale_evidence.has_positive_delivery:
            return False
        hold_s = float(self.runtime.sale_display_hold_seconds)
        if hold_s < 0:
            return True
        addr = session.address
        now = time.monotonic()
        started = self._sale_display_hold_since.get(addr)
        if started is None:
            self._sale_display_hold_since[addr] = now
            return hold_s > 0
        return (now - started) < hold_s

    async def _reset_after_sale_display_hold(self, session: PumpSession) -> None:
        """Clear retained face while hung up so the next lift is authorize-fast."""
        addr = session.address
        if self._sale_reset_blocked_by_handoff(addr):
            logger.info(
                "sale_reset_deferred_awaiting_durable_handoff",
                address=addr,
                detail="RESET held until persist recovery write-ahead completes",
            )
            return
        reset = await self._run_owned_command(
            session,
            encode_cd1_command(PumpControlCommand.RESET),
            PumpCommand.RESET,
            expect_status=ObservedStatus.RESET,
            idempotency=IdempotencyClass.NON_IDEMPOTENT,
            command_label="CD1_RESET_AFTER_SALE_HOLD",
        )
        print(
            f"[OWNED-LAB addr={addr}] post-sale RESET after display hold "
            f"result={reset.status.value}"
        )
        if reset.status in {
            ExchangeResultStatus.LINK_ACKNOWLEDGED,
            ExchangeResultStatus.APPLICATION_CONFIRMED,
        }:
            self._startup_reset_done.add(addr)
            self._last_dc2.pop(addr, None)
            session.state.filled_volume_raw = 0
            session.state.filled_amount_raw = 0
            session.state.sale_evidence.reset_attempt()
            await self._apply_pending_cloud_set_price()

    async def _owned_lab_tick(self, session: PumpSession) -> None:
        flags = self.runtime.feature_flags
        if not self.runtime.safety.owned_lab_active_session:
            return
        addr = session.address
        # Pick up arm-<addr> as soon as the operator creates it (often while IN).
        self._refresh_arm_requests()
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
                hold_s = float(self.runtime.sale_display_hold_seconds)
                if hold_s < 0:
                    print(
                        f"[OWNED-LAB addr={addr}] holding pump display until next lift "
                        "(RESET deferred)"
                    )
                else:
                    print(
                        f"[OWNED-LAB addr={addr}] holding pump display for "
                        f"{hold_s:.0f}s then RESET while hung "
                        "(next lift AUTHORIZE-fast)"
                    )
            return

        was_holding = addr in self._sale_display_held
        self._sale_display_held.discard(addr)
        self._sale_display_hold_since.pop(addr, None)
        if (
            was_holding
            and session.state.nozzle_position is NozzlePosition.IN
            and session.state.observed_status
            in {
                ObservedStatus.FILLING_COMPLETED,
                ObservedStatus.MAX_AMOUNT_VOLUME_REACHED,
            }
        ):
            await self._reset_after_sale_display_hold(session)
            return

        if flags.automatic_startup_price_programming:
            await self._maybe_startup_price_restore(session)

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
            # Production owned-lab: AUTHORIZE on lift when automatic_authorization
            # is on. Optional arm-<addr> / authorize-<addr> still work when
            # auto-lift is disabled (--no-authorize-on-nozzle-lift).
            self._refresh_arm_requests()
            manual = self._consume_manual_authorize_request(addr)
            armed = addr in self._armed_for_lift
            if flags.automatic_authorization or manual or armed:
                if armed and not flags.automatic_authorization and not manual:
                    self._armed_for_lift.discard(addr)
                    logger.info(
                        "owned_lab_armed_authorize_on_lift",
                        address=addr,
                        source="arm_request_file",
                    )
                    print(
                        f"[OWNED-LAB addr={addr}] armed AUTHORIZE on lift "
                        f"(arm-{addr} consumed)"
                    )
                elif manual and not flags.automatic_authorization:
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
                        f"Arm first: touch /var/lib/intelipump/arm-{addr}, "
                        f"or authorize now: touch /var/lib/intelipump/authorize-{addr}."
                    ),
                )
                print(
                    f"[OWNED-LAB addr={addr}] nozzle OUT — AUTHORIZE deferred "
                    f"(arm with /var/lib/intelipump/arm-{addr}, then lift; "
                    f"or touch authorize-{addr} while OUT)"
                )

    def _authorize_request_dir(self) -> Path:
        return Path(
            os.environ.get("INTELIPUMP_AUTHORIZE_REQUEST_DIR", "/var/lib/intelipump")
        )

    def _startup_price_target(self) -> int | None:
        """Durable dashboard price preferred over stale CLI ``startup_unit_price``."""
        from intelipump_fdc.cloud.set_price_request import read_persisted_unit_price

        persisted = read_persisted_unit_price()
        if (
            persisted is not None
            and persisted.unit_price_raw > 0
            and persisted.unit_price_raw != self.runtime.startup_unit_price
        ):
            logger.info(
                "owned_lab_startup_price_deferred_to_persisted",
                cliStartupPrice=self.runtime.startup_unit_price,
                persistedUnitPriceRaw=persisted.unit_price_raw,
                persistedSource=persisted.source,
            )
            self.runtime.startup_unit_price = persisted.unit_price_raw
        elif (
            persisted is not None
            and persisted.unit_price_raw > 0
            and self.runtime.startup_unit_price is None
        ):
            self.runtime.startup_unit_price = persisted.unit_price_raw
        price = self.runtime.startup_unit_price
        if isinstance(price, int) and price > 0:
            return price
        return None

    def _startup_price_restore_unnecessary(
        self, session: PumpSession, target: int
    ) -> tuple[bool, str]:
        """Skip CD5 only with fresh observation evidence the pump is programmed.

        Matching ``unit_price_raw`` alone is insufficient: Wayne can retain a
        prior DC3 face while DC1 reports ``NOT_PROGRAMMED``. Freshness uses
        ``last_nozio_time`` / ``last_status_time`` ages (not sticky caches).
        Completed-sale / handoff / LCD-hold states never count as skip — they
        must flow through ``_set_price_defer_reason`` like cloud SET_PRICE.
        """
        addr = session.address
        status = session.state.observed_status
        if status is ObservedStatus.NOT_PROGRAMMED:
            return False, "not_programmed"
        if status is ObservedStatus.UNKNOWN:
            return False, "status_unknown"
        if status in {
            ObservedStatus.AUTHORIZED,
            ObservedStatus.FILLING,
            ObservedStatus.SUSPENDED,
        }:
            return False, "busy_live_sale"
        if addr in self._sale_display_held:
            return False, "sale_display_held"
        if addr in self._sale_handoff_pending:
            return False, "sale_handoff_pending"
        if self._sale_reset_blocked_by_handoff(addr):
            return False, "sale_handoff_pending"
        ev = session.state.sale_evidence
        if ev.has_positive_delivery and not (
            ev.sale_published or addr in self._last_completed_sale
        ):
            return False, "sale_unpersisted"
        if session.state.unit_price_raw != target:
            return False, "face_mismatch"
        if self._set_price_evidence_stale(session):
            return False, "observation_stale"
        if status is ObservedStatus.RESET:
            return True, "fresh_reset_face_match"
        # FILLING_COMPLETED / LCD hold: never "skip as done" — defer until idle
        # RESET so we do not compete with sale finalization / display hold / RESET.
        if status in {
            ObservedStatus.FILLING_COMPLETED,
            ObservedStatus.MAX_AMOUNT_VOLUME_REACHED,
        }:
            return False, "sale_face_held"
        return False, f"status_{status.value.lower()}"

    def _startup_price_mark(
        self, addr: int, state: str, *, reason: str, **extra: object
    ) -> None:
        prev = self._startup_price_state.get(addr)
        self._startup_price_state[addr] = state
        logger.info(
            "startup_price_restore",
            address=addr,
            state=state,
            previousState=prev,
            reason=reason,
            **extra,
        )

    def _startup_price_schedule_retry(self, addr: int, *, reason: str) -> None:
        fails = int(self._startup_price_fail_count.get(addr, 0)) + 1
        self._startup_price_fail_count[addr] = fails
        if fails >= self._startup_price_max_attempts:
            self._startup_price_mark(
                addr,
                "failed",
                reason=reason,
                failCount=fails,
                exhausted=True,
            )
            # Keep next_try far out; do not mark _price_programmed.
            self._startup_price_next_try[addr] = time.monotonic() + 300.0
            return
        delay = min(60.0, float(2 ** min(fails, 5)))
        self._startup_price_next_try[addr] = time.monotonic() + delay
        self._startup_price_mark(
            addr,
            "failed",
            reason=reason,
            failCount=fails,
            retryInS=delay,
            exhausted=False,
        )

    def _startup_price_mark_verified(
        self, session: PumpSession, target: int, *, reason: str
    ) -> None:
        addr = session.address
        self._price_programmed.add(addr)
        self._startup_price_attempted.add(addr)
        self._startup_price_fail_count.pop(addr, None)
        self._startup_price_next_try.pop(addr, None)
        self._startup_price_dc3_baseline.pop(addr, None)
        self._startup_price_verify_deadline.pop(addr, None)
        self._startup_price_mark(
            addr,
            "verified",
            reason=reason,
            unitPriceRaw=target,
            observedStatus=session.state.observed_status.value,
            faceUnitPriceRaw=session.state.unit_price_raw,
        )

    def _startup_price_check_awaiting_verify(
        self, session: PumpSession, target: int
    ) -> bool:
        """True while in awaiting_verify (no CD5 resend until timeout/retry)."""
        addr = session.address
        if self._startup_price_state.get(addr) != "awaiting_verify":
            return False
        deadline = self._startup_price_verify_deadline.get(addr)
        now = time.monotonic()
        status = session.state.observed_status
        face = session.state.unit_price_raw
        baseline = self._startup_price_dc3_baseline.get(addr)
        gen = int(getattr(session.state, "unit_price_obs_gen", 0) or 0)
        left_unprogrammed = status is not ObservedStatus.NOT_PROGRAMMED
        face_ok = face == target
        gen_advanced = baseline is not None and gen > int(baseline)
        fresh = not self._set_price_evidence_stale(session)
        # Prefer DC3 generation advance; allow RESET only with fresh observations.
        verified = False
        verify_reason = ""
        if left_unprogrammed and face_ok and gen_advanced and fresh:
            verified = True
            verify_reason = "post_link_ack_dc3_gen"
        elif (
            left_unprogrammed
            and face_ok
            and status is ObservedStatus.RESET
            and fresh
        ):
            verified = True
            verify_reason = "post_link_ack_fresh_reset"
        if verified:
            self._startup_price_mark_verified(
                session, target, reason=verify_reason
            )
            return True
        if deadline is not None and now >= deadline:
            self._startup_price_dc3_baseline.pop(addr, None)
            self._startup_price_verify_deadline.pop(addr, None)
            self._startup_price_schedule_retry(
                addr, reason="link_ack_verify_timeout"
            )
            return True
        # Rate-limit waiting logs (still no CD5 on this tick).
        throttle_key = f"startup_await:{addr}"
        last = self._set_price_defer_log_at.get(throttle_key)
        if last is None or (now - last) >= 30.0:
            self._set_price_defer_log_at[throttle_key] = now
            logger.info(
                "startup_price_restore",
                address=addr,
                state="awaiting_verify",
                reason="waiting_application_or_dc3",
                unitPriceRaw=target,
                observedStatus=status.value,
                faceUnitPriceRaw=face,
                unitPriceObsGen=gen,
                baselineObsGen=baseline,
                observationFresh=fresh,
                deadlineInS=None if deadline is None else round(deadline - now, 2),
            )
        return True

    async def _maybe_startup_price_restore(self, session: PumpSession) -> None:
        """Per-address CD5 restore from durable saved price after controller restart.

        Cloud ``set-price-request.json`` always wins the bus. Matching cached face
        price does not prove the pump is programmed when DC1 is NOT_PROGRAMMED.
        LINK_ACK alone is not verification — await status/DC3 evidence.
        """
        from intelipump_fdc.cloud.set_price_request import read_set_price_request

        addr = session.address
        target = self._startup_price_target()
        if target is None:
            return

        # Invalid sticky "programmed" if pump still reports NOT_PROGRAMMED.
        if (
            addr in self._price_programmed
            and session.state.observed_status is ObservedStatus.NOT_PROGRAMMED
        ):
            self._price_programmed.discard(addr)
            self._startup_price_mark(
                addr,
                "pending",
                reason="reopened_not_programmed_despite_cached_flag",
                unitPriceRaw=target,
                faceUnitPriceRaw=session.state.unit_price_raw,
            )

        if read_set_price_request() is not None:
            self._startup_price_mark(
                addr,
                "deferred",
                reason="cloud_set_price_pending",
                unitPriceRaw=target,
            )
            return

        if self._startup_price_check_awaiting_verify(session, target):
            return

        if addr in self._price_programmed or self._startup_price_state.get(addr) == "verified":
            return

        unnecessary, why = self._startup_price_restore_unnecessary(session, target)
        if unnecessary:
            self._startup_price_mark_verified(session, target, reason=f"skip_{why}")
            return

        now = time.monotonic()
        next_try = self._startup_price_next_try.get(addr)
        if next_try is not None and now < next_try:
            self._startup_price_mark(
                addr,
                "deferred",
                reason="backoff",
                unitPriceRaw=target,
                retryInS=round(next_try - now, 2),
                evidenceGap=why,
            )
            return

        # Do not CD5 (or treat as verified) while a completed sale face is held —
        # preserve finalization / persistence / RESET sequencing.
        if session.state.observed_status in {
            ObservedStatus.FILLING_COMPLETED,
            ObservedStatus.MAX_AMOUNT_VOLUME_REACHED,
        } or addr in self._sale_display_held:
            self._startup_price_mark(
                addr,
                "deferred",
                reason="eligible_sale_face_held",
                unitPriceRaw=target,
                evidenceGap=why,
                observedStatus=session.state.observed_status.value,
                nozzleState=session.state.nozzle_position.value,
            )
            return

        defer = self._set_price_defer_reason(addr, session)
        if defer is not None:
            self._startup_price_mark(
                addr,
                "deferred",
                reason=f"eligible_{defer}",
                unitPriceRaw=target,
                evidenceGap=why,
                observedStatus=session.state.observed_status.value,
                nozzleState=session.state.nozzle_position.value,
            )
            return

        if (
            self._startup_price_fail_count.get(addr, 0)
            >= self._startup_price_max_attempts
        ):
            self._startup_price_mark(
                addr,
                "failed",
                reason="attempts_exhausted",
                unitPriceRaw=target,
                failCount=self._startup_price_fail_count.get(addr),
            )
            return

        self._startup_price_attempted.add(addr)
        self._startup_price_mark(
            addr,
            "pending",
            reason="attempt_cd5",
            unitPriceRaw=target,
            evidenceGap=why,
            observedStatus=session.state.observed_status.value,
            faceUnitPriceRaw=session.state.unit_price_raw,
            failCount=self._startup_price_fail_count.get(addr, 0),
        )
        payload = encode_cd5_price_update(
            prices_raw=[target] * self.runtime.logical_nozzle_count
        )
        result = await self._run_owned_command(
            session,
            payload,
            PumpCommand.SET_PRICE,
            idempotency=IdempotencyClass.NON_IDEMPOTENT,
        )
        print(
            f"[OWNED-LAB addr={addr}] CD5 price {target} result={result.status.value}"
        )
        if result.status is ExchangeResultStatus.APPLICATION_CONFIRMED:
            self._note_command_price_lifecycle(
                session,
                unit_price_raw=target,
                link_acked=True,
                application_confirmed=True,
            )
            self._startup_price_mark_verified(
                session, target, reason="application_confirmed"
            )
            return
        if result.status is ExchangeResultStatus.LINK_ACKNOWLEDGED:
            self._note_command_price_lifecycle(
                session,
                unit_price_raw=target,
                link_acked=True,
                application_confirmed=False,
            )
            self._startup_price_dc3_baseline[addr] = int(
                getattr(session.state, "unit_price_obs_gen", 0) or 0
            )
            self._startup_price_verify_deadline[addr] = (
                time.monotonic() + self._startup_price_dc3_timeout_s
            )
            self._startup_price_mark(
                addr,
                "awaiting_verify",
                reason="link_ack_not_application_confirm",
                unitPriceRaw=target,
                baselineObsGen=self._startup_price_dc3_baseline[addr],
                verifyTimeoutS=self._startup_price_dc3_timeout_s,
            )
            return
        self._startup_price_schedule_retry(
            addr, reason=f"exchange_{result.status.value.lower()}"
        )

    @staticmethod
    def _note_command_price_lifecycle(
        session: PumpSession,
        *,
        unit_price_raw: int,
        link_acked: bool = False,
        application_confirmed: bool = False,
    ) -> bool:
        """Record CD5 lifecycle and keep a provisional sale face price.

        Positive DC3 still overwrites ``unit_price_raw`` (pump_session). When
        commanded differs from the last observation, clear the stale face so
        sales cannot inherit the old price across a change. After link-ack or
        application-confirm, seed the commanded face when observation is
        missing: idle Wayne DC3 often reports 0 after CD5, and without this
        hang-up sales publish ``raw_price=null`` → cloud Unit price Unknown.
        Amount÷volume estimates remain non-authoritative elsewhere.

        Returns True when this call provisionally seeded ``unit_price_raw``.
        """
        if not isinstance(unit_price_raw, int) or unit_price_raw <= 0:
            return False
        session.state.requested_unit_price_raw = unit_price_raw
        if link_acked:
            session.state.link_acked_unit_price_raw = unit_price_raw
        if application_confirmed:
            session.state.application_confirmed_unit_price_raw = unit_price_raw
        observed = session.state.unit_price_raw
        if (
            isinstance(observed, int)
            and not isinstance(observed, bool)
            and observed > 0
            and observed != unit_price_raw
        ):
            session.state.unit_price_raw = None
            observed = None
        seeded = False
        if link_acked or application_confirmed:
            if not (
                isinstance(observed, int)
                and not isinstance(observed, bool)
                and observed > 0
            ):
                session.state.unit_price_raw = unit_price_raw
                seeded = True
        return seeded

    def _capture_completed_sale_snapshot(self, session: PumpSession) -> bool:
        """Record completed-sale amount/volume/identity before any RESET.

        Returns False when a positive-delivery completed sale cannot be
        captured; callers must not RESET (that would wipe the pump face).
        """
        ev = session.state.sale_evidence
        life = session.state.sale_lifecycle
        status = session.state.observed_status
        completed = life in {
            SaleLifecycle.FILLING_COMPLETED,
            SaleLifecycle.CLOSED,
        } or status in {
            ObservedStatus.FILLING_COMPLETED,
            ObservedStatus.MAX_AMOUNT_VOLUME_REACHED,
        }
        if not completed:
            return True
        session._resync_amount_scale_with_price()
        vol = ev.peak_volume_raw or session.state.filled_volume_raw
        amt = ev.peak_amount_raw or session.state.filled_amount_raw
        if vol <= 0 or amt <= 0:
            return True
        addr = session.address
        self._last_completed_sale[addr] = {
            "volume_raw": vol,
            "amount_raw": amt,
            "unit_price_raw": session.state.unit_price_raw,
        }
        ev.sale_published = True
        return True

    def _set_price_needs_completed_clear(self, addr: int, session: PumpSession) -> bool:
        status = session.state.observed_status
        return (
            status
            in {
                ObservedStatus.FILLING_COMPLETED,
                ObservedStatus.MAX_AMOUNT_VOLUME_REACHED,
            }
            or addr in self._sale_display_held
        )

    def _set_price_nozio_age_s(self, session: PumpSession) -> float | None:
        t = session.state.last_nozio_time
        if t is None:
            return None
        return time.monotonic() - t

    def _set_price_status_age_s(self, session: PumpSession) -> float | None:
        t = session.state.last_status_time
        if t is None:
            return None
        return time.monotonic() - t

    def _set_price_sale_persist_state(self, addr: int, session: PumpSession) -> str:
        ev = session.state.sale_evidence
        if addr in self._last_completed_sale or ev.sale_published:
            return "captured"
        if ev.has_positive_delivery:
            return "positive_unpersisted"
        return "none"

    def _set_price_evidence_stale(self, session: PumpSession) -> bool:
        """True when status or nozzle observation ages exceed the fresh window."""
        nozio_age = self._set_price_nozio_age_s(session)
        status_age = self._set_price_status_age_s(session)
        if nozio_age is None or status_age is None:
            return True
        return (
            nozio_age > self._set_price_nozio_fresh_s
            or status_age > self._set_price_status_fresh_s
        )

    def _set_price_sibling_busy_reason(self, addr: int) -> str | None:
        """Wayne dual-hose: CD5 on one side often gets no ACK while the other is live.

        SAO one-Pi-per-pump owns dart 1+2 on the same dispenser. Logs show
        CD5 TIMED_OUT on addr=2 while addr=1 was FILLING — RESET/status still
        succeed. Do not block on a sibling hang-up with nozzle still OUT; that
        path must still allow CD5 on the idle hose after hang-up capture.
        """
        for other, session in self.sessions.items():
            if other == addr:
                continue
            status = session.state.observed_status
            if status in {
                ObservedStatus.AUTHORIZED,
                ObservedStatus.FILLING,
                ObservedStatus.SUSPENDED,
            }:
                return "sibling_busy"
        return None

    def _set_price_defer_reason(self, addr: int, session: PumpSession) -> str | None:
        """Why this dart address must wait before RESET/CD5 (None = eligible).

        ``saleDisplayHeld=False`` is not proof the nozzle is IN. Wayne often
        idles in FILLING_COMPLETED after hang-up without starting the LCD hold
        (hold requires nozzle IN). Idle RESET+IN with only EOT polls also goes
        stale — observation ages must be refreshed via RETURN_STATUS DATA, not
        poll ACK alone, before CD5.
        """
        sibling = self._set_price_sibling_busy_reason(addr)
        if sibling is not None:
            return sibling
        status = session.state.observed_status
        if status in {
            ObservedStatus.AUTHORIZED,
            ObservedStatus.FILLING,
            ObservedStatus.SUSPENDED,
        }:
            return "busy"
        pos = session.state.nozzle_position
        if pos is NozzlePosition.OUT:
            return "nozzle_out"
        if pos is NozzlePosition.UNKNOWN:
            return "nozzle_unknown"
        if status is ObservedStatus.UNKNOWN:
            return "status_unknown"
        nozio_age = self._set_price_nozio_age_s(session)
        status_age = self._set_price_status_age_s(session)
        if nozio_age is None or nozio_age > self._set_price_nozio_fresh_s:
            return "nozzle_stale"
        if status_age is None or status_age > self._set_price_status_fresh_s:
            return "status_stale"
        if self._set_price_needs_completed_clear(addr, session):
            if not self._capture_completed_sale_snapshot(session):
                return "sale_unpersisted"
            ev = session.state.sale_evidence
            if ev.has_positive_delivery and not (
                ev.sale_published or addr in self._last_completed_sale
            ):
                return "sale_unpersisted"
        return None

    def _log_set_price_deferred(
        self,
        reason: str,
        *,
        correlation_id: str,
        unit_price_raw: int,
        address: int,
        session: PumpSession,
        pump_id: str | None = None,
    ) -> None:
        """Log immediately when the defer reason changes; rate-limit repeats."""
        change_key = f"{correlation_id}:{address}"
        throttle_key = f"{change_key}:{reason}"
        now = time.monotonic()
        prev = self._set_price_defer_last_reason.get(change_key)
        changed = prev != reason
        self._set_price_defer_last_reason[change_key] = reason
        last = self._set_price_defer_log_at.get(throttle_key)
        # Unchanged reason: low-rate diagnostic (not every poll / 5s cloud retry).
        if not changed and last is not None and (now - last) < 30.0:
            return
        self._set_price_defer_log_at[throttle_key] = now
        nozio_age = self._set_price_nozio_age_s(session)
        status_age = self._set_price_status_age_s(session)
        logger.info(
            f"set_price_deferred_{reason}",
            correlationId=correlation_id,
            unitPriceRaw=unit_price_raw,
            address=address,
            pumpId=pump_id or session.state.pump_id,
            observedStatus=session.state.observed_status.value,
            nozzleState=session.state.nozzle_position.value,
            nozzleObservationAgeS=None if nozio_age is None else round(nozio_age, 3),
            statusObservationAgeS=None if status_age is None else round(status_age, 3),
            saleDisplayHeld=address in self._sale_display_held,
            displayHoldReason=(
                "lcd_hold_until_lift"
                if address in self._sale_display_held
                else "not_held"
            ),
            salePersistState=self._set_price_sale_persist_state(address, session),
            deferReasonChanged=changed,
        )

    def _set_price_request_age_s(self, pending: object, now: float) -> float:
        corr = pending.correlation_id  # type: ignore[attr-defined]
        if corr not in self._cloud_set_price_seen_at:
            seen = now
            requested_at = getattr(pending, "requested_at", None)
            if requested_at:
                try:
                    raw = str(requested_at).replace("Z", "+00:00")
                    dt = datetime.fromisoformat(raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    wall = (datetime.now(UTC) - dt).total_seconds()
                    if wall > 0:
                        seen = now - wall
                except ValueError:
                    pass
            self._cloud_set_price_seen_at[corr] = seen
        return now - self._cloud_set_price_seen_at[corr]

    async def _refresh_set_price_target_evidence(self, session: PumpSession) -> bool:
        """Obtain fresh DC1/NOZIO DATA for SET_PRICE; ACK alone is not evidence.

        Idle Wayne is often EOT-only on polls, so observation ages grow while
        state stays RESET/IN. RETURN_STATUS must elicit a status payload; a
        repeated RESET/IN DATA frame still advances ``last_status_time`` /
        ``last_nozio_time``. Returns True when both observations advanced.
        """
        await self._drain_pending_data(session)
        pos = session.state.nozzle_position
        status = session.state.observed_status
        needs_probe = (
            pos is NozzlePosition.UNKNOWN
            or status is ObservedStatus.UNKNOWN
            or self._set_price_evidence_stale(session)
        )
        if not needs_probe:
            return True
        addr = session.address
        now = time.monotonic()
        last = self._set_price_refresh_at.get(addr, 0.0)
        if now - last < self._set_price_refresh_min_interval_s:
            return not self._set_price_evidence_stale(session)
        self._set_price_refresh_at[addr] = now

        status_before = session.state.last_status_time
        nozio_before = session.state.last_nozio_time
        cmd_started = time.monotonic()
        result = await self._run_owned_command(
            session,
            encode_cd1_command(PumpControlCommand.RETURN_STATUS),
            PumpCommand.READ_STATUS,
            idempotency=IdempotencyClass.IDEMPOTENT,
            command_label="CD1_RETURN_STATUS_BEFORE_SET_PRICE",
        )
        # LINK_ACK / pre-existing known state is not fresh evidence — poll for
        # DATA payloads that advance observation timestamps.
        refreshed = False
        for attempt in range(8):
            await self._drain_pending_data(session)
            status_t = session.state.last_status_time
            nozio_t = session.state.last_nozio_time
            status_fresh = (
                status_t is not None
                and status_t > (status_before or 0.0)
                and status_t >= cmd_started - 0.05
            )
            nozio_fresh = (
                nozio_t is not None
                and nozio_t > (nozio_before or 0.0)
                and nozio_t >= cmd_started - 0.05
            )
            if status_fresh and nozio_fresh:
                refreshed = True
                break
            if attempt >= 7:
                break
            if not self.runtime.transport.is_open:
                break
            write_start, _wc = await self._write_frame(
                session.build_poll(),
                address=addr,
                note="POLL_SET_PRICE_REFRESH",
            )
            await self._read_poll_session(session, not_before_mono=write_start)

        logger.info(
            "set_price_status_refresh",
            address=addr,
            pumpId=session.state.pump_id,
            result=result.status.value,
            observedStatus=session.state.observed_status.value,
            nozzleState=session.state.nozzle_position.value,
            statusRefreshed=(
                session.state.last_status_time is not None
                and session.state.last_status_time > (status_before or 0.0)
            ),
            nozzleRefreshed=(
                session.state.last_nozio_time is not None
                and session.state.last_nozio_time > (nozio_before or 0.0)
            ),
            evidenceRefreshed=refreshed,
            commandResultOnlyAck=result.status
            is ExchangeResultStatus.LINK_ACKNOWLEDGED
            and not refreshed,
        )
        return refreshed

    def _clear_cloud_set_price_tracking(self, corr: str) -> None:
        self._cloud_set_price_applied.pop(corr, None)
        self._cloud_set_price_gave_up.pop(corr, None)
        self._cloud_set_price_awaiting_dc3.pop(corr, None)
        self._cloud_set_price_unverified.pop(corr, None)
        self._cloud_set_price_face_seeded.pop(corr, None)
        self._cloud_set_price_seen_at.pop(corr, None)
        self._cloud_set_price_unit_persisted.pop(corr, None)
        self._cloud_set_price_persist_fail_count.pop(corr, None)
        self._cloud_set_price_persist_next_try.pop(corr, None)
        for key in list(self._cloud_set_price_fail_count):
            if key.startswith(f"{corr}:"):
                self._cloud_set_price_fail_count.pop(key, None)
                self._cloud_set_price_next_try.pop(key, None)
        for key in list(self._cloud_set_price_dc3_baseline):
            if key.startswith(f"{corr}:"):
                self._cloud_set_price_dc3_baseline.pop(key, None)
                self._cloud_set_price_dc3_deadline.pop(key, None)
                self._cloud_set_price_verify_reads.pop(key, None)
                self._cloud_set_price_zero_dc3_log_at.pop(key, None)
        for key in list(self._set_price_defer_last_reason):
            if key.startswith(f"{corr}:"):
                self._set_price_defer_last_reason.pop(key, None)

    def _persist_cloud_unit_price_once(
        self,
        *,
        corr: str,
        unit_price_raw: int,
        prices: list[int] | tuple[int, ...],
        now: float,
    ) -> bool:
        """Write unit-price.json at most once per confirmed correlation/price.

        Local persistence is independent of cloud outcome ACK. While a sibling
        address is still deferred or the outcome file awaits ACK, do not rewrite
        an already-durable price. Failures keep recoverable state and back off.
        """
        from intelipump_fdc.cloud.set_price_request import (
            persisted_unit_price_matches,
            write_persisted_unit_price,
        )

        price_tuple = tuple(prices)
        if self._cloud_set_price_unit_persisted.get(corr) == unit_price_raw:
            return True
        if persisted_unit_price_matches(unit_price_raw, price_tuple):
            self._cloud_set_price_unit_persisted[corr] = unit_price_raw
            self._cloud_set_price_persist_fail_count.pop(corr, None)
            self._cloud_set_price_persist_next_try.pop(corr, None)
            logger.info(
                "cloud_set_price_persist_already_durable",
                correlationId=corr,
                unitPriceRaw=unit_price_raw,
            )
            return True
        next_try = self._cloud_set_price_persist_next_try.get(corr)
        if next_try is not None and now < next_try:
            return False
        try:
            path = write_persisted_unit_price(
                unit_price_raw,
                price_tuple,
                source="cloud",
            )
        except (OSError, ValueError) as exc:
            fails = self._cloud_set_price_persist_fail_count.get(corr, 0) + 1
            self._cloud_set_price_persist_fail_count[corr] = fails
            delay = min(60.0, 2.0 * (2 ** min(fails - 1, 4)))
            self._cloud_set_price_persist_next_try[corr] = now + delay
            logger.warning(
                "cloud_set_price_persist_failed",
                correlationId=corr,
                unitPriceRaw=unit_price_raw,
                failures=fails,
                retryInSeconds=delay,
                error=str(exc),
            )
            return False
        self._cloud_set_price_unit_persisted[corr] = unit_price_raw
        self._cloud_set_price_persist_fail_count.pop(corr, None)
        self._cloud_set_price_persist_next_try.pop(corr, None)
        logger.info(
            "cloud_set_price_persisted",
            correlationId=corr,
            unitPriceRaw=unit_price_raw,
            path=str(path),
        )
        print(
            f"[CLOUD-PRICE] persisted {unit_price_raw} → {path} "
            "(survives controller restart)"
        )
        return True

    @staticmethod
    def _dc3_price_unavailable(observed: int | None) -> bool:
        """Idle Wayne often emits DC3 with filling price 0 after CD5.

        That is not proof the face price failed — do not treat as mismatch.
        """
        return observed is None or (
            isinstance(observed, int)
            and not isinstance(observed, bool)
            and observed <= 0
        )

    def _set_price_mark_unverified(
        self,
        *,
        corr: str,
        addr: int,
        awaiting_dc3: set[int],
        unverified: set[int],
        reason: str,
        observed: int | None,
        unit_price_raw: int,
    ) -> None:
        """Stop CD5 retries after LINK_ACK verification bound; keep sent state."""
        awaiting_dc3.discard(addr)
        unverified.add(addr)
        retry_key = f"{corr}:{addr}"
        self._cloud_set_price_dc3_baseline.pop(retry_key, None)
        self._cloud_set_price_dc3_deadline.pop(retry_key, None)
        self._cloud_set_price_verify_reads.pop(retry_key, None)
        self._cloud_set_price_next_try.pop(retry_key, None)
        logger.warning(
            "cloud_set_price_sent_unverified",
            correlationId=corr,
            address=addr,
            unitPriceRaw=unit_price_raw,
            observedUnitPriceRaw=observed,
            reason=reason,
        )
        print(
            f"[CLOUD-PRICE addr={addr}] CD5 link-acked but DC3 verification "
            f"unresolved ({reason}, observed={observed}); not resending CD5"
        )

    def _finalize_cloud_set_price_outcome(
        self,
        *,
        pending: object,
        applied_final: set[int],
        gave_up_final: set[int],
        unverified_final: set[int] | None = None,
        exec_status: str,
        accepted: bool,
        detail: str,
    ) -> bool:
        """Write durable outcome first; only then durably remove the request.

        Returns False when the outcome could not be persisted or the request
        unlink/dir-sync failed — request is kept (or restored) and the outcome
        is retained so the next tick can finalize again. Cloud-sync must not
        ACK-delete the outcome until request removal is durable.
        """
        from intelipump_fdc.cloud.set_price_request import (
            SetPriceOutcome,
            SetPricePendingVerify,
            consume_set_price_request_durable,
            has_set_price_outcome,
            write_set_price_outcome,
            write_set_price_pending_verify,
        )

        corr = pending.correlation_id
        unverified = set(unverified_final or ())
        outcome = SetPriceOutcome(
            correlation_id=corr,
            command_id=pending.command_id,
            station_id=None,
            pump_id=pending.pump_id,
            unit_price_raw=pending.unit_price_raw,
            execution_status=exec_status,
            accepted=accepted,
            applied_addresses=tuple(sorted(applied_final)),
            gave_up_addresses=tuple(sorted(gave_up_final)),
            deferred_addresses=(),
            unverified_addresses=tuple(sorted(unverified)),
            detail=detail,
        )
        if not has_set_price_outcome(corr):
            try:
                write_set_price_outcome(outcome)
            except (OSError, ValueError) as exc:
                logger.warning(
                    "cloud_set_price_outcome_write_failed_request_retained",
                    correlationId=corr,
                    unitPriceRaw=pending.unit_price_raw,
                    executionStatus=exec_status,
                    error=str(exc),
                )
                return False
        # Retain late-verification metadata after the request is removed so a
        # later matching DC3 can upgrade SENT_UNVERIFIED / partial outcomes
        # without resending CD5 (survives outcome MQTT ACK + restart).
        if unverified:
            try:
                write_set_price_pending_verify(
                    SetPricePendingVerify(
                        correlation_id=corr,
                        command_id=pending.command_id,
                        station_id=None,
                        pump_id=pending.pump_id,
                        unit_price_raw=pending.unit_price_raw,
                        required_addresses=tuple(
                            sorted(set(applied_final) | unverified)
                        ),
                        verified_addresses=tuple(sorted(applied_final)),
                        gave_up_addresses=tuple(sorted(gave_up_final)),
                        emitted_verified_addresses=tuple(sorted(applied_final)),
                        outcome_revision=0,
                    )
                )
            except (OSError, ValueError) as exc:
                logger.warning(
                    "cloud_set_price_pending_verify_write_failed",
                    correlationId=corr,
                    unitPriceRaw=pending.unit_price_raw,
                    error=str(exc),
                )
                # Outcome is durable; late upgrade may still migrate from it.
        try:
            consumed = consume_set_price_request_durable()
        except OSError as exc:
            logger.warning(
                "cloud_set_price_request_unlink_sync_failed_outcome_retained",
                correlationId=corr,
                unitPriceRaw=pending.unit_price_raw,
                error=str(exc),
            )
            return False
        if consumed is None:
            logger.warning(
                "set_price_consume_missing_after_outcome",
                correlationId=corr,
                unitPriceRaw=pending.unit_price_raw,
            )
        self._clear_cloud_set_price_tracking(corr)
        logger.info(
            "cloud_set_price_complete",
            correlationId=corr,
            unitPriceRaw=pending.unit_price_raw,
            applied=sorted(applied_final),
            gaveUp=sorted(gave_up_final),
            unverified=sorted(unverified),
            executionStatus=exec_status,
        )
        if not applied_final and not unverified:
            print(
                f"[CLOUD-PRICE] cleared pending {pending.unit_price_raw} "
                f"corr={corr} with no confirmed price (gave up on "
                f"{sorted(gave_up_final)}); re-send from admin if needed"
            )
        elif unverified and not applied_final:
            print(
                f"[CLOUD-PRICE] cleared pending {pending.unit_price_raw} "
                f"corr={corr} as SENT_UNVERIFIED "
                f"(addrs={sorted(unverified)}; face may show price, DC3 idle=0)"
            )
        return True

    def _maybe_upgrade_unverified_set_price_outcomes(self) -> None:
        """Promote SENT_UNVERIFIED targets on later matching DC3 (no bus cmds).

        Uses durable pending-verify metadata that outlives request removal and
        cloud ACK of the initial SENT_UNVERIFIED outcome. Each newly matched
        address upgrades the publishable outcome (PARTIAL → CONFIRMED) without
        resending CD5 / RESET / AUTHORIZE. Superseded by a newer SET_PRICE.
        """
        from intelipump_fdc.cloud.set_price_request import (
            SetPriceOutcome,
            SetPricePendingVerify,
            clear_set_price_pending_verify,
            list_set_price_outcomes,
            list_set_price_pending_verifies,
            pending_verify_from_outcome,
            read_set_price_request,
            write_set_price_outcome,
            write_set_price_pending_verify,
        )

        active = read_set_price_request()
        # Migrate leftover SENT_UNVERIFIED outcomes into pending-verify (restart
        # / older builds that only wrote the outcome file).
        known = {p.correlation_id for p in list_set_price_pending_verifies()}
        for outcome in list_set_price_outcomes():
            if outcome.correlation_id in known:
                continue
            if active is not None and active.correlation_id != outcome.correlation_id:
                # Newer SET_PRICE superseded this correlation — do not revive.
                continue
            if outcome.execution_status not in {"SENT_UNVERIFIED", "PRICE_PARTIAL"}:
                continue
            if not outcome.unverified_addresses:
                continue
            migrated = pending_verify_from_outcome(outcome)
            if migrated is None:
                continue
            try:
                write_set_price_pending_verify(migrated)
                known.add(migrated.correlation_id)
            except (OSError, ValueError) as exc:
                logger.warning(
                    "cloud_set_price_pending_verify_migrate_failed",
                    correlationId=outcome.correlation_id,
                    error=str(exc),
                )

        for pending in list_set_price_pending_verifies():
            if active is not None and active.correlation_id != pending.correlation_id:
                # Newer command superseded this verification window.
                clear_set_price_pending_verify(pending.correlation_id)
                continue
            if (
                active is not None
                and active.correlation_id == pending.correlation_id
                and active.unit_price_raw != pending.unit_price_raw
            ):
                continue

            required = set(pending.required_addresses)
            verified = set(pending.verified_addresses)
            open_addrs = required - verified
            if not open_addrs:
                clear_set_price_pending_verify(pending.correlation_id)
                continue

            newly: set[int] = set()
            for addr in open_addrs:
                session = self.sessions.get(addr)
                if session is None:
                    continue
                observed = session.state.unit_price_raw
                if (
                    isinstance(observed, int)
                    and not isinstance(observed, bool)
                    and observed == pending.unit_price_raw
                ):
                    newly.add(addr)
                    self._price_programmed.add(addr)

            if not newly:
                continue

            verified |= newly
            still_open = required - verified
            emitted = set(pending.emitted_verified_addresses)
            if verified == emitted:
                # Duplicate observation of an already-emitted snapshot.
                continue

            if still_open:
                exec_status = "PRICE_PARTIAL"
                detail = "dc3_late_partial_after_sent_unverified"
            else:
                exec_status = "PRICE_CONFIRMED"
                detail = "dc3_late_match_after_sent_unverified"
            revision = pending.outcome_revision + 1
            upgraded = SetPriceOutcome(
                correlation_id=pending.correlation_id,
                command_id=pending.command_id,
                station_id=pending.station_id,
                pump_id=pending.pump_id,
                unit_price_raw=pending.unit_price_raw,
                execution_status=exec_status,
                accepted=True,
                applied_addresses=tuple(sorted(verified)),
                gave_up_addresses=pending.gave_up_addresses,
                deferred_addresses=(),
                unverified_addresses=tuple(sorted(still_open)),
                detail=detail,
            )
            try:
                write_set_price_outcome(upgraded)
            except (OSError, ValueError) as exc:
                logger.warning(
                    "cloud_set_price_unverified_upgrade_failed",
                    correlationId=pending.correlation_id,
                    error=str(exc),
                )
                continue

            if still_open:
                try:
                    write_set_price_pending_verify(
                        SetPricePendingVerify(
                            correlation_id=pending.correlation_id,
                            command_id=pending.command_id,
                            station_id=pending.station_id,
                            pump_id=pending.pump_id,
                            unit_price_raw=pending.unit_price_raw,
                            required_addresses=pending.required_addresses,
                            verified_addresses=tuple(sorted(verified)),
                            gave_up_addresses=pending.gave_up_addresses,
                            emitted_verified_addresses=tuple(sorted(verified)),
                            outcome_revision=revision,
                            created_at=pending.created_at,
                        )
                    )
                except (OSError, ValueError) as exc:
                    logger.warning(
                        "cloud_set_price_pending_verify_update_failed",
                        correlationId=pending.correlation_id,
                        error=str(exc),
                    )
            else:
                clear_set_price_pending_verify(pending.correlation_id)

            logger.info(
                "cloud_set_price_confirmed_via_late_dc3",
                correlationId=pending.correlation_id,
                unitPriceRaw=pending.unit_price_raw,
                applied=sorted(verified),
                stillUnverified=sorted(still_open),
                executionStatus=exec_status,
                outcomeRevision=revision,
                newlyVerified=sorted(newly),
            )
            print(
                f"[CLOUD-PRICE] late DC3 verified {sorted(newly)} for "
                f"{pending.unit_price_raw} corr={pending.correlation_id} "
                f"→ {exec_status} (no CD5 resend)"
            )

    def _meter_session_eligibility(
        self, session: PumpSession, *, startup_opening: bool = False
    ) -> tuple[bool, str, str]:
        from intelipump_fdc.services.meter_reading import evaluate_meter_tx_eligibility

        ctx = session.machine.context
        return evaluate_meter_tx_eligibility(
            current_state=ctx.current_state,
            nozzle_position=session.state.nozzle_position,
            last_nozio_mono=session.state.last_nozio_time,
            now_mono=time.monotonic(),
            nozzle_in_max_age_s=float(self.runtime.meter_nozzle_in_max_age_s),
            sale_lifecycle=str(session.state.sale_lifecycle.value),
            active_transaction_id=ctx.active_transaction_id,
            held_completion=session._held_completion is not None,
            pending_exchange=bool(session.state.pending_exchange),
            block_during_dispensing=True,
            startup_opening=startup_opening,
        )

    def _startup_meter_backoff(self, address: int, *, extra_s: float = 0.0) -> None:
        delay = max(
            float(self._meter_startup_defer_backoff_s),
            float(extra_s),
            float(self.runtime.meter_post_timeout_quarantine_s) + 1.0,
        )
        self._meter_startup_next_try_mono[int(address)] = time.monotonic() + delay

    def _note_startup_meter_outcome(
        self,
        *,
        address: int,
        correlation_id: str | None,
        status: str,
        error_code: str | None = None,
        startup_opening: bool,
    ) -> None:
        """Backoff DEFERRED; mark day-done on CAPTURED / terminal fail."""
        if not startup_opening:
            return
        status_u = str(status or "").upper()
        tz = self.runtime.meter_startup_capture_timezone or "Africa/Lagos"
        corr = str(correlation_id or "")
        try:
            if status_u.startswith("CAPTURED"):
                from intelipump_fdc.controller.meter_startup_capture import (
                    mark_address_captured,
                )

                mark_address_captured(
                    address=int(address),
                    correlation_id=corr,
                    timezone=tz,
                )
                self._meter_startup_next_try_mono.pop(int(address), None)
                self._meter_startup_timeout_attempts.pop(int(address), None)
                return
            if status_u in {"UNSUPPORTED", "ERROR"}:
                # Timeout after ignoring pre-TX DC101 is often recoverable — retry
                # a few times before marking the address done for the day.
                if (
                    status_u == "UNSUPPORTED"
                    and str(error_code or "") == "METER_DC101_TIMEOUT"
                ):
                    n = int(self._meter_startup_timeout_attempts.get(int(address), 0)) + 1
                    self._meter_startup_timeout_attempts[int(address)] = n
                    if n < int(self._meter_startup_timeout_max_attempts):
                        self._startup_meter_backoff(int(address))
                        logger.info(
                            "meter_startup_capture_timeout_retry",
                            address=address,
                            attempt=n,
                            maxAttempts=self._meter_startup_timeout_max_attempts,
                            correlationId=corr,
                        )
                        return
                from intelipump_fdc.controller.meter_startup_capture import (
                    mark_address_failed,
                )

                mark_address_failed(
                    address=int(address),
                    correlation_id=corr,
                    status=status_u,
                    error_code=error_code,
                    timezone=tz,
                )
                self._meter_startup_next_try_mono.pop(int(address), None)
                self._meter_startup_timeout_attempts.pop(int(address), None)
                logger.info(
                    "meter_startup_capture_address_finished",
                    address=address,
                    status=status_u,
                    errorCode=error_code,
                    correlationId=corr,
                )
                return
            if status_u == "DEFERRED":
                self._startup_meter_backoff(int(address))
        except OSError as exc:
            logger.warning(
                "meter_startup_capture_marker_failed",
                address=address,
                error=str(exc),
            )

    async def _maybe_queue_startup_meter_capture(self) -> None:
        """Once per local morning after boot: queue OPENING CD101 per address."""
        from uuid import uuid4

        from intelipump_fdc.controller.meter_read_request import (
            request_busy,
            write_meter_read_request,
            MeterReadRequest,
        )
        from intelipump_fdc.controller.meter_startup_capture import (
            finished_addresses_for_today,
        )

        if not self.runtime.meter_hardware_cd101:
            return
        if not self.runtime.meter_startup_capture_enabled:
            return
        if self._meter_startup_window_closed:
            return
        if self._meter_pending is not None or request_busy():
            return

        now = time.monotonic()
        if self._meter_loop_started_mono is None:
            self._meter_loop_started_mono = now
        elapsed = now - float(self._meter_loop_started_mono)
        if elapsed < float(self.runtime.meter_startup_capture_settle_s):
            return
        if elapsed > float(self.runtime.meter_startup_capture_window_s):
            self._meter_startup_window_closed = True
            logger.info(
                "meter_startup_capture_window_closed",
                elapsed_s=round(elapsed, 1),
                window_s=self.runtime.meter_startup_capture_window_s,
            )
            return

        tz = self.runtime.meter_startup_capture_timezone or "Africa/Lagos"
        done = finished_addresses_for_today(timezone=tz)
        safety = self.runtime.safety
        allowed = set(safety.hardware_meter_allowed_addresses)
        candidates = [
            int(a)
            for a in self.runtime.config.addresses
            if (not allowed or int(a) in allowed) and int(a) not in done
        ]
        if not candidates:
            self._meter_startup_window_closed = True
            logger.info(
                "meter_startup_capture_all_addresses_done",
                finished=sorted(done),
            )
            return

        for addr in candidates:
            next_try = self._meter_startup_next_try_mono.get(int(addr))
            if next_try is not None and now < float(next_try):
                continue
            session = self.sessions.get(int(addr))
            if session is None:
                continue
            q_until = session.state.meter_dc101_quarantine_until_mono
            if q_until is not None and now < float(q_until):
                self._startup_meter_backoff(
                    int(addr), extra_s=float(q_until) - now
                )
                continue
            ok, code, msg = self._meter_session_eligibility(
                session, startup_opening=True
            )
            if not ok:
                logger.debug(
                    "meter_startup_capture_wait",
                    address=addr,
                    errorCode=code,
                    detail=msg,
                )
                self._startup_meter_backoff(int(addr))
                continue
            cmap = self.runtime.meter_channel_map or {}
            meta = None
            if isinstance(cmap, dict):
                meta = cmap.get(int(addr))
                if meta is None:
                    meta = cmap.get(str(int(addr)))
            nozzle_hint = None
            pump_id = None
            if meta is not None:
                nozzle_hint = getattr(meta, "nozzle_id", None) or (
                    meta.get("nozzle_id") if isinstance(meta, dict) else None
                )
                pump_id = getattr(meta, "pump_id", None) or (
                    meta.get("pump_id") if isinstance(meta, dict) else None
                )
            corr = str(uuid4())
            try:
                write_meter_read_request(
                    MeterReadRequest(
                        correlation_id=corr,
                        dart_address=int(addr),
                        counter_select=int(self.runtime.meter_counter_select),
                        requested_by="startup-opening",
                        nozzle_hint=str(nozzle_hint) if nozzle_hint else None,
                        pump_id=str(pump_id) if pump_id else None,
                        notes="startup-opening-morning",
                        slot="OPENING",
                        startup_opening=True,
                    )
                )
            except FileExistsError:
                return
            except OSError as exc:
                logger.warning(
                    "meter_startup_capture_write_failed",
                    address=addr,
                    error=str(exc),
                )
                return
            logger.info(
                "meter_startup_capture_queued",
                address=addr,
                correlationId=corr,
                slot="OPENING",
                nozzleId=nozzle_hint,
                pumpId=pump_id,
            )
            print(
                f"[METER-READ] startup OPENING queued addr={addr} corr={corr}"
            )
            return  # one address per loop turn (exclusive bridge)

    def _meter_tx_still_allowed(
        self, session: PumpSession, item: OutboundDataItem
    ) -> bool:
        """Re-check gates immediately before CD101 TX; cancel pending if not."""
        pending = self._meter_pending
        if pending is None:
            return False
        if pending.get("outbound_correlation_id") != item.correlation_id:
            return False
        if int(pending.get("address", -1)) != int(session.address):
            self._meter_cancel_pending(
                status="ERROR",
                code="METER_ADDRESS_MISMATCH",
                message="pending meter read address mismatch at TX",
            )
            return False
        startup = bool(pending.get("startup_opening"))
        ok, code, msg = self._meter_session_eligibility(
            session, startup_opening=startup
        )
        if not ok:
            self._meter_cancel_pending(status="DEFERRED", code=code, message=msg)
            return False
        return True

    def _meter_mark_tx_started(
        self, item: OutboundDataItem, *, write_mono: float
    ) -> None:
        pending = self._meter_pending
        if pending is None:
            return
        if pending.get("outbound_correlation_id") != item.correlation_id:
            return
        # First TX stamp wins — never push the floor later on a retry path.
        first_tx = pending.get("tx_started_at_mono") is None
        if first_tx:
            pending["tx_started_at_mono"] = float(write_mono)
        addr = int(pending.get("address", item.address))
        session = self.sessions.get(addr)
        if session is not None and first_tx:
            q_until = session.state.meter_dc101_quarantine_until_mono
            # Claim already waited out quarantine; drop it so it cannot keep
            # raising the match floor above a valid post-TX DC101.
            if q_until is not None and float(write_mono) >= float(q_until):
                session.state.meter_dc101_quarantine_until_mono = None
            # Drop any pre-TX observation so a late/unsolicited DC101 cannot
            # stick as before_request_window for the whole timeout window.
            session.state.last_dc101 = None
            session.state.last_dc101_at_mono = None
            session.state.last_dc101_frame_hex = None

    def _meter_cancel_pending(
        self, *, status: str, code: str, message: str
    ) -> None:
        from intelipump_fdc.controller.meter_read_request import (
            clear_meter_read_request,
            write_meter_read_result,
        )

        pending = self._meter_pending
        if pending is None:
            clear_meter_read_request()
            return
        addr = int(pending["address"])
        outbound_corr = pending.get("outbound_correlation_id")
        # Drop only this meter item — never wipe SET_PRICE / other outbound.
        self.runtime.outbound.drop_where(
            lambda it: (
                it.command_type is PumpCommand.READ_METER
                and int(it.address) == addr
                and (
                    outbound_corr is None
                    or it.correlation_id == outbound_corr
                )
            )
        )
        result_base = dict(pending.get("result_base") or {})
        write_meter_read_result(
            {
                **result_base,
                "status": status,
                "errorCode": code,
                "errorMessage": message,
                "volumeLiters": None,
                "cumulativeVolumeRaw": None,
                "capturedAt": None,
                "protocolCorrelationNote": (
                    "Wayne DC101 does not echo a request UUID; correlation is "
                    "DART address + COUN + post-TX observation window."
                ),
            }
        )
        clear_meter_read_request()
        self._note_startup_meter_outcome(
            address=addr,
            correlation_id=pending.get("correlation_id"),
            status=status,
            error_code=code,
            startup_opening=bool(pending.get("startup_opening")),
        )
        logger.warning(
            "meter_read_cancelled",
            correlationId=pending.get("correlation_id"),
            address=addr,
            status=status,
            errorCode=code,
            detail=message,
        )
        print(
            f"[METER-READ] cancelled addr={addr} status={status} "
            f"code={code} corr={pending.get('correlation_id')}"
        )
        self._meter_pending = None

    async def _apply_pending_meter_read(self) -> None:
        """One-shot gated CD101 via existing outbound (attended canary).

        Never invents a zero totalizer. Never RESET / SET_PRICE / AUTHORIZE.
        Disabled unless ``runtime.meter_hardware_cd101`` and safety allowlist.
        """
        from intelipump_fdc.controller.meter_read_request import (
            claim_meter_read_request,
            clear_meter_read_request,
            write_meter_read_result,
        )
        from intelipump_fdc.protocol.cd101 import build_cd101_request

        if not self.runtime.meter_hardware_cd101:
            return

        # Finish in-flight wait for DC101 before accepting a new request.
        if self._meter_pending is not None:
            await self._finish_pending_meter_read()
            return

        req = claim_meter_read_request()
        if req is None:
            return

        safety = self.runtime.safety
        now = time.monotonic()
        addr = int(req.dart_address)
        coun = int(req.counter_select or self.runtime.meter_counter_select)
        startup_opening = bool(req.startup_opening) or str(req.slot or "").upper() == "OPENING"
        result_base = {
            "correlationId": req.correlation_id,
            "dartAddress": addr,
            "counterSelect": coun,
            "requestedAt": req.requested_at,
            "requestedBy": req.requested_by,
            "nozzleHint": req.nozzle_hint,
            "pumpId": req.pump_id,
            "slot": req.slot or ("OPENING" if startup_opening else "AD_HOC"),
            "startupOpening": startup_opening,
            "deviceId": safety.hardware_meter_device_id,
            "status": "ERROR",
            "readOnly": True,
            "softwareVersion": "intelipump-fdc-meter-reading-2",
            "specRef": {
                "cd101": "Pump Interface Rev 2.11, page 19, CD101",
                "dc101": "Pump Interface Rev 2.11, page 25, DC101",
            },
            "protocolCorrelationNote": (
                "Wayne DC101 does not echo a request UUID; correlation is "
                "DART address + requested COUN + observation after our CD101 TX. "
                "Unsolicited/late/wrong-COUN replies are ignored for completion."
            ),
        }

        def _fail(status: str, code: str, message: str) -> None:
            write_meter_read_result(
                {
                    **result_base,
                    "status": status,
                    "errorCode": code,
                    "errorMessage": message,
                    "volumeLiters": None,
                    "cumulativeVolumeRaw": None,
                    "capturedAt": None,
                }
            )
            clear_meter_read_request()
            self._note_startup_meter_outcome(
                address=addr,
                correlation_id=req.correlation_id,
                status=status,
                error_code=code,
                startup_opening=startup_opening,
            )
            logger.warning(
                "meter_read_refused",
                correlationId=req.correlation_id,
                address=addr,
                status=status,
                errorCode=code,
                detail=message,
            )

        if not safety.hardware_meter_cd101_enabled:
            _fail(
                "UNSUPPORTED",
                "METER_HARDWARE_GATE_OFF",
                "hardware_meter_cd101_enabled is false",
            )
            return
        if addr not in safety.hardware_meter_allowed_addresses:
            _fail(
                "UNSUPPORTED",
                "METER_ADDRESS_NOT_ALLOWLISTED",
                f"address {addr} not in hardware meter allowlist",
            )
            return
        if addr not in self.sessions:
            _fail(
                "ERROR",
                "METER_ADDRESS_NOT_IN_POLL_SET",
                f"address {addr} is not polled by this controller",
            )
            return

        if not startup_opening:
            last = self._meter_last_attempt_mono.get(addr)
            if last is not None and (now - last) < float(
                self.runtime.meter_min_interval_s
            ):
                _fail(
                    "RATE_LIMITED",
                    "METER_READ_RATE_LIMITED",
                    f"min interval {self.runtime.meter_min_interval_s}s not elapsed",
                )
                return

        session = self.sessions[addr]
        ok, code, msg = self._meter_session_eligibility(
            session, startup_opening=startup_opening
        )
        if not ok:
            _fail("DEFERRED", code, msg)
            return
        q_until = session.state.meter_dc101_quarantine_until_mono
        if q_until is not None and now < float(q_until):
            remaining = float(q_until) - now
            _fail(
                "DEFERRED",
                "METER_READ_QUARANTINE_AFTER_TIMEOUT",
                f"prior CD101 timed out; refusing new TX for {remaining:.1f}s "
                "so a late DC101 cannot bind to this request "
                "(Wayne has no request UUID)",
            )
            return

        try:
            cd101 = build_cd101_request(counter_select=coun)
        except Exception as exc:  # noqa: BLE001
            _fail("ERROR", "METER_CD101_BUILD_FAILED", str(exc))
            return

        item = OutboundDataItem.create(
            address=addr,
            application_payload=cd101.application_payload,
            command_type=PumpCommand.READ_METER,
            simulator_only=False,
            idempotency=IdempotencyClass.IDEMPOTENT,
            ttl_ms=int(max(5.0, self.runtime.meter_response_timeout_s) * 1000),
            max_retries=0,  # never retry — must not delay critical polling
        )
        decision = evaluate_outbound_safety(item, safety)
        if not decision.allowed:
            _fail(
                "UNSUPPORTED",
                "METER_SAFETY_BLOCKED",
                ",".join(decision.reasons),
            )
            return

        self._meter_last_attempt_mono[addr] = now
        # Clear prior DC101 so a stale/unsolicited reply cannot satisfy this request.
        session.state.last_dc101 = None
        session.state.last_dc101_at_mono = None
        session.state.last_dc101_frame_hex = None
        try:
            self.runtime.outbound.enqueue(item, safety)
        except OutboundRejectedError as exc:
            _fail("UNSUPPORTED", "METER_OUTBOUND_REJECTED", ",".join(exc.reasons))
            return

        self._meter_pending = {
            "correlation_id": req.correlation_id,
            "address": addr,
            "counter_select": coun,
            "request_payload_hex": cd101.payload_hex,
            "queued_at_mono": now,
            "tx_started_at_mono": None,
            "deadline_mono": now + float(self.runtime.meter_response_timeout_s),
            "requested_at": req.requested_at,
            "requested_by": req.requested_by,
            "nozzle_hint": req.nozzle_hint,
            "outbound_correlation_id": item.correlation_id,
            "result_base": result_base,
            "startup_opening": startup_opening,
            "slot": result_base.get("slot"),
        }
        # Inflight file remains until finish/cancel (serialized ownership).
        logger.info(
            "meter_read_cd101_queued",
            correlationId=req.correlation_id,
            address=addr,
            counterSelect=coun,
            payloadHex=cd101.payload_hex,
        )
        print(
            f"[METER-READ] queued CD101 addr={addr} coun={coun} "
            f"corr={req.correlation_id} payload={cd101.payload_hex}"
        )

    async def _finish_pending_meter_read(self) -> None:
        from intelipump_fdc.controller.meter_read_request import (
            clear_meter_read_request,
            write_meter_read_result,
        )
        from intelipump_fdc.services.meter_reading import (
            build_capture_flags,
            dc101_matches_pending,
            liters_from_raw_scaled,
        )

        pending = self._meter_pending
        if pending is None:
            return
        addr = int(pending["address"])
        session = self.sessions.get(addr)
        now = time.monotonic()
        result_base = dict(pending.get("result_base") or {})

        def _done(payload: dict) -> None:
            write_meter_read_result(payload)
            clear_meter_read_request()
            status = str(payload.get("status") or "").upper()
            self._note_startup_meter_outcome(
                address=addr,
                correlation_id=str(pending["correlation_id"]),
                status=status,
                error_code=(
                    str(payload.get("errorCode"))
                    if payload.get("errorCode") is not None
                    else None
                ),
                startup_opening=bool(pending.get("startup_opening")),
            )
            self._meter_pending = None
            logger.info(
                "meter_read_finished",
                correlationId=pending["correlation_id"],
                address=addr,
                status=payload.get("status"),
            )
            print(
                f"[METER-READ] finished addr={addr} status={payload.get('status')} "
                f"corr={pending['correlation_id']}"
            )

        if session is None:
            _done(
                {
                    **result_base,
                    "status": "ERROR",
                    "errorCode": "METER_SESSION_MISSING",
                    "errorMessage": "session disappeared during meter read",
                    "volumeLiters": None,
                    "cumulativeVolumeRaw": None,
                }
            )
            return

        # If still queued (not TX'd) and eligibility fails (nozzle lift), cancel.
        if pending.get("tx_started_at_mono") is None:
            ok, code, msg = self._meter_session_eligibility(
                session,
                startup_opening=bool(pending.get("startup_opening")),
            )
            if not ok:
                self._meter_cancel_pending(status="DEFERRED", code=code, message=msg)
                return

        dc101 = session.state.last_dc101
        dc101_at = session.state.last_dc101_at_mono
        matched, reason = dc101_matches_pending(
            decoded=dc101 if isinstance(dc101, dict) else None,
            observed_at_mono=dc101_at,
            expected_address=addr,
            observed_address=addr,
            expected_coun=int(pending["counter_select"]),
            queued_at_mono=float(pending["queued_at_mono"]),
            tx_started_at_mono=pending.get("tx_started_at_mono"),
            quarantine_until_mono=session.state.meter_dc101_quarantine_until_mono,
        )
        if matched and isinstance(dc101, dict):
            raw_scaled = (
                dc101.get("raw_scaled")
                if isinstance(dc101.get("raw_scaled"), dict)
                else {}
            )
            decimals = self.runtime.meter_volume_decimals
            coun = int(pending["counter_select"])
            # Retain all raw counter fields; litres only when COUN+scale verified.
            total_raw = raw_scaled.get("total_value")
            liters = liters_from_raw_scaled(
                total_raw if isinstance(total_raw, int) else None,
                decimals,
                counter_select=coun,
            )
            channel = None
            cmap = self.runtime.meter_channel_map or {}
            if str(addr) in cmap:
                channel = cmap[str(addr)]
            elif addr in cmap:
                channel = cmap[addr]
            flags = build_capture_flags(
                counter_select=coun,
                volume_decimals=decimals,
                liters=liters,
            )
            # Expose residual ambiguity if a prior timeout on same COUN was recent.
            prior_to = session.state.meter_last_timeout_mono
            prior_coun = session.state.meter_last_timeout_coun
            ambiguous = (
                prior_to is not None
                and prior_coun is not None
                and int(prior_coun) == coun
                and (now - float(prior_to))
                < (2.0 * float(self.runtime.meter_post_timeout_quarantine_s))
            )
            if ambiguous:
                flags["ambiguousCorrelation"] = True
                flags["priorTimeoutMonoAgeS"] = now - float(prior_to)
            status = "CAPTURED_AMBIGUOUS" if ambiguous else "CAPTURED"
            # Consume observation so it cannot satisfy a later request.
            session.state.last_dc101 = None
            session.state.last_dc101_at_mono = None
            frame_hex = session.state.last_dc101_frame_hex
            session.state.last_dc101_frame_hex = None
            session.state.meter_dc101_quarantine_until_mono = None
            _done(
                {
                    **result_base,
                    "status": status,
                    "capturedAt": datetime.now(UTC).isoformat(),
                    "requestPayloadHex": pending["request_payload_hex"],
                    "responseFrameHex": frame_hex,
                    "decoded": dc101,
                    "rawScaled": raw_scaled,
                    "rawCounters": {
                        "totalValue": raw_scaled.get("total_value"),
                        "totalMeter1OrNofill": raw_scaled.get("total_meter1_or_nofill"),
                        "totalMeter2": raw_scaled.get("total_meter2"),
                        "counterSelect": dc101.get("counter_select"),
                    },
                    "cumulativeVolumeRaw": (
                        total_raw if isinstance(total_raw, int) else None
                    ),
                    "volumeDecimals": decimals,
                    "volumeLiters": liters,
                    "channelMap": channel,
                    "flags": flags,
                    "mappingNote": (
                        f"{status} = correlated DC101 reply"
                        + (
                            " but a prior timeout on this address/COUN was recent; "
                            "treat as ambiguous (no wire request UUID)."
                            if ambiguous
                            else ". Nozzle/meter mapping and face scale are "
                            "unverified until attended comparison."
                        )
                    ),
                    "errorCode": (
                        "METER_CORRELATION_AMBIGUOUS" if ambiguous else None
                    ),
                    "errorMessage": (
                        "Possible late DC101 from a prior timed-out CD101; "
                        "re-read after quarantine if totals look wrong."
                        if ambiguous
                        else None
                    ),
                }
            )
            return

        if (
            isinstance(dc101, dict)
            and dc101_at is not None
            and reason in {"wrong_coun", "before_request_window", "wrong_address"}
        ):
            # Do not bind a wrong/stale reply. Clear before_request_window frames
            # so they cannot block matching a later post-TX DC101.
            logger.info(
                "meter_read_dc101_ignored",
                correlationId=pending["correlation_id"],
                address=addr,
                reason=reason,
                observedCoun=dc101.get("counter_select"),
                expectedCoun=pending["counter_select"],
                observedAtMono=dc101_at,
                txStartedAtMono=pending.get("tx_started_at_mono"),
                queuedAtMono=pending.get("queued_at_mono"),
            )
            if reason == "before_request_window":
                session.state.last_dc101 = None
                session.state.last_dc101_at_mono = None
                session.state.last_dc101_frame_hex = None

        if now >= float(pending["deadline_mono"]):
            # Quarantine: late DC101 must not satisfy the next same-address/COUN TX.
            q = float(self.runtime.meter_post_timeout_quarantine_s)
            session.state.meter_dc101_quarantine_until_mono = now + q
            session.state.meter_last_timeout_coun = int(pending["counter_select"])
            session.state.meter_last_timeout_mono = now
            session.state.last_dc101 = None
            session.state.last_dc101_at_mono = None
            session.state.last_dc101_frame_hex = None
            _done(
                {
                    **result_base,
                    "status": "UNSUPPORTED",
                    "errorCode": "METER_DC101_TIMEOUT",
                    "errorMessage": (
                        "CD101 queued but no matching DC101 (address+COUN+window) "
                        "before timeout; reporting unsupported (no invented zero); "
                        f"quarantine {q:.1f}s before next TX on this address"
                        + (f"; last_ignore={reason}" if reason else "")
                    ),
                    "requestPayloadHex": pending["request_payload_hex"],
                    "responseFrameHex": None,
                    "volumeLiters": None,
                    "cumulativeVolumeRaw": None,
                    "capturedAt": None,
                    "quarantineSeconds": q,
                }
            )

    async def _apply_pending_cloud_set_price(self) -> None:
        """Apply a cloud-queued SET_PRICE (CD5) written by intelipump-cloud-sync.

        Safety gates: owned-lab session required; never CD5 while AUTHORIZED /
        FILLING / SUSPENDED; never RESET/CD5 while nozzle is OUT, unknown, or
        stale, or while a positive completed sale is not yet captured. Idle
        FILLING_COMPLETED with a freshly confirmed nozzle IN progresses
        RESET-then-CD5 without a Pi restart or a new sale.

        Multi-address controllers apply per eligible address. A held sale on
        addr=1 must not block CD5 on addr=2.

        LINK_ACK alone and TIMED_OUT never count as an applied price — only
        APPLICATION_CONFIRMED or a matching DC3 unit_price_raw does. RESET
        must reach APPLICATION_CONFIRMED (or observed RESET) before CD5.
        """
        if not self.runtime.safety.owned_lab_active_session:
            return
        from intelipump_fdc.cloud.set_price_request import (
            consume_set_price_request_durable,
            has_set_price_outcome,
            read_set_price_request,
        )

        # Late DC3 match may upgrade a durable SENT_UNVERIFIED outcome.
        self._maybe_upgrade_unverified_set_price_outcomes()

        pending = read_set_price_request()
        if pending is None:
            return

        # Crash recovery: outcome already durable → durably clear leftover request.
        if has_set_price_outcome(pending.correlation_id):
            try:
                consume_set_price_request_durable()
            except OSError as exc:
                logger.warning(
                    "set_price_request_clear_sync_failed_outcome_retained",
                    correlationId=pending.correlation_id,
                    unitPriceRaw=pending.unit_price_raw,
                    error=str(exc),
                )
                return
            self._clear_cloud_set_price_tracking(pending.correlation_id)
            logger.info(
                "set_price_request_cleared_outcome_already_durable",
                correlationId=pending.correlation_id,
                unitPriceRaw=pending.unit_price_raw,
            )
            return

        # Defense in depth: cloned AGO Pi must not apply a PMS pumpId left in
        # the request file by a mis-filtered cloud-sync.
        if pending.pump_id:
            from intelipump_fdc.cloud.set_price_ownership import (
                owned_logical_pump_ids,
                pump_id_allowed_for_device,
            )

            device_id = (
                os.environ.get("INTELIPUMP_CONTROLLER__DEVICE_ID") or ""
            ).strip()
            if not pump_id_allowed_for_device(
                pump_id=pending.pump_id, device_id=device_id
            ):
                owned = owned_logical_pump_ids(device_id=device_id)
                logger.warning(
                    "cloud_set_price_rejected_wrong_pump",
                    pumpId=pending.pump_id,
                    unitPriceRaw=pending.unit_price_raw,
                    correlationId=pending.correlation_id,
                    deviceId=device_id or None,
                    ownedPumpIds=sorted(owned) if owned else None,
                )
                print(
                    f"[CLOUD-PRICE] discarding SET_PRICE for {pending.pump_id} "
                    f"(this Pi owns {sorted(owned) if owned else 'unscoped'}); "
                    f"corr={pending.correlation_id}"
                )
                if not self._finalize_cloud_set_price_outcome(
                    pending=pending,
                    applied_final=set(),
                    gave_up_final=set(),
                    exec_status="REJECTED",
                    accepted=False,
                    detail="set_price_not_for_this_device",
                ):
                    return
                return

        corr = pending.correlation_id
        now = time.monotonic()
        request_age_s = self._set_price_request_age_s(pending, now)
        for old in list(self._cloud_set_price_applied):
            if old != corr:
                self._clear_cloud_set_price_tracking(old)
        applied = self._cloud_set_price_applied.setdefault(corr, set())
        gave_up = self._cloud_set_price_gave_up.setdefault(corr, set())
        awaiting_dc3 = self._cloud_set_price_awaiting_dc3.setdefault(corr, set())
        unverified = self._cloud_set_price_unverified.setdefault(corr, set())

        # Promote LINK_ACK → applied on fresh matching DC3, or when this CD5
        # provisionally seeded the face (idle Wayne never bumps obs_gen). A
        # pre-existing stale face that already matched the commanded price must
        # still wait for fresh DC3 — otherwise re-SET of the same ₦/L would
        # false-confirm without bus evidence.
        face_seeded = self._cloud_set_price_face_seeded.setdefault(corr, set())
        for addr in list(awaiting_dc3):
            session = self.sessions.get(addr)
            if session is None:
                awaiting_dc3.discard(addr)
                self._cloud_set_price_dc3_baseline.pop(f"{corr}:{addr}", None)
                self._cloud_set_price_dc3_deadline.pop(f"{corr}:{addr}", None)
                continue
            baseline = self._cloud_set_price_dc3_baseline.get(f"{corr}:{addr}", 0)
            deadline = self._cloud_set_price_dc3_deadline.get(f"{corr}:{addr}")
            obs_gen = int(session.state.unit_price_obs_gen or 0)
            observed = session.state.unit_price_raw
            retry_key = f"{corr}:{addr}"
            face_matches = (
                isinstance(observed, int)
                and not isinstance(observed, bool)
                and observed == pending.unit_price_raw
            )
            confirm_via_seed = face_matches and addr in face_seeded
            confirm_via_dc3 = face_matches and obs_gen > baseline
            if confirm_via_seed or confirm_via_dc3:
                self._price_programmed.add(addr)
                applied.add(addr)
                awaiting_dc3.discard(addr)
                unverified.discard(addr)
                self._cloud_set_price_dc3_baseline.pop(retry_key, None)
                self._cloud_set_price_dc3_deadline.pop(retry_key, None)
                self._cloud_set_price_verify_reads.pop(retry_key, None)
                self._cloud_set_price_fail_count.pop(retry_key, None)
                self._cloud_set_price_next_try.pop(retry_key, None)
                logger.info(
                    "cloud_set_price_confirmed_via_face_match",
                    address=addr,
                    unitPriceRaw=pending.unit_price_raw,
                    correlationId=corr,
                    unitPriceObsGen=obs_gen,
                    baselineGen=baseline,
                    viaSeed=confirm_via_seed,
                    viaFreshDc3=confirm_via_dc3,
                )
            elif obs_gen > baseline:
                if deadline is not None and now >= deadline:
                    self._set_price_mark_unverified(
                        corr=corr,
                        addr=addr,
                        awaiting_dc3=awaiting_dc3,
                        unverified=unverified,
                        reason=(
                            "dc3_confirm_timeout_idle_zero"
                            if self._dc3_price_unavailable(
                                observed if isinstance(observed, int) else None
                            )
                            else "dc3_confirm_timeout_nonmatch"
                        ),
                        observed=observed if isinstance(observed, int) else None,
                        unit_price_raw=pending.unit_price_raw,
                    )
                elif self._dc3_price_unavailable(
                    observed if isinstance(observed, int) else None
                ):
                    # Idle/zero DC3: keep awaiting; bounded read-only probes only.
                    reads = self._cloud_set_price_verify_reads.get(retry_key, 0)
                    log_at = self._cloud_set_price_zero_dc3_log_at.get(retry_key)
                    if log_at is None or (now - log_at) >= 5.0:
                        self._cloud_set_price_zero_dc3_log_at[retry_key] = now
                        logger.info(
                            "cloud_set_price_dc3_idle_zero_awaiting",
                            correlationId=corr,
                            address=addr,
                            unitPriceRaw=pending.unit_price_raw,
                            observedUnitPriceRaw=observed,
                            unitPriceObsGen=obs_gen,
                            verifyReads=reads,
                        )
                    if reads < self._cloud_set_price_verify_max_reads:
                        self._cloud_set_price_verify_reads[retry_key] = reads + 1
                        await self._drain_pending_data(session)
                else:
                    # Non-zero non-match: do not resend CD5; wait for deadline.
                    log_at = self._cloud_set_price_zero_dc3_log_at.get(retry_key)
                    if log_at is None or (now - log_at) >= 5.0:
                        self._cloud_set_price_zero_dc3_log_at[retry_key] = now
                        logger.warning(
                            "cloud_set_price_dc3_nonmatch_awaiting",
                            correlationId=corr,
                            address=addr,
                            unitPriceRaw=pending.unit_price_raw,
                            observedUnitPriceRaw=observed,
                            unitPriceObsGen=obs_gen,
                        )
            elif deadline is not None and now >= deadline:
                self._set_price_mark_unverified(
                    corr=corr,
                    addr=addr,
                    awaiting_dc3=awaiting_dc3,
                    unverified=unverified,
                    reason="dc3_confirm_timeout",
                    observed=observed if isinstance(observed, int) else None,
                    unit_price_raw=pending.unit_price_raw,
                )
            else:
                # Still within window with no new DC3 — optional read-only drain.
                reads = self._cloud_set_price_verify_reads.get(retry_key, 0)
                if reads < self._cloud_set_price_verify_max_reads:
                    self._cloud_set_price_verify_reads[retry_key] = reads + 1
                    await self._drain_pending_data(session)

        expired = request_age_s >= self._set_price_defer_timeout_s
        eligible: list[tuple[int, PumpSession]] = []
        deferred_addrs: list[int] = []
        for addr, session in self.sessions.items():
            if addr in applied or addr in gave_up or addr in unverified:
                continue
            if addr in awaiting_dc3:
                # Waiting for DC3 confirm — do not re-blast CD5 every tick.
                continue
            # Refresh only when fresh DATA could make this address eligible.
            # OUT / busy already cannot apply — do not burn RETURN_STATUS/polls.
            status = session.state.observed_status
            pos = session.state.nozzle_position
            busy = status in {
                ObservedStatus.AUTHORIZED,
                ObservedStatus.FILLING,
                ObservedStatus.SUSPENDED,
            }
            if not busy and pos is not NozzlePosition.OUT:
                if (
                    self._set_price_needs_completed_clear(addr, session)
                    or pos is NozzlePosition.UNKNOWN
                    or status is ObservedStatus.UNKNOWN
                    or self._set_price_evidence_stale(session)
                ):
                    await self._refresh_set_price_target_evidence(session)
            reason = self._set_price_defer_reason(addr, session)
            if expired:
                gave_up.add(addr)
                logger.warning(
                    "set_price_deferred_timeout",
                    correlationId=corr,
                    address=addr,
                    pumpId=pending.pump_id or session.state.pump_id,
                    unitPriceRaw=pending.unit_price_raw,
                    observedStatus=session.state.observed_status.value,
                    nozzleState=session.state.nozzle_position.value,
                    requestAgeS=round(request_age_s, 3),
                    deferTimeoutS=self._set_price_defer_timeout_s,
                    lastDeferReason=reason,
                    salePersistState=self._set_price_sale_persist_state(addr, session),
                )
                continue
            if reason is not None:
                deferred_addrs.append(addr)
                self._log_set_price_deferred(
                    reason,
                    correlation_id=corr,
                    unit_price_raw=pending.unit_price_raw,
                    address=addr,
                    session=session,
                    pump_id=pending.pump_id,
                )
                continue
            retry_key = f"{corr}:{addr}"
            next_try = self._cloud_set_price_next_try.get(retry_key)
            if next_try is not None and now < next_try:
                continue
            eligible.append((addr, session))

        if not eligible and not awaiting_dc3:
            # May have just confirmed via DC3 with nothing left to send.
            settled_early = (
                bool(self.sessions)
                and all(
                    a in applied or a in gave_up or a in unverified
                    for a in self.sessions
                )
                and not awaiting_dc3
            )
            if not settled_early:
                # Sibling still deferred — keep request, but persist once if any
                # address already confirmed (do not rewrite every poll).
                if applied:
                    prices = list(pending.prices_raw) or [pending.unit_price_raw]
                    nozzle_n = max(1, int(self.runtime.logical_nozzle_count or 1))
                    if len(prices) == 1 and nozzle_n > 1:
                        prices = prices * nozzle_n
                    self._persist_cloud_unit_price_once(
                        corr=corr,
                        unit_price_raw=pending.unit_price_raw,
                        prices=prices[:nozzle_n],
                        now=now,
                    )
                return
            prices = list(pending.prices_raw) or [pending.unit_price_raw]
            nozzle_n = max(1, int(self.runtime.logical_nozzle_count or 1))
            if len(prices) == 1 and nozzle_n > 1:
                prices = prices * nozzle_n
            any_ok = bool(applied)
            # Jump to persist + settle using existing block below by not returning.
        else:
            prices = list(pending.prices_raw) or [pending.unit_price_raw]
            # Match startup CD5: one price per logical nozzle on each address.
            nozzle_n = max(1, int(self.runtime.logical_nozzle_count or 1))
            if len(prices) == 1 and nozzle_n > 1:
                prices = prices * nozzle_n
            self.runtime.startup_unit_price = pending.unit_price_raw

            any_ok = False
            for addr, session in eligible:
                # Suppress parallel startup CD5 while cloud is driving this address.
                # Startup restore also defers while set-price-request.json exists;
                # mark attempted so diagnostics show cloud owned this tick.
                self._startup_price_attempted.add(addr)
                self._startup_price_state[addr] = "deferred"
                status = session.state.observed_status
                needs_clear = (
                    status
                    in {
                        ObservedStatus.FILLING_COMPLETED,
                        ObservedStatus.MAX_AMOUNT_VOLUME_REACHED,
                    }
                    or addr in self._sale_display_held
                )
                if needs_clear:
                    if self._sale_reset_blocked_by_handoff(addr):
                        self._log_set_price_deferred(
                            "sale_handoff_pending",
                            correlation_id=corr,
                            unit_price_raw=pending.unit_price_raw,
                            address=addr,
                            session=session,
                            pump_id=pending.pump_id,
                        )
                        continue
                    if not self._capture_completed_sale_snapshot(session):
                        self._log_set_price_deferred(
                            "sale_unpersisted",
                            correlation_id=corr,
                            unit_price_raw=pending.unit_price_raw,
                            address=addr,
                            session=session,
                            pump_id=pending.pump_id,
                        )
                        continue
                    # Recheck after capture: a new lift must not race RESET.
                    race = self._set_price_defer_reason(addr, session)
                    if race is not None:
                        self._log_set_price_deferred(
                            race,
                            correlation_id=corr,
                            unit_price_raw=pending.unit_price_raw,
                            address=addr,
                            session=session,
                            pump_id=pending.pump_id,
                        )
                        continue
                    logger.info(
                        "set_price_reset_scheduled",
                        address=addr,
                        pumpId=pending.pump_id or session.state.pump_id,
                        correlationId=corr,
                        unitPriceRaw=pending.unit_price_raw,
                        observedStatus=session.state.observed_status.value,
                        nozzleState=session.state.nozzle_position.value,
                        salePersistState=self._set_price_sale_persist_state(
                            addr, session
                        ),
                    )
                    reset = await self._run_owned_command(
                        session,
                        encode_cd1_command(PumpControlCommand.RESET),
                        PumpCommand.RESET,
                        expect_status=ObservedStatus.RESET,
                        idempotency=IdempotencyClass.NON_IDEMPOTENT,
                        command_label="CD1_RESET_BEFORE_CLOUD_PRICE",
                    )
                    logger.info(
                        "set_price_reset_sent",
                        address=addr,
                        pumpId=pending.pump_id or session.state.pump_id,
                        correlationId=corr,
                        result=reset.status.value,
                        observedStatus=session.state.observed_status.value,
                    )
                    print(
                        f"[CLOUD-PRICE addr={addr}] clear retained face before CD5 "
                        f"result={reset.status.value}"
                    )
                    reset_ok = (
                        reset.status is ExchangeResultStatus.APPLICATION_CONFIRMED
                        or session.state.observed_status is ObservedStatus.RESET
                    )
                    if not reset_ok:
                        logger.warning(
                            "set_price_reset_before_cd5_failed",
                            address=addr,
                            correlationId=corr,
                            result=reset.status.value,
                            observedStatus=session.state.observed_status.value,
                        )
                        retry_key = f"{corr}:{addr}"
                        fails = self._cloud_set_price_fail_count.get(retry_key, 0) + 1
                        self._cloud_set_price_fail_count[retry_key] = fails
                        delay = min(120.0, 15.0 * (2 ** min(fails - 1, 3)))
                        self._cloud_set_price_next_try[retry_key] = now + delay
                        continue
                    logger.info(
                        "set_price_reset_confirmed",
                        address=addr,
                        pumpId=pending.pump_id or session.state.pump_id,
                        correlationId=corr,
                        observedStatus=session.state.observed_status.value,
                    )
                    # Retained-sale bookkeeping only after RESET is confirmed.
                    self._sale_display_held.discard(addr)
                    self._sale_display_hold_since.pop(addr, None)
                    self._last_dc2.pop(addr, None)
                    session.state.filled_volume_raw = 0
                    session.state.filled_amount_raw = 0
                    # Recheck eligibility: a nozzle lift during RESET must not
                    # receive CD5 (would change price mid-sale).
                    race = self._set_price_defer_reason(addr, session)
                    if (
                        race is not None
                        or session.state.nozzle_position is NozzlePosition.OUT
                    ):
                        logger.warning(
                            "set_price_aborted_before_cd5",
                            address=addr,
                            correlationId=corr,
                            deferReason=race or "nozzle_out",
                            nozzleState=session.state.nozzle_position.value,
                            observedStatus=session.state.observed_status.value,
                        )
                        continue
                logger.info(
                    "set_price_cd5_scheduled",
                    address=addr,
                    pumpId=pending.pump_id or session.state.pump_id,
                    correlationId=corr,
                    unitPriceRaw=pending.unit_price_raw,
                )
                payload = encode_cd5_price_update(prices_raw=prices[:nozzle_n])
                result = await self._run_owned_command(
                    session,
                    payload,
                    PumpCommand.SET_PRICE,
                    idempotency=IdempotencyClass.NON_IDEMPOTENT,
                    command_label="CD5_SET_PRICE_CLOUD",
                )
                print(
                    f"[CLOUD-PRICE addr={addr}] CD5 price {pending.unit_price_raw} "
                    f"result={result.status.value} corr={corr}"
                )
                logger.info(
                    "cloud_set_price_cd5_result",
                    address=addr,
                    unitPriceRaw=pending.unit_price_raw,
                    correlationId=corr,
                    result=result.status.value,
                    requestedBy=pending.requested_by,
                )
                retry_key = f"{corr}:{addr}"
                if result.status is ExchangeResultStatus.APPLICATION_CONFIRMED:
                    # Only application-confirmed (or fresh DC3 match) counts as applied.
                    self._price_programmed.add(addr)
                    applied.add(addr)
                    awaiting_dc3.discard(addr)
                    self._cloud_set_price_dc3_baseline.pop(f"{corr}:{addr}", None)
                    self._cloud_set_price_dc3_deadline.pop(f"{corr}:{addr}", None)
                    self._cloud_set_price_fail_count.pop(retry_key, None)
                    self._cloud_set_price_next_try.pop(retry_key, None)
                    if (
                        isinstance(pending.unit_price_raw, int)
                        and pending.unit_price_raw > 0
                    ):
                        if self._note_command_price_lifecycle(
                            session,
                            unit_price_raw=pending.unit_price_raw,
                            application_confirmed=True,
                        ):
                            self._cloud_set_price_face_seeded.setdefault(
                                corr, set()
                            ).add(addr)
                    any_ok = True
                elif result.status is ExchangeResultStatus.LINK_ACKNOWLEDGED:
                    # Link ACK alone is not yet PRICE_CONFIRMED — await matching
                    # DC3 or confirm next tick via provisional face seed (idle
                    # Wayne DC3 often stays 0 and never bumps obs_gen).
                    awaiting_dc3.add(addr)
                    unverified.discard(addr)
                    self._cloud_set_price_dc3_baseline[f"{corr}:{addr}"] = int(
                        session.state.unit_price_obs_gen or 0
                    )
                    self._cloud_set_price_dc3_deadline[f"{corr}:{addr}"] = (
                        now + float(self._cloud_set_price_dc3_timeout_s)
                    )
                    self._cloud_set_price_verify_reads[f"{corr}:{addr}"] = 0
                    self._price_programmed.add(addr)
                    if (
                        isinstance(pending.unit_price_raw, int)
                        and pending.unit_price_raw > 0
                    ):
                        if self._note_command_price_lifecycle(
                            session,
                            unit_price_raw=pending.unit_price_raw,
                            link_acked=True,
                        ):
                            self._cloud_set_price_face_seeded.setdefault(
                                corr, set()
                            ).add(addr)
                    logger.info(
                        "cloud_set_price_link_ack_awaiting_dc3",
                        address=addr,
                        unitPriceRaw=pending.unit_price_raw,
                        observedUnitPriceRaw=session.state.unit_price_raw,
                        baselineGen=self._cloud_set_price_dc3_baseline[
                            f"{corr}:{addr}"
                        ],
                        dc3DeadlineInSeconds=self._cloud_set_price_dc3_timeout_s,
                        correlationId=corr,
                    )
                    # Persist once on link-ack (pump accepted TX); do not wait
                    # for DC3 zero-or-match. Confirmation status is separate.
                    self._persist_cloud_unit_price_once(
                        corr=corr,
                        unit_price_raw=pending.unit_price_raw,
                        prices=prices[:nozzle_n],
                        now=now,
                    )
                else:
                    # TIMED_OUT / REJECTED — never report as applied.
                    fails = self._cloud_set_price_fail_count.get(retry_key, 0) + 1
                    self._cloud_set_price_fail_count[retry_key] = fails
                    delay = min(120.0, 15.0 * (2 ** min(fails - 1, 3)))
                    self._cloud_set_price_next_try[retry_key] = now + delay
                    if fails >= 5:
                        gave_up.add(addr)
                        awaiting_dc3.discard(addr)
                        self._cloud_set_price_dc3_baseline.pop(f"{corr}:{addr}", None)
                        self._cloud_set_price_dc3_deadline.pop(f"{corr}:{addr}", None)
                        logger.warning(
                            "set_price_gave_up_address",
                            correlationId=corr,
                            address=addr,
                            unitPriceRaw=pending.unit_price_raw,
                            failures=fails,
                            lastResult=result.status.value,
                        )
                        print(
                            f"[CLOUD-PRICE addr={addr}] giving up after {fails} "
                            f"failed CD5 ({result.status.value}); "
                            "other nozzles / next lift can still apply"
                        )

        settled = (
            bool(self.sessions)
            and all(
                a in applied or a in gave_up or a in unverified for a in self.sessions
            )
            and not awaiting_dc3
        )
        if any_ok or applied or unverified:
            self._persist_cloud_unit_price_once(
                corr=corr,
                unit_price_raw=pending.unit_price_raw,
                prices=prices[:nozzle_n],
                now=now,
            )

        if settled:
            # Finish when every address succeeded, gave up, or sent-unverified.
            # Outcome must be durable before the request file is removed.
            applied_final = set(applied)
            gave_up_final = set(gave_up)
            unverified_final = set(unverified)
            # Local unit-price.json must land before we publish the cloud outcome
            # when any address confirmed or link-acked — retain for persist retry.
            if (
                (applied_final or unverified_final)
                and self._cloud_set_price_unit_persisted.get(corr)
                != pending.unit_price_raw
            ):
                logger.warning(
                    "cloud_set_price_settle_waiting_local_persist",
                    correlationId=corr,
                    unitPriceRaw=pending.unit_price_raw,
                    applied=sorted(applied_final),
                    unverified=sorted(unverified_final),
                )
                return
            if applied_final and not gave_up_final and not unverified_final:
                exec_status = "PRICE_CONFIRMED"
                accepted = True
                detail = "cd5_application_or_dc3_confirmed"
            elif unverified_final and not applied_final and not gave_up_final:
                exec_status = "SENT_UNVERIFIED"
                accepted = True
                detail = "cd5_link_ack_dc3_idle_or_timeout"
            elif applied_final and unverified_final and not gave_up_final:
                exec_status = "PRICE_PARTIAL"
                accepted = True
                detail = "some_addresses_dc3_confirmed_some_unverified"
            elif applied_final and gave_up_final:
                exec_status = "PRICE_PARTIAL"
                accepted = True
                detail = "some_addresses_confirmed_some_gave_up"
            else:
                exec_status = "PRICE_FAILED"
                accepted = False
                detail = (
                    "set_price_deferred_timeout"
                    if expired
                    else "cd5_timeout_or_reject_no_confirmed_price"
                )
            self._finalize_cloud_set_price_outcome(
                pending=pending,
                applied_final=applied_final,
                gave_up_final=gave_up_final,
                unverified_final=unverified_final,
                exec_status=exec_status,
                accepted=accepted,
                detail=detail,
            )
        elif not any_ok and eligible and not awaiting_dc3:
            if (now - self._cloud_set_price_retry_log_at) >= 5.0:
                self._cloud_set_price_retry_log_at = now
                logger.warning(
                    "set_price_apply_failed_will_retry",
                    correlationId=corr,
                    unitPriceRaw=pending.unit_price_raw,
                    addresses=[a for a, _ in eligible],
                )
                print(
                    f"[CLOUD-PRICE] retry pending {pending.unit_price_raw} "
                    f"corr={corr} (CD5 not application-confirmed; backing off)"
                )

    def _refresh_arm_requests(self) -> None:
        """Load arm-<addr> files; arm persists until consumed on next lift AUTHORIZE."""
        base = self._authorize_request_dir()
        for addr in self.sessions:
            path = base / f"arm-{addr}"
            if not path.is_file():
                continue
            try:
                path.unlink()
            except OSError as exc:
                logger.warning(
                    "owned_lab_arm_request_unlink_failed",
                    address=addr,
                    path=str(path),
                    error=str(exc),
                )
                continue
            self._armed_for_lift.add(addr)
            logger.info(
                "owned_lab_armed_for_next_lift",
                address=addr,
                path=str(path),
            )
            print(
                f"[OWNED-LAB addr={addr}] ARMED for next nozzle lift "
                f"(will AUTHORIZE once)"
            )

    def _consume_manual_authorize_request(self, address: int) -> bool:
        """True once if operator requested AUTHORIZE via request file.

        While auto-AUTHORIZE-on-lift is disabled: touch authorize-<addr> with
        nozzle OUT for immediate AUTHORIZE, or arm-<addr> then lift.
        """
        path = self._authorize_request_dir() / f"authorize-{address}"
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
        # Keep display-hold until this lift; clear face with RESET then AUTH fast.
        # Skip the inter-command bus sleep when clearing a retained sale face —
        # every 80ms stacks on the already-required RESET → CD2 → AUTHORIZE.
        clearing_retained_face = (
            addr in self._sale_display_held
            or session.state.observed_status
            in {
                ObservedStatus.FILLING_COMPLETED,
                ObservedStatus.MAX_AMOUNT_VOLUME_REACHED,
            }
        )
        if self._bus_delays_enabled() and not clearing_retained_face:
            await asyncio.sleep(0.08)
        latest = self._last_dc2.get(addr)
        face_nonzero = bool(latest and (latest[0] > 0 or latest[1] > 0))
        if session.state.observed_status is not ObservedStatus.RESET or face_nonzero:
            if self._sale_reset_blocked_by_handoff(addr):
                logger.info(
                    "pre_auth_reset_deferred_awaiting_durable_handoff",
                    address=addr,
                )
                return
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
                f"{' (clear retained face)' if face_nonzero or clearing_retained_face else ''}"
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
            self._sale_display_held.discard(addr)
            self._sale_display_hold_since.pop(addr, None)
            session.state.filled_volume_raw = 0
            session.state.filled_amount_raw = 0
            session.state.sale_evidence.reset_attempt()
            # Face is cleared by this lift's RESET — apply any deferred cloud
            # price now so SET_PRICE does not need a service restart and does
            # not wipe last-sale totals while the hose is still hung.
            await self._apply_pending_cloud_set_price()
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

        After display-hold RESET, Wayne usually sends no DC2 at all — do not
        burn four polls waiting; one empty poll is enough to proceed.
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
                    # Same nonzero totals on later polls are still a live meter;
                    # do not treat unchanged DC2 as "no fresh frame".
            elif saw_fresh_nonzero:
                # Persistently non-zero DC2 after RESET — keep waiting / fail closed.
                logger.info(
                    "pre_auth_waiting_zero_baseline",
                    address=addr,
                    attempt=attempt,
                    volumeMinorUnits=int(after[0]) if after else 0,
                    amountMinorUnits=int(after[1]) if after else 0,
                    reason="persistent_nonzero_dc2",
                )
            else:
                # No fresh DC2 after RESET (normal after display-hold). Proceed.
                session.state.filled_volume_raw = 0
                session.state.filled_amount_raw = 0
                session.state.sale_evidence.reset_attempt()
                logger.info(
                    "pre_auth_zero_baseline_confirmed",
                    address=addr,
                    attempt=attempt,
                    volumeMinorUnits=0,
                    amountMinorUnits=0,
                    reason="reset_without_fresh_dc2",
                )
                return True
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
