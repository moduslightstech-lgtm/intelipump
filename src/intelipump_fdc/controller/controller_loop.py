"""Async polling scheduler and controller loop."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from intelipump_fdc.controller.outbound import OutboundQueue
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.safety import (
    ControllerSafetyContext,
    evaluate_polling_allowed,
)
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.controller.session_models import IdempotencyClass, OutboundDataItem
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.core.liveness import LivenessSnapshot, LivenessTracker
from intelipump_fdc.core.systemd_notify import Notifier, NullNotifier
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.protocol.dart.line.stream import (
    AssemblerEventKind,
    FrameStreamAssembler,
)
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


@dataclass
class ControllerRuntime:
    transport: ByteTransport
    safety: ControllerSafetyContext
    config: PollSchedulerConfig = field(default_factory=PollSchedulerConfig)
    events: EventBus = field(default_factory=EventBus)
    outbound: OutboundQueue = field(default_factory=OutboundQueue)
    log_frames: bool = False
    liveness: LivenessTracker = field(default_factory=LivenessTracker)
    notifier: Notifier = field(default_factory=NullNotifier)
    status_interval_s: float = 15.0


class ControllerLoop:
    """Round-robin DART polling over an abstract transport."""

    def __init__(self, runtime: ControllerRuntime) -> None:
        self.runtime = runtime
        self.sessions: dict[int, PumpSession] = {
            addr: PumpSession(
                address=addr,
                pump_id=f"pump-{addr}",
                events=runtime.events,
                sequence_policy=runtime.config.sequence_policy,
            )
            for addr in runtime.config.addresses
        }
        self.assembler = FrameStreamAssembler()
        self.totals = ControllerTotals()
        self._stop = asyncio.Event()
        self._last_status_mono: float | None = None
        # Align liveness metadata with safety context.
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
        return self.runtime.liveness.snapshot(
            total_polls=self.totals.poll_count,
            total_valid_responses=self.totals.eot_count + self.totals.data_count,
            total_timeouts=self.totals.timeouts,
        )

    def _sync_serial_status(self) -> None:
        transport = self.runtime.transport
        if transport.is_open:
            self.runtime.liveness.serial_device_status = "open"
        else:
            self.runtime.liveness.serial_device_status = "closed"

    def _on_loop_progress(self) -> None:
        """Mark iteration complete and feed systemd watchdog from real progress."""
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

    async def run(self, *, duration_s: float | None = None) -> None:
        """Run the poll loop until stop, optional deadline, or transport close.

        ``duration_s=None`` runs continuously until ``request_stop()`` (SIGINT /
        SIGTERM from the CLI). Positive ``duration_s`` stops after that many
        seconds. Zero and negative values are rejected.
        """
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

        await self.runtime.transport.open()
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
                    await self._reconnect()
                    # Still mark progress so a reconnect wait does not look hung
                    # to systemd while we are actively recovering.
                    self._on_loop_progress()
                    continue
                for address in self.runtime.config.addresses:
                    if self._stop.is_set():
                        break
                    try:
                        await self._poll_one(address)
                    except Exception as exc:
                        session = self.sessions[address]
                        session.state.last_error = f"poll_error:{type(exc).__name__}:{exc}"
                        if self.runtime.log_frames:
                            print(f"pump {address} error: {exc}")
                    await asyncio.sleep(self.runtime.config.inter_poll_delay_ms / 1000)
                # Full round-robin iteration completed — feed liveness + watchdog.
                self._on_loop_progress()
                await asyncio.sleep(self.runtime.config.idle_sleep_ms / 1000)
        finally:
            await self.runtime.transport.close()
            self._sync_serial_status()

    async def _reconnect(self) -> None:
        await asyncio.sleep(self.runtime.config.reconnect_delay_s)
        try:
            await self.runtime.transport.open()
        except Exception:
            return

    async def _poll_one(self, address: int) -> None:
        session = self.sessions[address]
        await self._maybe_send_outbound(session)

        await self._write_frame(session.build_poll(), address=address, note="POLL")

        frame = await self._read_one_frame(
            self.runtime.config.response_timeout_ms, address=address
        )
        if frame is None:
            await self._handle_timeout_with_retries(session)
            self._refresh_totals()
            return

        if frame.address != address:
            session.state.last_error = f"response_address_{frame.address}"
            self._refresh_totals()
            return

        ack = session.handle_response_frame(frame)
        if ack is not None:
            await self._write_frame(ack, address=address, note="ACK")
        self.runtime.liveness.mark_successful_poll()
        self._refresh_totals()

    async def _handle_timeout_with_retries(self, session: PumpSession) -> None:
        session.on_timeout(
            max_consecutive=self.runtime.config.max_consecutive_timeouts
        )
        for _ in range(self.runtime.config.max_retries):
            if self._stop.is_set():
                return
            await self._write_frame(
                session.build_poll(), address=session.address, note="POLL_RETRY"
            )
            frame = await self._read_one_frame(
                self.runtime.config.response_timeout_ms, address=session.address
            )
            if frame is not None and frame.address == session.address:
                ack = session.handle_response_frame(frame)
                if ack is not None:
                    await self._write_frame(
                        ack, address=session.address, note="ACK"
                    )
                self.runtime.liveness.mark_successful_poll()
                return
            session.on_timeout(
                max_consecutive=self.runtime.config.max_consecutive_timeouts
            )

    async def _maybe_send_outbound(self, session: PumpSession) -> None:
        item = self.runtime.outbound.pop_for_address(session.address)
        if item is None:
            return
        seq = session.state.tx_sequence
        wire = build_data_frame(session.address, seq, item.application_payload)
        await self._write_frame(wire, address=session.address, note="DATA_OUT")
        resp = await self._read_one_frame(
            self.runtime.config.response_timeout_ms, address=session.address
        )
        if resp is None:
            session.state.stats.timeout_count += 1
            return
        if resp.control_type is ControlType.ACK and resp.sequence == seq:
            session.state.tx_sequence = next_sequence(
                seq, self.runtime.config.sequence_policy
            )
        elif resp.control_type is ControlType.NAK:
            session.state.stats.nak_count += 1

    async def _write_frame(self, data: bytes, *, address: int, note: str) -> None:
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

    async def _read_one_frame(
        self, timeout_ms: int, *, address: int | None = None
    ) -> DartLineFrame | None:
        """Read until one frame or software deadline.

        The deadline is ``timeout_ms`` (configured bench/protocol timeout).
        Transport read timeout must stay short so pyserial returns as soon as
        bytes arrive; it must not equal this deadline or every sample clusters
        near the timeout.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + (timeout_ms / 1000)
        first_byte_marked = False
        while loop.time() < deadline:
            chunk = await self.runtime.transport.read(
                self.runtime.config.read_chunk_size
            )
            if chunk:
                if not first_byte_marked and address is not None:
                    first_byte_marked = True
                    self.runtime.events.publish(
                        ControllerEvent(
                            type=ControllerEventType.FIRST_RESPONSE_BYTE,
                            address=address,
                            timestamp=datetime.now(UTC),
                            payload={
                                "first_byte_monotonic_s": time.monotonic(),
                                "raw_hex": chunk.hex(" "),
                            },
                        )
                    )
                if self.runtime.log_frames:
                    print(f"RX raw {chunk.hex(' ')}")
                for event in self.assembler.feed(chunk):
                    if event.kind is AssemblerEventKind.FRAME and event.frame is not None:
                        if self.runtime.log_frames:
                            print(
                                f"RX frame {event.frame.control_type} "
                                f"{event.raw.hex(' ')}"
                            )
                        return event.frame
                    if event.kind in {
                        AssemblerEventKind.REJECTED,
                        AssemblerEventKind.OVERFLOW,
                        AssemblerEventKind.NOISE,
                    }:
                        self.runtime.events.publish(
                            ControllerEvent(
                                type=ControllerEventType.FRAME_REJECTED,
                                timestamp=datetime.now(UTC),
                                detail=event.kind.value,
                                payload={"raw_hex": event.raw.hex(" ")},
                            )
                        )
            else:
                # Transport read already blocked up to read_timeout_s; yield only.
                await asyncio.sleep(0)
        return None

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
            },
            "pumps": {
                str(addr): {
                    "state": s.state.last_state.value,
                    "communication": s.state.communication.value,
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
