"""Phase 11C serial and communication health monitoring tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from intelipump_fdc.controller.comm_health import (
    HealthThresholds,
    HealthTransitionLog,
    SerialHealth,
    SerialHealthMonitor,
    SerialReconnectBackoff,
    pump_health_compact,
    pump_health_summary,
)
from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.controller.session_models import CommunicationHealth
from intelipump_fdc.core.systemd_notify import NullNotifier
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.constants import SF
from intelipump_fdc.protocol.dart.line.escaping import escape_dle, unescape_dle
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_eot
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.protocol.dart.transport.base import ByteTransport, TransportMetadata
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.simulator.encoding import encode_dc1_status
from intelipump_fdc.simulator.serial_bridge import SerialBridgeConfig, SimulatorSerialBridge


@dataclass
class RecordingNotifier:
    enabled: bool = True
    notify_socket_present: bool = True
    sent: list[str] = field(default_factory=list)
    watchdog_count: int = 0

    def ready(self, status: str | None = None) -> bool:
        self.sent.append("READY=1")
        return True

    def watchdog(self) -> bool:
        self.sent.append("WATCHDOG=1")
        self.watchdog_count += 1
        return True

    def status(self, message: str) -> bool:
        self.sent.append(f"STATUS={message}")
        return True

    def stopping(self) -> bool:
        self.sent.append("STOPPING=1")
        return True


class FlakySerialTransport(ByteTransport):
    """Transport that fails open N times, then succeeds; can be force-closed."""

    def __init__(self, *, fail_opens: int = 0, path: str = "/dev/missing-serial") -> None:
        self._fail_opens = fail_opens
        self._open_attempts = 0
        self._open = False
        self._path = path
        self._peer: FlakySerialTransport | None = None
        self._buf = bytearray()

    def pair_with(self, other: FlakySerialTransport) -> None:
        self._peer = other
        other._peer = self

    @property
    def is_open(self) -> bool:
        return self._open

    @property
    def metadata(self) -> TransportMetadata:
        return TransportMetadata(
            name="flaky",
            device=self._path,
            kind="serial_physical",
        )

    async def open(self) -> None:
        self._open_attempts += 1
        if self._open_attempts <= self._fail_opens:
            raise OSError("No such device")
        self._open = True

    async def close(self) -> None:
        self._open = False

    def force_close(self) -> None:
        self._open = False

    async def read(self, max_bytes: int) -> bytes:
        if not self._open:
            raise OSError("transport closed")
        if not self._buf:
            await asyncio.sleep(0.01)
            return b""
        data = bytes(self._buf[:max_bytes])
        del self._buf[:max_bytes]
        return data

    async def write(self, data: bytes) -> int:
        if not self._open:
            raise OSError("transport closed")
        if self._peer is not None:
            self._peer._buf.extend(data)
        return len(data)

    async def drain(self) -> None:
        return None


def _parse(raw: bytes) -> DartLineFrame:
    parsed = parse_frame(raw)
    assert isinstance(parsed, DartLineFrame)
    return parsed


def test_backoff_increases_and_caps() -> None:
    backoff = SerialReconnectBackoff(min_delay_s=0.5, max_delay_s=15.0, jitter=0.0)
    delays = [backoff.next_delay_s() for _ in range(8)]
    assert delays[0] == pytest.approx(0.5)
    assert delays[1] == pytest.approx(1.0)
    assert delays[2] == pytest.approx(2.0)
    assert delays[3] == pytest.approx(4.0)
    assert delays[4] == pytest.approx(8.0)
    assert delays[5] == pytest.approx(15.0)
    assert delays[6] == pytest.approx(15.0)
    backoff.reset()
    assert backoff.next_delay_s() == pytest.approx(0.5)


def test_three_timeouts_degraded_ten_disconnected() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    for _ in range(2):
        s.on_timeout()
        assert s.state.communication is not CommunicationHealth.DEGRADED
    s.on_timeout()
    assert s.state.communication is CommunicationHealth.DEGRADED
    assert s.state.stats.timeout_count == 3
    for _ in range(7):
        s.on_timeout()
    assert s.state.consecutive_timeouts == 10
    assert s.state.communication is CommunicationHealth.DISCONNECTED
    assert s.state.stats.timeout_count == 10


def test_eot_and_data_restore_healthy_preserve_timeout_count() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    for _ in range(3):
        s.on_timeout()
    assert s.state.communication is CommunicationHealth.DEGRADED
    s.handle_response_frame(_parse(build_eot(encode_wire_address(1), 0)))
    assert s.state.communication is CommunicationHealth.HEALTHY
    assert s.state.last_transient_error is None
    assert s.state.stats.timeout_count == 3

    for _ in range(3):
        s.on_timeout()
    frame = _parse(build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1)))
    assert s.handle_response_frame(frame) is not None
    assert s.state.communication is CommunicationHealth.HEALTHY
    assert s.state.stats.timeout_count == 6


def test_persistent_fault_remains_after_valid_response() -> None:
    s = PumpSession(
        address=1,
        pump_id="p1",
        events=EventBus(),
        thresholds=HealthThresholds(faulted_after_protocol_errors=1),
    )
    good = build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1))
    body = bytearray(unescape_dle(good[:-1]))
    body[-3] ^= 0xFF
    bad = escape_dle(bytes(body)) + bytes((SF,))
    assert s.handle_response_frame(_parse(bad)) is None
    assert s.state.last_persistent_fault == "invalid_crc"
    assert s.state.communication is CommunicationHealth.FAULTED
    s.handle_response_frame(_parse(build_eot(encode_wire_address(1), 0)))
    assert s.state.communication is CommunicationHealth.HEALTHY
    assert s.state.last_persistent_fault == "invalid_crc"


def test_transition_logs_once_per_state_change() -> None:
    log = HealthTransitionLog()
    s = PumpSession(
        address=1, pump_id="p1", events=EventBus(), transitions=log
    )
    for _ in range(3):
        s.on_timeout()
    degraded = [e for e in log.events if e["event"] == "pump_communication_degraded"]
    assert len(degraded) == 1
    for _ in range(7):
        s.on_timeout()
    disconnected = [e for e in log.events if e["event"] == "pump_disconnected"]
    assert len(disconnected) == 1
    s.handle_response_frame(_parse(build_eot(encode_wire_address(1), 0)))
    recovered = [e for e in log.events if e["event"] == "pump_communication_recovered"]
    assert len(recovered) == 1


def test_pump_health_summary() -> None:
    states = {
        1: CommunicationHealth.HEALTHY,
        2: CommunicationHealth.DEGRADED,
        3: CommunicationHealth.DISCONNECTED,
        4: CommunicationHealth.FAULTED,
    }
    assert pump_health_summary(states) == "1/4 healthy"
    assert pump_health_compact(states) == "H1/D1/X1/F1"


def test_duplicate_data_restores_health_without_double_apply() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    for _ in range(3):
        s.on_timeout()
    assert s.state.communication is CommunicationHealth.DEGRADED
    payload = encode_dc1_status(1)
    frame = _parse(build_data_frame(encode_wire_address(1), 0, payload))
    assert s.handle_response_frame(frame) is not None
    assert s.state.communication is CommunicationHealth.HEALTHY
    version = s.machine.context.state_version
    data_count = s.state.stats.data_count
    ack = s.handle_response_frame(frame)
    assert ack is not None
    assert s.state.stats.duplicate_count == 1
    assert s.state.stats.data_count == data_count
    assert s.machine.context.state_version == version
    assert s.state.communication is CommunicationHealth.HEALTHY
    assert s.state.last_valid_data_mono is not None


def test_serial_missing_and_faulted_events() -> None:
    log = HealthTransitionLog()
    mon = SerialHealthMonitor(
        configured_path="/dev/does-not-exist-intelipump-11c",
        thresholds=HealthThresholds(serial_faulted_after_failures=3),
        transitions=log,
        virtual=False,
    )
    assert any(e["event"] == "serial_missing" for e in log.events)
    mon.observe_open_failure("FileNotFoundError")
    mon.observe_open_failure("FileNotFoundError")
    mon.observe_open_failure("FileNotFoundError")
    assert mon.state.health is SerialHealth.FAULTED
    faulted = [e for e in log.events if e["event"] == "serial_faulted"]
    assert len(faulted) == 1
    missing = [e for e in log.events if e["event"] == "serial_missing"]
    assert len(missing) == 1


@pytest.mark.asyncio
async def test_missing_serial_does_not_crash_and_uses_backoff() -> None:
    transport = FlakySerialTransport(fail_opens=3, path="/dev/ttyUSB-missing")
    transitions = HealthTransitionLog()
    notifier = RecordingNotifier()
    runtime = ControllerRuntime(
        transport=transport,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=20,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
            reconnect_min_delay_s=0.05,
            reconnect_max_delay_s=0.2,
            reconnect_jitter=0.0,
        ),
        notifier=notifier,
        health_transitions=transitions,
        status_interval_s=60.0,
    )
    loop = ControllerLoop(runtime)
    assert runtime.safety.mode.value == "LISTEN_ONLY"
    assert runtime.safety.active_commands_enabled is False

    await loop.run(duration_s=0.45)
    assert transport._open_attempts >= 3
    delays = [
        e["reconnect_delay_seconds"]
        for e in transitions.events
        if e["event"] == "serial_reconnect_attempt"
    ]
    assert delays
    assert delays[0] == pytest.approx(0.05)
    if len(delays) > 1:
        assert delays[1] >= delays[0]
    assert max(delays) <= 0.2 + 1e-9
    assert notifier.watchdog_count > 0  # fed by reconnect progress
    assert loop.serial_health.state.reconnect_attempt_count >= 1
    # No outbound commands generated during reconnect.
    assert len(runtime.outbound) == 0


@pytest.mark.asyncio
async def test_successful_reconnect_resets_backoff() -> None:
    transport = FlakySerialTransport(fail_opens=2, path="/dev/ttyUSB-flaky")
    peer = FlakySerialTransport(fail_opens=0, path="/dev/ttyUSB-peer")
    transport.pair_with(peer)
    # Peer always open for simulator side.
    await peer.open()
    transitions = HealthTransitionLog()
    runtime = ControllerRuntime(
        transport=transport,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=30,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
            reconnect_min_delay_s=0.05,
            reconnect_max_delay_s=1.0,
            reconnect_jitter=0.0,
        ),
        notifier=NullNotifier(),
        health_transitions=transitions,
        status_interval_s=60.0,
    )
    loop = ControllerLoop(runtime)
    bridge = SimulatorSerialBridge(
        peer, config=SerialBridgeConfig(idle_sleep_ms=0, sim_time_step_ms=10)
    )

    async def run_ctrl() -> None:
        await loop.run(duration_s=0.6)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    assert any(e["event"] == "serial_reconnected" for e in transitions.events)
    assert loop.serial_health.backoff.failures == 0
    assert loop.serial_health.state.successful_reconnect_count >= 1


@pytest.mark.asyncio
async def test_serial_loss_marks_pumps_disconnected() -> None:
    a, b = create_memory_transport_pair()
    transitions = HealthTransitionLog()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1, 2),
            response_timeout_ms=30,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
        ),
        notifier=NullNotifier(),
        health_transitions=transitions,
        status_interval_s=60.0,
    )
    loop = ControllerLoop(runtime)
    bridge = SimulatorSerialBridge(
        b, config=SerialBridgeConfig(idle_sleep_ms=0, sim_time_step_ms=10)
    )

    async def run_and_drop() -> None:
        task = asyncio.create_task(loop.run(duration_s=2.0))
        await asyncio.sleep(0.15)
        await a.close()
        loop.serial_health.observe_closed(reason="unplug")
        loop._mark_all_pumps_disconnected()
        await asyncio.sleep(0.05)
        loop.request_stop()
        bridge.request_stop()
        await task

    await asyncio.gather(bridge.run(), run_and_drop())
    assert all(
        s.state.communication is CommunicationHealth.DISCONNECTED
        for s in loop.sessions.values()
    )
    assert any(e["event"] == "serial_disconnected" for e in transitions.events)


@pytest.mark.asyncio
async def test_watchdog_not_independent_timer_during_healthy_gap() -> None:
    notifier = RecordingNotifier()
    a, _b = create_memory_transport_pair()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=20,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
        ),
        notifier=notifier,
        status_interval_s=60.0,
    )
    loop = ControllerLoop(runtime)
    # No run started — independent time passing must not feed watchdog.
    await asyncio.sleep(0.05)
    assert notifier.watchdog_count == 0
    assert loop.runtime.liveness.last_loop_progress_mono is None


@pytest.mark.asyncio
async def test_status_includes_serial_and_pump_summary() -> None:
    a, b = create_memory_transport_pair()
    notifier = RecordingNotifier()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=30,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
        ),
        notifier=notifier,
        status_interval_s=0.05,
    )
    loop = ControllerLoop(runtime)
    bridge = SimulatorSerialBridge(
        b, config=SerialBridgeConfig(idle_sleep_ms=0, sim_time_step_ms=10)
    )

    async def run_ctrl() -> None:
        await loop.run(duration_s=0.35)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    status_lines = [s for s in notifier.sent if s.startswith("STATUS=")]
    assert status_lines
    assert "serial=" in status_lines[-1]
    assert "pumps=" in status_lines[-1]
    assert "reconnects=" in status_lines[-1]
    assert "timeouts=" in status_lines[-1]
    assert "crc=" in status_lines[-1]
    assert "healthy" in status_lines[-1]
    snap = loop.liveness_snapshot()
    assert "healthy" in snap.pump_health_summary
    diag = loop.health_diagnostic_snapshot()
    assert diag["overall"]["listen_only"] is True
    assert diag["overall"]["active_commands_enabled"] is False
    assert "serial" in diag and "pumps" in diag and "thresholds" in diag


def test_serial_monitor_missing_path_status() -> None:
    mon = SerialHealthMonitor(
        configured_path="/dev/does-not-exist-intelipump",
        thresholds=HealthThresholds(),
        virtual=False,
    )
    mon.observe_open_failure("FileNotFoundError")
    assert mon.state.status == "missing"
    assert mon.state.health is SerialHealth.DISCONNECTED
    assert mon.state.reconnect_attempt_count == 1


@pytest.mark.asyncio
async def test_no_busy_loop_while_serial_missing() -> None:
    transport = FlakySerialTransport(fail_opens=10_000, path="/dev/ttyUSB-missing")
    runtime = ControllerRuntime(
        transport=transport,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=20,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
            reconnect_min_delay_s=0.1,
            reconnect_max_delay_s=0.1,
            reconnect_jitter=0.0,
        ),
        notifier=NullNotifier(),
        status_interval_s=60.0,
    )
    loop = ControllerLoop(runtime)
    await loop.run(duration_s=0.35)
    # With 0.1s backoff, about 3-4 open attempts fit in 0.35s, not dozens.
    assert transport._open_attempts <= 6
    assert transport._open_attempts >= 2


@pytest.mark.asyncio
async def test_healthy_polling_cadence_unchanged() -> None:
    a, b = create_memory_transport_pair()
    poll_times: list[float] = []
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=40,
            inter_poll_delay_ms=5,
            idle_sleep_ms=20,
            max_retries=0,
        ),
        notifier=NullNotifier(),
        status_interval_s=60.0,
    )
    loop = ControllerLoop(runtime)
    original_poll = loop.sessions[1].build_poll

    def _timed_poll() -> bytes:
        poll_times.append(asyncio.get_running_loop().time())
        return original_poll()

    loop.sessions[1].build_poll = _timed_poll  # type: ignore[method-assign]
    bridge = SimulatorSerialBridge(
        b, config=SerialBridgeConfig(idle_sleep_ms=0, sim_time_step_ms=10)
    )

    async def run_ctrl() -> None:
        await loop.run(duration_s=0.35)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    assert len(poll_times) >= 3
    gaps = [poll_times[i + 1] - poll_times[i] for i in range(len(poll_times) - 1)]
    # One address: inter_poll + idle ≈ 25ms; allow generous simulator slack.
    assert min(gaps) >= 0.015
    assert max(gaps) < 0.2
    assert runtime.safety.mode.value == "LISTEN_ONLY"
    assert len(runtime.outbound) == 0
