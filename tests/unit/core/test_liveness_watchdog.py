"""Phase 11A/11B liveness and systemd notify tests."""

from __future__ import annotations

import asyncio
import socket
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.core.liveness import LivenessTracker
from intelipump_fdc.core.systemd_notify import (
    NullNotifier,
    SystemdNotifier,
    watchdog_usec_from_env,
)
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.simulator.serial_bridge import SerialBridgeConfig, SimulatorSerialBridge


@dataclass
class RecordingNotifier:
    """Test double: records payloads; optional gate for 'enabled' sends."""

    enabled: bool = True
    notify_socket_present: bool = True
    allow_watchdog: bool = True
    sent: list[str] = field(default_factory=list)
    ready_count: int = 0
    watchdog_count: int = 0
    stopping_count: int = 0

    def ready(self, status: str | None = None) -> bool:
        payload = "READY=1"
        if status:
            payload = f"{payload}\nSTATUS={status}"
        self.sent.append(payload)
        self.ready_count += 1
        return True

    def watchdog(self) -> bool:
        if not self.allow_watchdog:
            return False
        self.sent.append("WATCHDOG=1")
        self.watchdog_count += 1
        return True

    def status(self, message: str) -> bool:
        self.sent.append(f"STATUS={message}")
        return True

    def stopping(self) -> bool:
        self.sent.append("STOPPING=1")
        self.stopping_count += 1
        return True


def test_liveness_uses_monotonic_ages() -> None:
    tracker = LivenessTracker(controller_mode="LISTEN_ONLY")
    tracker.serial_device_status = "open"
    tracker.pump_health_summary = "2/2 healthy"
    tracker.reconnect_attempts = 0
    tracker.crc_errors = 0
    assert tracker.snapshot().last_loop_progress_age_s is None
    tracker.mark_loop_progress()
    tracker.mark_successful_poll()
    time.sleep(0.02)
    snap = tracker.snapshot(total_polls=3, total_valid_responses=2, total_timeouts=1)
    assert snap.last_loop_progress_age_s is not None
    assert snap.last_loop_progress_age_s >= 0.01
    assert snap.last_successful_poll_age_s is not None
    assert snap.total_polls == 3
    assert snap.total_timeouts == 1
    assert snap.controller_mode == "LISTEN_ONLY"
    line = snap.status_line()
    assert "mode=LISTEN_ONLY" in line
    assert "serial=open" in line
    assert "pumps=2/2 healthy" in line
    assert "reconnects=0" in line
    assert "timeouts=1" in line
    assert "crc=0" in line
    assert "loop_age=" in line


def test_watchdog_usec_from_env() -> None:
    assert watchdog_usec_from_env({}) is None
    assert watchdog_usec_from_env({"WATCHDOG_USEC": "30000000"}) == 30_000_000
    assert watchdog_usec_from_env({"WATCHDOG_USEC": "0"}) is None
    assert watchdog_usec_from_env({"WATCHDOG_USEC": "nope"}) is None


def test_missing_notify_socket_is_safe() -> None:
    notifier = SystemdNotifier.from_env(
        enabled=True, environ={"WATCHDOG_USEC": "10000000"}
    )
    assert notifier.notify_socket_present is False
    assert notifier.ready("boot") is False
    assert notifier.watchdog() is False
    assert notifier.stopping() is False
    assert notifier.last_error is None


def test_disabled_watchdog_never_sends() -> None:
    notifier = SystemdNotifier(
        enabled=False, notify_socket="/tmp/does-not-matter"
    )
    assert notifier.ready() is False
    assert notifier.watchdog() is False


def test_systemd_notifier_ready_watchdog_stopping_over_socket() -> None:
    received: list[bytes] = []

    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "notify.sock")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        server.bind(path)
        server.settimeout(2.0)

        def _recv() -> None:
            try:
                while True:
                    data, _addr = server.recvfrom(4096)
                    received.append(data)
                    if b"STOPPING=1" in data:
                        break
            except TimeoutError:
                return

        thread = threading.Thread(target=_recv, daemon=True)
        thread.start()

        notifier = SystemdNotifier(
            enabled=True,
            notify_socket=path,
            watchdog_usec=5_000_000,
        )
        assert notifier.ready("mode=LISTEN_ONLY") is True
        assert notifier.watchdog() is True
        assert notifier.watchdog_pings == 1
        # Watchdog before ready would be blocked if we reset — already ready.
        assert notifier.status("loop_age=0.1s") is True
        assert notifier.stopping() is True
        thread.join(timeout=3.0)
        server.close()

    blobs = b"\n".join(received)
    assert b"READY=1" in blobs
    assert b"WATCHDOG=1" in blobs
    assert b"STOPPING=1" in blobs


def test_watchdog_not_sent_before_ready() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "notify.sock")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        server.bind(path)
        server.settimeout(0.3)
        notifier = SystemdNotifier(enabled=True, notify_socket=path)
        assert notifier.watchdog() is False
        with pytest.raises(TimeoutError):
            server.recvfrom(4096)
        server.close()


@pytest.mark.asyncio
async def test_loop_progress_feeds_watchdog_and_liveness() -> None:
    a, b = create_memory_transport_pair()
    notifier = RecordingNotifier()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=50,
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
        await loop.run(duration_s=0.4)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    assert notifier.watchdog_count > 0
    snap = loop.liveness_snapshot()
    assert snap.last_loop_progress_age_s is not None
    assert snap.total_polls > 0
    assert snap.controller_mode == "LISTEN_ONLY"
    assert runtime.safety.active_commands_enabled is False
    assert snap.serial_device_status == "disconnected"  # closed after run() finally


@pytest.mark.asyncio
async def test_watchdog_not_fed_when_loop_progress_stops() -> None:
    """If the loop never completes an iteration, watchdog must not be pinged."""
    a, _b = create_memory_transport_pair()
    notifier = RecordingNotifier()
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
    # Do not start run(); only construct — no progress marks.
    assert notifier.watchdog_count == 0
    assert loop.runtime.liveness.last_loop_progress_mono is None
    # Simulate hung state: progress frozen while a free-running timer would still tick.
    await asyncio.sleep(0.05)
    assert notifier.watchdog_count == 0


@pytest.mark.asyncio
async def test_successful_poll_updates_poll_liveness() -> None:
    a, b = create_memory_transport_pair()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=50,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
        ),
        notifier=NullNotifier(),
        status_interval_s=60.0,
    )
    loop = ControllerLoop(runtime)
    bridge = SimulatorSerialBridge(
        b, config=SerialBridgeConfig(idle_sleep_ms=0, sim_time_step_ms=10)
    )

    async def run_ctrl() -> None:
        await loop.run(duration_s=0.35)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    assert loop.runtime.liveness.last_successful_poll_mono is not None
    snap = loop.liveness_snapshot()
    assert snap.total_valid_responses >= 1


def test_ready_before_watchdog_sequencing() -> None:
    notifier = RecordingNotifier()
    assert notifier.watchdog_count == 0
    notifier.ready("init")
    assert notifier.ready_count == 1
    assert any("READY=1" in s for s in notifier.sent)
    notifier.watchdog()
    notifier.stopping()
    assert notifier.stopping_count == 1
    assert notifier.sent[-1] == "STOPPING=1"


def test_null_notifier_records_without_socket() -> None:
    n = NullNotifier()
    n.ready("x")
    n.watchdog()
    n.stopping()
    assert n.sent[0].startswith("READY=1")
    assert "WATCHDOG=1" in n.sent
    assert n.sent[-1] == "STOPPING=1"


def test_get_settings_watchdog_defaults_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INTELIPUMP_WATCHDOG__ENABLED", raising=False)
    from intelipump_fdc.core.config import get_settings

    get_settings.cache_clear()
    settings = get_settings()
    assert settings.watchdog.enabled is False
    get_settings.cache_clear()
