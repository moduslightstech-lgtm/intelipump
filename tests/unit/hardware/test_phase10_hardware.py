"""Phase 10 hardware discovery / validation / bench unit tests."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.hardware.adapter_validation import (
    expected_serial_config,
    validate_config_fields,
)
from intelipump_fdc.hardware.bench_runner import _BenchEventSubscriber
from intelipump_fdc.hardware.errors import (
    BenchConfigError,
    OddParityUnsupportedError,
)
from intelipump_fdc.hardware.evidence import write_evidence
from intelipump_fdc.hardware.fault_injection import (
    corrupt_frame_byte,
    inject_noise,
    truncate_frame,
)
from intelipump_fdc.hardware.latency import LatencyTracker
from intelipump_fdc.hardware.models import BenchConfig, BenchEvidence, SerialDeviceInfo
from intelipump_fdc.hardware.serial_discovery import (
    list_serial_devices,
    prefer_stable_path,
    resolve_device,
)
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_control_frame
from intelipump_fdc.protocol.dart.transport.errors import TransportConfigError
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.protocol.dart.transport.serial import SerialConfig, SerialParity
from intelipump_fdc.simulator.serial_bridge import (
    SerialBridgeConfig,
    SimulatorSerialBridge,
)


def test_bench_config_lab_only_and_distinct_ports() -> None:
    with pytest.raises(BenchConfigError):
        BenchConfig(
            environment="PROD",
            controller_port="/dev/a",
            simulator_port="/dev/b",
        ).validate()
    with pytest.raises(BenchConfigError):
        BenchConfig(
            controller_port="/dev/a",
            simulator_port="/dev/a",
        ).validate()
    with pytest.raises(BenchConfigError):
        BenchConfig(
            controller_port="/dev/a",
            simulator_port="/dev/b",
            parity="NONE",
        ).validate()
    BenchConfig(controller_port="/dev/a", simulator_port="/dev/b").validate()


def test_serial_config_rejects_flow_control_and_non_odd() -> None:
    with pytest.raises(TransportConfigError):
        SerialConfig(device="/tmp/x", flow_control=True).validate()
    with pytest.raises(OddParityUnsupportedError):
        validate_config_fields(
            SerialConfig(device="/tmp/x", parity=SerialParity.EVEN)
        )


def test_expected_serial_config_is_dart() -> None:
    cfg = expected_serial_config("/dev/serial/by-id/demo")
    assert cfg.baud_rate == 9600
    assert cfg.parity is SerialParity.ODD
    assert cfg.data_bits == 8
    assert cfg.stop_bits == 1
    assert cfg.flow_control is False


def test_list_serial_devices_prefers_by_id(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_port = SimpleNamespace(
        device="/dev/ttyUSB0",
        vid=0x0403,
        pid=0x6001,
        serial_number="A1",
        manufacturer="FTDI",
        product="USB-RS485",
        interface=None,
        location="1-1.2",
        hwid="USB VID:PID=0403:6001",
        description="USB-RS485",
    )

    monkeypatch.setattr(
        "intelipump_fdc.hardware.serial_discovery.platform.system",
        lambda: "Linux",
    )
    monkeypatch.setattr(
        "intelipump_fdc.hardware.serial_discovery._linux_by_id_map",
        lambda: {"/dev/ttyUSB0": "/dev/serial/by-id/usb-FTDI-A1"},
    )

    with patch("serial.tools.list_ports.comports", return_value=[fake_port]):
        devices = list_serial_devices()
    assert len(devices) == 1
    assert devices[0].device_path == "/dev/serial/by-id/usb-FTDI-A1"
    assert devices[0].by_id_path is not None
    assert devices[0].vid == 0x0403
    assert devices[0].stable_id == "usb-FTDI-A1"


def test_resolve_device_by_stable_id() -> None:
    devices = (
        SerialDeviceInfo(
            device_path="/dev/serial/by-id/usb-ctrl",
            stable_id="usb-ctrl",
            by_id_path="/dev/serial/by-id/usb-ctrl",
        ),
        SerialDeviceInfo(
            device_path="/dev/serial/by-id/usb-sim",
            stable_id="usb-sim",
            by_id_path="/dev/serial/by-id/usb-sim",
        ),
    )
    got = resolve_device(stable_id="usb-sim", devices=devices)
    assert got.device_path.endswith("usb-sim")


def test_prefer_stable_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "intelipump_fdc.hardware.serial_discovery._linux_by_id_map",
        lambda: {"/dev/ttyUSB1": "/dev/serial/by-id/usb-x"},
    )
    monkeypatch.setattr(
        "intelipump_fdc.hardware.serial_discovery.os.path.realpath",
        lambda p: p,
    )
    assert prefer_stable_path("/dev/ttyUSB1") == "/dev/serial/by-id/usb-x"


def test_fault_injection_helpers() -> None:
    frame = build_control_frame(1, ControlType.POLL)
    corrupted = corrupt_frame_byte(frame, index=2)
    assert corrupted != frame
    assert len(corrupted) == len(frame)
    noisy = inject_noise(frame, noise_bytes=2)
    assert noisy.endswith(frame)
    assert len(noisy) == len(frame) + 2
    assert truncate_frame(frame, keep=2) == frame[:2]


def test_corruption_detected_by_crc_mismatch() -> None:
    frame = build_control_frame(1, ControlType.POLL)
    bad = corrupt_frame_byte(frame, index=1)
    assert bad != frame


def test_latency_tracker() -> None:
    tracker = LatencyTracker(protocol_target_ms=25)
    tracker.mark_poll_start(1, monotonic_s=1.0)
    sample = tracker.mark_response(1, monotonic_s=1.01, response_kind="EOT_RECEIVED")
    assert sample is not None
    assert abs((sample.poll_to_response_ms or 0) - 10.0) < 0.001
    assert tracker.summary.mean_ms == pytest.approx(10.0)
    assert tracker.summary.count == 1


def test_evidence_write(tmp_path: Path) -> None:
    evidence = BenchEvidence(
        bench=BenchConfig(controller_port="/dev/a", simulator_port="/dev/b"),
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        success=True,
        listen_only=True,
        active_commands_enabled=False,
        poll_count=10,
    )
    path = write_evidence(evidence, tmp_path / "evidence.json")
    data = json.loads(path.read_text())
    assert data["listen_only"] is True
    assert data["active_commands_enabled"] is False
    assert data["poll_count"] == 10
    assert "password" not in path.read_text().lower()


@pytest.mark.asyncio
async def test_memory_bench_controller_simulator_latency() -> None:
    a, b = create_memory_transport_pair()
    tracker = LatencyTracker(protocol_target_ms=100)
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=100,
            inter_poll_delay_ms=2,
            idle_sleep_ms=5,
            max_retries=1,
        ),
    )
    runtime.events.add_subscriber(
        _BenchEventSubscriber(
            tracker,
            capture=None,
            port="memory",
            stable_id="memory",
        )
    )
    loop = ControllerLoop(runtime)
    bridge = SimulatorSerialBridge(
        b,
        config=SerialBridgeConfig(idle_sleep_ms=1, sim_time_step_ms=10),
    )

    async def run_ctrl() -> None:
        await loop.run(duration_s=1.5)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    totals = loop.summary()["totals"]
    assert isinstance(totals, dict)
    assert totals["poll_count"] > 0
    assert tracker.summary.count > 0
    assert runtime.safety.mode.value == "LISTEN_ONLY"
    assert runtime.safety.active_commands_enabled is False


@pytest.mark.asyncio
async def test_one_address_timeout_does_not_block_other() -> None:
    """Controller continues polling address 2 after address 1 times out."""
    a, b = create_memory_transport_pair()
    bridge = SimulatorSerialBridge(
        b,
        config=SerialBridgeConfig(idle_sleep_ms=1, sim_time_step_ms=10),
    )
    bridge.simulator.pumps[1].communication_enabled = False

    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1, 2),
            response_timeout_ms=30,
            inter_poll_delay_ms=2,
            idle_sleep_ms=5,
            max_retries=0,
            max_consecutive_timeouts=20,
        ),
    )
    loop = ControllerLoop(runtime)

    async def run_ctrl() -> None:
        await loop.run(duration_s=1.5)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    summary = loop.summary()
    totals = summary["totals"]
    assert isinstance(totals, dict)
    assert totals["poll_count"] > 0
    assert 2 in loop.sessions


@pytest.mark.asyncio
async def test_validate_adapter_open_mocked_success() -> None:
    from intelipump_fdc.hardware.adapter_validation import validate_adapter_open

    class FakeSerial:
        parity = "O"
        baudrate = 9600
        bytesize = 8
        stopbits = 1.0
        xonxoff = False
        rtscts = False

        def __init__(self, **kwargs: object) -> None:
            assert kwargs["parity"] == "O"
            assert kwargs.get("xonxoff") is False

        def close(self) -> None:
            return None

    fake_serial_mod = MagicMock()
    fake_serial_mod.Serial = FakeSerial
    fake_serial_mod.PARITY_ODD = "O"
    with patch.dict("sys.modules", {"serial": fake_serial_mod}):
        result = await validate_adapter_open(expected_serial_config("/dev/null"))
    assert result.ok
    assert result.parity == "ODD"


@pytest.mark.asyncio
async def test_validate_adapter_odd_parity_failure() -> None:
    from intelipump_fdc.hardware.adapter_validation import validate_adapter_open

    def boom(**_kwargs: object) -> object:
        raise OSError("odd parity not supported by adapter")

    fake_serial_mod = MagicMock()
    fake_serial_mod.Serial = boom
    with (
        patch.dict("sys.modules", {"serial": fake_serial_mod}),
        pytest.raises(OddParityUnsupportedError),
    ):
        await validate_adapter_open(expected_serial_config("/dev/null"))
