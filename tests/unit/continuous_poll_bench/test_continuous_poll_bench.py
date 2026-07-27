"""Deterministic tests for intelipump-continuous-poll-bench."""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from itertools import pairwise
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from intelipump_fdc.continuous_poll_bench.cli import build_parser
from intelipump_fdc.continuous_poll_bench.cli import run as continuous_run
from intelipump_fdc.continuous_poll_bench.evidence import ContinuousBenchResult
from intelipump_fdc.continuous_poll_bench.guards import (
    REAL_WAYNE_MAX_DURATION_S,
    REAL_WAYNE_MAX_WRITES,
    ContinuousPollBenchParams,
    ContinuousPollConfirmations,
    ContinuousPollRefusedError,
    run_continuous_poll_preflight,
    validate_continuous_params,
    validate_continuous_settings,
)
from intelipump_fdc.continuous_poll_bench.session import (
    ContinuousPollSession,
    ContinuousPollSessionConfig,
)
from intelipump_fdc.controller.safety import (
    ControllerSafetyContext,
    evaluate_outbound_safety,
    evaluate_polling_allowed,
)
from intelipump_fdc.controller.session_models import (
    IdempotencyClass,
    OutboundDataItem,
)
from intelipump_fdc.core.config import ControllerMode, Settings
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.constants import SF
from intelipump_fdc.protocol.dart.line.escaping import escape_dle, unescape_dle
from intelipump_fdc.protocol.dart.line.frame_builder import (
    build_ack,
    build_data_frame,
    build_eot,
    build_poll,
)
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError
from intelipump_fdc.simulator.encoding import encode_dc1_status


def _as_int(value: object) -> int:
    return cast(int, value)


def _as_float(value: object) -> float:
    return cast(float, value)


def _as_str(value: object) -> str:
    return cast(str, value)


def _confirms(**overrides: bool) -> ContinuousPollConfirmations:
    base = dict(
        owned_lab_pump=True,
        technician_present=True,
        emergency_isolation_ready=True,
        no_fuel_test=True,
        authorization_disabled=True,
        status_poll_only=True,
        bounded_duration=True,
    )
    base.update(overrides)
    return ContinuousPollConfirmations(**base)


def _lab_settings(**kwargs: object) -> Settings:
    s = Settings(
        environment="LAB",
        controller={"mode": ControllerMode.CONTINUOUS_POLL_BENCH},
        safety={
            "active_commands_enabled": False,
            "remote_authorization_enabled": False,
            "automatic_authorization_enabled": False,
            "command_replay_enabled": False,
            "allow_lab_simulator_commands": False,
        },
        mqtt={"enabled": False},
    )
    for key, value in kwargs.items():
        setattr(s, key, value)
    return s


def _params(tmp_path: Path, **overrides: object) -> ContinuousPollBenchParams:
    base: dict[str, object] = dict(
        port="/tmp/fake-cpb",
        address=1,
        duration_seconds=3.0,
        poll_interval_ms=100,
        response_timeout_ms=50,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        simulator_validation=True,
        skip_service_check=True,
        skip_port_check=True,
    )
    base.update(overrides)
    return ContinuousPollBenchParams(**base)  # type: ignore[arg-type]


@dataclass
class FakeBenchTransport:
    device: str = "/tmp/fake-cpb"
    chunks: list[bytes] = field(default_factory=list)
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    write_delay_s: float = 0.0
    disconnect_after_writes: int | None = None
    _open: bool = False
    auto_eot_address: int | None = 1

    @property
    def is_open(self) -> bool:
        return self._open

    async def open(self) -> None:
        self._open = True

    async def close(self) -> None:
        self._open = False

    async def read(self, max_bytes: int) -> bytes:
        if not self.chunks:
            await asyncio.sleep(0.005)
            return b""
        data = self.chunks.pop(0)
        return data[:max_bytes]

    def device_path_exists(self) -> bool:
        return True

    async def flush(self) -> None:
        return None

    async def write(self, data: bytes) -> int:
        if (
            self.disconnect_after_writes is not None
            and self.write_count >= self.disconnect_after_writes
        ):
            raise OSError("serial disconnect")
        if self.write_delay_s > 0:
            await asyncio.sleep(self.write_delay_s)
        self.write_count += 1
        self.written.append(data)
        if self.auto_eot_address is not None:
            self.chunks.append(build_eot(encode_wire_address(self.auto_eot_address), 0))
        return len(data)


def _session(
    transport: FakeBenchTransport,
    tmp_path: Path,
    **overrides: object,
) -> ContinuousPollSession:
    cfg_kwargs: dict[str, object] = dict(
        port="/tmp/fake",
        address=1,
        baud=9600,
        duration_seconds=3.0,
        poll_interval_ms=100,
        response_timeout_ms=50,
        evidence_jsonl=tmp_path / "e.jsonl",
        evidence_md=tmp_path / "e.md",
        max_writes=50,
        target_type="SIMULATOR",
        simulator_validation=True,
    )
    cfg_kwargs.update(overrides)
    return ContinuousPollSession(
        transport, ContinuousPollSessionConfig(**cfg_kwargs)  # type: ignore[arg-type]
    )


def test_parser_has_no_raw_command_payload_options() -> None:
    parser = build_parser()
    option_strings = {
        opt
        for action in parser._actions
        for opt in (action.option_strings or [])
    }
    forbidden = {
        "--raw-hex",
        "--payload",
        "--command",
        "--authorize",
        "--replay",
        "--hex",
        "--daemon",
        "--indefinite",
    }
    assert forbidden.isdisjoint(option_strings)
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--port",
                "/tmp/x",
                "--address",
                "1",
                "--duration-seconds",
                "3",
                "--poll-interval-ms",
                "100",
                "--response-timeout-ms",
                "50",
                "--evidence-dir",
                "/tmp/ev",
                "--raw-hex",
                "01 fa",
            ]
        )


def test_one_address_only_cli() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--port",
                "/tmp/x",
                "--address",
                "1,2",
                "--duration-seconds",
                "3",
                "--poll-interval-ms",
                "100",
                "--response-timeout-ms",
                "50",
                "--evidence-dir",
                "/tmp/ev",
            ]
        )


def test_missing_confirmation_refuses(tmp_path: Path) -> None:
    with pytest.raises(ContinuousPollRefusedError, match="missing confirmation"):
        validate_continuous_params(
            _params(tmp_path, confirmations=_confirms(status_poll_only=False))
        )


def test_duration_maximum_enforced(tmp_path: Path) -> None:
    with pytest.raises(ContinuousPollRefusedError, match="duration"):
        validate_continuous_params(
            _params(tmp_path, duration_seconds=31, simulator_validation=True)
        )
    with pytest.raises(ContinuousPollRefusedError, match="duration"):
        validate_continuous_params(
            _params(
                tmp_path,
                duration_seconds=REAL_WAYNE_MAX_DURATION_S + 1,
                simulator_validation=False,
            )
        )


def test_real_wayne_max_5_seconds(tmp_path: Path) -> None:
    validate_continuous_params(
        _params(
            tmp_path,
            duration_seconds=5,
            poll_interval_ms=300,
            response_timeout_ms=250,
            simulator_validation=False,
        )
    )
    with pytest.raises(ContinuousPollRefusedError, match="5"):
        validate_continuous_params(
            _params(
                tmp_path,
                duration_seconds=5.1,
                poll_interval_ms=300,
                response_timeout_ms=250,
                simulator_validation=False,
            )
        )


def test_real_wayne_min_poll_interval_300ms(tmp_path: Path) -> None:
    with pytest.raises(ContinuousPollRefusedError, match="300"):
        validate_continuous_params(
            _params(
                tmp_path,
                duration_seconds=3,
                poll_interval_ms=100,
                response_timeout_ms=50,
                simulator_validation=False,
            )
        )
    validate_continuous_params(
        _params(
            tmp_path,
            duration_seconds=3,
            poll_interval_ms=300,
            response_timeout_ms=250,
            simulator_validation=False,
        )
    )


def test_max_50_writes_enforced_for_real_wayne(tmp_path: Path) -> None:
    # Real-Wayne min interval 300ms keeps 5s sessions under the 50-write cap.
    validate_continuous_params(
        _params(
            tmp_path,
            duration_seconds=5,
            poll_interval_ms=300,
            response_timeout_ms=250,
            simulator_validation=False,
        )
    )
    assert REAL_WAYNE_MAX_WRITES == 50
    duration_ms = 5.0 * 1000.0
    approx = int((duration_ms - 1e-9) // 300) + 1
    assert approx <= REAL_WAYNE_MAX_WRITES


def test_authorization_flags_must_remain_false() -> None:
    cases = (
        "active_commands_enabled",
        "remote_authorization_enabled",
        "automatic_authorization_enabled",
        "command_replay_enabled",
    )
    for attr in cases:
        s = _lab_settings()
        setattr(s.safety, attr, True)
        with pytest.raises(ContinuousPollRefusedError):
            validate_continuous_settings(s)
    s = _lab_settings()
    s.mqtt.enabled = True
    with pytest.raises(ContinuousPollRefusedError, match="MQTT"):
        validate_continuous_settings(s)


def test_continuous_mode_blocks_controller_queue() -> None:
    ctx = ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.CONTINUOUS_POLL_BENCH,
        active_commands_enabled=False,
        require_physical_control_enable=True,
    )
    assert evaluate_polling_allowed(ctx).allowed is False
    item = OutboundDataItem.create(
        address=1,
        application_payload=b"\x00",
        command_type=PumpCommand.READ_STATUS,
        simulator_only=True,
        idempotency=IdempotencyClass.IDEMPOTENT,
    )
    assert evaluate_outbound_safety(item, ctx).allowed is False


def test_service_active_refusal(tmp_path: Path) -> None:
    def runner(*_a: object, **_k: object) -> MagicMock:
        m = MagicMock()
        m.returncode = 0
        return m

    port = tmp_path / "ttyUSB0"
    port.touch()
    with pytest.raises(ContinuousPollRefusedError, match="active"):
        run_continuous_poll_preflight(
            _params(
                tmp_path,
                port=str(port),
                skip_service_check=False,
                skip_port_check=True,
            ),
            _lab_settings(),
            systemctl_runner=runner,
        )


def test_real_mode_refuses_simulator_process(tmp_path: Path) -> None:
    port = tmp_path / "ttyUSB0"
    port.touch()
    with pytest.raises(ContinuousPollRefusedError, match="simulator"):
        run_continuous_poll_preflight(
            _params(
                tmp_path,
                port=str(port),
                poll_interval_ms=300,
                response_timeout_ms=250,
                simulator_validation=False,
                skip_service_check=True,
                skip_port_check=True,
            ),
            _lab_settings(),
            simulator_pid_finder=lambda **_k: [4242],
        )


def test_simulator_ownership_rules(tmp_path: Path) -> None:
    ctrl = tmp_path / "ttyUSB0"
    sim = tmp_path / "ttyUSB1"
    ctrl.touch()
    sim.touch()
    ctrl_alias = tmp_path / "intelipump-controller"
    sim_alias = tmp_path / "intelipump-simulator"
    ctrl_alias.symlink_to(ctrl)
    sim_alias.symlink_to(sim)
    sim_canon = os.path.realpath(sim)

    # Simulator may own sim adapter; controller must be free.
    canonical, lock = run_continuous_poll_preflight(
        _params(
            tmp_path,
            port=str(ctrl_alias),
            simulator_validation=True,
            simulator_ports=(str(sim_alias),),
            skip_service_check=True,
            skip_port_check=False,
            lock_dir=tmp_path / "locks",
        ),
        _lab_settings(),
        systemctl_runner=lambda *_a, **_k: MagicMock(returncode=3),
        holder_finder=lambda p: [3085] if os.path.realpath(p) == sim_canon else [],
        simulator_checker=lambda pid: pid == 3085,
        simulator_pid_finder=lambda **_k: [3085],
    )
    assert canonical == os.path.realpath(ctrl)
    assert lock is not None
    lock.release()

    # Simulator owning controller refused.
    with pytest.raises(ContinuousPollRefusedError):
        run_continuous_poll_preflight(
            _params(
                tmp_path,
                port=str(ctrl_alias),
                simulator_validation=True,
                simulator_ports=(str(sim_alias),),
                skip_service_check=True,
                skip_port_check=False,
                lock_dir=tmp_path / "locks2",
            ),
            _lab_settings(),
            systemctl_runner=lambda *_a, **_k: MagicMock(returncode=3),
            holder_finder=lambda p: [3085],
            simulator_checker=lambda pid: pid == 3085,
            simulator_pid_finder=lambda **_k: [3085],
        )


@pytest.mark.asyncio
async def test_simulator_continuous_polling_100ms_3s(tmp_path: Path) -> None:
    transport = FakeBenchTransport()
    session = _session(
        transport,
        tmp_path,
        duration_seconds=3.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
        max_writes=50,
    )
    t0 = time.monotonic()
    summary = await session.run()
    elapsed = time.monotonic() - t0
    assert 2.8 <= elapsed <= 3.8
    assert summary["pollsSent"] == transport.write_count
    # Monotonic 100ms over 3s → ~30 polls; allow small edge variance.
    assert 28 <= _as_int(summary["pollsSent"]) <= 31
    assert all(w == build_poll(1) for w in transport.written)
    assert _as_int(summary["validResponses"]) >= 1
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0
    assert summary["result"] == ContinuousBenchResult.PASS.value
    assert summary["stopReason"] == "duration_expired"


@pytest.mark.asyncio
async def test_exact_bounded_build_poll_writes(tmp_path: Path) -> None:
    transport = FakeBenchTransport()
    session = _session(
        transport,
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
        max_writes=50,
    )
    summary = await session.run()
    assert summary["writeCount"] == summary["pollsSent"]
    assert transport.write_count == summary["pollsSent"]
    assert all(frame == build_poll(1) for frame in transport.written)


@pytest.mark.asyncio
async def test_no_catchup_burst_after_scheduler_delay(tmp_path: Path) -> None:
    # Each write takes 250ms; interval 100ms → must skip slots, not burst.
    transport = FakeBenchTransport(write_delay_s=0.25)
    session = _session(
        transport,
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
        max_writes=50,
    )
    summary = await session.run()
    # Without skip: ~10 polls; with 250ms/write catch-up would still try many.
    # With skip: roughly 1s/0.25s ≈ 4 polls.
    assert _as_int(summary["pollsSent"]) <= 6
    assert _as_int(summary["scheduleLagEvents"]) >= 1
    # No back-to-back bursts: successive TX timestamps spaced.
    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    tx_ns = [
        r["monotonicNs"]
        for r in records
        if r.get("direction") == "TX"
    ]
    if len(tx_ns) >= 2:
        gaps_ms = [(b - a) / 1e6 for a, b in pairwise(tx_ns)]
        assert all(g >= 90 for g in gaps_ms)


@pytest.mark.asyncio
async def test_monotonic_scheduling(tmp_path: Path) -> None:
    transport = FakeBenchTransport()
    session = _session(
        transport,
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
    )
    await session.run()
    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    tx = [r for r in records if r.get("direction") == "TX"]
    monos = [r["monotonicNs"] for r in tx]
    assert monos == sorted(monos)
    seqs = [r["pollSequence"] for r in tx]
    assert seqs == sorted(seqs)
    assert seqs == list(range(1, len(seqs) + 1))


@pytest.mark.asyncio
async def test_timeout_does_not_extend_duration(tmp_path: Path) -> None:
    transport = FakeBenchTransport(auto_eot_address=None, chunks=[])
    session = _session(
        transport,
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
    )
    t0 = time.monotonic()
    summary = await session.run()
    elapsed = time.monotonic() - t0
    assert elapsed < 1.5
    assert _as_float(summary["actualDurationS"]) < 1.5
    assert _as_int(summary["timeouts"]) >= 1
    assert summary["validResponses"] == 0
    assert summary["result"] == ContinuousBenchResult.INCONCLUSIVE.value


@pytest.mark.asyncio
async def test_stale_partial_not_concatenated_into_next_poll(
    tmp_path: Path,
) -> None:
    """Timed-out 16-byte prefix must not glue onto the next complete DATA frame."""
    partial_16 = bytes.fromhex(
        "50 30 02 08 00 00 00 00 00 00 00 00 03 04 00 99"
    )
    full_25 = bytes.fromhex(
        "50 30 02 08 00 00 00 00 00 00 00 00 03 04 00 99 "
        "07 07 01 01 00 0e 55 03 fa"
    )
    assert len(partial_16) == 16
    assert len(full_25) == 25
    concatenated_41 = partial_16 + full_25
    assert len(concatenated_41) == 41

    @dataclass
    class PartialThenFullTransport:
        device: str = "/tmp/fake-stale"
        written: list[bytes] = field(default_factory=list)
        write_count: int = 0
        chunks: list[bytes] = field(default_factory=list)
        _open: bool = False

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        async def read(self, max_bytes: int) -> bytes:
            if not self.chunks:
                await asyncio.sleep(0.005)
                return b""
            data = self.chunks.pop(0)
            return data[:max_bytes]

        def device_path_exists(self) -> bool:
            return True

        async def flush(self) -> None:
            return None

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            self.written.append(data)
            if self.write_count == 1:
                self.chunks.append(partial_16)
            elif self.write_count == 2:
                self.chunks.append(full_25)
            return len(data)

    transport = PartialThenFullTransport()
    session = _session(
        transport,  # type: ignore[arg-type]
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
        max_writes=10,
    )
    summary = await session.run()
    assert _as_int(summary["timeouts"]) >= 1
    assert _as_int(summary["validResponses"]) >= 1
    assert summary["crcErrors"] == 0
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0

    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    # Poll 1 timed out with PARTIAL_OR_NOISE evidence of the 16-byte prefix.
    partial_events = [
        r
        for r in records
        if r.get("pollSequence") == 1
        and r.get("responseClassification") == "PARTIAL_OR_NOISE"
    ]
    assert partial_events
    assert any(
        bytes(int(p, 16) for p in r["rawHex"].split()) == partial_16
        for r in partial_events
        if r.get("rawHex")
    )

    # Poll 2 must accept exactly the 25-byte CRC-valid DATA_FRAME once.
    poll2_rx = [
        r
        for r in records
        if r.get("pollSequence") == 2
        and r.get("direction") == "RX"
        and r.get("responseClassification") == "DATA_FRAME"
        and r.get("crcValid") is True
    ]
    assert len(poll2_rx) == 1
    rx_raw = bytes(int(p, 16) for p in poll2_rx[0]["rawHex"].split())
    assert rx_raw == full_25
    assert len(rx_raw) == 25

    # Never emit the 41-byte stale-prefix concatenation.
    all_rx = [
        bytes(int(p, 16) for p in r["rawHex"].split())
        for r in records
        if r.get("direction") == "RX" and r.get("rawHex")
    ]
    assert concatenated_41 not in all_rx
    assert all(len(raw) != 41 for raw in all_rx)


@pytest.mark.asyncio
async def test_ctrl_c_bounded_clean_stop(tmp_path: Path) -> None:
    transport = FakeBenchTransport()
    session = _session(
        transport,
        tmp_path,
        duration_seconds=5.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
    )

    async def _intr() -> None:
        await asyncio.sleep(0.25)
        session.request_stop()

    summary, _ = await asyncio.gather(session.run(), _intr())
    assert summary["stopReason"] == "operator_interrupt"
    assert summary["result"] == ContinuousBenchResult.FAIL.value
    assert "bench_stopped" in (tmp_path / "e.jsonl").read_text()
    assert not transport.is_open
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0


@pytest.mark.asyncio
async def test_unexpected_address_immediate_stop(tmp_path: Path) -> None:
    transport = FakeBenchTransport(auto_eot_address=2)
    session = _session(transport, tmp_path, duration_seconds=2.0)
    summary = await session.run()
    assert summary["pollsSent"] == 1
    assert summary["stopReason"] == "address_mismatch"
    assert summary["result"] == ContinuousBenchResult.FAIL.value


@pytest.mark.asyncio
async def test_unsupported_response_immediate_stop(tmp_path: Path) -> None:
    transport = FakeBenchTransport(auto_eot_address=None)
    transport.chunks = []  # filled on write manually

    async def write(data: bytes) -> int:
        transport.write_count += 1
        transport.written.append(data)
        transport.chunks.append(build_ack(encode_wire_address(1), 0))
        return len(data)

    transport.write = write  # type: ignore[method-assign]
    session = _session(transport, tmp_path, duration_seconds=2.0)
    summary = await session.run()
    assert summary["pollsSent"] == 1
    assert summary["unexpectedFrames"] == 1
    assert summary["stopReason"] == "unexpected_frame"
    assert summary["result"] == ContinuousBenchResult.FAIL.value


@pytest.mark.asyncio
async def test_crc_error_handling(tmp_path: Path) -> None:
    good = build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1))
    body = bytearray(unescape_dle(good[:-1]))
    body[-3] ^= 0xFF
    bad = escape_dle(bytes(body)) + bytes((SF,))
    transport = FakeBenchTransport(auto_eot_address=None)

    async def write(data: bytes) -> int:
        transport.write_count += 1
        transport.written.append(data)
        # Queue after TX so pre-poll stale-drain cannot consume it.
        transport.chunks.append(bad)
        return len(data)

    transport.write = write  # type: ignore[method-assign]
    session = _session(transport, tmp_path, duration_seconds=2.0)
    summary = await session.run()
    assert summary["pollsSent"] == 1
    assert summary["crcErrors"] == 1
    assert summary["result"] == ContinuousBenchResult.FAIL.value


@pytest.mark.asyncio
async def test_serial_disconnect_handling(tmp_path: Path) -> None:
    transport = FakeBenchTransport(disconnect_after_writes=0)
    session = _session(transport, tmp_path, duration_seconds=2.0)
    summary = await session.run()
    assert summary["stopReason"] == "serial_disconnect"
    assert summary["result"] == ContinuousBenchResult.FAIL.value


@pytest.mark.asyncio
async def test_max_writes_enforced_at_runtime(tmp_path: Path) -> None:
    transport = FakeBenchTransport()
    session = _session(
        transport,
        tmp_path,
        duration_seconds=3.0,
        poll_interval_ms=50,
        response_timeout_ms=20,
        max_writes=5,
        simulator_validation=True,
    )
    summary = await session.run()
    assert _as_int(summary["pollsSent"]) <= 5
    assert _as_int(summary["writeCount"]) <= 5
    assert summary["stopReason"] == "max_writes"
    assert summary["result"] == ContinuousBenchResult.FAIL.value


@pytest.mark.asyncio
async def test_command_queue_and_auth_objects_remain_zero(tmp_path: Path) -> None:
    transport = FakeBenchTransport()
    session = _session(
        transport,
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
    )
    summary = await session.run()
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0
    assert session.command_queue_created is False
    assert session.authorization_objects_created == 0


@pytest.mark.asyncio
async def test_evidence_records_every_poll_and_stop_reason(tmp_path: Path) -> None:
    transport = FakeBenchTransport()
    session = _session(
        transport,
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
    )
    summary = await session.run()
    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    tx = [r for r in records if r.get("direction") == "TX"]
    assert len(tx) == summary["pollsSent"]
    assert all(r.get("softwareCommit") for r in records)
    stopped = [r for r in records if r.get("event") == "bench_stopped"]
    assert len(stopped) == 1
    assert stopped[0]["stopReason"] == summary["stopReason"]
    md = (tmp_path / "e.md").read_text()
    assert "Stop reason:" in md
    assert _as_str(summary["result"]) in md


@pytest.mark.asyncio
async def test_poll_runner_sends_only_build_poll(tmp_path: Path) -> None:
    transport = FakeBenchTransport()
    session = _session(
        transport,
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=40,
        address=2,
    )
    transport.auto_eot_address = 2
    await session.run()
    expected = build_poll(2)
    assert transport.written
    assert all(frame == expected for frame in transport.written)


def test_cli_refuses_without_mode(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("INTELIPUMP_ENVIRONMENT", "LAB")
    monkeypatch.setenv("INTELIPUMP_CONTROLLER__MODE", "LISTEN_ONLY")
    from intelipump_fdc.core.config import get_settings

    get_settings.cache_clear()
    with pytest.raises(SystemExit) as excinfo:
        continuous_run(
            [
                "--port",
                str(tmp_path / "p"),
                "--address",
                "1",
                "--duration-seconds",
                "3",
                "--poll-interval-ms",
                "100",
                "--response-timeout-ms",
                "50",
                "--evidence-dir",
                str(tmp_path / "ev"),
                "--confirm-owned-lab-pump",
                "--confirm-technician-present",
                "--confirm-emergency-isolation-ready",
                "--confirm-no-fuel-test",
                "--confirm-authorization-disabled",
                "--confirm-status-poll-only",
                "--confirm-bounded-duration",
                "--simulator-validation",
                "--skip-service-check",
                "--skip-port-check",
            ]
        )
    assert excinfo.value.code == 2
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_transport_not_open_maps_to_disconnect(tmp_path: Path) -> None:
    @dataclass
    class BrokenTransport:
        device: str = "/tmp/x"
        write_count: int = 0
        _open: bool = False

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        async def read(self, max_bytes: int) -> bytes:
            raise TransportNotOpenError("gone")

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            return len(data)

    session = _session(BrokenTransport(), tmp_path, duration_seconds=1.0)  # type: ignore[arg-type]
    summary = await session.run()
    assert summary["stopReason"] == "serial_disconnect"


WAYNE_25 = bytes.fromhex(
    "50 30 02 08 00 00 00 00 00 00 00 00 03 04 00 99 "
    "07 07 01 01 00 0e 55 03 fa"
)


@pytest.mark.asyncio
async def test_three_consecutive_wayne_25_byte_data_frames(
    tmp_path: Path,
) -> None:
    """Each of three poll cycles must yield one CRC-valid DATA_FRAME."""
    assert len(WAYNE_25) == 25

    @dataclass
    class WayneFrameTransport:
        device: str = "/tmp/fake-wayne25"
        written: list[bytes] = field(default_factory=list)
        write_count: int = 0
        buf: bytearray = field(default_factory=bytearray)
        _open: bool = False

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        def device_path_exists(self) -> bool:
            return True

        async def flush(self) -> None:
            return None

        async def read(self, max_bytes: int) -> bytes:
            if not self.buf:
                await asyncio.sleep(0.002)
                return b""
            data = bytes(self.buf[:max_bytes])
            del self.buf[:max_bytes]
            return data

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            self.written.append(data)
            self.buf.extend(WAYNE_25)
            return len(data)

    transport = WayneFrameTransport()
    session = _session(
        transport,  # type: ignore[arg-type]
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=50,
        max_writes=3,
    )
    summary = await session.run()
    assert summary["pollsSent"] == 3
    assert summary["validResponses"] == 3
    assert summary["crcErrors"] == 0
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0

    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    for seq in (1, 2, 3):
        rx = [
            r
            for r in records
            if r.get("pollSequence") == seq
            and r.get("direction") == "RX"
            and r.get("responseClassification") == "DATA_FRAME"
            and r.get("crcValid") is True
        ]
        assert len(rx) == 1
        raw = bytes(int(p, 16) for p in rx[0]["rawHex"].split())
        assert raw == WAYNE_25

    chunks = [r for r in records if r.get("source") == "serial_read_chunk"]
    assert chunks
    assert all(r.get("event") == "serial_read_chunk" for r in chunks)


@pytest.mark.asyncio
async def test_transient_empty_read_recovers(tmp_path: Path) -> None:
    @dataclass
    class TransientEmptyTransport:
        device: str = "/tmp/fake-transient"
        written: list[bytes] = field(default_factory=list)
        write_count: int = 0
        chunks: list[bytes] = field(default_factory=list)
        empty_raises_left: int = 1
        _open: bool = False

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        def device_path_exists(self) -> bool:
            return True

        async def flush(self) -> None:
            return None

        async def read(self, max_bytes: int) -> bytes:
            if self.empty_raises_left > 0:
                self.empty_raises_left -= 1
                raise OSError(
                    "device reports readiness to read but returned no data "
                    "(device disconnected or multiple access on port?)"
                )
            if not self.chunks:
                await asyncio.sleep(0.002)
                return b""
            data = self.chunks.pop(0)
            return data[:max_bytes]

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            self.written.append(data)
            self.chunks.append(WAYNE_25)
            return len(data)

    transport = TransientEmptyTransport()
    session = _session(
        transport,  # type: ignore[arg-type]
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=80,
        max_writes=2,
    )
    summary = await session.run()
    assert summary["stopReason"] != "serial_disconnect"
    assert _as_int(summary["validResponses"]) >= 1
    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    assert any(r.get("event") == "transient_empty_read" for r in records)


@pytest.mark.asyncio
async def test_repeated_transient_empty_read_disconnects(tmp_path: Path) -> None:
    @dataclass
    class RepeatedEmptyTransport:
        device: str = "/tmp/fake-repeated-empty"
        written: list[bytes] = field(default_factory=list)
        write_count: int = 0
        _open: bool = False

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        def device_path_exists(self) -> bool:
            return True

        async def flush(self) -> None:
            return None

        async def read(self, max_bytes: int) -> bytes:
            raise OSError(
                "device reports readiness to read but returned no data "
                "(device disconnected or multiple access on port?)"
            )

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            self.written.append(data)
            return len(data)

    transport = RepeatedEmptyTransport()
    session = _session(
        transport,  # type: ignore[arg-type]
        tmp_path,
        duration_seconds=1.0,
        poll_interval_ms=100,
        response_timeout_ms=80,
        max_writes=5,
    )
    summary = await session.run()
    assert summary["stopReason"] == "serial_disconnect"
    assert summary["result"] == ContinuousBenchResult.FAIL.value
    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    assert any(r.get("event") == "transient_empty_read" for r in records)
    assert any(r.get("event") == "serial_disconnect" for r in records)


def test_cli_defaults_are_conservative_for_real_wayne() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--port",
            "/tmp/x",
            "--address",
            "1",
            "--evidence-dir",
            "/tmp/ev",
            "--confirm-owned-lab-pump",
            "--confirm-technician-present",
            "--confirm-emergency-isolation-ready",
            "--confirm-no-fuel-test",
            "--confirm-authorization-disabled",
            "--confirm-status-poll-only",
            "--confirm-bounded-duration",
        ]
    )
    assert args.poll_interval_ms == 300
    assert args.response_timeout_ms == 250
    assert args.duration_seconds == 3
