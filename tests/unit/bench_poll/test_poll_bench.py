"""Deterministic tests for intelipump-poll-bench."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from intelipump_fdc.bench_poll.cli import build_parser
from intelipump_fdc.bench_poll.cli import run as poll_bench_run
from intelipump_fdc.bench_poll.evidence import BenchResult
from intelipump_fdc.bench_poll.guards import (
    TARGET_OWNED_LAB_WAYNE,
    TARGET_SIMULATOR,
    PollBenchConfirmations,
    PollBenchParams,
    PollBenchRefusedError,
    assert_simulator_not_owning_adapters,
    run_poll_bench_preflight,
    validate_poll_bench_params,
    validate_poll_bench_settings,
)
from intelipump_fdc.bench_poll.session import PollBenchSession, PollBenchSessionConfig
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
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_eot, build_poll
from intelipump_fdc.simulator.encoding import encode_dc1_status


def _confirms(**overrides: bool) -> PollBenchConfirmations:
    base = dict(
        owned_lab_pump=True,
        technician_present=True,
        emergency_isolation_ready=True,
        no_fuel_test=True,
        authorization_disabled=True,
    )
    base.update(overrides)
    return PollBenchConfirmations(**base)


def _lab_poll_settings(**kwargs: object) -> Settings:
    s = Settings(
        environment="LAB",
        controller={"mode": ControllerMode.POLL_ONLY_BENCH},
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


@dataclass
class FakeBenchTransport:
    device: str = "/tmp/fake-poll-bench"
    chunks: list[bytes] = field(default_factory=list)
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

    async def read(self, max_bytes: int) -> bytes:
        if not self.chunks:
            await asyncio.sleep(0.01)
            return b""
        data = self.chunks.pop(0)
        return data[:max_bytes]

    async def write(self, data: bytes) -> int:
        self.write_count += 1
        self.written.append(data)
        return len(data)


def test_parser_has_no_raw_hex_or_payload() -> None:
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
    }
    assert forbidden.isdisjoint(option_strings)
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--port",
                "/tmp/x",
                "--address",
                "1",
                "--raw-hex",
                "01 fa",
            ]
        )


def test_missing_confirmation_refuses() -> None:
    with pytest.raises(PollBenchRefusedError, match="missing confirmation"):
        validate_poll_bench_params(
            PollBenchParams(
                port="/tmp/x",
                address=1,
                baud=9600,
                max_polls=1,
                response_timeout_ms=100,
                evidence_dir=Path("/tmp"),
                confirmations=_confirms(technician_present=False),
            )
        )


def test_non_lab_refuses() -> None:
    s = _lab_poll_settings()
    s.environment = "PROD"
    with pytest.raises(PollBenchRefusedError, match="LAB"):
        validate_poll_bench_settings(s)


def test_wrong_mode_refuses() -> None:
    s = _lab_poll_settings()
    s.controller.mode = ControllerMode.LISTEN_ONLY
    with pytest.raises(PollBenchRefusedError, match="POLL_ONLY_BENCH"):
        validate_poll_bench_settings(s)


def test_max_polls_bounds() -> None:
    base = dict(
        port="/tmp/x",
        address=1,
        baud=9600,
        response_timeout_ms=100,
        evidence_dir=Path("/tmp"),
        confirmations=_confirms(),
    )
    with pytest.raises(PollBenchRefusedError, match="max-polls"):
        validate_poll_bench_params(PollBenchParams(**base, max_polls=0))
    with pytest.raises(PollBenchRefusedError, match="max-polls"):
        validate_poll_bench_params(PollBenchParams(**base, max_polls=11))


def test_authorization_replay_mqtt_refuse() -> None:
    cases: list[tuple[str, str]] = [
        ("active_commands_enabled", "active"),
        ("remote_authorization_enabled", "remote"),
        ("automatic_authorization_enabled", "automatic"),
        ("command_replay_enabled", "replay"),
    ]
    for attr, match in cases:
        s = _lab_poll_settings()
        setattr(s.safety, attr, True)
        with pytest.raises(PollBenchRefusedError, match=match):
            validate_poll_bench_settings(s)
    s = _lab_poll_settings()
    s.mqtt.enabled = True
    with pytest.raises(PollBenchRefusedError, match="MQTT"):
        validate_poll_bench_settings(s)


def test_service_active_refuses(tmp_path: Path) -> None:
    def runner(*_a: object, **_k: object) -> MagicMock:
        m = MagicMock()
        m.returncode = 0
        return m

    params = PollBenchParams(
        port=str(tmp_path / "ttyUSB0"),
        address=1,
        baud=9600,
        max_polls=1,
        response_timeout_ms=50,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        lock_dir=tmp_path / "locks",
        skip_port_check=True,
    )
    (tmp_path / "ttyUSB0").touch()
    with pytest.raises(PollBenchRefusedError, match="active"):
        run_poll_bench_preflight(
            params, _lab_poll_settings(), systemctl_runner=runner
        )


def test_simulator_process_active_refuses(tmp_path: Path) -> None:
    port = tmp_path / "ttyUSB1"
    port.touch()
    with pytest.raises(PollBenchRefusedError, match="simulator"):
        assert_simulator_not_owning_adapters(
            [str(port)],
            holder_finder=lambda _p: [4242],
            simulator_checker=lambda pid: pid == 4242,
        )


def _alias_pair(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    """Return (ctrl, sim, ctrl_alias, sim_alias)."""
    ctrl = tmp_path / "ttyUSB0"
    sim = tmp_path / "ttyUSB1"
    ctrl.touch()
    sim.touch()
    ctrl_alias = tmp_path / "intelipump-controller"
    sim_alias = tmp_path / "intelipump-simulator"
    ctrl_alias.symlink_to(ctrl)
    sim_alias.symlink_to(sim)
    return ctrl, sim, ctrl_alias, sim_alias


def test_simulator_owns_sim_adapter_with_flag_allowed(tmp_path: Path) -> None:
    ctrl, sim, ctrl_alias, sim_alias = _alias_pair(tmp_path)
    sim_canon = os.path.realpath(sim)
    ctrl_canon = os.path.realpath(ctrl)

    def holders(path: str) -> list[int]:
        if path == sim_canon:
            return [3085]
        if path == ctrl_canon:
            return []
        return []

    params = PollBenchParams(
        port=str(ctrl_alias),
        address=1,
        baud=9600,
        max_polls=1,
        response_timeout_ms=50,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        lock_dir=tmp_path / "locks",
        simulator_ports=(str(sim_alias),),
        simulator_validation=True,
    )
    canonical, lock = run_poll_bench_preflight(
        params,
        _lab_poll_settings(),
        systemctl_runner=lambda *_a, **_k: MagicMock(returncode=3),
        holder_finder=holders,
        simulator_checker=lambda pid: pid == 3085,
        simulator_pid_finder=lambda **_k: [3085],
    )
    assert canonical == ctrl_canon
    assert params.target_type == "SIMULATOR"
    assert lock is not None
    lock.release()


def test_simulator_owns_sim_adapter_without_flag_refused(tmp_path: Path) -> None:
    _ctrl, sim, ctrl_alias, sim_alias = _alias_pair(tmp_path)
    sim_canon = os.path.realpath(sim)

    def holders(path: str) -> list[int]:
        return [3085] if path == sim_canon else []

    params = PollBenchParams(
        port=str(ctrl_alias),
        address=1,
        baud=9600,
        max_polls=1,
        response_timeout_ms=50,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        lock_dir=tmp_path / "locks",
        simulator_ports=(str(sim_alias),),
        simulator_validation=False,
    )
    with pytest.raises(PollBenchRefusedError, match="simulator"):
        run_poll_bench_preflight(
            params,
            _lab_poll_settings(),
            systemctl_runner=lambda *_a, **_k: MagicMock(returncode=3),
            holder_finder=holders,
            simulator_checker=lambda pid: pid == 3085,
            simulator_pid_finder=lambda **_k: [3085],
        )


def test_simulator_owns_controller_adapter_refused(tmp_path: Path) -> None:
    ctrl, _sim, ctrl_alias, sim_alias = _alias_pair(tmp_path)
    ctrl_canon = os.path.realpath(ctrl)

    def holders(path: str) -> list[int]:
        return [3085] if path == ctrl_canon else []

    for sim_validation in (True, False):
        params = PollBenchParams(
            port=str(ctrl_alias),
            address=1,
            baud=9600,
            max_polls=1,
            response_timeout_ms=50,
            evidence_dir=tmp_path / "ev",
            confirmations=_confirms(),
            lock_dir=tmp_path / "locks",
            simulator_ports=(str(sim_alias),),
            simulator_validation=sim_validation,
            skip_port_check=False,
        )
        with pytest.raises(PollBenchRefusedError, match="simulator"):
            run_poll_bench_preflight(
                params,
                _lab_poll_settings(),
                systemctl_runner=lambda *_a, **_k: MagicMock(returncode=3),
                holder_finder=holders,
                simulator_checker=lambda pid: pid == 3085,
                simulator_pid_finder=lambda _sv=sim_validation, **_k: (
                    [] if _sv else [3085]
                ),
            )


def test_unrelated_process_owns_simulator_adapter_refused(tmp_path: Path) -> None:
    ctrl, sim, ctrl_alias, sim_alias = _alias_pair(tmp_path)
    sim_canon = os.path.realpath(sim)

    def holders(path: str) -> list[int]:
        if path == sim_canon:
            return [9999]
        return []

    params = PollBenchParams(
        port=str(ctrl_alias),
        address=1,
        baud=9600,
        max_polls=1,
        response_timeout_ms=50,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        lock_dir=tmp_path / "locks",
        simulator_ports=(str(sim_alias),),
        simulator_validation=True,
    )
    with pytest.raises(PollBenchRefusedError, match="unrelated"):
        run_poll_bench_preflight(
            params,
            _lab_poll_settings(),
            systemctl_runner=lambda *_a, **_k: MagicMock(returncode=3),
            holder_finder=holders,
            simulator_checker=lambda pid: pid == 3085,
            simulator_pid_finder=lambda **_k: [3085],
        )
    del ctrl


def test_controller_adapter_already_owned_refused(tmp_path: Path) -> None:
    ctrl, _sim, ctrl_alias, sim_alias = _alias_pair(tmp_path)
    ctrl_canon = os.path.realpath(ctrl)

    def holders(path: str) -> list[int]:
        return [777] if path == ctrl_canon else []

    params = PollBenchParams(
        port=str(ctrl_alias),
        address=1,
        baud=9600,
        max_polls=1,
        response_timeout_ms=50,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        lock_dir=tmp_path / "locks",
        simulator_ports=(str(sim_alias),),
        simulator_validation=True,
    )
    with pytest.raises(PollBenchRefusedError, match="busy"):
        run_poll_bench_preflight(
            params,
            _lab_poll_settings(),
            systemctl_runner=lambda *_a, **_k: MagicMock(returncode=3),
            holder_finder=holders,
            simulator_checker=lambda pid: pid == 3085,
            simulator_pid_finder=lambda **_k: [3085],
        )


def test_real_pump_mode_refuses_any_simulator_process(tmp_path: Path) -> None:
    ctrl, _sim, ctrl_alias, sim_alias = _alias_pair(tmp_path)
    params = PollBenchParams(
        port=str(ctrl_alias),
        address=1,
        baud=9600,
        max_polls=1,
        response_timeout_ms=50,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        lock_dir=tmp_path / "locks",
        simulator_ports=(str(sim_alias),),
        simulator_validation=False,
        skip_port_check=True,
    )
    with pytest.raises(PollBenchRefusedError, match="simulator process running"):
        run_poll_bench_preflight(
            params,
            _lab_poll_settings(),
            systemctl_runner=lambda *_a, **_k: MagicMock(returncode=3),
            holder_finder=lambda _p: [],
            simulator_checker=lambda pid: pid == 3085,
            simulator_pid_finder=lambda **_k: [3085],
        )
    del ctrl


def test_poll_only_bench_blocks_controller_queue() -> None:
    ctx = ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.POLL_ONLY_BENCH,
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


@pytest.mark.asyncio
async def test_exactly_one_verified_poll_and_eot(tmp_path: Path) -> None:
    eot = build_eot(encode_wire_address(1), 0)
    transport = FakeBenchTransport(chunks=[eot])
    session = PollBenchSession(
        transport,
        PollBenchSessionConfig(
            port="/tmp/fake",
            address=1,
            baud=9600,
            max_polls=1,
            response_timeout_ms=200,
            evidence_jsonl=tmp_path / "e.jsonl",
            evidence_md=tmp_path / "e.md",
            target_type=TARGET_OWNED_LAB_WAYNE,
            simulator_validation=False,
        ),
    )
    summary = await session.run()
    assert summary["pollsSent"] == 1
    assert transport.write_count == 1
    assert transport.written[0] == build_poll(1)
    assert summary["validResponses"] == 1
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0
    assert summary["result"] == BenchResult.PASS.value
    assert summary["targetType"] == TARGET_OWNED_LAB_WAYNE
    assert summary["simulatorValidation"] is False
    lines = (tmp_path / "e.jsonl").read_text().splitlines()
    records = [json.loads(line) for line in lines]
    tx = [r for r in records if r.get("direction") == "TX"]
    rx = [r for r in records if r.get("direction") == "RX"]
    assert tx and rx
    assert bytes(int(p, 16) for p in tx[0]["rawHex"].split()) == build_poll(1)
    assert bytes(int(p, 16) for p in rx[0]["rawHex"].split()) == eot
    assert any(r.get("event") == "bench_stopped" for r in records)
    assert all(r.get("targetType") == TARGET_OWNED_LAB_WAYNE for r in records)
    assert all(r.get("simulatorValidation") is False for r in records)


@pytest.mark.asyncio
async def test_simulator_validation_one_poll_and_evidence(tmp_path: Path) -> None:
    eot = build_eot(encode_wire_address(1), 0)
    transport = FakeBenchTransport(chunks=[eot])
    session = PollBenchSession(
        transport,
        PollBenchSessionConfig(
            port="/tmp/fake",
            address=1,
            baud=9600,
            max_polls=1,
            response_timeout_ms=200,
            evidence_jsonl=tmp_path / "sim.jsonl",
            evidence_md=tmp_path / "sim.md",
            target_type=TARGET_SIMULATOR,
            simulator_validation=True,
        ),
    )
    summary = await session.run()
    assert summary["pollsSent"] == 1
    assert transport.written[0] == build_poll(1)
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0
    assert summary["targetType"] == TARGET_SIMULATOR
    assert summary["simulatorValidation"] is True
    records = [
        json.loads(line)
        for line in (tmp_path / "sim.jsonl").read_text().splitlines()
        if line
    ]
    assert all(r.get("targetType") == TARGET_SIMULATOR for r in records)
    assert all(r.get("simulatorValidation") is True for r in records)
    md = (tmp_path / "sim.md").read_text()
    assert "Target type: `SIMULATOR`" in md
    assert "Simulator validation: `True`" in md


@pytest.mark.asyncio
async def test_timeout_exits_cleanly(tmp_path: Path) -> None:
    transport = FakeBenchTransport(chunks=[])
    session = PollBenchSession(
        transport,
        PollBenchSessionConfig(
            port="/tmp/fake",
            address=1,
            baud=9600,
            max_polls=1,
            response_timeout_ms=40,
            evidence_jsonl=tmp_path / "t.jsonl",
            evidence_md=tmp_path / "t.md",
        ),
    )
    summary = await session.run()
    assert summary["timeouts"] == 1
    assert summary["pollsSent"] == 1
    assert not transport.is_open
    text = (tmp_path / "t.jsonl").read_text()
    assert "response_timeout" in text
    assert "bench_stopped" in text


@pytest.mark.asyncio
async def test_crc_invalid_reported_and_stops(tmp_path: Path) -> None:
    good = build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1))
    body = bytearray(unescape_dle(good[:-1]))
    body[-3] ^= 0xFF
    bad = escape_dle(bytes(body)) + bytes((SF,))
    transport = FakeBenchTransport(chunks=[bad])
    session = PollBenchSession(
        transport,
        PollBenchSessionConfig(
            port="/tmp/fake",
            address=1,
            baud=9600,
            max_polls=3,
            response_timeout_ms=200,
            evidence_jsonl=tmp_path / "c.jsonl",
            evidence_md=tmp_path / "c.md",
        ),
    )
    summary = await session.run()
    assert summary["pollsSent"] == 1  # stops after CRC error
    assert summary["crcErrors"] == 1
    assert summary["result"] == BenchResult.FAIL.value


@pytest.mark.asyncio
async def test_sigterm_closes_and_writes_bench_stopped(tmp_path: Path) -> None:
    transport = FakeBenchTransport(chunks=[])
    session = PollBenchSession(
        transport,
        PollBenchSessionConfig(
            port="/tmp/fake",
            address=1,
            baud=9600,
            max_polls=10,
            response_timeout_ms=500,
            evidence_jsonl=tmp_path / "s.jsonl",
            evidence_md=tmp_path / "s.md",
        ),
    )

    async def _stop_soon() -> None:
        await asyncio.sleep(0.05)
        session.request_stop()

    summary, _ = await asyncio.gather(session.run(), _stop_soon())
    assert "bench_stopped" in (tmp_path / "s.jsonl").read_text()
    assert not transport.is_open
    assert summary["commandQueueCreated"] is False


@pytest.mark.asyncio
async def test_ctrl_c_path_writes_bench_stopped(tmp_path: Path) -> None:
    transport = FakeBenchTransport(chunks=[build_eot(encode_wire_address(1), 0)])
    session = PollBenchSession(
        transport,
        PollBenchSessionConfig(
            port="/tmp/fake",
            address=1,
            baud=9600,
            max_polls=5,
            response_timeout_ms=200,
            evidence_jsonl=tmp_path / "i.jsonl",
            evidence_md=tmp_path / "i.md",
        ),
    )

    async def _intr() -> None:
        await asyncio.sleep(0.02)
        session.request_stop()

    await asyncio.gather(session.run(), _intr())
    assert "bench_stopped" in (tmp_path / "i.jsonl").read_text()
    assert not transport.is_open


def test_cli_refuses_without_mode(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("INTELIPUMP_ENVIRONMENT", "LAB")
    monkeypatch.setenv("INTELIPUMP_CONTROLLER__MODE", "LISTEN_ONLY")
    from intelipump_fdc.core.config import get_settings

    get_settings.cache_clear()
    with pytest.raises(SystemExit) as excinfo:
        poll_bench_run(
            [
                "--port",
                str(tmp_path / "p"),
                "--address",
                "1",
                "--max-polls",
                "1",
                "--evidence-dir",
                str(tmp_path / "ev"),
                "--confirm-owned-lab-pump",
                "--confirm-technician-present",
                "--confirm-emergency-isolation-ready",
                "--confirm-no-fuel-test",
                "--confirm-authorization-disabled",
                "--skip-service-check",
                "--skip-port-check",
            ]
        )
    assert excinfo.value.code == 2
    get_settings.cache_clear()


def test_multiple_addresses_not_accepted_by_cli() -> None:
    parser = build_parser()
    # --address is a single int; comma lists are rejected by argparse type=int
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["--port", "/tmp/x", "--address", "1,2", "--max-polls", "1"]
        )
