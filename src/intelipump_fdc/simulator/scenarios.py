"""Reusable virtual-dispenser scenarios and runner (no real serial)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import (
    build_ack,
    build_data_frame,
    build_poll,
)
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.simulator.config import (
    FillingConfig,
    PumpConfig,
    SequencePolicy,
    SimulatorConfig,
)
from intelipump_fdc.simulator.encoding import (
    encode_cd1_command,
    encode_cd3_preset_volume,
    encode_cd4_preset_amount,
    encode_cd5_price_update,
)
from intelipump_fdc.simulator.faults import ProtocolFaultKind
from intelipump_fdc.simulator.models import SimulatorSnapshot
from intelipump_fdc.simulator.pump import SimulatedPump
from intelipump_fdc.simulator.session import SimulatorSession


def _assert_state(pump: SimulatedPump, expected: PumpState) -> None:
    actual = pump.normalized_state
    assert actual == expected, f"expected {expected.value}, got {actual}"


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    name: str
    ok: bool
    snapshot: SimulatorSnapshot
    observations: tuple[str, ...]
    errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    description: str
    build: Callable[[], SimulatorSession]
    run: Callable[[SimulatorSession, ScenarioRunner], list[str]]


class ScenarioRunner:
    """Drive scenarios against an in-memory simulator session."""

    def __init__(self) -> None:
        self.controller_seq: dict[int, int] = {}

    def run(self, scenario: Scenario) -> ScenarioResult:
        session = scenario.build()
        self.controller_seq = {addr: 0 for addr in session.pumps}
        errors: list[str] = []
        observations: list[str] = []
        try:
            observations = scenario.run(session, self)
        except AssertionError as exc:
            errors.append(str(exc))
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
        snap = session.snapshot()
        return ScenarioResult(
            name=scenario.name,
            ok=not errors,
            snapshot=snap,
            observations=tuple(observations),
            errors=tuple(errors),
        )

    def send_poll_drain(
        self, session: SimulatorSession, address: int, *, max_frames: int = 32
    ) -> list[DartLineFrame]:
        """POLL until EOT, ACKing any DATA frames."""
        frames: list[DartLineFrame] = []
        for _ in range(max_frames):
            result = session.receive(build_poll(address))
            if not result.responses:
                break
            raw = result.responses[0]
            parsed = parse_frame(raw)
            if isinstance(parsed, DartLineFrame):
                frames.append(parsed)
                if parsed.control_type is ControlType.DATA:
                    session.receive(build_ack(address, parsed.sequence))
                if parsed.control_type is ControlType.EOT:
                    break
        return frames

    def send_app(
        self, session: SimulatorSession, address: int, payload: bytes
    ) -> None:
        seq = self.controller_seq.get(address, 0)
        wire = build_data_frame(address, seq, payload)
        result = session.receive(wire)
        if not result.responses:
            raise AssertionError(f"no ACK/NAK for DATA to address {address}")
        parsed = parse_frame(result.responses[0])
        if not isinstance(parsed, DartLineFrame):
            raise AssertionError("invalid response frame")
        if parsed.control_type is ControlType.NAK:
            raise AssertionError(f"NAK for seq={seq}")
        if parsed.control_type is not ControlType.ACK:
            raise AssertionError(f"expected ACK got {parsed.control_type}")
        # Advance controller sequence using session policy.
        from intelipump_fdc.simulator.config import next_sequence

        self.controller_seq[address] = next_sequence(seq, session.config.sequence_policy)


def _default_session(
    *,
    filling: FillingConfig | None = None,
    sequence_policy: SequencePolicy = SequencePolicy.SPEC_F_TO_1,
) -> SimulatorSession:
    fill = filling or FillingConfig(
        flow_rate_liters_per_second=Decimal("0.5"),
        update_interval_ms=200,
        natural_complete_volume_raw=2_000,  # 2.000 L for fast scenarios
    )
    cfg = SimulatorConfig(
        sequence_policy=sequence_policy,
        pumps=(
            PumpConfig(pump_id="fp-1", dart_address=1, default_price_raw=1175, filling=fill),
            PumpConfig(pump_id="fp-2", dart_address=2, default_price_raw=1299, filling=fill),
        ),
    )
    return SimulatorSession(cfg)


def cold_start_scenario(*, pump_id: str = "fp-1") -> Scenario:
    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        _assert_state(pump, PumpState.DISCONNECTED)
        pump.cold_start_to_ready()
        _assert_state(pump, PumpState.READY)
        runner.send_poll_drain(session, pump.config.dart_address)
        return ["cold start → READY", f"state={pump.normalized_state}"]

    return Scenario(
        name="cold_start",
        description="DISCONNECTED → DISCOVERING → RESET → READY",
        build=_default_session,
        run=run,
    )


def normal_sale_scenario(
    *,
    pump_id: str = "fp-1",
    nozzle: int = 1,
    price_raw: int = 1175,
) -> Scenario:
    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        addr = pump.config.dart_address
        pump.cold_start_to_ready()
        runner.send_app(
            session, addr, encode_cd5_price_update(logical_nozzle=nozzle, price_raw=price_raw)
        )
        pump.lift_nozzle(nozzle)
        _assert_state(pump, PumpState.NOZZLE_UP)
        runner.send_app(session, addr, encode_cd1_command(PumpControlCommand.AUTHORIZE))
        _assert_state(pump, PumpState.FILLING)
        # Fill to natural completion.
        for _ in range(50):
            session.advance(200)
            runner.send_poll_drain(session, addr)
            if pump.normalized_state in {
                PumpState.FILLING_COMPLETE,
                PumpState.LIMIT_REACHED,
            }:
                break
        assert pump.normalized_state in {
            PumpState.FILLING_COMPLETE,
            PumpState.LIMIT_REACHED,
        }
        final_volume = pump.volume_raw
        final_amount = pump.amount_raw
        session.advance(1000)
        assert pump.volume_raw == final_volume
        assert pump.amount_raw == final_amount
        pump.return_nozzle()
        runner.send_app(session, addr, encode_cd1_command(PumpControlCommand.RESET))
        _assert_state(pump, PumpState.READY)
        return [
            "normal sale complete",
            f"volume_raw={final_volume}",
            f"amount_raw={final_amount}",
        ]

    return Scenario(
        name="normal_sale",
        description="READY → nozzle → authorize → fill → complete → reset → READY",
        build=_default_session,
        run=run,
    )


def preset_amount_sale_scenario(
    *, pump_id: str = "fp-1", nozzle: int = 1, amount_raw: int = 500
) -> Scenario:
    def build() -> SimulatorSession:
        return _default_session(
            filling=FillingConfig(
                flow_rate_liters_per_second=Decimal("1.0"),
                update_interval_ms=100,
                natural_complete_volume_raw=50_000,
            )
        )

    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        addr = pump.config.dart_address
        pump.cold_start_to_ready()
        pump.lift_nozzle(nozzle)
        runner.send_app(session, addr, encode_cd4_preset_amount(amount_raw))
        runner.send_app(session, addr, encode_cd1_command(PumpControlCommand.AUTHORIZE))
        for _ in range(80):
            session.advance(100)
            if pump.normalized_state in {
                PumpState.FILLING_COMPLETE,
                PumpState.LIMIT_REACHED,
            }:
                break
        assert pump.amount_raw <= amount_raw
        assert pump.normalized_state in {
            PumpState.FILLING_COMPLETE,
            PumpState.LIMIT_REACHED,
        }
        return [f"preset amount sale amount_raw={pump.amount_raw}"]

    return Scenario(
        name="preset_amount_sale",
        description="Sale stops at preset amount",
        build=build,
        run=run,
    )


def preset_volume_sale_scenario(
    *, pump_id: str = "fp-1", nozzle: int = 1, volume_raw: int = 1500
) -> Scenario:
    def build() -> SimulatorSession:
        return _default_session(
            filling=FillingConfig(
                flow_rate_liters_per_second=Decimal("1.0"),
                update_interval_ms=100,
                natural_complete_volume_raw=50_000,
            )
        )

    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        addr = pump.config.dart_address
        pump.cold_start_to_ready()
        pump.lift_nozzle(nozzle)
        runner.send_app(session, addr, encode_cd3_preset_volume(volume_raw))
        runner.send_app(session, addr, encode_cd1_command(PumpControlCommand.AUTHORIZE))
        for _ in range(80):
            session.advance(100)
            if pump.normalized_state in {
                PumpState.FILLING_COMPLETE,
                PumpState.LIMIT_REACHED,
            }:
                break
        assert pump.volume_raw == volume_raw
        return [f"preset volume sale volume_raw={pump.volume_raw}"]

    return Scenario(
        name="preset_volume_sale",
        description="Sale stops at preset volume",
        build=build,
        run=run,
    )


def suspend_resume_scenario(*, pump_id: str = "fp-1") -> Scenario:
    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        addr = pump.config.dart_address
        pump.cold_start_to_ready()
        pump.lift_nozzle(1)
        runner.send_app(session, addr, encode_cd1_command(PumpControlCommand.AUTHORIZE))
        session.advance(400)
        vol_before = pump.volume_raw
        runner.send_app(
            session, addr, encode_cd1_command(PumpControlCommand.SUSPEND_FUELLING_POINT)
        )
        _assert_state(pump, PumpState.SUSPENDED)
        session.advance(1000)
        assert pump.volume_raw == vol_before
        runner.send_app(
            session, addr, encode_cd1_command(PumpControlCommand.RESUME_FUELLING_POINT)
        )
        _assert_state(pump, PumpState.FILLING)
        session.advance(400)
        assert pump.volume_raw > vol_before
        return ["suspend froze progress; resume continued"]

    return Scenario(
        name="suspend_resume",
        description="Suspend freezes filling; resume continues",
        build=_default_session,
        run=run,
    )


def communication_loss_recovery_scenario(*, pump_id: str = "fp-1") -> Scenario:
    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        pump.cold_start_to_ready()
        pump.disable_communication()
        _assert_state(pump, PumpState.DISCONNECTED)
        empty = session.receive(build_poll(pump.config.dart_address))
        assert empty.responses == ()
        pump.enable_communication()
        pump.cold_start_to_ready()
        _assert_state(pump, PumpState.READY)
        return ["communication loss and recovery"]

    return Scenario(
        name="communication_loss_recovery",
        description="Disable bus then recover to READY",
        build=_default_session,
        run=run,
    )


def restart_idle_scenario(*, pump_id: str = "fp-1") -> Scenario:
    def run(session: SimulatorSession, _runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        pump.cold_start_to_ready()
        pump.restart_pump()
        _assert_state(pump, PumpState.READY)
        return ["restart during idle → READY"]

    return Scenario(
        name="restart_idle",
        description="Pump restart while idle",
        build=_default_session,
        run=run,
    )


def restart_during_filling_scenario(*, pump_id: str = "fp-1") -> Scenario:
    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        addr = pump.config.dart_address
        pump.cold_start_to_ready()
        pump.lift_nozzle(1)
        runner.send_app(session, addr, encode_cd1_command(PumpControlCommand.AUTHORIZE))
        session.advance(400)
        _assert_state(pump, PumpState.FILLING)
        pump.restart_pump()
        _assert_state(pump, PumpState.READY)
        assert pump.filling_active is False
        return ["restart during filling cleared filling"]

    return Scenario(
        name="restart_during_filling",
        description="Pump restart while FILLING",
        build=_default_session,
        run=run,
    )


def fault_during_filling_scenario(*, pump_id: str = "fp-1") -> Scenario:
    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        addr = pump.config.dart_address
        pump.cold_start_to_ready()
        pump.lift_nozzle(1)
        runner.send_app(session, addr, encode_cd1_command(PumpControlCommand.AUTHORIZE))
        session.advance(200)
        pump.inject_fault(7)
        _assert_state(pump, PumpState.FAULTED)
        return ["fault during filling → FAULTED"]

    return Scenario(
        name="fault_during_filling",
        description="Fault injection while FILLING",
        build=_default_session,
        run=run,
    )


def price_change_during_filling_scenario(*, pump_id: str = "fp-1") -> Scenario:
    def run(session: SimulatorSession, runner: ScenarioRunner) -> list[str]:
        pump = session.pump_by_id(pump_id)
        addr = pump.config.dart_address
        pump.cold_start_to_ready()
        pump.lift_nozzle(1)
        runner.send_app(session, addr, encode_cd1_command(PumpControlCommand.AUTHORIZE))
        _assert_state(pump, PumpState.FILLING)
        before = pump.prices_raw[1]
        runner.send_app(
            session, addr, encode_cd5_price_update(logical_nozzle=1, price_raw=9999)
        )
        assert pump.prices_raw[1] == before
        assert any(
            f.kind is ProtocolFaultKind.INELIGIBLE_COMMAND for f in session.faults
        )
        return ["SET_PRICE during FILLING rejected by guards"]

    return Scenario(
        name="price_change_during_filling",
        description="Price update rejected while FILLING",
        build=_default_session,
        run=run,
    )


ALL_SCENARIOS: tuple[Callable[[], Scenario], ...] = (
    cold_start_scenario,
    normal_sale_scenario,
    preset_amount_sale_scenario,
    preset_volume_sale_scenario,
    suspend_resume_scenario,
    communication_loss_recovery_scenario,
    restart_idle_scenario,
    restart_during_filling_scenario,
    fault_during_filling_scenario,
    price_change_during_filling_scenario,
)


def list_scenarios() -> tuple[str, ...]:
    return tuple(factory().name for factory in ALL_SCENARIOS)


def get_scenario(name: str) -> Scenario:
    for factory in ALL_SCENARIOS:
        scenario = factory()
        if scenario.name == name:
            return scenario
    raise KeyError(f"unknown scenario: {name}")
