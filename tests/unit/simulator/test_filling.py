"""Filling engine and pump command tests."""

from __future__ import annotations

from decimal import Decimal

from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.simulator.config import FillingConfig, PumpConfig, SimulatorConfig
from intelipump_fdc.simulator.encoding import encode_cd1_command, encode_cd5_price_update
from intelipump_fdc.simulator.faults import ProtocolFaultKind
from intelipump_fdc.simulator.session import SimulatorSession


def _session(**fill_kwargs: object) -> SimulatorSession:
    fill = FillingConfig(
        flow_rate_liters_per_second=Decimal("0.5"),
        update_interval_ms=200,
        natural_complete_volume_raw=1_000,
        **fill_kwargs,  # type: ignore[arg-type]
    )
    return SimulatorSession(
        SimulatorConfig(
            pumps=(PumpConfig(pump_id="fp-1", dart_address=1, filling=fill),)
        )
    )


def test_volume_and_amount_never_decrease() -> None:
    session = _session()
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    pump.lift_nozzle(1)
    pump.handle_application_payload(encode_cd1_command(PumpControlCommand.AUTHORIZE))
    session.advance(200)
    v1, a1 = pump.volume_raw, pump.amount_raw
    session.advance(200)
    assert pump.volume_raw >= v1
    assert pump.amount_raw >= a1


def test_duplicate_time_advance_does_not_double_apply() -> None:
    session = _session()
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    pump.lift_nozzle(1)
    pump.handle_application_payload(encode_cd1_command(PumpControlCommand.AUTHORIZE))
    session.advance(200)
    v = pump.volume_raw
    session.advance(0)
    assert pump.volume_raw == v


def test_final_values_stable_after_completion() -> None:
    session = _session()
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    pump.lift_nozzle(1)
    pump.handle_application_payload(encode_cd1_command(PumpControlCommand.AUTHORIZE))
    for _ in range(20):
        session.advance(200)
        if pump.normalized_state is PumpState.FILLING_COMPLETE:
            break
    assert pump.normalized_state is PumpState.FILLING_COMPLETE
    v, a = pump.volume_raw, pump.amount_raw
    session.advance(5000)
    assert pump.volume_raw == v
    assert pump.amount_raw == a


def test_set_price_rejected_while_filling() -> None:
    session = _session()
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    pump.lift_nozzle(1)
    pump.handle_application_payload(encode_cd1_command(PumpControlCommand.AUTHORIZE))
    before = pump.prices_raw[1]
    faults = pump.handle_application_payload(
        encode_cd5_price_update(logical_nozzle=1, price_raw=9999)
    )
    assert any(f.kind is ProtocolFaultKind.INELIGIBLE_COMMAND for f in faults)
    assert pump.prices_raw[1] == before
