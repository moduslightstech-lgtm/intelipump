"""Simulator models documented CD5 price programming transition."""

from __future__ import annotations

from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.simulator.clock import SimulatedClock
from intelipump_fdc.simulator.config import PumpConfig, SimulatorConfig
from intelipump_fdc.simulator.encoding import encode_cd5_price_update
from intelipump_fdc.simulator.pump import SimulatedPump
from intelipump_fdc.state_machine.models import PumpContext


def test_encode_cd5_two_nozzle_payload() -> None:
    assert encode_cd5_price_update(prices_raw=[1175, 1175]) == bytes.fromhex(
        "05 06 00 11 75 00 11 75"
    )


def test_cd5_from_not_programmed_to_filling_complete() -> None:
    clock = SimulatedClock()
    pump = SimulatedPump(
        config=PumpConfig(pump_id="fp-1", dart_address=1, nozzle_count=2),
        clock=clock,
        simulator_config=SimulatorConfig(
            simulator_physical_enable=True,
            simulator_active_commands_enabled=True,
        ),
    )
    pump.wayne_status = WaynePumpStatus.PUMP_NOT_PROGRAMMED
    pump.communication_enabled = True
    pump._machine.replace_context(
        PumpContext(
            pump_id="fp-1",
            dart_address=1,
            current_state=PumpState.NOT_PROGRAMMED,
            communication_healthy=True,
            price_verified=False,
        )
    )
    faults = pump.handle_application_payload(
        encode_cd5_price_update(prices_raw=[1175, 1175])
    )
    assert faults == []
    assert pump.prices_raw[1] == 1175
    assert pump.prices_raw[2] == 1175
    assert pump.price_verified is True
    assert pump.wayne_status is WaynePumpStatus.FILLING_COMPLETED
    assert pump.normalized_state is PumpState.FILLING_COMPLETE
