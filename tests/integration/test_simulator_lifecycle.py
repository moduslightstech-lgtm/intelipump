"""Integration: line frames ↔ application ↔ state machine via simulator."""

from __future__ import annotations

from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.simulator.config import SimulatorConfig
from intelipump_fdc.simulator.encoding import encode_cd1_command
from intelipump_fdc.simulator.scenarios import ScenarioRunner
from intelipump_fdc.simulator.session import SimulatorSession
from intelipump_fdc.state_machine.wayne_mapper import MapperContext, map_wayne_observation


def test_status_request_round_trip_decodes_as_ambiguous_or_dc1() -> None:
    session = SimulatorSession(SimulatorConfig(pumps=SimulatorConfig().pumps[:1]))
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    runner = ScenarioRunner()
    runner.controller_seq = {1: 0}
    runner.send_app(session, 1, encode_cd1_command(PumpControlCommand.RETURN_STATUS))
    # Drain DC1/DC3 responses.
    frames = runner.send_poll_drain(session, 1)
    data_frames = [f for f in frames if f.control_type is ControlType.DATA]
    assert data_frames
    for frame in data_frames:
        bundle = decode_data_payload(frame.payload)
        assert bundle.transactions
        mapped = map_wayne_observation(
            bundle.transactions[0],
            context=MapperContext(resolve_as_dc1=True),
        )
        # Status or unknown depending on transaction type in payload.
        assert mapped.event is not None
    assert pump.wayne_status is WaynePumpStatus.RESET
    assert pump.normalized_state is PumpState.READY


def test_two_fueling_positions_independent() -> None:
    session = SimulatorSession()
    assert set(session.pumps) == {1, 2}
    session.get_pump(1).cold_start_to_ready()
    assert session.get_pump(1).normalized_state is PumpState.READY
    assert session.get_pump(2).normalized_state is PumpState.DISCONNECTED
    # POLL addr 2 while disabled → no response
    result = session.receive(build_poll(2))
    assert result.responses == ()
