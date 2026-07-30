"""Comprehensive Wayne nozzle / READY / authorize / filling flow tests."""

from __future__ import annotations

from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.constants import (
    DecodeStatus,
    MessageDirection,
    PumpControlCommand,
    TransactionType,
)
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.nozio import decode_nozio
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.simulator.config import FillingConfig, PumpConfig, SimulatorConfig
from intelipump_fdc.simulator.encoding import encode_cd1_command
from intelipump_fdc.simulator.session import SimulatorSession
from intelipump_fdc.state_machine.guards import evaluate_command_eligibility
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import PumpContext
from intelipump_fdc.state_machine.readiness import can_derive_ready
from intelipump_fdc.state_machine.wayne_mapper import MapperContext, map_wayne_observation


def _dc3(nozio: int) -> bytes:
    return bytes.fromhex(f"03 04 00 11 75 {nozio:02x}")


def _map_dc3(
    nozio: int,
    *,
    ctx: MapperContext,
) -> object:
    tx = decode_data_payload(_dc3(nozio)).transactions[0]
    return map_wayne_observation(tx, context=ctx)


# --- NOZIO decoding ---------------------------------------------------------


def test_nozio_documented_examples() -> None:
    cases = [
        (0x01, 1, 1, False),
        (0x11, 1, 1, True),
        (0x02, 2, 2, False),
        (0x12, 2, 2, True),
        (0x10, 0, None, True),
        (0x00, 0, None, False),
    ]
    for raw, logical_raw, selected, out in cases:
        dec = decode_nozio(raw)
        assert dec.logical_nozzle_raw == logical_raw
        assert dec.selected_logical_nozzle == selected
        assert dec.nozzle_out is out


def test_nozio_reserved_bits_warn_preserve() -> None:
    dec = decode_nozio(0x91)  # reserved 0x80 + nozzle 1 OUT
    assert dec.logical_nozzle_raw == 1
    assert dec.selected_logical_nozzle == 1
    assert dec.nozzle_out is True
    assert dec.reserved_bits == 0x80
    assert any("reserved" in w for w in dec.warnings)
    # Transaction still decodes (not discarded).
    tx = decode_data_payload(bytes.fromhex("03 04 00 11 75 91")).transactions[0]
    assert tx.decode_status in {DecodeStatus.PARTIAL, DecodeStatus.MALFORMED}
    assert tx.decoded_body is not None
    assert tx.decoded_body["nozio_raw"] == 0x91


# --- Edge detection ---------------------------------------------------------


def test_repeated_out_emits_one_lift() -> None:
    base = MapperContext(
        current_state=PumpState.READY,
        previous_wayne_status=1,
        last_raw_wayne_status=1,
        resolve_as_dc3=True,
        nozzle_out=False,
        communication_healthy=True,
    )
    m1 = _map_dc3(0x11, ctx=base)
    assert m1.event is PumpEvent.NOZZLE_LIFTED
    m2 = _map_dc3(
        0x11,
        ctx=MapperContext(
            current_state=PumpState.NOZZLE_UP,
            resolve_as_dc3=True,
            nozzle_out=True,
            selected_nozzle=1,
            communication_healthy=True,
            last_raw_wayne_status=1,
        ),
    )
    assert m2.event is PumpEvent.NOZZLE_STATUS_OBSERVED


def test_out_selection_change_not_second_lift() -> None:
    mapped = _map_dc3(
        0x12,
        ctx=MapperContext(
            current_state=PumpState.NOZZLE_UP,
            resolve_as_dc3=True,
            nozzle_out=True,
            selected_nozzle=1,
            communication_healthy=True,
        ),
    )
    assert mapped.event is PumpEvent.NOZZLE_SELECTION_CHANGED
    assert mapped.selected_nozzle == 2


def test_return_edge_and_repeated_in() -> None:
    ret = _map_dc3(
        0x02,
        ctx=MapperContext(
            current_state=PumpState.NOZZLE_UP,
            resolve_as_dc3=True,
            nozzle_out=True,
            selected_nozzle=2,
            communication_healthy=True,
            last_raw_wayne_status=1,
            previous_wayne_status=1,
        ),
    )
    assert ret.event in {PumpEvent.NOZZLE_RETURNED, PumpEvent.READY_OBSERVED}
    steady = _map_dc3(
        0x02,
        ctx=MapperContext(
            current_state=PumpState.RESET,
            resolve_as_dc3=True,
            nozzle_out=False,
            selected_nozzle=2,
            communication_healthy=True,
            last_raw_wayne_status=1,
            previous_wayne_status=1,
            was_ready_derivable=True,
        ),
    )
    assert steady.event is PumpEvent.NOZZLE_STATUS_OBSERVED


# --- READY ------------------------------------------------------------------


def test_ready_predicate_and_mapper() -> None:
    ready_ctx = PumpContext(
        pump_id="p",
        dart_address=1,
        current_state=PumpState.RESET,
        last_raw_wayne_status=int(WaynePumpStatus.RESET),
        nozzle_out=False,
        communication_healthy=True,
    )
    assert can_derive_ready(ready_ctx) is True

    assert (
        can_derive_ready(
            ready_ctx.with_updates(
                last_raw_wayne_status=int(WaynePumpStatus.FILLING_COMPLETED)
            )
        )
        is False
    )
    assert (
        can_derive_ready(
            ready_ctx.with_updates(last_raw_wayne_status=int(WaynePumpStatus.SWITCHED_OFF))
        )
        is False
    )
    assert can_derive_ready(ready_ctx.with_updates(nozzle_out=True)) is False
    assert (
        can_derive_ready(ready_ctx.with_updates(active_transaction_id="tx")) is False
    )
    assert can_derive_ready(ready_ctx.with_updates(fault_code=1)) is False

    mapped = _map_dc3(
        0x01,
        ctx=MapperContext(
            current_state=PumpState.RESET,
            resolve_as_dc3=True,
            nozzle_out=False,
            previous_wayne_status=1,
            last_raw_wayne_status=1,
            communication_healthy=True,
            was_ready_derivable=False,
        ),
    )
    assert mapped.event is PumpEvent.READY_OBSERVED


# --- Direction ambiguity ----------------------------------------------------


def test_cd3_dc3_direction_resolution() -> None:
    raw = bytes.fromhex("03 04 00 11 75 11")
    tx = decode_data_payload(raw).transactions[0]
    assert tx.transaction_type is TransactionType.AMBIGUOUS_CD3_OR_DC3
    assert tx.direction is MessageDirection.UNKNOWN

    ambiguous = map_wayne_observation(tx)
    assert ambiguous.event is PumpEvent.UNKNOWN_OBSERVATION
    assert ambiguous.nozzle_out is None

    as_dc3 = map_wayne_observation(
        tx,
        context=MapperContext(
            resolve_as_dc3=True,
            bus_direction=MessageDirection.SLAVE_TO_MASTER,
            nozzle_out=False,
            current_state=PumpState.READY,
        ),
    )
    assert as_dc3.event is PumpEvent.NOZZLE_LIFTED

    as_cd3 = map_wayne_observation(
        tx,
        context=MapperContext(
            resolve_as_cd3=True,
            bus_direction=MessageDirection.MASTER_TO_SLAVE,
        ),
    )
    assert as_cd3.event is PumpEvent.UNKNOWN_OBSERVATION


# --- Authorization ----------------------------------------------------------


def test_authorize_before_and_after_lift_and_blocks() -> None:
    session = SimulatorSession(
        SimulatorConfig(
            pumps=(PumpConfig(pump_id="fp-1", dart_address=1, filling=FillingConfig()),)
        )
    )
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    assert pump.normalized_state is PumpState.READY

    # Block: nozzle 0 / unknown
    blocked = evaluate_command_eligibility(
        PumpCommand.AUTHORIZE,
        pump.context.with_updates(selected_nozzle=None, price_verified=True),
    )
    assert blocked.eligible is False
    assert "selected_nozzle_unknown" in blocked.blocking_reasons

    # Block: unverified price
    blocked_price = evaluate_command_eligibility(
        PumpCommand.AUTHORIZE,
        pump.context.with_updates(selected_nozzle=1, price_verified=False),
    )
    assert "price_not_verified" in blocked_price.blocking_reasons

    # Block: fault
    blocked_fault = evaluate_command_eligibility(
        PumpCommand.AUTHORIZE,
        pump.context.with_updates(
            selected_nozzle=1, price_verified=True, fault_code=9
        ),
    )
    assert "fault_present" in blocked_fault.blocking_reasons

    # Authorize before lift (protocol-complete)
    pump.selected_nozzle = 1
    pump._refresh_price_verification()
    pump._sync_context_fields()
    faults = pump.handle_application_payload(
        encode_cd1_command(PumpControlCommand.AUTHORIZE)
    )
    assert faults == []
    assert pump.normalized_state is PumpState.AUTHORIZED
    pump.lift_nozzle(1)
    assert pump.normalized_state is PumpState.FILLING


def test_authorize_after_lift() -> None:
    session = SimulatorSession(
        SimulatorConfig(pumps=(PumpConfig(pump_id="fp-1", dart_address=1),))
    )
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    pump.lift_nozzle(1)
    assert pump.normalized_state is PumpState.NOZZLE_UP
    assert pump.price_verified is True
    faults = pump.handle_application_payload(
        encode_cd1_command(PumpControlCommand.AUTHORIZE)
    )
    assert faults == []
    assert pump.normalized_state is PumpState.FILLING


def test_require_nozzle_lift_before_authorize_policy() -> None:
    ctx = PumpContext(
        pump_id="p",
        dart_address=1,
        current_state=PumpState.READY,
        selected_nozzle=1,
        price_verified=True,
        communication_healthy=True,
        require_nozzle_lift_before_authorize=True,
    )
    result = evaluate_command_eligibility(PumpCommand.AUTHORIZE, ctx)
    assert result.eligible is False
    assert "require_nozzle_lift_before_authorize" in result.blocking_reasons


# --- Filling / hang-up / idempotency ----------------------------------------


def test_dc1_filling_creates_one_session_and_hangup_completes() -> None:
    machine = PumpStateMachine(
        PumpContext(
            pump_id="p",
            dart_address=1,
            current_state=PumpState.AUTHORIZED,
            communication_healthy=True,
            nozzle_out=True,
            selected_nozzle=1,
            last_raw_wayne_status=2,
        )
    )
    r1 = machine.apply(
        PumpEvent.FILLING_STARTED,
        raw_wayne_status=4,
    )
    assert r1.current_state is PumpState.FILLING
    assert r1.context.fueling_session_uuid is not None
    session = r1.context.fueling_session_uuid
    r2 = machine.apply(PumpEvent.FILLING_STARTED, raw_wayne_status=4)
    assert r2.noop is True
    assert r2.context.fueling_session_uuid == session

    # Nozzle return during filling → leave FILLING, await DC1.
    ret = machine.apply(
        PumpEvent.NOZZLE_RETURNED,
        nozzle_out=False,
        completion_evidence_key="hang:1",
        awaiting_filling_complete=True,
    )
    assert ret.current_state is PumpState.FILLING_COMPLETE
    assert ret.context.awaiting_filling_complete is True

    done = machine.apply(
        PumpEvent.FILLING_COMPLETED,
        raw_wayne_status=5,
        completion_evidence_key="complete:frame:5",
    )
    assert done.current_state is PumpState.FILLING_COMPLETE
    dup = machine.apply(
        PumpEvent.FILLING_COMPLETED,
        raw_wayne_status=5,
        completion_evidence_key="complete:frame:5",
    )
    assert dup.noop is True


def test_simulator_hangup_and_no_synthetic_ready_on_reset() -> None:
    session = SimulatorSession(
        SimulatorConfig(pumps=(PumpConfig(pump_id="fp-1", dart_address=1),))
    )
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    pump.lift_nozzle(1)
    pump.handle_application_payload(encode_cd1_command(PumpControlCommand.AUTHORIZE))
    session.advance(200)
    assert pump.normalized_state is PumpState.FILLING
    pump.return_nozzle()
    assert pump.normalized_state is PumpState.FILLING_COMPLETE
    # RESET alone does not imply READY until nozzle IN + gates (already IN).
    pump.handle_application_payload(encode_cd1_command(PumpControlCommand.RESET))
    assert pump.wayne_status is WaynePumpStatus.RESET
    assert pump.normalized_state in {PumpState.RESET, PumpState.READY}


def test_decreasing_dc2_volume_ignored() -> None:
    machine = PumpStateMachine(
        PumpContext(
            pump_id="p",
            dart_address=1,
            current_state=PumpState.FILLING,
            communication_healthy=True,
            nozzle_out=True,
            dispensed_volume_raw=500,
            last_raw_wayne_status=4,
        )
    )
    result = machine.apply(
        PumpEvent.FILLING_UPDATED,
        dispensed_volume_raw=400,
    )
    assert result.context.dispensed_volume_raw == 500
    assert any("Decreasing DC2 volume ignored" in w for w in result.warnings)


def test_switched_off_not_ready() -> None:
    machine = PumpStateMachine(
        PumpContext(
            pump_id="p",
            dart_address=1,
            current_state=PumpState.READY,
            communication_healthy=True,
            nozzle_out=False,
            last_raw_wayne_status=1,
        )
    )
    result = machine.apply(
        PumpEvent.SWITCHED_OFF_OBSERVED,
        raw_wayne_status=7,
    )
    assert result.current_state is PumpState.DISCONNECTED
    assert result.current_state is not PumpState.READY
