"""Meter CD101 cancelled if nozzle lifts while queued (before TX)."""

from __future__ import annotations

from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.outbound import OutboundQueue
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.controller.session_models import (
    IdempotencyClass,
    NozzlePosition,
    OutboundDataItem,
)
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.cd101 import build_cd101_request
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair


def _loop(tmp_path, monkeypatch) -> ControllerLoop:
    monkeypatch.setenv("INTELIPUMP_METER_READ_REQUEST_DIR", str(tmp_path))
    safety = ControllerSafetyContext(
        environment="PRODUCTION",
        mode=ControllerMode.BENCH_CONTROL,
        active_commands_enabled=True,
        require_physical_control_enable=True,
        physical_enable_present=True,
        owned_lab_active_session=True,
        production_sole_controller_session=True,
        hardware_meter_cd101_enabled=True,
        hardware_meter_allowed_addresses=frozenset({1}),
        hardware_meter_allowed_device_id="pi-002",
        hardware_meter_device_id="pi-002",
    )
    ctrl_transport, _pump = create_memory_transport_pair()
    runtime = ControllerRuntime(
        transport=ctrl_transport,
        safety=safety,
        config=PollSchedulerConfig(addresses=[1]),
        events=EventBus(),
        outbound=OutboundQueue(),
        meter_hardware_cd101=True,
        meter_nozzle_in_max_age_s=300.0,
        meter_response_timeout_s=8.0,
    )
    loop = ControllerLoop(runtime)
    session = PumpSession(address=1, pump_id="pump-2", events=runtime.events)
    session.state.nozzle_position = NozzlePosition.IN
    session.state.last_nozio_time = 1_000.0
    # Force READY-like idle SM for eligibility (seed via context replace if needed).
    from dataclasses import replace

    session.machine._context = replace(  # noqa: SLF001
        session.machine.context,
        current_state=PumpState.READY,
        active_transaction_id=None,
    )
    loop.sessions[1] = session
    return loop


def test_nozzle_lift_before_tx_cancels_meter_read(tmp_path, monkeypatch) -> None:
    import time

    monkeypatch.setattr(time, "monotonic", lambda: 1_010.0)
    loop = _loop(tmp_path, monkeypatch)
    session = loop.sessions[1]
    payload = build_cd101_request(counter_select=1).application_payload
    item = OutboundDataItem.create(
        address=1,
        application_payload=payload,
        command_type=PumpCommand.READ_METER,
        simulator_only=False,
        idempotency=IdempotencyClass.IDEMPOTENT,
        max_retries=0,
    )
    loop._meter_pending = {
        "correlation_id": "corr-1",
        "address": 1,
        "counter_select": 1,
        "outbound_correlation_id": item.correlation_id,
        "queued_at_mono": 1_000.0,
        "tx_started_at_mono": None,
        "result_base": {"correlationId": "corr-1", "dartAddress": 1},
    }
    loop.runtime.outbound.enqueue(item, loop.runtime.safety)

    # Operator lifts nozzle while CD101 still queued.
    session.state.nozzle_position = NozzlePosition.OUT
    assert loop._meter_tx_still_allowed(session, item) is False
    assert loop._meter_pending is None
    assert len(loop.runtime.outbound) == 0

    from intelipump_fdc.controller.meter_read_request import read_meter_read_result

    result = read_meter_read_result(correlation_id="corr-1")
    assert result is not None
    assert result["status"] == "DEFERRED"
    assert result["errorCode"] == "METER_READ_DEFERRED_NOZZLE_OUT"
    assert result.get("volumeLiters") is None
    assert result.get("cumulativeVolumeRaw") is None


def test_post_timeout_quarantine_blocks_new_tx(tmp_path, monkeypatch) -> None:
    import time

    monkeypatch.setattr(time, "monotonic", lambda: 1_050.0)
    loop = _loop(tmp_path, monkeypatch)
    loop.runtime.meter_post_timeout_quarantine_s = 8.0
    session = loop.sessions[1]
    session.state.meter_dc101_quarantine_until_mono = 1_055.0  # still active
    session.state.meter_last_timeout_coun = 1
    session.state.meter_last_timeout_mono = 1_047.0

    from intelipump_fdc.controller.meter_read_request import (
        MeterReadRequest,
        write_meter_read_request,
    )
    from intelipump_fdc.controller.meter_read_request import read_meter_read_result

    write_meter_read_request(
        MeterReadRequest(correlation_id="corr-q", dart_address=1, counter_select=1)
    )
    import asyncio

    asyncio.run(loop._apply_pending_meter_read())
    result = read_meter_read_result(correlation_id="corr-q")
    assert result is not None
    assert result["status"] == "DEFERRED"
    assert result["errorCode"] == "METER_READ_QUARANTINE_AFTER_TIMEOUT"
    assert result.get("cumulativeVolumeRaw") is None
    assert loop._meter_pending is None
