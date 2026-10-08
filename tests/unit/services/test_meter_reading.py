"""Meter reading helpers — unsupported by default; never invent zeros."""

from __future__ import annotations

from intelipump_fdc.core.config import MeterReadingSettings
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.services import meter_reading as meter_svc
from intelipump_fdc.state_machine.guards import evaluate_command_eligibility
from intelipump_fdc.state_machine.models import PumpContext


def setup_function() -> None:
    meter_svc.clear_rate_limits()


def test_read_meter_command_exists_and_is_idempotent():
    assert PumpCommand.READ_METER.value == "READ_METER"
    from intelipump_fdc.domain.pump_command import NON_IDEMPOTENT_COMMANDS

    assert PumpCommand.READ_METER not in NON_IDEMPOTENT_COMMANDS


def test_decide_default_unsupported():
    status, code, msg = meter_svc.decide_read_meter(
        settings=MeterReadingSettings(auto_cd101=False),
        current_state=PumpState.READY,
        pending_count=0,
        rate_limited=False,
    )
    assert status == "UNSUPPORTED"
    assert code == "METER_READ_UNSUPPORTED"
    assert "manual" in msg.lower() or "unverified" in msg.lower()


def test_decide_rate_limit_and_dispensing():
    status, code, _ = meter_svc.decide_read_meter(
        settings=MeterReadingSettings(),
        current_state=PumpState.READY,
        pending_count=0,
        rate_limited=True,
    )
    assert status == "RATE_LIMITED"
    assert code == "METER_READ_RATE_LIMITED"

    status2, code2, _ = meter_svc.decide_read_meter(
        settings=MeterReadingSettings(block_during_dispensing=True),
        current_state=PumpState.FILLING,
        pending_count=0,
        rate_limited=False,
    )
    assert status2 == "DEFERRED"
    assert code2 == "METER_READ_DEFERRED_DISPENSING"


def test_decide_auto_cd101_gated_pending():
    status, code, _ = meter_svc.decide_read_meter(
        settings=MeterReadingSettings(auto_cd101=True),
        current_state=PumpState.READY,
        pending_count=0,
        rate_limited=False,
    )
    assert status == "PENDING_CONTROLLER"
    assert code == "METER_READ_QUEUED"


def test_unsupported_payload_never_zero():
    payload = meter_svc.build_unsupported_payload(
        station_id="SAO",
        device_id="pi-1",
        pump_id="pump-5",
        nozzle_id="nozzle-1",
        dart_address=5,
        correlation_id="corr-1",
        reason="unsupported",
    )
    assert payload["status"] == "UNSUPPORTED"
    assert payload["cumulativeVolumeRaw"] is None
    assert payload["volumeLiters"] is None
    assert payload["capturedAt"] is None


def test_rate_limit_bounds_frequency():
    assert not meter_svc.is_rate_limited(
        station_id="S", pump_id="p", nozzle_id="n1", min_interval_seconds=60
    )
    meter_svc.mark_read_attempt(station_id="S", pump_id="p", nozzle_id="n1", now_mono=100.0)
    assert meter_svc.is_rate_limited(
        station_id="S",
        pump_id="p",
        nozzle_id="n1",
        min_interval_seconds=60,
        now_mono=130.0,
    )
    assert not meter_svc.is_rate_limited(
        station_id="S",
        pump_id="p",
        nozzle_id="n1",
        min_interval_seconds=60,
        now_mono=170.0,
    )


def test_nozzle_mapping():
    assert meter_svc.nozzle_from_payload({"nozzleId": "2"}) == "nozzle-2"
    assert meter_svc.nozzle_from_payload({"nozzle_id": "nozzle-1"}) == "nozzle-1"
    assert meter_svc.nozzle_from_payload(None) == "nozzle-1"


def test_eligibility_read_meter_does_not_require_active_commands():
    ctx = PumpContext(
        pump_id="pump-1",
        dart_address=1,
        current_state=PumpState.READY,
        communication_healthy=True,
        state_version=1,
    )
    result = evaluate_command_eligibility(
        PumpCommand.READ_METER,
        ctx,
        physical_enable_present=False,
        active_commands_enabled=False,
    )
    assert result.eligible is True
    assert result.requires_active_commands_enabled is False


def test_cd101_payload_is_read_only_application_bytes():
    payload = meter_svc.build_cd101_outbound_payload(counter_select=1)
    assert payload[0] == 0x65
    assert len(payload) == 3
