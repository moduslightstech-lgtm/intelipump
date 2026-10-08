"""Gated hardware CD101 safety — read-only, allowlisted, default off."""

from __future__ import annotations

from intelipump_fdc.controller.safety import (
    ControllerSafetyContext,
    evaluate_outbound_safety,
)
from intelipump_fdc.controller.session_models import IdempotencyClass, OutboundDataItem
from intelipump_fdc.core.config import ControllerMode, MeterReadingSettings
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.protocol.cd101 import build_cd101_request


def _item(address: int = 1) -> OutboundDataItem:
    payload = build_cd101_request(counter_select=1).application_payload
    return OutboundDataItem.create(
        address=address,
        application_payload=payload,
        command_type=PumpCommand.READ_METER,
        simulator_only=False,
        idempotency=IdempotencyClass.IDEMPOTENT,
    )


def _ctx(**kwargs) -> ControllerSafetyContext:
    base = dict(
        environment="PRODUCTION",
        mode=ControllerMode.BENCH_CONTROL,
        active_commands_enabled=True,
        require_physical_control_enable=True,
        physical_enable_present=True,
        owned_lab_active_session=True,
        production_sole_controller_session=True,
        hardware_meter_cd101_enabled=False,
        hardware_meter_allowed_addresses=frozenset({1}),
        hardware_meter_allowed_device_id="InteliPump-SAO-RS1-pi-005",
        hardware_meter_device_id="InteliPump-SAO-RS1-pi-005",
    )
    base.update(kwargs)
    return ControllerSafetyContext(**base)


def test_hardware_gate_default_blocks():
    decision = evaluate_outbound_safety(_item(), _ctx())
    assert decision.allowed is False


def test_hardware_gate_allows_allowlisted_address_only():
    ctx = _ctx(hardware_meter_cd101_enabled=True)
    assert evaluate_outbound_safety(_item(1), ctx).allowed is True
    blocked = evaluate_outbound_safety(_item(2), ctx)
    assert blocked.allowed is False
    assert any("not_allowlisted" in r for r in blocked.reasons)


def test_hardware_gate_requires_device_match():
    ctx = _ctx(
        hardware_meter_cd101_enabled=True,
        hardware_meter_device_id="other-device",
    )
    decision = evaluate_outbound_safety(_item(), ctx)
    assert decision.allowed is False
    assert "hardware_meter_device_id_mismatch" in decision.reasons


def test_cd101_payload_is_documented_read_only_bytes():
    req = build_cd101_request(counter_select=1)
    assert req.application_payload == bytes((0x65, 0x01, 0x01))
    assert "page 19" in req.source_reference
    assert "Read-only" in __import__(
        "intelipump_fdc.protocol.cd101", fromlist=["__doc__"]
    ).__doc__


def test_settings_allowed_address_set():
    cfg = MeterReadingSettings(allowed_addresses="1, 2")
    assert cfg.allowed_address_set() == frozenset({1, 2})
