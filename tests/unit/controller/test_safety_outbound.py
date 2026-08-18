"""Controller safety and outbound queue tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from intelipump_fdc.controller.outbound import (
    OutboundQueue,
    OutboundQueueFullError,
    OutboundRejectedError,
)
from intelipump_fdc.controller.safety import (
    ControllerSafetyContext,
    evaluate_outbound_safety,
    evaluate_polling_allowed,
)
from intelipump_fdc.controller.session_models import IdempotencyClass, OutboundDataItem
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.domain.pump_command import PumpCommand


def _lab() -> ControllerSafetyContext:
    return ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.LISTEN_ONLY,
        active_commands_enabled=False,
        require_physical_control_enable=True,
        allow_virtual_polling=True,
    )


def test_listen_only_blocks_active_commands() -> None:
    item = OutboundDataItem.create(
        address=1,
        application_payload=b"\x01\x01\x06",
        command_type=PumpCommand.AUTHORIZE,
        simulator_only=True,
        idempotency=IdempotencyClass.NON_IDEMPOTENT,
    )
    decision = evaluate_outbound_safety(item, _lab())
    assert decision.allowed is False
    assert any("LISTEN_ONLY" in r or "blocked" in r for r in decision.reasons)


def test_simulator_only_read_allowed_in_lab() -> None:
    item = OutboundDataItem.create(
        address=1,
        application_payload=b"\x01\x01\x00",
        command_type=PumpCommand.READ_STATUS,
        simulator_only=True,
        idempotency=IdempotencyClass.IDEMPOTENT,
    )
    assert evaluate_outbound_safety(item, _lab()).allowed is True


def test_simulator_only_requires_lab() -> None:
    item = OutboundDataItem.create(
        address=1,
        application_payload=b"\x01\x01\x00",
        command_type=PumpCommand.READ_STATUS,
        simulator_only=True,
        idempotency=IdempotencyClass.IDEMPOTENT,
    )
    ctx = ControllerSafetyContext(
        environment="PROD",
        mode=ControllerMode.LISTEN_ONLY,
        active_commands_enabled=False,
        require_physical_control_enable=True,
        allow_virtual_polling=True,
    )
    assert evaluate_outbound_safety(item, ctx).allowed is False


def test_outbound_queue_expiry() -> None:
    q = OutboundQueue(max_size=4)
    item = OutboundDataItem(
        correlation_id="x",
        address=1,
        application_payload=b"\x00",
        created_at=datetime.now(UTC) - timedelta(seconds=10),
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
        idempotency=IdempotencyClass.IDEMPOTENT,
        max_retries=1,
        simulator_only=True,
        command_type=PumpCommand.READ_STATUS,
    )
    with pytest.raises(OutboundRejectedError):
        q.enqueue(item, _lab())


def test_outbound_queue_bound() -> None:
    q = OutboundQueue(max_size=1)
    a = OutboundDataItem.create(
        address=1,
        application_payload=b"\x01\x01\x00",
        command_type=PumpCommand.READ_STATUS,
        simulator_only=True,
        idempotency=IdempotencyClass.IDEMPOTENT,
    )
    b = OutboundDataItem.create(
        address=1,
        application_payload=b"\x01\x01\x00",
        command_type=PumpCommand.READ_STATUS,
        simulator_only=True,
        idempotency=IdempotencyClass.IDEMPOTENT,
    )
    q.enqueue(a, _lab())
    with pytest.raises(OutboundQueueFullError):
        q.enqueue(b, _lab())


def test_polling_allowed_in_lab_listen_only() -> None:
    assert evaluate_polling_allowed(_lab()).allowed is True


def _owned_lab() -> ControllerSafetyContext:
    return ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.BENCH_CONTROL,
        active_commands_enabled=True,
        require_physical_control_enable=True,
        physical_enable_present=True,
        allow_virtual_polling=True,
        owned_lab_active_session=True,
    )


def test_owned_lab_allows_authorize_reset_price() -> None:
    ctx = _owned_lab()
    for command, payload in (
        (PumpCommand.AUTHORIZE, b"\x01\x01\x06"),
        (PumpCommand.RESET, b"\x01\x01\x05"),
        (PumpCommand.SET_PRICE, b"\x05\x03\x00\x01\x20"),
        (PumpCommand.READ_STATUS, b"\x01\x01\x00"),
    ):
        item = OutboundDataItem.create(
            address=1,
            application_payload=payload,
            command_type=command,
            simulator_only=False,
            idempotency=IdempotencyClass.NON_IDEMPOTENT,
        )
        assert evaluate_outbound_safety(item, ctx).allowed is True, command


def test_owned_lab_requires_physical_enable() -> None:
    ctx = ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.BENCH_CONTROL,
        active_commands_enabled=True,
        require_physical_control_enable=True,
        physical_enable_present=False,
        owned_lab_active_session=True,
    )
    item = OutboundDataItem.create(
        address=1,
        application_payload=b"\x01\x01\x06",
        command_type=PumpCommand.AUTHORIZE,
        simulator_only=False,
        idempotency=IdempotencyClass.NON_IDEMPOTENT,
    )
    decision = evaluate_outbound_safety(item, ctx)
    assert decision.allowed is False
    assert "physical_enable_required" in decision.reasons


def test_listen_only_still_blocks_without_owned_lab_flag() -> None:
    item = OutboundDataItem.create(
        address=1,
        application_payload=b"\x01\x01\x06",
        command_type=PumpCommand.AUTHORIZE,
        simulator_only=False,
        idempotency=IdempotencyClass.NON_IDEMPOTENT,
    )
    decision = evaluate_outbound_safety(item, _lab())
    assert decision.allowed is False
