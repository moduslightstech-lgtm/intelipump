"""Command and audit persistence tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from intelipump_fdc.persistence.errors import AuditChainError
from intelipump_fdc.persistence.models import AuditLogRow
from intelipump_fdc.persistence.unit_of_work import unit_of_work

STATION = "InteliPump-US-Lab"


@pytest.mark.asyncio
async def test_rejected_command_persisted_with_audit(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-1", dart_address=1
        )
        cmd = await uow.commands.create(
            correlation_id="corr-auth-1",
            station_id=STATION,
            pump_id=pump.id,
            command_type="AUTHORIZE",
            status="REJECTED",
            idempotency_class="NON_IDEMPOTENT",
            simulator_only=True,
            blocking_reasons=("listen_only", "active_commands_disabled"),
            completed_at=datetime.now(UTC),
            request_payload={"address": 1},
        )
        audit = await uow.audit.append(
            actor="controller",
            source="safety",
            action="COMMAND_REJECTED:AUTHORIZE",
            station_id=STATION,
            pump_id=pump.id,
            previous_state="READY",
            resulting_state="READY",
            result="REJECTED",
            correlation_id=cmd.correlation_id,
            details={"blocking_reasons": list(cmd.blocking_reasons)},
        )
        assert cmd.status == "REJECTED"
        assert cmd.simulator_only is True
        assert "listen_only" in cmd.blocking_reasons
        assert audit.previous_hash == "GENESIS_V1"
        assert len(audit.record_hash) == 64


@pytest.mark.asyncio
async def test_command_attempt_history(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        await uow.commands.create(
            correlation_id="corr-2",
            station_id=STATION,
            pump_id=None,
            command_type="READ_STATUS",
            status="IN_PROGRESS",
            idempotency_class="IDEMPOTENT",
            simulator_only=True,
        )
        await uow.commands.add_attempt(
            correlation_id="corr-2", attempt_number=1, outcome="TIMEOUT"
        )
        await uow.commands.add_attempt(
            correlation_id="corr-2",
            attempt_number=2,
            outcome="ACK",
            sequence_number=3,
        )
        attempts = await uow.commands.list_attempts("corr-2")
        assert len(attempts) == 2
        assert attempts[0].outcome == "TIMEOUT"
        assert attempts[1].sequence_number == 3


@pytest.mark.asyncio
async def test_audit_hash_chain_verify_and_corruption(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        a1 = await uow.audit.append(
            actor="a",
            source="s",
            action="ONE",
            station_id=STATION,
            result="OK",
        )
        a2 = await uow.audit.append(
            actor="a",
            source="s",
            action="TWO",
            station_id=STATION,
            result="OK",
        )
        assert a2.previous_hash == a1.record_hash
        await uow.audit.verify_chain()

    async with unit_of_work(factory) as uow:
        await uow.session.execute(
            update(AuditLogRow)
            .where(AuditLogRow.id == a2.id)
            .values(action="TAMPERED")
        )
        with pytest.raises(AuditChainError):
            await uow.audit.verify_chain()


@pytest.mark.asyncio
async def test_pending_authorize_exists_for_recovery(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        await uow.commands.create(
            correlation_id="pending-auth",
            station_id=STATION,
            pump_id=None,
            command_type="AUTHORIZE",
            status="PENDING",
            idempotency_class="NON_IDEMPOTENT",
            simulator_only=True,
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
        pending = await uow.commands.list_pending_or_in_progress(station_id=STATION)
        assert any(c.correlation_id == "pending-auth" for c in pending)
