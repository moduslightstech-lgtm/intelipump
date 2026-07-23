"""Command request and attempt repository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.dto import CommandAttemptRecord, CommandRecord
from intelipump_fdc.persistence.models import CommandAttemptRow, CommandRow


def _cmd(row: CommandRow) -> CommandRecord:
    reasons = row.blocking_reasons or []
    return CommandRecord(
        correlation_id=row.correlation_id,
        station_id=row.station_id,
        pump_id=row.pump_id,
        command_type=row.command_type,
        status=row.status,
        idempotency_class=row.idempotency_class,
        simulator_only=row.simulator_only,
        requested_at=row.requested_at,
        expires_at=row.expires_at,
        completed_at=row.completed_at,
        request_payload=row.request_payload,
        result_payload=row.result_payload,
        blocking_reasons=tuple(reasons),
    )


def _att(row: CommandAttemptRow) -> CommandAttemptRecord:
    return CommandAttemptRecord(
        id=row.id,
        correlation_id=row.correlation_id,
        attempt_number=row.attempt_number,
        sequence_number=row.sequence_number,
        outcome=row.outcome,
        error_code=row.error_code,
        raw_frame=row.raw_frame,
        attempted_at=row.attempted_at,
    )


class CommandRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        correlation_id: str,
        station_id: str,
        pump_id: str | None,
        command_type: str,
        status: str,
        idempotency_class: str,
        simulator_only: bool,
        requested_at: datetime | None = None,
        expires_at: datetime | None = None,
        request_payload: dict[str, Any] | None = None,
        result_payload: dict[str, Any] | None = None,
        blocking_reasons: tuple[str, ...] | list[str] | None = None,
        completed_at: datetime | None = None,
    ) -> CommandRecord:
        row = CommandRow(
            correlation_id=correlation_id,
            station_id=station_id,
            pump_id=pump_id,
            command_type=command_type,
            status=status,
            idempotency_class=idempotency_class,
            simulator_only=simulator_only,
            requested_at=requested_at or datetime.now(UTC),
            expires_at=expires_at,
            completed_at=completed_at,
            request_payload=request_payload,
            result_payload=result_payload,
            blocking_reasons=list(blocking_reasons) if blocking_reasons else None,
        )
        self._session.add(row)
        await self._session.flush()
        return _cmd(row)

    async def update_status(
        self,
        correlation_id: str,
        *,
        status: str,
        completed_at: datetime | None = None,
        result_payload: dict[str, Any] | None = None,
    ) -> CommandRecord | None:
        result = await self._session.execute(
            select(CommandRow).where(CommandRow.correlation_id == correlation_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        row.status = status
        if completed_at is not None:
            row.completed_at = completed_at
        if result_payload is not None:
            row.result_payload = result_payload
        await self._session.flush()
        return _cmd(row)

    async def get(self, correlation_id: str) -> CommandRecord | None:
        result = await self._session.execute(
            select(CommandRow).where(CommandRow.correlation_id == correlation_id)
        )
        row = result.scalar_one_or_none()
        return _cmd(row) if row else None

    async def list_pending_or_in_progress(
        self, *, station_id: str
    ) -> tuple[CommandRecord, ...]:
        result = await self._session.execute(
            select(CommandRow).where(
                CommandRow.station_id == station_id,
                CommandRow.status.in_(("PENDING", "IN_PROGRESS")),
            )
        )
        return tuple(_cmd(r) for r in result.scalars().all())

    async def add_attempt(
        self,
        *,
        correlation_id: str,
        attempt_number: int,
        outcome: str,
        sequence_number: int | None = None,
        error_code: str | None = None,
        raw_frame: str | None = None,
        attempted_at: datetime | None = None,
    ) -> CommandAttemptRecord:
        row = CommandAttemptRow(
            id=str(uuid4()),
            correlation_id=correlation_id,
            attempt_number=attempt_number,
            sequence_number=sequence_number,
            outcome=outcome,
            error_code=error_code,
            raw_frame=raw_frame,
            attempted_at=attempted_at or datetime.now(UTC),
        )
        self._session.add(row)
        await self._session.flush()
        return _att(row)

    async def list_attempts(self, correlation_id: str) -> tuple[CommandAttemptRecord, ...]:
        result = await self._session.execute(
            select(CommandAttemptRow)
            .where(CommandAttemptRow.correlation_id == correlation_id)
            .order_by(CommandAttemptRow.attempt_number)
        )
        return tuple(_att(r) for r in result.scalars().all())
