"""Pump state snapshot repository."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.dto import StateSnapshotRecord
from intelipump_fdc.persistence.models import PumpStateSnapshotRow


def _to_record(row: PumpStateSnapshotRow) -> StateSnapshotRecord:
    return StateSnapshotRecord(
        id=row.id,
        pump_id=row.pump_id,
        normalized_state=row.normalized_state,
        previous_state=row.previous_state,
        selected_nozzle=row.selected_nozzle,
        active_transaction_id=row.active_transaction_id,
        communication_healthy=row.communication_healthy,
        raw_wayne_status=row.raw_wayne_status,
        source_frame_ref=row.source_frame_ref,
        state_version=row.state_version,
        observed_at=row.observed_at,
        persisted_at=row.persisted_at,
    )


class StateRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def latest(self, pump_id: str) -> StateSnapshotRecord | None:
        result = await self._session.execute(
            select(PumpStateSnapshotRow)
            .where(PumpStateSnapshotRow.pump_id == pump_id)
            .order_by(
                PumpStateSnapshotRow.state_version.desc(),
                PumpStateSnapshotRow.persisted_at.desc(),
            )
            .limit(1)
        )
        row = result.scalar_one_or_none()
        return _to_record(row) if row else None

    async def list_for_pump(self, pump_id: str) -> tuple[StateSnapshotRecord, ...]:
        result = await self._session.execute(
            select(PumpStateSnapshotRow)
            .where(PumpStateSnapshotRow.pump_id == pump_id)
            .order_by(PumpStateSnapshotRow.state_version.asc())
        )
        return tuple(_to_record(r) for r in result.scalars().all())

    async def insert_if_meaningful(
        self,
        *,
        pump_id: str,
        normalized_state: str,
        previous_state: str | None,
        selected_nozzle: int | None,
        active_transaction_id: str | None,
        communication_healthy: bool,
        raw_wayne_status: int | None,
        source_frame_ref: str | None,
        state_version: int,
        observed_at: datetime | None,
    ) -> StateSnapshotRecord | None:
        """Insert only when state_version advances or context meaningfully changes."""
        latest = await self.latest(pump_id)
        if latest is not None:
            if state_version < latest.state_version:
                return None
            if state_version == latest.state_version and (
                latest.normalized_state == normalized_state
                and latest.selected_nozzle == selected_nozzle
                and latest.active_transaction_id == active_transaction_id
                and latest.communication_healthy == communication_healthy
                and latest.raw_wayne_status == raw_wayne_status
            ):
                return None
        row = PumpStateSnapshotRow(
            id=str(uuid4()),
            pump_id=pump_id,
            normalized_state=normalized_state,
            previous_state=previous_state,
            selected_nozzle=selected_nozzle,
            active_transaction_id=active_transaction_id,
            communication_healthy=communication_healthy,
            raw_wayne_status=raw_wayne_status,
            source_frame_ref=source_frame_ref,
            state_version=state_version,
            observed_at=observed_at,
            persisted_at=datetime.now(UTC),
        )
        self._session.add(row)
        await self._session.flush()
        return _to_record(row)
