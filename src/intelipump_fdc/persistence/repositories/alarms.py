"""Alarm repository."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.dto import AlarmRecord
from intelipump_fdc.persistence.models import AlarmRow


def _to(row: AlarmRow) -> AlarmRecord:
    return AlarmRecord(
        id=row.id,
        station_id=row.station_id,
        pump_id=row.pump_id,
        severity=row.severity,
        alarm_type=row.alarm_type,
        message=row.message,
        active=row.active,
        first_seen_at=row.first_seen_at,
        last_seen_at=row.last_seen_at,
        cleared_at=row.cleared_at,
        source_key=row.source_key,
    )


class AlarmRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_active(
        self,
        *,
        station_id: str,
        pump_id: str | None,
        severity: str,
        alarm_type: str,
        message: str,
        source_key: str,
    ) -> AlarmRecord:
        result = await self._session.execute(
            select(AlarmRow).where(
                AlarmRow.station_id == station_id,
                AlarmRow.source_key == source_key,
            )
        )
        row = result.scalar_one_or_none()
        now = datetime.now(UTC)
        if row is None:
            row = AlarmRow(
                id=str(uuid4()),
                station_id=station_id,
                pump_id=pump_id,
                severity=severity,
                alarm_type=alarm_type,
                message=message,
                active=True,
                first_seen_at=now,
                last_seen_at=now,
                source_key=source_key,
            )
            self._session.add(row)
        else:
            row.active = True
            row.last_seen_at = now
            row.message = message
            row.severity = severity
            row.cleared_at = None
        await self._session.flush()
        return _to(row)

    async def get(self, alarm_id: str) -> AlarmRecord | None:
        result = await self._session.execute(
            select(AlarmRow).where(AlarmRow.id == alarm_id)
        )
        row = result.scalar_one_or_none()
        return _to(row) if row else None

    async def list_filtered(
        self,
        *,
        station_id: str | None = None,
        pump_id: str | None = None,
        severity: str | None = None,
        active: bool | None = None,
        alarm_type: str | None = None,
        first_seen_from: datetime | None = None,
        first_seen_to: datetime | None = None,
        offset: int = 0,
        limit: int = 25,
    ) -> tuple[tuple[AlarmRecord, ...], int]:
        from sqlalchemy import func

        filters = []
        if station_id is not None:
            filters.append(AlarmRow.station_id == station_id)
        if pump_id is not None:
            filters.append(AlarmRow.pump_id == pump_id)
        if severity is not None:
            filters.append(AlarmRow.severity == severity)
        if active is not None:
            filters.append(AlarmRow.active == active)
        if alarm_type is not None:
            filters.append(AlarmRow.alarm_type == alarm_type)
        if first_seen_from is not None:
            filters.append(AlarmRow.first_seen_at >= first_seen_from)
        if first_seen_to is not None:
            filters.append(AlarmRow.first_seen_at <= first_seen_to)
        count_q = select(func.count()).select_from(AlarmRow)
        list_q = select(AlarmRow)
        if filters:
            count_q = count_q.where(*filters)
            list_q = list_q.where(*filters)
        total = int((await self._session.execute(count_q)).scalar_one())
        rows = (
            (
                await self._session.execute(
                    list_q.order_by(AlarmRow.last_seen_at.desc())
                    .offset(offset)
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return tuple(_to(r) for r in rows), total
