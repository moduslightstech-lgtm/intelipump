"""Pump repository."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.dto import PumpRecord
from intelipump_fdc.persistence.models import PumpRow


def _to_record(row: PumpRow) -> PumpRecord:
    return PumpRecord(
        id=row.id,
        station_id=row.station_id,
        logical_pump_id=row.logical_pump_id,
        dart_address=row.dart_address,
        enabled=row.enabled,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


class PumpRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        *,
        station_id: str,
        logical_pump_id: str,
        dart_address: int,
        enabled: bool = True,
        pump_id: str | None = None,
    ) -> PumpRecord:
        result = await self._session.execute(
            select(PumpRow).where(
                PumpRow.station_id == station_id,
                PumpRow.logical_pump_id == logical_pump_id,
            )
        )
        row = result.scalar_one_or_none()
        now = datetime.now(UTC)
        if row is None:
            row = PumpRow(
                id=pump_id or str(uuid4()),
                station_id=station_id,
                logical_pump_id=logical_pump_id,
                dart_address=dart_address,
                enabled=enabled,
                created_at=now,
                updated_at=now,
            )
            self._session.add(row)
        else:
            row.dart_address = dart_address
            row.enabled = enabled
            row.updated_at = now
        await self._session.flush()
        return _to_record(row)

    async def get_by_address(
        self, *, station_id: str, dart_address: int
    ) -> PumpRecord | None:
        result = await self._session.execute(
            select(PumpRow).where(
                PumpRow.station_id == station_id,
                PumpRow.dart_address == dart_address,
            )
        )
        row = result.scalar_one_or_none()
        return _to_record(row) if row else None

    async def get_by_id(self, pump_id: str) -> PumpRecord | None:
        result = await self._session.execute(select(PumpRow).where(PumpRow.id == pump_id))
        row = result.scalar_one_or_none()
        return _to_record(row) if row else None

    async def list_for_station(self, station_id: str) -> tuple[PumpRecord, ...]:
        result = await self._session.execute(
            select(PumpRow)
            .where(PumpRow.station_id == station_id)
            .order_by(PumpRow.dart_address)
        )
        return tuple(_to_record(r) for r in result.scalars().all())
