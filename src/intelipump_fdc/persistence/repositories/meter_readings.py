"""Local immutable meter readings (additive reconciliation; not sales)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.models import MeterReadingRow


class MeterReadingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_dedupe(
        self, *, station_id: str, deduplication_key: str
    ) -> MeterReadingRow | None:
        result = await self._session.execute(
            select(MeterReadingRow).where(
                MeterReadingRow.station_id == station_id,
                MeterReadingRow.deduplication_key == deduplication_key,
            )
        )
        return result.scalar_one_or_none()

    async def count_pending(self, *, station_id: str, pump_id: str) -> int:
        result = await self._session.execute(
            select(MeterReadingRow).where(
                MeterReadingRow.station_id == station_id,
                MeterReadingRow.pump_id == pump_id,
                MeterReadingRow.status.in_(("PENDING", "PENDING_CONTROLLER")),
            )
        )
        return len(list(result.scalars().all()))

    async def resolve_pending_by_correlation(
        self,
        *,
        station_id: str,
        correlation_id: str,
        status: str,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> int:
        """Clear intake PENDING_CONTROLLER rows so max_pending does not stick."""
        result = await self._session.execute(
            select(MeterReadingRow).where(
                MeterReadingRow.station_id == station_id,
                MeterReadingRow.correlation_id == correlation_id,
                MeterReadingRow.status.in_(("PENDING", "PENDING_CONTROLLER")),
            )
        )
        rows = list(result.scalars().all())
        for row in rows:
            row.status = status
            if error_code is not None:
                row.error_code = error_code
            if error_message is not None:
                row.error_message = error_message
        if rows:
            await self._session.flush()
        return len(rows)

    async def latest_for_nozzle(
        self, *, station_id: str, pump_id: str, nozzle_id: str
    ) -> MeterReadingRow | None:
        result = await self._session.execute(
            select(MeterReadingRow)
            .where(
                MeterReadingRow.station_id == station_id,
                MeterReadingRow.pump_id == pump_id,
                MeterReadingRow.nozzle_id == nozzle_id,
            )
            .order_by(MeterReadingRow.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        *,
        station_id: str,
        device_id: str | None,
        pump_id: str,
        nozzle_id: str,
        dart_address: int | None,
        source: str,
        status: str,
        deduplication_key: str,
        correlation_id: str | None = None,
        cumulative_volume_raw: int | None = None,
        volume_decimals: int = 2,
        volume_liters: str | None = None,
        units: str = "liters",
        captured_at: datetime | None = None,
        requested_at: datetime | None = None,
        scheduled_for: datetime | None = None,
        slot: str | None = None,
        raw_evidence: dict[str, Any] | None = None,
        software_version: str | None = None,
        flags: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> MeterReadingRow:
        existing = await self.get_by_dedupe(
            station_id=station_id, deduplication_key=deduplication_key
        )
        if existing is not None:
            return existing
        now = datetime.now(UTC)
        row = MeterReadingRow(
            id=str(uuid4()),
            station_id=station_id,
            device_id=device_id,
            pump_id=pump_id,
            nozzle_id=nozzle_id,
            dart_address=dart_address,
            cumulative_volume_raw=cumulative_volume_raw,
            volume_decimals=volume_decimals,
            volume_liters=volume_liters,
            units=units,
            captured_at=captured_at,
            requested_at=requested_at or now,
            scheduled_for=scheduled_for,
            slot=slot,
            source=source,
            status=status,
            correlation_id=correlation_id,
            deduplication_key=deduplication_key,
            raw_evidence=raw_evidence,
            software_version=software_version,
            flags=flags or {},
            error_code=error_code,
            error_message=error_message,
            created_at=now,
        )
        self._session.add(row)
        await self._session.flush()
        return row
