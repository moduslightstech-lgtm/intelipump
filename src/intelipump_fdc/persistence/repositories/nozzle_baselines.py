"""Per-nozzle completed-sale baseline / last-published state (survives restart)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.models import NozzleSaleBaselineRow


@dataclass(frozen=True, slots=True)
class NozzleSaleBaseline:
    station_id: str
    pump_id: str
    dart_address: int
    nozzle_id: int
    last_completed_fingerprint: str | None
    last_published_fingerprint: str | None
    last_transaction_uuid: str | None
    last_raw_volume: int | None
    last_raw_amount: int | None
    last_completed_at: datetime | None
    initialized: bool


def _record(row: NozzleSaleBaselineRow) -> NozzleSaleBaseline:
    return NozzleSaleBaseline(
        station_id=row.station_id,
        pump_id=row.pump_id,
        dart_address=row.dart_address,
        nozzle_id=row.nozzle_id,
        last_completed_fingerprint=row.last_completed_fingerprint,
        last_published_fingerprint=row.last_published_fingerprint,
        last_transaction_uuid=row.last_transaction_uuid,
        last_raw_volume=row.last_raw_volume,
        last_raw_amount=row.last_raw_amount,
        last_completed_at=row.last_completed_at,
        initialized=bool(row.initialized),
    )


class NozzleSaleBaselineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(
        self,
        *,
        station_id: str,
        dart_address: int,
        nozzle_id: int,
    ) -> NozzleSaleBaseline | None:
        result = await self._session.execute(
            select(NozzleSaleBaselineRow).where(
                NozzleSaleBaselineRow.station_id == station_id,
                NozzleSaleBaselineRow.dart_address == dart_address,
                NozzleSaleBaselineRow.nozzle_id == nozzle_id,
            )
        )
        row = result.scalar_one_or_none()
        return _record(row) if row else None

    async def upsert_baseline(
        self,
        *,
        station_id: str,
        pump_id: str,
        dart_address: int,
        nozzle_id: int,
        fingerprint: str,
        raw_volume: int | None = None,
        raw_amount: int | None = None,
        transaction_uuid: str | None = None,
        mark_published: bool = False,
    ) -> NozzleSaleBaseline:
        """Record the observed completed face. Does not publish."""
        result = await self._session.execute(
            select(NozzleSaleBaselineRow).where(
                NozzleSaleBaselineRow.station_id == station_id,
                NozzleSaleBaselineRow.dart_address == dart_address,
                NozzleSaleBaselineRow.nozzle_id == nozzle_id,
            )
        )
        row = result.scalar_one_or_none()
        now = datetime.now(UTC)
        if row is None:
            row = NozzleSaleBaselineRow(
                station_id=station_id,
                pump_id=pump_id,
                dart_address=dart_address,
                nozzle_id=nozzle_id,
                last_completed_fingerprint=fingerprint,
                last_published_fingerprint=fingerprint if mark_published else None,
                last_transaction_uuid=transaction_uuid,
                last_raw_volume=raw_volume,
                last_raw_amount=raw_amount,
                last_completed_at=now,
                initialized=True,
                updated_at=now,
            )
            self._session.add(row)
        else:
            row.pump_id = pump_id
            row.last_completed_fingerprint = fingerprint
            if mark_published:
                row.last_published_fingerprint = fingerprint
            if transaction_uuid:
                row.last_transaction_uuid = transaction_uuid
            if raw_volume is not None:
                row.last_raw_volume = raw_volume
            if raw_amount is not None:
                row.last_raw_amount = raw_amount
            row.last_completed_at = now
            row.initialized = True
            row.updated_at = now
        await self._session.flush()
        return _record(row)

    def is_already_observed(self, baseline: NozzleSaleBaseline | None, fingerprint: str) -> bool:
        if baseline is None or not fingerprint:
            return False
        return fingerprint in {
            baseline.last_completed_fingerprint,
            baseline.last_published_fingerprint,
        }
