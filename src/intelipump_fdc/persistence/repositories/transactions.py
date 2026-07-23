"""Transaction and transaction-event repository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.dto import TransactionEventRecord, TransactionRecord
from intelipump_fdc.persistence.errors import DuplicateEntityError
from intelipump_fdc.persistence.models import TransactionEventRow, TransactionRow


def _tx(row: TransactionRow) -> TransactionRecord:
    return TransactionRecord(
        id=row.id,
        transaction_uuid=row.transaction_uuid,
        station_id=row.station_id,
        pump_id=row.pump_id,
        nozzle_id=row.nozzle_id,
        status=row.status,
        raw_price=row.raw_price,
        price_decimals=row.price_decimals,
        raw_volume=row.raw_volume,
        volume_decimals=row.volume_decimals,
        raw_amount=row.raw_amount,
        amount_decimals=row.amount_decimals,
        started_at=row.started_at,
        completed_at=row.completed_at,
        closed_at=row.closed_at,
        source_completion_key=row.source_completion_key,
        simulated=row.simulated,
        environment=row.environment,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _ev(row: TransactionEventRow) -> TransactionEventRecord:
    return TransactionEventRecord(
        id=row.id,
        transaction_id=row.transaction_id,
        event_type=row.event_type,
        event_key=row.event_key,
        raw_payload=row.raw_payload,
        source_frame_ref=row.source_frame_ref,
        observed_at=row.observed_at,
        created_at=row.created_at,
    )


class TransactionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        transaction_uuid: str,
        station_id: str,
        pump_id: str,
        nozzle_id: int | None,
        status: str,
        raw_price: int | None,
        price_decimals: int | None,
        volume_decimals: int | None,
        amount_decimals: int | None,
        simulated: bool,
        environment: str,
        started_at: datetime | None = None,
    ) -> TransactionRecord:
        now = datetime.now(UTC)
        row = TransactionRow(
            id=str(uuid4()),
            transaction_uuid=transaction_uuid,
            station_id=station_id,
            pump_id=pump_id,
            nozzle_id=nozzle_id,
            status=status,
            raw_price=raw_price,
            price_decimals=price_decimals,
            raw_volume=0,
            volume_decimals=volume_decimals,
            raw_amount=0,
            amount_decimals=amount_decimals,
            started_at=started_at or now,
            simulated=simulated,
            environment=environment,
            created_at=now,
            updated_at=now,
        )
        self._session.add(row)
        await self._session.flush()
        return _tx(row)

    async def get_by_uuid(self, transaction_uuid: str) -> TransactionRecord | None:
        result = await self._session.execute(
            select(TransactionRow).where(
                TransactionRow.transaction_uuid == transaction_uuid
            )
        )
        row = result.scalar_one_or_none()
        return _tx(row) if row else None

    async def get_by_completion_key(self, key: str) -> TransactionRecord | None:
        result = await self._session.execute(
            select(TransactionRow).where(TransactionRow.source_completion_key == key)
        )
        row = result.scalar_one_or_none()
        return _tx(row) if row else None

    async def list_unresolved(self, *, station_id: str) -> tuple[TransactionRecord, ...]:
        result = await self._session.execute(
            select(TransactionRow).where(
                TransactionRow.station_id == station_id,
                TransactionRow.status.in_(("ACTIVE", "SUSPENDED", "OPEN")),
            )
        )
        return tuple(_tx(r) for r in result.scalars().all())

    async def update_filling(
        self,
        transaction_uuid: str,
        *,
        raw_volume: int,
        raw_amount: int,
        raw_price: int | None = None,
    ) -> TransactionRecord | None:
        result = await self._session.execute(
            select(TransactionRow).where(
                TransactionRow.transaction_uuid == transaction_uuid
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        # Never decrease.
        row.raw_volume = max(row.raw_volume, raw_volume)
        row.raw_amount = max(row.raw_amount, raw_amount)
        if raw_price is not None:
            row.raw_price = raw_price
        row.updated_at = datetime.now(UTC)
        await self._session.flush()
        return _tx(row)

    async def complete_once(
        self,
        transaction_uuid: str,
        *,
        source_completion_key: str,
        raw_volume: int,
        raw_amount: int,
        completed_at: datetime | None = None,
    ) -> tuple[TransactionRecord, bool]:
        """Complete transaction once. Returns (record, newly_completed)."""
        existing = await self.get_by_completion_key(source_completion_key)
        if existing is not None:
            return existing, False
        result = await self._session.execute(
            select(TransactionRow).where(
                TransactionRow.transaction_uuid == transaction_uuid
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise LookupError(f"transaction not found: {transaction_uuid}")
        if row.status == "COMPLETED" and row.source_completion_key:
            return _tx(row), False
        now = completed_at or datetime.now(UTC)
        row.status = "COMPLETED"
        row.source_completion_key = source_completion_key
        row.raw_volume = max(row.raw_volume, raw_volume)
        row.raw_amount = max(row.raw_amount, raw_amount)
        row.completed_at = now
        row.updated_at = now
        try:
            await self._session.flush()
        except IntegrityError as exc:
            raise DuplicateEntityError(
                f"completion key already used: {source_completion_key}"
            ) from exc
        return _tx(row), True

    async def add_event(
        self,
        *,
        transaction_id: str,
        event_type: str,
        event_key: str,
        raw_payload: dict[str, Any] | None,
        source_frame_ref: str | None,
        observed_at: datetime | None,
    ) -> tuple[TransactionEventRecord | None, bool]:
        """Insert event; returns (record, inserted). Duplicate key → (None, False)."""
        result = await self._session.execute(
            select(TransactionEventRow).where(
                TransactionEventRow.transaction_id == transaction_id,
                TransactionEventRow.event_key == event_key,
            )
        )
        if result.scalar_one_or_none() is not None:
            return None, False
        row = TransactionEventRow(
            id=str(uuid4()),
            transaction_id=transaction_id,
            event_type=event_type,
            event_key=event_key,
            raw_payload=raw_payload,
            source_frame_ref=source_frame_ref,
            observed_at=observed_at,
            created_at=datetime.now(UTC),
        )
        self._session.add(row)
        try:
            await self._session.flush()
        except IntegrityError:
            return None, False
        return _ev(row), True

    async def count_completed(self, *, station_id: str) -> int:
        result = await self._session.execute(
            select(TransactionRow).where(
                TransactionRow.station_id == station_id,
                TransactionRow.status == "COMPLETED",
            )
        )
        return len(list(result.scalars().all()))

    async def list_events(
        self, transaction_id: str
    ) -> tuple[TransactionEventRecord, ...]:
        result = await self._session.execute(
            select(TransactionEventRow)
            .where(TransactionEventRow.transaction_id == transaction_id)
            .order_by(TransactionEventRow.created_at.asc())
        )
        return tuple(_ev(r) for r in result.scalars().all())

    async def list_filtered(
        self,
        *,
        station_id: str | None = None,
        pump_id: str | None = None,
        nozzle_id: int | None = None,
        status: str | None = None,
        simulated: bool | None = None,
        environment: str | None = None,
        started_from: datetime | None = None,
        started_to: datetime | None = None,
        completed_from: datetime | None = None,
        completed_to: datetime | None = None,
        offset: int = 0,
        limit: int = 25,
    ) -> tuple[tuple[TransactionRecord, ...], int]:
        from sqlalchemy import func

        filters = []
        if station_id is not None:
            filters.append(TransactionRow.station_id == station_id)
        if pump_id is not None:
            filters.append(TransactionRow.pump_id == pump_id)
        if nozzle_id is not None:
            filters.append(TransactionRow.nozzle_id == nozzle_id)
        if status is not None:
            filters.append(TransactionRow.status == status)
        if simulated is not None:
            filters.append(TransactionRow.simulated == simulated)
        if environment is not None:
            filters.append(TransactionRow.environment == environment)
        if started_from is not None:
            filters.append(TransactionRow.started_at >= started_from)
        if started_to is not None:
            filters.append(TransactionRow.started_at <= started_to)
        if completed_from is not None:
            filters.append(TransactionRow.completed_at >= completed_from)
        if completed_to is not None:
            filters.append(TransactionRow.completed_at <= completed_to)

        count_q = select(func.count()).select_from(TransactionRow)
        list_q = select(TransactionRow)
        if filters:
            count_q = count_q.where(*filters)
            list_q = list_q.where(*filters)
        total = int((await self._session.execute(count_q)).scalar_one())
        list_q = (
            list_q.order_by(
                TransactionRow.created_at.desc(),
                TransactionRow.started_at.desc(),
            )
            .offset(offset)
            .limit(limit)
        )
        rows = (await self._session.execute(list_q)).scalars().all()
        return tuple(_tx(r) for r in rows), total

    async def get_by_id(self, transaction_id: str) -> TransactionRecord | None:
        result = await self._session.execute(
            select(TransactionRow).where(TransactionRow.id == transaction_id)
        )
        row = result.scalar_one_or_none()
        if row is not None:
            return _tx(row)
        return await self.get_by_uuid(transaction_id)
