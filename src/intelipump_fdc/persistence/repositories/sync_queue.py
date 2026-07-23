"""Durable sync queue repository (no MQTT delivery)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.dto import SyncQueueRecord
from intelipump_fdc.persistence.models import SyncQueueRow


def _to(row: SyncQueueRow) -> SyncQueueRecord:
    return SyncQueueRecord(
        id=row.id,
        entity_type=row.entity_type,
        entity_id=row.entity_id,
        event_type=row.event_type,
        payload=row.payload,
        status=row.status,
        attempt_count=row.attempt_count,
        available_at=row.available_at,
        locked_at=row.locked_at,
        last_error=row.last_error,
        created_at=row.created_at,
        updated_at=row.updated_at,
        deduplication_key=row.deduplication_key,
    )


class SyncQueueRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def enqueue(
        self,
        *,
        entity_type: str,
        entity_id: str,
        event_type: str,
        payload: dict[str, Any],
        deduplication_key: str,
        available_at: datetime | None = None,
    ) -> SyncQueueRecord | None:
        """Enqueue; returns None if dedupe key already exists."""
        existing = await self._session.execute(
            select(SyncQueueRow).where(
                SyncQueueRow.deduplication_key == deduplication_key
            )
        )
        if existing.scalar_one_or_none() is not None:
            return None
        now = datetime.now(UTC)
        row = SyncQueueRow(
            id=str(uuid4()),
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=event_type,
            payload=payload,
            status="PENDING",
            attempt_count=0,
            available_at=available_at or now,
            locked_at=None,
            last_error=None,
            created_at=now,
            updated_at=now,
            deduplication_key=deduplication_key,
        )
        self._session.add(row)
        await self._session.flush()
        return _to(row)

    async def enqueue_checked(
        self,
        *,
        entity_type: str,
        entity_id: str,
        event_type: str,
        payload: dict[str, Any],
        deduplication_key: str,
        available_at: datetime | None = None,
    ) -> SyncQueueRecord | None:
        return await self.enqueue(
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=event_type,
            payload=payload,
            deduplication_key=deduplication_key,
            available_at=available_at,
        )

    async def claim_batch(self, *, limit: int = 10) -> tuple[SyncQueueRecord, ...]:
        now = datetime.now(UTC)
        result = await self._session.execute(
            select(SyncQueueRow)
            .where(
                SyncQueueRow.status == "PENDING",
                SyncQueueRow.available_at <= now,
                SyncQueueRow.locked_at.is_(None),
            )
            .order_by(SyncQueueRow.created_at.asc())
            .limit(limit)
        )
        rows = list(result.scalars().all())
        for row in rows:
            row.status = "CLAIMED"
            row.locked_at = now
            row.updated_at = now
        await self._session.flush()
        return tuple(_to(r) for r in rows)

    async def mark_delivered(self, item_id: str) -> None:
        result = await self._session.execute(
            select(SyncQueueRow).where(SyncQueueRow.id == item_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return
        row.status = "DELIVERED"
        row.locked_at = None
        row.updated_at = datetime.now(UTC)
        await self._session.flush()

    async def release_stale_locks(self, *, older_than_seconds: int = 60) -> int:
        cutoff = datetime.now(UTC) - timedelta(seconds=older_than_seconds)
        result = await self._session.execute(
            select(SyncQueueRow).where(
                SyncQueueRow.status == "CLAIMED",
                SyncQueueRow.locked_at.is_not(None),
                SyncQueueRow.locked_at <= cutoff,
            )
        )
        rows = list(result.scalars().all())
        for row in rows:
            row.status = "PENDING"
            row.locked_at = None
            row.updated_at = datetime.now(UTC)
        await self._session.flush()
        return len(rows)

    async def pending_count(self) -> int:
        result = await self._session.execute(
            select(func.count())
            .select_from(SyncQueueRow)
            .where(SyncQueueRow.status.in_(("PENDING", "CLAIMED")))
        )
        return int(result.scalar_one())

    async def delivered_count(self) -> int:
        result = await self._session.execute(
            select(func.count())
            .select_from(SyncQueueRow)
            .where(SyncQueueRow.status == "DELIVERED")
        )
        return int(result.scalar_one())

    async def failed_count(self) -> int:
        """Rows permanently failed or pending with prior errors."""
        result = await self._session.execute(
            select(func.count())
            .select_from(SyncQueueRow)
            .where(
                (SyncQueueRow.status == "FAILED")
                | (
                    (SyncQueueRow.status == "PENDING")
                    & (SyncQueueRow.attempt_count > 0)
                )
            )
        )
        return int(result.scalar_one())

    async def oldest_pending_age_seconds(self) -> float | None:
        result = await self._session.execute(
            select(func.min(SyncQueueRow.created_at)).where(
                SyncQueueRow.status.in_(("PENDING", "CLAIMED"))
            )
        )
        oldest = result.scalar_one_or_none()
        if oldest is None:
            return None
        if oldest.tzinfo is None:
            oldest = oldest.replace(tzinfo=UTC)
        return (datetime.now(UTC) - oldest).total_seconds()

    async def mark_failed(
        self,
        item_id: str,
        *,
        error: str,
        backoff_seconds: float,
        max_attempts: int | None = None,
    ) -> None:
        result = await self._session.execute(
            select(SyncQueueRow).where(SyncQueueRow.id == item_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return
        row.attempt_count += 1
        row.last_error = error
        row.locked_at = None
        row.updated_at = datetime.now(UTC)
        if max_attempts is not None and row.attempt_count >= max_attempts:
            row.status = "FAILED"
        else:
            row.status = "PENDING"
            row.available_at = datetime.now(UTC) + timedelta(seconds=backoff_seconds)
        await self._session.flush()
