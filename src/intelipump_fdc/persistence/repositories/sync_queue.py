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

    async def mark_awaiting_app_ack(self, item_id: str) -> None:
        """MQTT PUBACK received; waiting for cloud SALE_COMMITTED."""
        result = await self._session.execute(
            select(SyncQueueRow).where(SyncQueueRow.id == item_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return
        row.status = "AWAITING_APP_ACK"
        row.locked_at = None
        row.updated_at = datetime.now(UTC)
        await self._session.flush()

    async def ack_payload_conflicts(
        self,
        *,
        deduplication_key: str | None,
        entity_id: str | None,
        amount: Any = None,
        volume_liters: Any = None,
    ) -> str | None:
        """Return conflict detail if ACK money/volume disagrees with outbox payload.

        Missing ACK money fields are treated as compatible (legacy cloud ACKs).
        """
        if amount is None and volume_liters is None:
            return None
        clauses = []
        key = (deduplication_key or "").strip()
        tx = (entity_id or "").strip()
        if key:
            clauses.append(SyncQueueRow.deduplication_key == key)
        if tx:
            clauses.append(SyncQueueRow.entity_id == tx)
        if not clauses:
            return None
        from sqlalchemy import or_

        result = await self._session.execute(
            select(SyncQueueRow).where(
                or_(*clauses),
                SyncQueueRow.status.in_(("AWAITING_APP_ACK", "CLAIMED", "PENDING")),
                SyncQueueRow.event_type.in_(
                    ("TRANSACTION_COMPLETED", "TX_COMPLETED", "SALE_COMPLETED")
                ),
            )
        )
        rows = list(result.scalars().all())
        for row in rows:
            payload = row.payload if isinstance(row.payload, dict) else {}
            nested = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
            stored_amount = (
                payload.get("amount")
                if payload.get("amount") is not None
                else nested.get("amount")
            )
            stored_volume = (
                payload.get("volume")
                if payload.get("volume") is not None
                else payload.get("volume_liters")
                if payload.get("volume_liters") is not None
                else nested.get("volume")
                if nested.get("volume") is not None
                else nested.get("volume_liters")
            )
            if amount is not None and stored_amount is not None:
                try:
                    if abs(float(amount) - float(stored_amount)) > 1e-6:
                        return (
                            f"amount_mismatch ack={amount} stored={stored_amount} "
                            f"entity={row.entity_id}"
                        )
                except (TypeError, ValueError):
                    if str(amount) != str(stored_amount):
                        return (
                            f"amount_mismatch ack={amount} stored={stored_amount} "
                            f"entity={row.entity_id}"
                        )
            if volume_liters is not None and stored_volume is not None:
                try:
                    if abs(float(volume_liters) - float(stored_volume)) > 1e-6:
                        return (
                            f"volume_mismatch ack={volume_liters} stored={stored_volume} "
                            f"entity={row.entity_id}"
                        )
                except (TypeError, ValueError):
                    if str(volume_liters) != str(stored_volume):
                        return (
                            f"volume_mismatch ack={volume_liters} stored={stored_volume} "
                            f"entity={row.entity_id}"
                        )
        return None

    async def mark_delivered_by_dedupe_key(self, deduplication_key: str) -> int:
        """Application ACK: promote matching awaiting rows to DELIVERED."""
        key = (deduplication_key or "").strip()
        if not key:
            return 0
        result = await self._session.execute(
            select(SyncQueueRow).where(
                SyncQueueRow.deduplication_key == key,
                SyncQueueRow.status.in_(("AWAITING_APP_ACK", "CLAIMED", "PENDING")),
            )
        )
        rows = list(result.scalars().all())
        now = datetime.now(UTC)
        for row in rows:
            row.status = "DELIVERED"
            row.locked_at = None
            row.updated_at = now
        await self._session.flush()
        return len(rows)

    async def mark_delivered_by_entity_id(self, entity_id: str) -> int:
        tx = (entity_id or "").strip()
        if not tx:
            return 0
        result = await self._session.execute(
            select(SyncQueueRow).where(
                SyncQueueRow.entity_id == tx,
                SyncQueueRow.event_type.in_(
                    ("TRANSACTION_COMPLETED", "TX_COMPLETED", "SALE_COMPLETED")
                ),
                SyncQueueRow.status.in_(("AWAITING_APP_ACK", "CLAIMED", "PENDING")),
            )
        )
        rows = list(result.scalars().all())
        now = datetime.now(UTC)
        for row in rows:
            row.status = "DELIVERED"
            row.locked_at = None
            row.updated_at = now
        await self._session.flush()
        return len(rows)

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
            .where(
                SyncQueueRow.status.in_(
                    ("PENDING", "CLAIMED", "AWAITING_APP_ACK")
                )
            )
        )
        return int(result.scalar_one())

    async def awaiting_app_ack_count(self) -> int:
        result = await self._session.execute(
            select(func.count())
            .select_from(SyncQueueRow)
            .where(SyncQueueRow.status == "AWAITING_APP_ACK")
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
                SyncQueueRow.status.in_(
                    ("PENDING", "CLAIMED", "AWAITING_APP_ACK")
                )
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
