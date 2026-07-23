"""Tamper-evident audit log repository (hash chain)."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from intelipump_fdc.persistence.dto import AuditRecord
from intelipump_fdc.persistence.errors import AuditChainError
from intelipump_fdc.persistence.models import AUDIT_GENESIS_HASH, AuditLogRow


def canonicalize_audit_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _created_at_iso(value: datetime) -> str:
    """Normalize SQLite naive timestamps to UTC ISO for stable hashing."""
    value = (
        value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    )
    return value.isoformat()


def compute_record_hash(previous_hash: str, payload: dict[str, Any]) -> str:
    canonical = canonicalize_audit_payload(payload)
    material = f"{previous_hash}|{canonical}".encode()
    return hashlib.sha256(material).hexdigest()


def _to(row: AuditLogRow) -> AuditRecord:
    return AuditRecord(
        id=row.id,
        correlation_id=row.correlation_id,
        actor=row.actor,
        source=row.source,
        action=row.action,
        station_id=row.station_id,
        pump_id=row.pump_id,
        previous_state=row.previous_state,
        resulting_state=row.resulting_state,
        result=row.result,
        details=row.details,
        created_at=row.created_at,
        previous_hash=row.previous_hash,
        record_hash=row.record_hash,
    )


class AuditRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def _latest_hash(self) -> str:
        result = await self._session.execute(
            select(AuditLogRow).order_by(AuditLogRow.created_at.desc()).limit(1)
        )
        row = result.scalar_one_or_none()
        return row.record_hash if row else AUDIT_GENESIS_HASH

    async def append(
        self,
        *,
        actor: str,
        source: str,
        action: str,
        station_id: str,
        result: str,
        correlation_id: str | None = None,
        pump_id: str | None = None,
        previous_state: str | None = None,
        resulting_state: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> AuditRecord:
        previous_hash = await self._latest_hash()
        record_id = str(uuid4())
        created_at = datetime.now(UTC)
        payload = {
            "id": record_id,
            "correlation_id": correlation_id,
            "actor": actor,
            "source": source,
            "action": action,
            "station_id": station_id,
            "pump_id": pump_id,
            "previous_state": previous_state,
            "resulting_state": resulting_state,
            "result": result,
            "details": details,
            "created_at": _created_at_iso(created_at),
        }
        record_hash = compute_record_hash(previous_hash, payload)
        row = AuditLogRow(
            id=record_id,
            correlation_id=correlation_id,
            actor=actor,
            source=source,
            action=action,
            station_id=station_id,
            pump_id=pump_id,
            previous_state=previous_state,
            resulting_state=resulting_state,
            result=result,
            details=details,
            created_at=created_at,
            previous_hash=previous_hash,
            record_hash=record_hash,
        )
        self._session.add(row)
        await self._session.flush()
        return _to(row)

    async def list_all(self) -> tuple[AuditRecord, ...]:
        result = await self._session.execute(
            select(AuditLogRow).order_by(AuditLogRow.created_at.asc())
        )
        return tuple(_to(r) for r in result.scalars().all())

    async def list_filtered(
        self,
        *,
        station_id: str | None = None,
        correlation_id: str | None = None,
        pump_id: str | None = None,
        action: str | None = None,
        result_value: str | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        offset: int = 0,
        limit: int = 25,
    ) -> tuple[tuple[AuditRecord, ...], int]:
        from sqlalchemy import func

        filters = []
        if station_id is not None:
            filters.append(AuditLogRow.station_id == station_id)
        if correlation_id is not None:
            filters.append(AuditLogRow.correlation_id == correlation_id)
        if pump_id is not None:
            filters.append(AuditLogRow.pump_id == pump_id)
        if action is not None:
            filters.append(AuditLogRow.action == action)
        if result_value is not None:
            filters.append(AuditLogRow.result == result_value)
        if created_from is not None:
            filters.append(AuditLogRow.created_at >= created_from)
        if created_to is not None:
            filters.append(AuditLogRow.created_at <= created_to)
        count_q = select(func.count()).select_from(AuditLogRow)
        list_q = select(AuditLogRow)
        if filters:
            count_q = count_q.where(*filters)
            list_q = list_q.where(*filters)
        total = int((await self._session.execute(count_q)).scalar_one())
        rows = (
            (
                await self._session.execute(
                    list_q.order_by(AuditLogRow.created_at.desc())
                    .offset(offset)
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return tuple(_to(r) for r in rows), total

    async def verify_chain_report(self) -> dict[str, object]:
        """Return verification result without raising on first failure."""
        records = await self.list_all()
        expected_prev = AUDIT_GENESIS_HASH
        for idx, rec in enumerate(records):
            if rec.previous_hash != expected_prev:
                return {
                    "valid": False,
                    "records_checked": idx,
                    "first_invalid_record_id": rec.id,
                    "genesis": AUDIT_GENESIS_HASH,
                    "reason": "previous_hash_mismatch",
                }
            payload = {
                "id": rec.id,
                "correlation_id": rec.correlation_id,
                "actor": rec.actor,
                "source": rec.source,
                "action": rec.action,
                "station_id": rec.station_id,
                "pump_id": rec.pump_id,
                "previous_state": rec.previous_state,
                "resulting_state": rec.resulting_state,
                "result": rec.result,
                "details": rec.details,
                "created_at": _created_at_iso(rec.created_at),
            }
            expected = compute_record_hash(rec.previous_hash, payload)
            if expected != rec.record_hash:
                return {
                    "valid": False,
                    "records_checked": idx,
                    "first_invalid_record_id": rec.id,
                    "genesis": AUDIT_GENESIS_HASH,
                    "reason": "record_hash_mismatch",
                }
            expected_prev = rec.record_hash
        return {
            "valid": True,
            "records_checked": len(records),
            "first_invalid_record_id": None,
            "genesis": AUDIT_GENESIS_HASH,
            "reason": None,
        }

    async def verify_chain(self) -> None:
        records = await self.list_all()
        expected_prev = AUDIT_GENESIS_HASH
        for rec in records:
            if rec.previous_hash != expected_prev:
                raise AuditChainError(
                    f"broken previous_hash at {rec.id}: "
                    f"expected {expected_prev}, got {rec.previous_hash}"
                )
            payload = {
                "id": rec.id,
                "correlation_id": rec.correlation_id,
                "actor": rec.actor,
                "source": rec.source,
                "action": rec.action,
                "station_id": rec.station_id,
                "pump_id": rec.pump_id,
                "previous_state": rec.previous_state,
                "resulting_state": rec.resulting_state,
                "result": rec.result,
                "details": rec.details,
                "created_at": _created_at_iso(rec.created_at),
            }
            expected = compute_record_hash(rec.previous_hash, payload)
            if expected != rec.record_hash:
                raise AuditChainError(f"hash mismatch at {rec.id}")
            expected_prev = rec.record_hash
