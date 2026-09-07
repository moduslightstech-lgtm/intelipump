"""Persistence-aware transaction lifecycle (no MQTT)."""

from __future__ import annotations

from intelipump_fdc.cloud.fill_throttle import FillPublishBook
from intelipump_fdc.persistence.dto import TransactionRecord
from intelipump_fdc.persistence.unit_of_work import UnitOfWork
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
    FillingUpdateRequest,
)


class TransactionService:
    def __init__(
        self, uow: UnitOfWork, *, fill_book: FillPublishBook | None = None
    ) -> None:
        self._uow = uow
        self._fill_book = fill_book

    async def begin(self, req: BeginTransactionRequest) -> TransactionRecord:
        existing = await self._uow.transactions.get_by_uuid(req.transaction_uuid)
        if existing is not None:
            return existing
        tx = await self._uow.transactions.create(
            transaction_uuid=req.transaction_uuid,
            station_id=req.station_id,
            pump_id=req.pump_db_id,
            nozzle_id=req.nozzle_id,
            status="ACTIVE",
            raw_price=req.raw_price,
            price_decimals=req.price_decimals,
            volume_decimals=req.volume_decimals,
            amount_decimals=req.amount_decimals,
            simulated=req.simulated,
            environment=req.environment,
            started_at=req.started_at,
        )
        await self._uow.transactions.add_event(
            transaction_id=tx.id,
            event_type="STARTED",
            event_key=f"started:{tx.transaction_uuid}",
            raw_payload={"status": "ACTIVE"},
            source_frame_ref=None,
            observed_at=req.started_at,
        )
        await self._uow.sync_queue.enqueue_checked(
            entity_type="transaction",
            entity_id=tx.transaction_uuid,
            event_type="TRANSACTION_STARTED",
            payload={
                "transaction_uuid": tx.transaction_uuid,
                "station_id": tx.station_id,
                "environment": tx.environment,
                "simulated": tx.simulated,
            },
            deduplication_key=f"tx-started:{tx.transaction_uuid}",
        )
        return tx

    async def update_filling(self, req: FillingUpdateRequest) -> TransactionRecord | None:
        tx = await self._uow.transactions.update_filling(
            req.transaction_uuid,
            raw_volume=req.raw_volume,
            raw_amount=req.raw_amount,
            raw_price=req.raw_price,
        )
        if tx is None:
            return None
        if req.event_key:
            await self._uow.transactions.add_event(
                transaction_id=tx.id,
                event_type="FILLING_UPDATE",
                event_key=req.event_key,
                raw_payload={
                    "raw_volume": req.raw_volume,
                    "raw_amount": req.raw_amount,
                },
                source_frame_ref=req.source_frame_ref,
                observed_at=None,
            )
        if self._fill_book is not None and self._fill_book.decide(
            tx.transaction_uuid,
            raw_volume=req.raw_volume,
            raw_amount=req.raw_amount,
            is_final=False,
        ):
            pump = await self._uow.pumps.get_by_id(tx.pump_id)
            await self._uow.sync_queue.enqueue_checked(
                entity_type="transaction",
                entity_id=tx.transaction_uuid,
                event_type="FILLING_UPDATED",
                payload={
                    "transaction_uuid": tx.transaction_uuid,
                    "station_id": tx.station_id,
                    "pump_id": pump.logical_pump_id if pump else tx.pump_id,
                    "raw_volume": req.raw_volume,
                    "volume_decimals": tx.volume_decimals
                    if tx.volume_decimals is not None
                    else 2,
                    "raw_amount": req.raw_amount,
                    "amount_decimals": tx.amount_decimals
                    if tx.amount_decimals is not None
                    else 2,
                    "environment": tx.environment,
                    "simulated": tx.simulated,
                },
                deduplication_key=(
                    f"fill:{tx.transaction_uuid}:{req.raw_volume}:{req.raw_amount}"
                ),
            )
        return tx

    async def complete(
        self, req: CompleteTransactionRequest
    ) -> tuple[TransactionRecord, bool]:
        tx, newly = await self._uow.transactions.complete_once(
            req.transaction_uuid,
            source_completion_key=req.source_completion_key,
            raw_volume=req.raw_volume,
            raw_amount=req.raw_amount,
            completed_at=req.completed_at,
        )
        if newly:
            await self._uow.transactions.add_event(
                transaction_id=tx.id,
                event_type="COMPLETED",
                event_key=f"completed:{req.source_completion_key}",
                raw_payload={
                    "raw_volume": tx.raw_volume,
                    "raw_amount": tx.raw_amount,
                    "source_completion_key": req.source_completion_key,
                    "completion_inferred": req.completion_inferred,
                    "completion_warnings": list(req.completion_warnings),
                },
                source_frame_ref=req.source_frame_ref,
                observed_at=req.completed_at,
            )
            pump = await self._uow.pumps.get_by_id(tx.pump_id)
            if self._fill_book is not None:
                self._fill_book.decide(
                    tx.transaction_uuid,
                    raw_volume=tx.raw_volume,
                    raw_amount=tx.raw_amount,
                    is_final=True,
                )
            if req.publish_completion:
                await self._uow.sync_queue.enqueue_checked(
                    entity_type="transaction",
                    entity_id=tx.transaction_uuid,
                    event_type="TRANSACTION_COMPLETED",
                    payload={
                        "transaction_uuid": tx.transaction_uuid,
                        "station_id": tx.station_id,
                        "pump_id": pump.logical_pump_id if pump else tx.pump_id,
                        "pump_db_id": tx.pump_id,
                        "nozzle_id": tx.nozzle_id,
                        "product": None,
                        "raw_unit_price": tx.raw_price,
                        "price_decimals": tx.price_decimals,
                        "raw_volume": tx.raw_volume,
                        "volume_decimals": tx.volume_decimals
                        if tx.volume_decimals is not None
                        else 2,
                        "raw_amount": tx.raw_amount,
                        "amount_decimals": tx.amount_decimals
                        if tx.amount_decimals is not None
                        else 2,
                        "started_at": (
                            tx.started_at.isoformat() if tx.started_at else None
                        ),
                        "completed_at": (
                            tx.completed_at.isoformat() if tx.completed_at else None
                        ),
                        "final_status": tx.status,
                        "source_completion_key": req.source_completion_key,
                        "completion_inferred": req.completion_inferred,
                        "environment": tx.environment,
                        "simulated": tx.simulated,
                    },
                    deduplication_key=f"tx-completed:{req.source_completion_key}",
                )
        return tx, newly
