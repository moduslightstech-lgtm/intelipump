"""Pump state snapshot orchestration (outside pure state machine)."""

from __future__ import annotations

from datetime import datetime

from intelipump_fdc.persistence.dto import StateSnapshotRecord
from intelipump_fdc.persistence.unit_of_work import UnitOfWork
from intelipump_fdc.state_machine.models import PumpContext


class PumpStateService:
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def persist_context(
        self,
        *,
        pump_db_id: str,
        context: PumpContext,
        source_frame_ref: str | None = None,
        observed_at: datetime | None = None,
        enqueue_sync: bool = True,
    ) -> StateSnapshotRecord | None:
        snap = await self._uow.states.insert_if_meaningful(
            pump_id=pump_db_id,
            normalized_state=context.current_state.value,
            previous_state=(
                context.previous_state.value if context.previous_state else None
            ),
            selected_nozzle=context.selected_nozzle,
            active_transaction_id=context.active_transaction_id,
            communication_healthy=context.communication_healthy,
            raw_wayne_status=context.last_raw_wayne_status,
            source_frame_ref=source_frame_ref,
            state_version=context.state_version,
            observed_at=observed_at,
        )
        if snap is not None and enqueue_sync:
            await self._uow.sync_queue.enqueue_checked(
                entity_type="pump_state",
                entity_id=pump_db_id,
                event_type="STATE_CHANGED",
                payload={
                    "pump_id": pump_db_id,
                    "normalized_state": snap.normalized_state,
                    "state_version": snap.state_version,
                },
                deduplication_key=(
                    f"state:{pump_db_id}:{snap.state_version}:{snap.normalized_state}"
                ),
            )
        return snap
