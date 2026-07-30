"""Bridge controller events to durable persistence without blocking polls."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.fill_throttle import FillPublishBook
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.events.broker import EventBroker
from intelipump_fdc.events.models import LiveEventType
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_worker import PersistenceWorker, PersistPriority
from intelipump_fdc.services.pump_state_service import PumpStateService
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
    FillingUpdateRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService
from intelipump_fdc.state_machine.models import PumpContext


class PersistenceBridge:
    """Subscribe to controller events and enqueue durable writes."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        station_id: str,
        environment: str,
        simulated: bool,
        worker: PersistenceWorker,
        pump_id_by_address: dict[int, str],
        logical_by_address: dict[int, str],
        events: EventBus,
        live_broker: EventBroker | None = None,
        fill_book: FillPublishBook | None = None,
    ) -> None:
        self._factory = session_factory
        self._station_id = station_id
        self._environment = environment
        self._simulated = simulated
        self._worker = worker
        self._pump_id_by_address = pump_id_by_address
        self._logical_by_address = logical_by_address
        self._events = events
        self._live = live_broker
        self._fill_book = fill_book
        self._tx_by_address: dict[int, str] = {}

    def attach(self) -> None:
        self._events.add_subscriber(self.on_event)

    def detach(self) -> None:
        self._events.remove_subscriber(self.on_event)

    def on_event(self, event: ControllerEvent) -> None:
        self._publish_live(event)
        if event.type is ControllerEventType.STATE_CHANGED:
            self._worker.submit(
                kind="state_changed",
                payload={
                    "address": event.address,
                    "detail": event.detail,
                    "payload": dict(event.payload),
                },
                handler=self._handle_state_changed,
                priority=PersistPriority.NORMAL,
            )
        elif event.type is ControllerEventType.APPLICATION_TRANSACTION_DECODED:
            detail = event.detail or ""
            is_dc2 = "DC2" in detail.upper() or detail == "DC2_FILLED_VOLUME_AMOUNT"
            priority = (
                PersistPriority.CRITICAL
                if "COMPLETE" in detail.upper()
                else PersistPriority.NORMAL
            )
            self._worker.submit(
                kind="app_decoded",
                payload={
                    "address": event.address,
                    "detail": detail,
                    "payload": dict(event.payload),
                    "is_dc2": is_dc2,
                },
                handler=self._handle_app_decoded,
                priority=priority,
            )
        elif event.type in {
            ControllerEventType.PUMP_CONNECTED,
            ControllerEventType.PUMP_DISCONNECTED,
        }:
            self._worker.submit(
                kind="comm",
                payload={"address": event.address, "type": event.type.value},
                handler=self._handle_comm,
                priority=PersistPriority.NORMAL,
            )
        elif event.type is ControllerEventType.FRAME_REJECTED:
            self._worker.submit(
                kind="protocol_error",
                payload={
                    "address": event.address,
                    "detail": event.detail,
                    "payload": dict(event.payload),
                },
                handler=self._handle_protocol_error,
                priority=PersistPriority.NORMAL,
            )

    def _publish_live(self, event: ControllerEvent) -> None:
        if self._live is None:
            return
        logical = None
        if isinstance(event.address, int):
            logical = self._logical_by_address.get(event.address, f"pump-{event.address}")
        mapping: dict[ControllerEventType, LiveEventType] = {
            ControllerEventType.PUMP_CONNECTED: LiveEventType.PUMP_CONNECTED,
            ControllerEventType.PUMP_DISCONNECTED: LiveEventType.PUMP_DISCONNECTED,
            ControllerEventType.STATE_CHANGED: LiveEventType.PUMP_STATE_CHANGED,
            ControllerEventType.FRAME_REJECTED: LiveEventType.PROTOCOL_ERROR,
        }
        live_type = mapping.get(event.type)
        detail_payload = event.payload or {}
        if event.type is ControllerEventType.STATE_CHANGED:
            new_state = str(detail_payload.get("normalized_state") or "")
            if new_state == "NOZZLE_UP":
                live_type = LiveEventType.NOZZLE_LIFTED
            elif new_state == "AUTHORIZED":
                live_type = LiveEventType.AUTHORIZATION_CONFIRMED
            elif new_state == "FILLING":
                live_type = LiveEventType.FILLING_STARTED
            elif new_state in {"FILLING_COMPLETE", "LIMIT_REACHED"}:
                live_type = LiveEventType.FILLING_COMPLETED
        if live_type is None:
            return
        self._live.publish_typed(
            live_type,
            station_id=self._station_id,
            environment=self._environment,
            simulated=self._simulated,
            pump_id=logical,
            transaction_id=self._tx_by_address.get(event.address)
            if isinstance(event.address, int)
            else None,
            severity="WARNING" if live_type is LiveEventType.PROTOCOL_ERROR else None,
            payload={"detail": event.detail, **dict(detail_payload)},
            state_version=detail_payload.get("state_version")
            if isinstance(detail_payload.get("state_version"), int)
            else None,
        )

    def enqueue_rejected_command(
        self,
        *,
        address: int,
        command: PumpCommand,
        current_state: str,
        blocking_reasons: tuple[str, ...],
        simulator_only: bool,
        correlation_id: str | None = None,
    ) -> None:
        self._worker.submit(
            kind="rejected_command",
            payload={
                "correlation_id": correlation_id or str(uuid4()),
                "address": address,
                "command": command.value,
                "current_state": current_state,
                "blocking_reasons": list(blocking_reasons),
                "simulator_only": simulator_only,
            },
            handler=self._handle_rejected_command,
            priority=PersistPriority.CRITICAL,
        )

    async def _handle_rejected_command(self, payload: dict[str, Any]) -> None:
        async with unit_of_work(self._factory) as uow:
            cid = str(payload["correlation_id"])
            address = int(payload["address"])
            pump_db = self._pump_id_by_address.get(address)
            await uow.commands.create(
                correlation_id=cid,
                station_id=self._station_id,
                pump_id=pump_db,
                command_type=str(payload["command"]),
                status="REJECTED",
                idempotency_class="NON_IDEMPOTENT",
                simulator_only=bool(payload["simulator_only"]),
                completed_at=datetime.now(UTC),
                request_payload={"address": address},
                result_payload={
                    "blocking_reasons": list(payload["blocking_reasons"])
                },
                blocking_reasons=list(payload["blocking_reasons"]),
            )
            await uow.audit.append(
                actor="controller",
                source="safety",
                action=f"COMMAND_REJECTED:{payload['command']}",
                station_id=self._station_id,
                pump_id=pump_db,
                previous_state=str(payload["current_state"]),
                resulting_state=str(payload["current_state"]),
                result="REJECTED",
                correlation_id=cid,
                details={"blocking_reasons": list(payload["blocking_reasons"])},
            )

    async def _handle_state_changed(self, payload: dict[str, Any]) -> None:
        address = payload.get("address")
        if not isinstance(address, int):
            return
        pump_db = self._pump_id_by_address.get(address)
        logical = self._logical_by_address.get(address, f"pump-{address}")
        if not pump_db:
            return
        detail_payload = payload.get("payload") or {}
        if not isinstance(detail_payload, dict):
            detail_payload = {}

        new_state_s = str(
            detail_payload.get("normalized_state")
            or str(payload.get("detail") or "").split("->")[-1]
        )
        prev_state_s = detail_payload.get("previous_state")
        try:
            new_state = PumpState(new_state_s)
        except ValueError:
            return
        prev_state: PumpState | None = None
        if isinstance(prev_state_s, str):
            try:
                prev_state = PumpState(prev_state_s)
            except ValueError:
                prev_state = None

        state_version = int(detail_payload.get("state_version") or 0)
        active_tx = detail_payload.get("active_transaction_id")
        active_tx_s = str(active_tx) if active_tx else None
        if active_tx_s:
            # Keep in-memory mapping across restart/reconcile without new begin.
            self._tx_by_address[address] = active_tx_s
        completion_key = detail_payload.get("completion_evidence_key")
        completion_key_s = str(completion_key) if completion_key else None

        async with unit_of_work(self._factory) as uow:
            latest = await uow.states.latest(pump_db)
            if state_version <= 0:
                state_version = (latest.state_version + 1) if latest else 1
            ctx = PumpContext(
                pump_id=logical,
                dart_address=address,
                current_state=new_state,
                previous_state=prev_state,
                selected_nozzle=detail_payload.get("selected_nozzle"),
                active_transaction_id=active_tx_s,
                communication_healthy=bool(
                    detail_payload.get("communication_healthy", True)
                ),
                last_raw_wayne_status=detail_payload.get("raw_wayne_status"),
                state_version=state_version,
                last_source_frame_hex=detail_payload.get("source_frame_ref"),
            )
            await PumpStateService(uow).persist_context(
                pump_db_id=pump_db,
                context=ctx,
                source_frame_ref=detail_payload.get("source_frame_ref"),
                observed_at=datetime.now(UTC),
            )

            if new_state is PumpState.FILLING and prev_state is not PumpState.FILLING:
                tx_uuid = active_tx_s or self._tx_by_address.get(address) or str(uuid4())
                self._tx_by_address[address] = tx_uuid
                # Restore mapping for restart without creating a duplicate when
                # the active transaction id was already persisted.
                existing = await uow.transactions.get_by_uuid(tx_uuid)
                if existing is None:
                    nozzle = detail_payload.get("selected_nozzle")
                    nozzle_id = nozzle if isinstance(nozzle, int) else None
                    await TransactionService(uow, fill_book=self._fill_book).begin(
                        BeginTransactionRequest(
                            station_id=self._station_id,
                            pump_db_id=pump_db,
                            transaction_uuid=tx_uuid,
                            nozzle_id=nozzle_id,
                            raw_price=None,
                            price_decimals=None,
                            volume_decimals=None,
                            amount_decimals=None,
                            simulated=self._simulated,
                            environment=self._environment,
                        )
                    )
                    if self._live is not None:
                        self._live.publish_typed(
                            LiveEventType.TRANSACTION_CREATED,
                            station_id=self._station_id,
                            environment=self._environment,
                            simulated=self._simulated,
                            pump_id=logical,
                            transaction_id=tx_uuid,
                        )

            event_name = str(detail_payload.get("event") or "")
            awaiting = bool(detail_payload.get("awaiting_filling_complete"))
            completion_inferred = bool(detail_payload.get("completion_inferred"))
            audit_only = bool(detail_payload.get("audit_only"))
            warn_list = detail_payload.get("warnings")
            warn_tuple = (
                tuple(str(w) for w in warn_list)
                if isinstance(warn_list, list)
                else ()
            )

            if audit_only or event_name == "RECONCILIATION_WARNING":
                await uow.audit.append(
                    actor="controller",
                    source="reconciliation",
                    action="RECONCILIATION_WARNING",
                    station_id=self._station_id,
                    pump_id=pump_db,
                    previous_state=str(prev_state_s) if prev_state_s else None,
                    resulting_state=new_state_s,
                    result="WARNING",
                    details={
                        "warnings": list(warn_tuple),
                        "inferences": detail_payload.get("inferences"),
                        "active_transaction_id": active_tx_s,
                        "awaiting_filling_complete": awaiting,
                        "completion_inferred": completion_inferred,
                    },
                )
                return

            # Hang-up enters FILLING_COMPLETE while awaiting DC1 — do not
            # finalize the sale yet; keep accepting final DC2 updates.
            if (
                new_state in {PumpState.FILLING_COMPLETE, PumpState.LIMIT_REACHED}
                and awaiting
                and event_name == PumpEvent.NOZZLE_RETURNED.value
            ):
                await uow.audit.append(
                    actor="controller",
                    source="state_machine",
                    action="AWAITING_FILLING_COMPLETE",
                    station_id=self._station_id,
                    pump_id=pump_db,
                    previous_state=str(prev_state_s) if prev_state_s else None,
                    resulting_state=new_state_s,
                    result="PENDING",
                    details={
                        "active_transaction_id": active_tx_s,
                        "completion_evidence_key": completion_key_s,
                        "warnings": list(warn_tuple),
                    },
                )
                return

            if new_state in {PumpState.FILLING_COMPLETE, PumpState.LIMIT_REACHED}:
                complete_uuid = active_tx_s or self._tx_by_address.get(address)
                if complete_uuid and not awaiting:
                    key = (
                        completion_key_s
                        or f"complete:{complete_uuid}:{new_state.value}"
                    )
                    _tx, newly = await TransactionService(
                        uow, fill_book=self._fill_book
                    ).complete(
                        CompleteTransactionRequest(
                            transaction_uuid=complete_uuid,
                            source_completion_key=key,
                            raw_volume=0,
                            raw_amount=0,
                            source_frame_ref=detail_payload.get("source_frame_ref"),
                            completion_inferred=completion_inferred,
                            completion_warnings=warn_tuple,
                        )
                    )
                    # Keep mapping for duplicate DATA handling until new sale.
                    self._tx_by_address[address] = complete_uuid
                    if newly:
                        await uow.audit.append(
                            actor="controller",
                            source="state_machine",
                            action=(
                                "TRANSACTION_COMPLETED_INFERRED"
                                if completion_inferred
                                else "TRANSACTION_COMPLETED"
                            ),
                            station_id=self._station_id,
                            pump_id=pump_db,
                            previous_state=str(prev_state_s) if prev_state_s else None,
                            resulting_state=new_state_s,
                            result="OK",
                            details={
                                "transaction_uuid": complete_uuid,
                                "source_completion_key": key,
                                "completion_inferred": completion_inferred,
                                "warnings": list(warn_tuple),
                            },
                        )
                    if newly and self._live is not None:
                        self._live.publish_typed(
                            LiveEventType.TRANSACTION_COMPLETED,
                            station_id=self._station_id,
                            environment=self._environment,
                            simulated=self._simulated,
                            pump_id=logical,
                            transaction_id=complete_uuid,
                            payload={
                                "source_completion_key": key,
                                "completion_inferred": completion_inferred,
                            },
                        )

    async def _handle_app_decoded(self, payload: dict[str, Any]) -> None:
        address = payload.get("address")
        if not isinstance(address, int):
            return
        if not payload.get("is_dc2"):
            return
        tx_uuid = self._tx_by_address.get(address)
        if not tx_uuid:
            return
        detail_payload = payload.get("payload") or {}
        if not isinstance(detail_payload, dict):
            return
        raw_volume = detail_payload.get("raw_volume")
        raw_amount = detail_payload.get("raw_amount")
        if not isinstance(raw_volume, int) or not isinstance(raw_amount, int):
            return
        # Stable event key from scaled values — duplicate DATA with same
        # totals is ignored; progressive fills still update.
        event_key = f"fill:{tx_uuid}:{raw_volume}:{raw_amount}"
        async with unit_of_work(self._factory) as uow:
            await TransactionService(uow, fill_book=self._fill_book).update_filling(
                FillingUpdateRequest(
                    transaction_uuid=tx_uuid,
                    raw_volume=raw_volume,
                    raw_amount=raw_amount,
                    raw_price=detail_payload.get("raw_price")
                    if isinstance(detail_payload.get("raw_price"), int)
                    else None,
                    event_key=event_key,
                    source_frame_ref=detail_payload.get("source_frame_ref"),
                )
            )
            if self._live is not None:
                self._live.publish_typed(
                    LiveEventType.FILLING_UPDATED,
                    station_id=self._station_id,
                    environment=self._environment,
                    simulated=self._simulated,
                    pump_id=self._logical_by_address.get(address),
                    transaction_id=tx_uuid,
                    payload={"raw_volume": raw_volume, "raw_amount": raw_amount},
                )

    async def _handle_comm(self, payload: dict[str, Any]) -> None:
        address = payload.get("address")
        pump_db = (
            self._pump_id_by_address.get(address) if isinstance(address, int) else None
        )
        async with unit_of_work(self._factory) as uow:
            await uow.audit.append(
                actor="controller",
                source="transport",
                action=str(payload.get("type")),
                station_id=self._station_id,
                pump_id=pump_db,
                result="OK",
                details={"address": address, "environment": self._environment},
            )
            if isinstance(address, int) and pump_db:
                healthy = payload.get("type") == ControllerEventType.PUMP_CONNECTED.value
                logical = self._logical_by_address.get(address, f"pump-{address}")
                latest = await uow.states.latest(pump_db)
                version = (latest.state_version + 1) if latest else 1
                state = (
                    PumpState(latest.normalized_state)
                    if latest
                    else PumpState.DISCONNECTED
                )
                try:
                    if latest:
                        state = PumpState(latest.normalized_state)
                except ValueError:
                    state = PumpState.DISCOVERING
                await PumpStateService(uow).persist_context(
                    pump_db_id=pump_db,
                    context=PumpContext(
                        pump_id=logical,
                        dart_address=address,
                        current_state=state,
                        communication_healthy=healthy,
                        state_version=version,
                    ),
                    observed_at=datetime.now(UTC),
                )

    async def _handle_protocol_error(self, payload: dict[str, Any]) -> None:
        address = payload.get("address")
        pump_db = (
            self._pump_id_by_address.get(address) if isinstance(address, int) else None
        )
        async with unit_of_work(self._factory) as uow:
            alarm = await uow.alarms.upsert_active(
                station_id=self._station_id,
                pump_id=pump_db,
                severity="WARNING",
                alarm_type="PROTOCOL_ERROR",
                message=str(payload.get("detail") or "frame rejected"),
                source_key=f"proto:{address}:{payload.get('detail')}",
            )
            await uow.sync_queue.enqueue_checked(
                entity_type="alarm",
                entity_id=alarm.id,
                event_type="ALARM_ACTIVE",
                payload={
                    "alarm_id": alarm.id,
                    "station_id": self._station_id,
                    "environment": self._environment,
                    "simulated": self._simulated,
                    "alarm_type": alarm.alarm_type,
                },
                deduplication_key=f"alarm:{alarm.source_key}:active",
            )
