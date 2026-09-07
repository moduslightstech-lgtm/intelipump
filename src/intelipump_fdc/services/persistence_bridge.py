"""Bridge controller events to durable persistence without blocking polls."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import structlog
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

logger = structlog.get_logger(__name__)

_OPEN_TX_STATUSES = frozenset({"ACTIVE", "SUSPENDED", "OPEN"})


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
        automatic_transaction_publishing: bool = False,
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
        self._auto_publish_sales = automatic_transaction_publishing
        self._tx_by_address: dict[int, str] = {}

    def attach(self) -> None:
        self._events.add_subscriber(self.on_event)

    def detach(self) -> None:
        self._events.remove_subscriber(self.on_event)

    async def _open_uuid(self, uow: Any, uuid: str | None) -> str | None:
        if not uuid:
            return None
        row = await uow.transactions.get_by_uuid(uuid)
        if row is not None and row.status in _OPEN_TX_STATUSES:
            return uuid
        return None

    async def _fueled_open_uuid(self, uow: Any, pump_db: str) -> str | None:
        """ACTIVE row on this pump that already has DC2 totals (not a 0/0 zombie)."""
        rows = await uow.transactions.list_unresolved(station_id=self._station_id)
        fueled = [
            tx
            for tx in rows
            if tx.pump_id == pump_db
            and (int(tx.raw_amount or 0) > 0 or int(tx.raw_volume or 0) > 0)
        ]
        if not fueled:
            return None
        fueled.sort(
            key=lambda tx: tx.updated_at or tx.started_at or tx.created_at,
            reverse=True,
        )
        return fueled[0].transaction_uuid

    async def _open_uuid_for_hangup(
        self,
        uow: Any,
        *,
        address: int,
        pump_db: str,
        vol_raw: int,
        amt_raw: int,
    ) -> str | None:
        """Complete the live fill only. Never mint a second sale on holster."""
        fueled = await self._fueled_open_uuid(uow, pump_db)
        if fueled:
            self._tx_by_address[address] = fueled
            return fueled
        mapped = await self._open_uuid(uow, self._tx_by_address.get(address))
        if mapped:
            self._tx_by_address[address] = mapped
            return mapped
        already = await uow.transactions.find_recent_completed_same_totals(
            station_id=self._station_id,
            pump_id=pump_db,
            raw_volume=vol_raw,
            raw_amount=amt_raw,
        )
        if already is not None:
            logger.info(
                "hangup_already_completed",
                address=address,
                transaction_uuid=already.transaction_uuid,
                raw_volume=vol_raw,
                raw_amount=amt_raw,
            )
            self._tx_by_address[address] = already.transaction_uuid
            return None
        return None

    async def _begin_sale(
        self,
        uow: Any,
        *,
        pump_db: str,
        tx_uuid: str,
        nozzle_id: int | None,
        raw_price: int | None = None,
        price_decimals: int | None = None,
        volume_decimals: int | None = None,
        amount_decimals: int | None = None,
    ) -> None:
        await TransactionService(uow, fill_book=self._fill_book).begin(
            BeginTransactionRequest(
                station_id=self._station_id,
                pump_db_id=pump_db,
                transaction_uuid=tx_uuid,
                nozzle_id=nozzle_id,
                raw_price=raw_price,
                price_decimals=price_decimals,
                volume_decimals=volume_decimals if volume_decimals is not None else 2,
                amount_decimals=amount_decimals if amount_decimals is not None else 2,
                simulated=self._simulated,
                environment=self._environment,
            )
        )

    async def _ensure_open_sale(
        self,
        uow: Any,
        *,
        address: int,
        pump_db: str,
        candidate: str | None,
        nozzle_id: int | None,
        raw_price: int | None = None,
        price_decimals: int | None = None,
        volume_decimals: int | None = None,
        amount_decimals: int | None = None,
        reason: str,
    ) -> str:
        """Return an ACTIVE sale UUID. Never write ticks onto a COMPLETED row.

        After hang-up the Wayne state machine keeps the old UUID. Reusing that
        row swallows the next fill (₦750 on the wire, nothing in SQLite).
        """
        mapped = await self._open_uuid(uow, self._tx_by_address.get(address))
        mapped_row = (
            await uow.transactions.get_by_uuid(mapped) if mapped else None
        )
        mapped_has_fuel = bool(
            mapped_row
            and (
                int(mapped_row.raw_amount or 0) > 0
                or int(mapped_row.raw_volume or 0) > 0
            )
        )
        if mapped and mapped_has_fuel:
            self._tx_by_address[address] = mapped
            return mapped
        fueled = await self._fueled_open_uuid(uow, pump_db)
        if fueled:
            self._tx_by_address[address] = fueled
            return fueled
        if mapped:
            self._tx_by_address[address] = mapped
            return mapped
        if candidate:
            row = await uow.transactions.get_by_uuid(candidate)
            if row is None:
                await self._begin_sale(
                    uow,
                    pump_db=pump_db,
                    tx_uuid=candidate,
                    nozzle_id=nozzle_id,
                    raw_price=raw_price,
                    price_decimals=price_decimals,
                    volume_decimals=volume_decimals,
                    amount_decimals=amount_decimals,
                )
                self._tx_by_address[address] = candidate
                return candidate
            if row.status in _OPEN_TX_STATUSES:
                self._tx_by_address[address] = candidate
                return candidate
            logger.info(
                "new_fill_after_completed_sale",
                previous_uuid=candidate,
                previous_status=row.status,
                address=address,
                reason=reason,
            )
        tx_uuid = str(uuid4())
        await self._begin_sale(
            uow,
            pump_db=pump_db,
            tx_uuid=tx_uuid,
            nozzle_id=nozzle_id,
            raw_price=raw_price,
            price_decimals=price_decimals,
            volume_decimals=volume_decimals,
            amount_decimals=amount_decimals,
        )
        self._tx_by_address[address] = tx_uuid
        return tx_uuid

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
                nozzle = detail_payload.get("selected_nozzle")
                nozzle_id = nozzle if isinstance(nozzle, int) else None
                previous = self._tx_by_address.get(address)
                tx_uuid = await self._ensure_open_sale(
                    uow,
                    address=address,
                    pump_db=pump_db,
                    candidate=active_tx_s,
                    nozzle_id=nozzle_id,
                    reason="filling_started",
                )
                if self._live is not None and tx_uuid != previous:
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
                may_publish = detail_payload.get("may_publish_sale")
                filled_vol = detail_payload.get("filled_volume_raw")
                filled_amt = detail_payload.get("filled_amount_raw")
                dispensed = detail_payload.get("dispensed_volume_raw")
                vol_raw = (
                    filled_vol
                    if isinstance(filled_vol, int)
                    else (dispensed if isinstance(dispensed, int) else 0)
                )
                amt_raw = filled_amt if isinstance(filled_amt, int) else 0
                sale_lifecycle = detail_payload.get("sale_lifecycle")
                suppress = (
                    may_publish is False
                    or sale_lifecycle == "ABORTED_NO_DELIVERY"
                    or event_name == "SALE_SUPPRESSED"
                    or (
                        may_publish is True
                        and vol_raw <= 0
                        and amt_raw <= 0
                    )
                )
                if suppress:
                    await uow.audit.append(
                        actor="controller",
                        source="state_machine",
                        action="SALE_SUPPRESSED_NO_DELIVERY",
                        station_id=self._station_id,
                        pump_id=pump_db,
                        previous_state=str(prev_state_s) if prev_state_s else None,
                        resulting_state=new_state_s,
                        result="ABORTED_NO_DELIVERY",
                        details={
                            "active_transaction_id": active_tx_s,
                            "sale_lifecycle": sale_lifecycle,
                            "filled_volume_raw": vol_raw,
                            "filled_amount_raw": amt_raw,
                            "may_publish_sale": may_publish,
                            "warnings": list(warn_tuple),
                        },
                    )
                    return
                if not awaiting and (vol_raw > 0 or amt_raw > 0):
                    complete_uuid = await self._open_uuid_for_hangup(
                        uow,
                        address=address,
                        pump_db=pump_db,
                        vol_raw=vol_raw,
                        amt_raw=amt_raw,
                    )
                    if complete_uuid is None:
                        logger.info(
                            "hangup_skipped_no_open_sale",
                            address=address,
                            pump_id=pump_db,
                            filled_volume_raw=vol_raw,
                            filled_amount_raw=amt_raw,
                            candidate=active_tx_s,
                        )
                        return
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
                            raw_volume=vol_raw,
                            raw_amount=amt_raw,
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
                    # Cloud/MQTT publish deferred unless automatic_transaction_publishing
                    # is enabled on the controller feature flags (default OFF).
                    if newly and self._live is not None and self._auto_publish_sales:
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
        pump_db = self._pump_id_by_address.get(address)
        if not pump_db:
            return
        detail_payload = payload.get("payload") or {}
        if not isinstance(detail_payload, dict):
            return
        raw_volume = detail_payload.get("raw_volume")
        raw_amount = detail_payload.get("raw_amount")
        if not isinstance(raw_volume, int) or not isinstance(raw_amount, int):
            return
        nozzle = detail_payload.get("selected_nozzle")
        async with unit_of_work(self._factory) as uow:
            tx_uuid = await self._ensure_open_sale(
                uow,
                address=address,
                pump_db=pump_db,
                candidate=self._tx_by_address.get(address),
                nozzle_id=nozzle if isinstance(nozzle, int) else None,
                raw_price=detail_payload.get("raw_price")
                if isinstance(detail_payload.get("raw_price"), int)
                else None,
                price_decimals=detail_payload.get("price_decimals")
                if isinstance(detail_payload.get("price_decimals"), int)
                else None,
                volume_decimals=detail_payload.get("volume_decimals")
                if isinstance(detail_payload.get("volume_decimals"), int)
                else None,
                amount_decimals=detail_payload.get("amount_decimals")
                if isinstance(detail_payload.get("amount_decimals"), int)
                else None,
                reason="dc2_tick",
            )
            # Stable event key from scaled values — duplicate DATA with same
            # totals is ignored; progressive fills still update.
            event_key = f"fill:{tx_uuid}:{raw_volume}:{raw_amount}"
            updated = await TransactionService(
                uow, fill_book=self._fill_book
            ).update_filling(
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
            if updated is None:
                return
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
