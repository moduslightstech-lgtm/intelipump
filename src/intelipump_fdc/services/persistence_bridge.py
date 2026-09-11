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
from intelipump_fdc.domain.sale_fingerprint import sale_fingerprint, stable_completion_key
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
        mqtt_pump_by_address: dict[int, str] | None = None,
        mqtt_nozzle_by_address: dict[int, str] | None = None,
        mqtt_source_by_address: dict[int, str] | None = None,
    ) -> None:
        self._factory = session_factory
        self._station_id = station_id
        self._environment = environment
        self._simulated = simulated
        self._worker = worker
        self._pump_id_by_address = pump_id_by_address
        self._logical_by_address = logical_by_address
        self._mqtt_pump_by_address = mqtt_pump_by_address or dict(logical_by_address)
        self._mqtt_nozzle_by_address = mqtt_nozzle_by_address or {}
        self._mqtt_source_by_address = mqtt_source_by_address or {
            a: f"pump-{a}" for a in logical_by_address
        }
        self._events = events
        self._live = live_broker
        self._fill_book = fill_book
        self._auto_publish_sales = automatic_transaction_publishing
        self._tx_by_address: dict[int, str] = {}

    def _channel_identity(self, address: int) -> tuple[str, str, str]:
        """Immutable cloud identity for a DART address (pump, nozzle, source)."""
        pump = self._mqtt_pump_by_address.get(
            address, self._logical_by_address.get(address, f"pump-{address}")
        )
        nozzle = self._mqtt_nozzle_by_address.get(address) or "nozzle-1"
        source = self._mqtt_source_by_address.get(address, f"pump-{address}")
        return pump, nozzle, source

    def _wayne_nozzle_index(self, address: int, selected: int | None) -> int | None:
        if isinstance(selected, int):
            return selected
        _, nozzle, _ = self._channel_identity(address)
        if nozzle.startswith("nozzle-"):
            try:
                return int(nozzle.rsplit("-", 1)[-1])
            except ValueError:
                return None
        return None

    def attach(self) -> None:
        self._events.add_subscriber(self.on_event)

    def detach(self) -> None:
        self._events.remove_subscriber(self.on_event)

    def _address_for_pump_db(self, pump_db: str) -> int | None:
        for addr, pid in self._pump_id_by_address.items():
            if pid == pump_db:
                return addr
        return None

    def _source_for_pump_db(self, pump_db: str) -> str | None:
        addr = self._address_for_pump_db(pump_db)
        if addr is None:
            return None
        return self._mqtt_source_by_address.get(
            addr, self._logical_by_address.get(addr, f"pump-{addr}")
        )

    async def _mark_controller_filling(
        self,
        uow: Any,
        *,
        address: int,
        pump_db: str,
        nozzle_id: int | None,
        transaction_id: str | None,
        previous_state: str | None,
    ) -> None:
        """Align SQLite controller state with a live fill so cloud-sync publishes.

        Console DC1 AUTHORIZED→FILLING is ObservedStatus; the SM/states table can
        remain DISCOVERING. Without FILLING here, LiveFillStream treats the sale
        as retained/idle and never emits MQTT progress.
        """
        can_pump, can_nozzle, _ = self._channel_identity(address)
        latest = await uow.states.latest(pump_db)
        version = (latest.state_version + 1) if latest else 1
        prev: PumpState | None = None
        if previous_state:
            try:
                prev = PumpState(previous_state)
            except ValueError:
                prev = None
        await PumpStateService(uow).persist_context(
            pump_db_id=pump_db,
            context=PumpContext(
                pump_id=can_pump,
                dart_address=address,
                current_state=PumpState.FILLING,
                previous_state=prev,
                selected_nozzle=nozzle_id,
                active_transaction_id=transaction_id,
                communication_healthy=True,
                state_version=version,
            ),
            observed_at=datetime.now(UTC),
            enqueue_sync=False,
        )
        logger.info(
            "controller_state_aligned_for_live_fill",
            stationId=self._station_id,
            sourceAddress=address,
            pumpId=can_pump,
            nozzleId=can_nozzle,
            previousState=previous_state,
            newState=PumpState.FILLING.value,
            transactionId=transaction_id,
        )

    async def _open_uuid(self, uow: Any, uuid: str | None) -> str | None:
        if not uuid:
            return None
        row = await uow.transactions.get_by_uuid(uuid)
        if row is not None and row.status in _OPEN_TX_STATUSES:
            return uuid
        return None

    async def _begin_sale(
        self,
        uow: Any,
        *,
        pump_db: str,
        tx_uuid: str,
        nozzle_id: int | None,
        address: int | None = None,
        raw_price: int | None = None,
        price_decimals: int | None = None,
        volume_decimals: int | None = None,
        amount_decimals: int | None = None,
    ) -> None:
        addr = address if address is not None else self._address_for_pump_db(pump_db)
        can_pump, can_nozzle, source = (
            self._channel_identity(addr) if addr is not None else (None, None, None)
        )
        await TransactionService(uow, fill_book=self._fill_book).begin(
            BeginTransactionRequest(
                station_id=self._station_id,
                pump_db_id=pump_db,
                transaction_uuid=tx_uuid,
                nozzle_id=nozzle_id,
                canonical_pump_id=can_pump,
                canonical_nozzle_id=can_nozzle,
                source_identifier=source or self._source_for_pump_db(pump_db),
                raw_price=raw_price,
                price_decimals=price_decimals,
                volume_decimals=volume_decimals,
                amount_decimals=amount_decimals,
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
                    address=address,
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
            address=address,
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
        nozzle_id = None
        source_id = None
        if isinstance(event.address, int):
            logical, nozzle_id, source_id = self._channel_identity(event.address)
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
            payload={
                "detail": event.detail,
                **dict(detail_payload),
                "nozzleId": nozzle_id,
                "sourceIdentifier": source_id,
            },
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

            # Entering FILLING always opens a sale. Remaining in FILLING without an
            # ACTIVE row (orphaned after sidecar settle / missed STATE_CHANGED) heals.
            if new_state is PumpState.FILLING:
                nozzle = detail_payload.get("selected_nozzle")
                nozzle_id = self._wayne_nozzle_index(
                    address, nozzle if isinstance(nozzle, int) else None
                )
                can_pump, can_nozzle, _ = self._channel_identity(address)
                previous = self._tx_by_address.get(address)
                open_before = await self._open_uuid(uow, previous or active_tx_s)
                reason = (
                    "filling_started"
                    if prev_state is not PumpState.FILLING
                    else "heal_orphaned_filling"
                )
                if open_before is None or prev_state is not PumpState.FILLING:
                    tx_uuid = await self._ensure_open_sale(
                        uow,
                        address=address,
                        pump_db=pump_db,
                        candidate=active_tx_s,
                        nozzle_id=nozzle_id,
                        reason=reason,
                    )
                    logger.info(
                        "live_sale_session_opened",
                        stationId=self._station_id,
                        sourceAddress=address,
                        pumpId=can_pump,
                        nozzleId=can_nozzle,
                        wayneNozzleIndex=nozzle_id,
                        transactionId=tx_uuid,
                        reason=reason,
                        previousState=str(prev_state.value) if prev_state else None,
                        newState=new_state.value,
                    )
                    if self._live is not None and tx_uuid != previous:
                        self._live.publish_typed(
                            LiveEventType.TRANSACTION_CREATED,
                            station_id=self._station_id,
                            environment=self._environment,
                            simulated=self._simulated,
                            pump_id=can_pump,
                            transaction_id=tx_uuid,
                            payload={"nozzleId": can_nozzle},
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

            if event_name == "STARTUP_BASELINE_OBSERVED" or bool(
                detail_payload.get("startup_baseline")
            ):
                await self._record_startup_baseline(
                    uow,
                    address=address,
                    pump_db=pump_db,
                    detail_payload=detail_payload,
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
                if not awaiting:
                    nozzle = detail_payload.get("selected_nozzle")
                    nozzle_id = nozzle if isinstance(nozzle, int) else None
                    price_raw = detail_payload.get("filling_price_raw")
                    fp = sale_fingerprint(
                        station_id=self._station_id,
                        dart_address=address,
                        nozzle_id=nozzle_id,
                        raw_volume=int(vol_raw),
                        raw_amount=int(amt_raw),
                        raw_price=price_raw if isinstance(price_raw, int) else None,
                    )
                    baseline = await uow.nozzle_baselines.get(
                        station_id=self._station_id,
                        dart_address=address,
                        nozzle_id=int(nozzle_id if nozzle_id is not None else 0),
                    )
                    # Only suppress when there is no open/in-progress sale UUID.
                    # A real IDLE→FILLING→COMPLETE lifecycle always has an
                    # active candidate or mapped open row.
                    open_mapped = await self._open_uuid(
                        uow, self._tx_by_address.get(address) or active_tx_s
                    )
                    if (
                        open_mapped is None
                        and uow.nozzle_baselines.is_already_observed(baseline, fp)
                    ):
                        logger.info(
                            "duplicate_transaction_ignored",
                            event_name="duplicate_transaction_ignored",
                            stationId=self._station_id,
                            pumpId=logical,
                            nozzleId=nozzle_id,
                            fingerprint=fp,
                            source="startup_baseline",
                        )
                        await uow.audit.append(
                            actor="controller",
                            source="state_machine",
                            action="STARTUP_BASELINE_SUPPRESSED",
                            station_id=self._station_id,
                            pump_id=pump_db,
                            previous_state=str(prev_state_s) if prev_state_s else None,
                            resulting_state=new_state_s,
                            result="IGNORED",
                            details={
                                "fingerprint": fp,
                                "raw_volume": vol_raw,
                                "raw_amount": amt_raw,
                                "source": "startup_baseline",
                            },
                        )
                        return
                    complete_uuid = await self._ensure_open_sale(
                        uow,
                        address=address,
                        pump_db=pump_db,
                        candidate=active_tx_s,
                        nozzle_id=nozzle_id,
                        reason="sale_complete",
                    )
                    key = (
                        completion_key_s
                        or stable_completion_key(
                            transaction_uuid=complete_uuid, fingerprint=fp
                        )
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
                    await uow.nozzle_baselines.upsert_baseline(
                        station_id=self._station_id,
                        pump_id=pump_db,
                        dart_address=address,
                        nozzle_id=int(nozzle_id if nozzle_id is not None else 0),
                        fingerprint=fp,
                        raw_volume=int(vol_raw),
                        raw_amount=int(amt_raw),
                        transaction_uuid=complete_uuid,
                        mark_published=newly,
                    )
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
                                "fingerprint": fp,
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
                                "deduplicationKey": (
                                    f"tx-completed:{self._station_id}:{key}"
                                ),
                                "completion_inferred": completion_inferred,
                                "fingerprint": fp,
                            },
                        )

    async def _record_startup_baseline(
        self,
        uow: Any,
        *,
        address: int,
        pump_db: str,
        detail_payload: dict[str, Any],
    ) -> None:
        nozzle = detail_payload.get("selected_nozzle")
        nozzle_id = nozzle if isinstance(nozzle, int) else 0
        vol = detail_payload.get("filled_volume_raw")
        amt = detail_payload.get("filled_amount_raw")
        if not isinstance(vol, int):
            vol = detail_payload.get("dispensed_volume_raw")
        if not isinstance(vol, int) or not isinstance(amt, int):
            logger.info(
                "startup_baseline_skipped_missing_totals",
                stationId=self._station_id,
                pumpId=pump_db,
                address=address,
            )
            return
        if vol <= 0 and amt <= 0:
            return
        price_raw = detail_payload.get("filling_price_raw")
        fp = sale_fingerprint(
            station_id=self._station_id,
            dart_address=address,
            nozzle_id=nozzle_id if nozzle_id else None,
            raw_volume=vol,
            raw_amount=amt,
            raw_price=price_raw if isinstance(price_raw, int) else None,
        )
        await uow.nozzle_baselines.upsert_baseline(
            station_id=self._station_id,
            pump_id=pump_db,
            dart_address=address,
            nozzle_id=int(nozzle_id),
            fingerprint=fp,
            raw_volume=vol,
            raw_amount=amt,
            mark_published=True,
        )
        await uow.audit.append(
            actor="controller",
            source="reconciliation",
            action="STARTUP_BASELINE_RECORDED",
            station_id=self._station_id,
            pump_id=pump_db,
            resulting_state=str(detail_payload.get("normalized_state") or ""),
            result="OK",
            details={
                "fingerprint": fp,
                "raw_volume": vol,
                "raw_amount": amt,
                "nozzle_id": nozzle_id,
                "source": "startup_baseline",
            },
        )
        logger.info(
            "startup_baseline_recorded",
            event_name="duplicate_transaction_ignored",
            stationId=self._station_id,
            pumpId=pump_db,
            nozzleId=nozzle_id,
            fingerprint=fp,
            source="startup_baseline",
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
            can_pump, can_nozzle, source_id = self._channel_identity(address)
            wire_nozzle = nozzle if isinstance(nozzle, int) else None
            # Channel-map hose for the sale row; wire nozzle for baseline keys only.
            nozzle_id = self._wayne_nozzle_index(address, wire_nozzle)
            baseline_nozzle_key = int(wire_nozzle if wire_nozzle is not None else 0)
            price_raw = (
                detail_payload.get("raw_price")
                if isinstance(detail_payload.get("raw_price"), int)
                else None
            )
            fp = sale_fingerprint(
                station_id=self._station_id,
                dart_address=address,
                nozzle_id=wire_nozzle,
                raw_volume=raw_volume,
                raw_amount=raw_amount,
                raw_price=price_raw,
            )
            open_mapped = await self._open_uuid(uow, self._tx_by_address.get(address))
            baseline = await uow.nozzle_baselines.get(
                station_id=self._station_id,
                dart_address=address,
                nozzle_id=baseline_nozzle_key,
            )
            if open_mapped is None and uow.nozzle_baselines.is_already_observed(baseline, fp):
                logger.info(
                    "duplicate_transaction_ignored",
                    event_name="duplicate_transaction_ignored",
                    stationId=self._station_id,
                    pumpId=can_pump,
                    nozzleId=can_nozzle,
                    fingerprint=fp,
                    source="startup_baseline",
                    detail="dc2_tick_suppressed",
                )
                return
            if open_mapped is None:
                # Retained COMPLETED face must not mint a sale. A live fill after
                # AUTHORIZE often emits DC2 before DC1 FILLING / STATE_CHANGED —
                # open from controller snapshot when the lifecycle is in progress.
                # After restart the SM can sit in DISCOVERING while Wayne already
                # reports FILLING (console DC1); still open on positive delivery
                # once the fingerprint is not the startup baseline.
                snap = await uow.states.latest(pump_db)
                state_s = (snap.normalized_state or "").upper() if snap else ""
                lifecycle_open = state_s in {
                    PumpState.FILLING.value,
                    PumpState.AUTHORIZED.value,
                    PumpState.NOZZLE_UP.value,
                    PumpState.SUSPENDED.value,
                }
                if state_s == PumpState.AUTHORIZED.value and raw_volume <= 0:
                    lifecycle_open = False
                stuck_discovering = state_s in {
                    "",
                    PumpState.DISCOVERING.value,
                    PumpState.RESET.value,
                    PumpState.READY.value,
                    PumpState.FILLING_COMPLETE.value,
                }
                if (
                    not lifecycle_open
                    and stuck_discovering
                    and raw_volume > 0
                    and raw_amount > 0
                ):
                    lifecycle_open = True
                if not lifecycle_open:
                    logger.info(
                        "dc2_tick_ignored_no_open_sale",
                        stationId=self._station_id,
                        pumpId=can_pump,
                        nozzleId=can_nozzle,
                        fingerprint=fp,
                        source="awaiting_filling_lifecycle",
                        controllerState=state_s or None,
                        amount=raw_amount,
                        volume=raw_volume,
                    )
                    return
                open_reason = (
                    "dc2_progress_while_filling"
                    if state_s
                    in {
                        PumpState.FILLING.value,
                        PumpState.AUTHORIZED.value,
                        PumpState.NOZZLE_UP.value,
                        PumpState.SUSPENDED.value,
                    }
                    else "dc2_progress_while_discovering"
                )
                open_mapped = await self._ensure_open_sale(
                    uow,
                    address=address,
                    pump_db=pump_db,
                    candidate=None,
                    nozzle_id=nozzle_id,
                    raw_price=price_raw,
                    price_decimals=(
                        detail_payload.get("price_decimals")
                        if isinstance(detail_payload.get("price_decimals"), int)
                        else None
                    ),
                    volume_decimals=(
                        detail_payload.get("volume_decimals")
                        if isinstance(detail_payload.get("volume_decimals"), int)
                        else None
                    ),
                    amount_decimals=(
                        detail_payload.get("amount_decimals")
                        if isinstance(detail_payload.get("amount_decimals"), int)
                        else None
                    ),
                    reason=open_reason,
                )
                if state_s != PumpState.FILLING.value:
                    await self._mark_controller_filling(
                        uow,
                        address=address,
                        pump_db=pump_db,
                        nozzle_id=nozzle_id,
                        transaction_id=open_mapped,
                        previous_state=state_s or None,
                    )
                logger.info(
                    "live_source_event_received",
                    kind="dc2_open_sale",
                    stationId=self._station_id,
                    pumpId=can_pump,
                    nozzleId=can_nozzle,
                    sourceIdentifier=source_id,
                    sourceAddress=address,
                    controllerState=PumpState.FILLING.value,
                    previousControllerState=state_s or None,
                    transactionId=open_mapped,
                    amountMinorUnits=raw_amount,
                    volumeMinorUnits=raw_volume,
                    amount=round(raw_amount / 100.0, 2),
                    volumeLitres=round(raw_volume / 100.0, 2),
                    reason=open_reason,
                )
            tx_uuid = open_mapped
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
            logger.info(
                "live_source_event_received",
                kind="dc2_progress",
                stationId=self._station_id,
                pumpId=can_pump,
                nozzleId=can_nozzle,
                sourceIdentifier=source_id,
                sourceAddress=address,
                transactionId=tx_uuid,
                state="DISPENSING",
                amountMinorUnits=raw_amount,
                volumeMinorUnits=raw_volume,
                amount=round(raw_amount / 100.0, 2),
                volumeLitres=round(raw_volume / 100.0, 2),
            )
            if self._live is not None:
                self._live.publish_typed(
                    LiveEventType.FILLING_UPDATED,
                    station_id=self._station_id,
                    environment=self._environment,
                    simulated=self._simulated,
                    pump_id=can_pump,
                    transaction_id=tx_uuid,
                    payload={
                        "raw_volume": raw_volume,
                        "raw_amount": raw_amount,
                        "nozzleId": can_nozzle,
                        "sourceIdentifier": source_id,
                        "amount": round(raw_amount / 100.0, 2),
                        "volumeLitres": round(raw_volume / 100.0, 2),
                    },
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
