"""Publish in-progress fill totals from SQLite without touching the controller loop."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.channel_map import enrich_transaction_payload
from intelipump_fdc.cloud.fill_throttle import FillPublishBook
from intelipump_fdc.cloud.messages import build_envelope
from intelipump_fdc.cloud.mqtt.base import MqttClient
from intelipump_fdc.cloud.mqtt.errors import MqttError, MqttNotConnectedError
from intelipump_fdc.cloud.qos import qos_for_event
from intelipump_fdc.cloud.topics import TopicBuilder
from intelipump_fdc.persistence.dto import StateSnapshotRecord, TransactionRecord
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.transaction_models import CompleteTransactionRequest
from intelipump_fdc.services.transaction_service import TransactionService

logger = structlog.get_logger(__name__)

# Do not auto-complete while the controller is still reporting a live fill.
# A paused meter tick is not completion. Only age-out a truly stale FILLING snapshot.
_LIVE_FILL_STATES = frozenset({"FILLING", "AUTHORIZED", "NOZZLE_UP", "SUSPENDED"})
# Terminal / idle faces — safe for the short settle timer.
_SETTLE_OK_STATES = frozenset(
    {
        "FILLING_COMPLETE",
        "FILLING_COMPLETED",
        "LIMIT_REACHED",
        "RESET",
        "READY",
        "IDLE",
        "CLOSED",
    }
)
_LIVE_STATE_MAX_AGE_SECONDS = 90.0
# ACTIVE row updated this recently ⇒ treat as live even if snap is DISCOVERING.
_ACTIVE_TX_RECENT_SECONDS = 30.0
# Pump face: 170 raw → 1.70 L (2 dp). Null decimals used to become 0.17 L on the cloud.
_LAB_VOLUME_DECIMALS = 2
_LAB_AMOUNT_DECIMALS = 2


def _wire_decimals(value: int | None, default: int) -> int:
    return default if value is None else value


def _pump_is_actively_filling(
    snap: StateSnapshotRecord | None, now: datetime
) -> bool:
    if snap is None:
        return False
    if (snap.normalized_state or "").upper() not in _LIVE_FILL_STATES:
        return False
    observed = snap.observed_at or snap.persisted_at
    if observed is None:
        return True
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=UTC)
    age = (now - observed).total_seconds()
    return age <= _LIVE_STATE_MAX_AGE_SECONDS


def _tx_recently_active(tx: TransactionRecord, now: datetime) -> bool:
    """True when an ACTIVE sale was updated recently (DC2 gap ≠ hang-up)."""
    stamp = tx.updated_at or tx.started_at
    if stamp is None:
        return False
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return (now - stamp).total_seconds() <= _ACTIVE_TX_RECENT_SECONDS


@dataclass
class LiveFillStream:
    """Poll ACTIVE lab sales and publish throttled FILLING_UPDATED envelopes."""

    session_factory: async_sessionmaker[AsyncSession]
    mqtt: MqttClient
    topics: TopicBuilder
    fill_book: FillPublishBook
    device_id: str
    station_id: str
    environment: str
    simulated: bool
    poll_interval_seconds: float = 1.0
    settle_seconds: float = 4.0
    # Hang-up often leaves Wayne snapshot as FILLING. Complete only after a long pause.
    force_settle_seconds: float = 90.0
    keepalive_seconds: float = 10.0
    channel_mappings: dict | None = None
    _task: asyncio.Task[None] | None = None
    _stop: asyncio.Event = field(default_factory=asyncio.Event)
    _seq: int = 0
    _tx_seq: dict[str, int] = field(default_factory=dict)
    _unchanged_since: dict[str, tuple[int, int, datetime]] = field(default_factory=dict)
    _last_fill_publish: dict[str, datetime] = field(default_factory=dict)
    _finalized: set[str] = field(default_factory=set)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            logger.info(
                "live_source_started",
                source_type="sqlite_poll_transactions",
                database="shared_controller_sqlite",
                station_id=self.station_id,
                poll_interval_seconds=self.poll_interval_seconds,
                starting_cursor="list_unresolved",
            )
            self._task = asyncio.create_task(self._run(), name="mqtt-live-fill")

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=timeout_s)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.mqtt.is_connected:
                    await self.publish_active_fills()
            except Exception as exc:
                logger.warning("live_fill_cycle_error", error=f"{type(exc).__name__}: {exc}")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval_seconds)
            except TimeoutError:
                continue

    async def publish_active_fills(self, *, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        published = 0
        async with unit_of_work(self.session_factory) as uow:
            active = await uow.transactions.list_unresolved(station_id=self.station_id)
            pump_ids = {tx.pump_id for tx in active}
            logical: dict[str, str] = {}
            dart_by_pump: dict[str, int] = {}
            filling_now: dict[str, bool] = {}
            prev_state: dict[str, str] = {}
            skip_fill_pub: set[str] = set()
            for pump_id in pump_ids:
                pump = await uow.pumps.get_by_id(pump_id)
                if pump is not None:
                    logical[pump_id] = pump.logical_pump_id
                    dart_by_pump[pump_id] = pump.dart_address
                snap = await uow.states.latest(pump_id)
                filling_now[pump_id] = _pump_is_actively_filling(snap, now)
                if snap is not None:
                    prev_state[pump_id] = (snap.normalized_state or "").upper()
            for tx in active:
                twin = await uow.transactions.find_recent_completed_same_totals(
                    station_id=tx.station_id,
                    pump_id=tx.pump_id,
                    raw_volume=int(tx.raw_volume or 0),
                    raw_amount=int(tx.raw_amount or 0),
                    exclude_uuid=tx.transaction_uuid,
                    within_seconds=120.0,
                )
                if twin is not None:
                    skip_fill_pub.add(tx.transaction_uuid)

        live_ids = {tx.transaction_uuid for tx in active}
        self._unchanged_since = {
            key: value for key, value in self._unchanged_since.items() if key in live_ids
        }
        self._last_fill_publish = {
            key: value for key, value in self._last_fill_publish.items() if key in live_ids
        }
        self._tx_seq = {key: value for key, value in self._tx_seq.items() if key in live_ids}
        self._finalized &= live_ids

        for tx in active:
            raw_volume = int(tx.raw_volume or 0)
            raw_amount = int(tx.raw_amount or 0)
            if raw_volume <= 0 and raw_amount <= 0:
                continue
            prev = self._unchanged_since.get(tx.transaction_uuid)
            filling = filling_now.get(tx.pump_id, False)
            # Short settle only on idle/complete faces (true hang-up), or when a
            # twin COMPLETED already has the same totals (orphan ACTIVE after
            # hang-up). FILLING/AUTHORIZED/DISCOVERING without a twin requires
            # the long force timer so a DC2 gap cannot complete mid-hose.
            state_u = (prev_state.get(tx.pump_id) or "").upper()
            allow_short = state_u in _SETTLE_OK_STATES or (
                tx.transaction_uuid in skip_fill_pub
            )
            if not allow_short and _tx_recently_active(tx, now):
                filling = True
            if prev and prev[0] == raw_volume and prev[1] == raw_amount:
                unchanged = (now - prev[2]).total_seconds()
                if allow_short:
                    ready = unchanged >= self.settle_seconds
                else:
                    ready = unchanged >= self.force_settle_seconds
                if tx.transaction_uuid not in self._finalized and ready:
                    if await self._finalize_settled(
                        tx,
                        raw_volume,
                        raw_amount,
                        dart=dart_by_pump.get(tx.pump_id),
                        controller_state=prev_state.get(tx.pump_id),
                    ):
                        published += 1
                    continue
                if (
                    filling
                    and tx.transaction_uuid not in skip_fill_pub
                    and self._keepalive_due(tx.transaction_uuid, now)
                    and await self._publish_fill_update(
                        tx,
                        raw_volume=raw_volume,
                        raw_amount=raw_amount,
                        now=now,
                        logical=logical,
                        dart=dart_by_pump.get(tx.pump_id),
                        controller_state=prev_state.get(tx.pump_id),
                        reason="keepalive_unchanged_meter",
                    )
                ):
                    published += 1
                continue
            # First sight of this ACTIVE row: seed meter baseline. Do not publish
            # DISPENSING unless the controller reports a live fill — retained
            # FILLING_COMPLETE / orphan ACTIVE rows must not animate the twin.
            if prev is None:
                self._unchanged_since[tx.transaction_uuid] = (
                    raw_volume,
                    raw_amount,
                    now,
                )
                if not filling:
                    can_pump = tx.canonical_pump_id or logical.get(tx.pump_id)
                    logger.info(
                        "live_fill_startup_baseline_seeded",
                        transaction_id=tx.transaction_uuid,
                        pump_id=can_pump,
                        nozzle_id=tx.canonical_nozzle_id,
                        source_identifier=tx.source_identifier,
                        controller_state=prev_state.get(tx.pump_id),
                        amountMinorUnits=raw_amount,
                        volumeMinorUnits=raw_volume,
                        amount=round(raw_amount / 100.0, 2),
                        volumeLitres=round(raw_volume / 100.0, 2),
                        reason="retained_or_idle_active_row",
                    )
                    continue
            else:
                self._unchanged_since[tx.transaction_uuid] = (
                    raw_volume,
                    raw_amount,
                    now,
                )
            if tx.transaction_uuid in skip_fill_pub:
                continue
            if not filling:
                continue
            if not self.fill_book.decide(
                tx.transaction_uuid,
                raw_volume=raw_volume,
                raw_amount=raw_amount,
                is_final=False,
                now=now,
            ):
                continue
            if await self._publish_fill_update(
                tx,
                raw_volume=raw_volume,
                raw_amount=raw_amount,
                now=now,
                logical=logical,
                dart=dart_by_pump.get(tx.pump_id),
                controller_state=prev_state.get(tx.pump_id),
                reason="meter_progress",
            ):
                published += 1
        return published

    def _keepalive_due(self, transaction_uuid: str, now: datetime) -> bool:
        last = self._last_fill_publish.get(transaction_uuid)
        if last is None:
            return True
        return (now - last).total_seconds() >= self.keepalive_seconds

    async def _publish_fill_update(
        self,
        tx: TransactionRecord,
        *,
        raw_volume: int,
        raw_amount: int,
        now: datetime,
        logical: dict[str, str],
        dart: int | None,
        controller_state: str | None,
        reason: str,
    ) -> bool:
        pump_id = logical.get(tx.pump_id)
        if not pump_id and not tx.canonical_pump_id:
            return False
        session_seq = self._tx_seq.get(tx.transaction_uuid, 0) + 1
        self._tx_seq[tx.transaction_uuid] = session_seq
        amount = round(raw_amount / (10 ** _wire_decimals(tx.amount_decimals, _LAB_AMOUNT_DECIMALS)), 2)
        volume_litres = round(
            raw_volume / (10 ** _wire_decimals(tx.volume_decimals, _LAB_VOLUME_DECIMALS)), 2
        )
        locked_pump = tx.canonical_pump_id
        locked_nozzle = tx.canonical_nozzle_id
        payload = {
            "transaction_uuid": tx.transaction_uuid,
            "station_id": tx.station_id,
            "pump_id": locked_pump or pump_id,
            "nozzle_id": locked_nozzle if locked_nozzle is not None else tx.nozzle_id,
            "nozzleId": locked_nozzle,
            "canonical_pump_id": locked_pump,
            "canonical_nozzle_id": locked_nozzle,
            "sourceIdentifier": tx.source_identifier or pump_id,
            "raw_unit_price": tx.raw_price,
            "price_decimals": tx.price_decimals,
            "raw_volume": raw_volume,
            "volume_decimals": _wire_decimals(tx.volume_decimals, _LAB_VOLUME_DECIMALS),
            "raw_amount": raw_amount,
            "amount_decimals": _wire_decimals(tx.amount_decimals, _LAB_AMOUNT_DECIMALS),
            "amountMinorUnits": raw_amount,
            "volumeMinorUnits": raw_volume,
            "started_at": tx.started_at.isoformat() if tx.started_at else None,
            "final_status": "DISPENSING",
            "status": "DISPENSING",
            "environment": tx.environment,
            "simulated": tx.simulated,
            "sessionSequence": session_seq,
        }
        if self.channel_mappings:
            payload = enrich_transaction_payload(payload, self.channel_mappings)
            if payload.get("identityQuarantined"):
                logger.error(
                    "transaction_identity_mismatch",
                    transaction_id=tx.transaction_uuid,
                    pump_id=payload.get("pumpId") or payload.get("pump_id"),
                    nozzle_id=payload.get("nozzleId"),
                    source_identifier=payload.get("sourceIdentifier"),
                    reason="quarantined_before_publish",
                )
                self._tx_seq[tx.transaction_uuid] = max(session_seq - 1, 0)
                return False
        mqtt_pump = str(payload.get("pumpId") or payload.get("pump_id") or pump_id)
        mqtt_nozzle = str(payload.get("nozzleId") or payload.get("nozzle_id") or "")
        if locked_pump and mqtt_pump != locked_pump:
            logger.error(
                "transaction_identity_mismatch",
                transaction_id=tx.transaction_uuid,
                locked_pump=locked_pump,
                published_pump=mqtt_pump,
                locked_nozzle=locked_nozzle,
                published_nozzle=mqtt_nozzle,
            )
            self._tx_seq[tx.transaction_uuid] = max(session_seq - 1, 0)
            return False
        if locked_nozzle and mqtt_nozzle and mqtt_nozzle != locked_nozzle:
            logger.error(
                "transaction_identity_mismatch",
                transaction_id=tx.transaction_uuid,
                locked_pump=locked_pump,
                published_pump=mqtt_pump,
                locked_nozzle=locked_nozzle,
                published_nozzle=mqtt_nozzle,
            )
            self._tx_seq[tx.transaction_uuid] = max(session_seq - 1, 0)
            return False
        self._seq += 1
        topic = self.topics.transactions(self.station_id)
        event_id = f"fill:{tx.transaction_uuid}:{session_seq}:{raw_volume}:{raw_amount}"
        envelope = build_envelope(
            event_type="FILLING_UPDATED",
            environment=self.environment,
            device_id=self.device_id,
            station_id=self.station_id,
            sequence=self._seq,
            simulated=bool(tx.simulated if tx.simulated is not None else self.simulated),
            deduplication_key=event_id,
            payload=payload,
            pump_id=mqtt_pump,
            transaction_id=tx.transaction_uuid,
            occurred_at=now.isoformat(),
        )
        qos = qos_for_event("FILLING_UPDATED")
        logger.info(
            "live_event_created",
            eventId=event_id,
            transactionId=tx.transaction_uuid,
            eventType="FILLING_UPDATED",
            stationId=self.station_id,
            pumpId=mqtt_pump,
            nozzleId=mqtt_nozzle,
            sequence=session_seq,
            amountMinorUnits=raw_amount,
            volumeMinorUnits=raw_volume,
            amount=amount,
            volumeLitres=volume_litres,
            mqttTopic=topic,
        )
        logger.info(
            "live_event_outbox_inserted",
            eventId=event_id,
            transactionId=tx.transaction_uuid,
            eventType="FILLING_UPDATED",
            durable=False,
            reason="ephemeral_live_progress",
            mqttTopic=topic,
        )
        logger.info(
            "live_event_publish_attempt",
            eventId=event_id,
            transactionId=tx.transaction_uuid,
            eventType="FILLING_UPDATED",
            stationId=self.station_id,
            pumpId=mqtt_pump,
            nozzleId=mqtt_nozzle,
            sequence=session_seq,
            amount=amount,
            volumeLitres=volume_litres,
            mqttTopic=topic,
            qos=qos,
            retain=False,
        )
        try:
            result = await self.mqtt.publish(
                topic,
                json.dumps(envelope.to_dict(), separators=(",", ":")).encode(),
                qos=qos,
                retain=False,
            )
        except (MqttNotConnectedError, MqttError) as exc:
            logger.warning(
                "live_fill_publish_failed",
                eventId=event_id,
                transactionId=tx.transaction_uuid,
                mqttTopic=topic,
                error=str(exc),
            )
            self._tx_seq[tx.transaction_uuid] = max(session_seq - 1, 0)
            return False
        logger.info(
            "live_event_publish_acknowledged",
            eventId=event_id,
            transactionId=tx.transaction_uuid,
            eventType="FILLING_UPDATED",
            stationId=self.station_id,
            pumpId=mqtt_pump,
            nozzleId=mqtt_nozzle,
            sequence=session_seq,
            amount=amount,
            volumeLitres=volume_litres,
            mqttTopic=topic,
            mqttMid=result.mid,
            acknowledged=result.acknowledged,
            qos=qos,
        )
        self._last_fill_publish[tx.transaction_uuid] = now
        logger.info(
            "live_fill_state_transition",
            source_address=dart,
            pump_id=mqtt_pump,
            nozzle_id=mqtt_nozzle,
            transaction_id=tx.transaction_uuid,
            previous_state=controller_state or "FILLING",
            new_state="DISPENSING",
            sequence=self._seq,
            session_sequence=session_seq,
            amountMinorUnits=raw_amount,
            volumeMinorUnits=raw_volume,
            amount=amount,
            volumeLitres=volume_litres,
            reason=reason,
        )
        return True

    async def _finalize_settled(
        self,
        tx: TransactionRecord,
        raw_volume: int,
        raw_amount: int,
        *,
        dart: int | None = None,
        controller_state: str | None = None,
    ) -> bool:
        """Hang-up holds DISPLAY; controller may leave the SQLite row ACTIVE.

        Complete it here so TRANSACTION_COMPLETED is queued without touching
        the Wayne loop.
        """
        published = False
        try:
            async with unit_of_work(self.session_factory) as uow:
                already = await uow.transactions.find_recent_completed_same_totals(
                    station_id=tx.station_id,
                    pump_id=tx.pump_id,
                    raw_volume=raw_volume,
                    raw_amount=raw_amount,
                    exclude_uuid=tx.transaction_uuid,
                    within_seconds=120.0,
                )
                _row, newly = await TransactionService(uow).complete(
                    CompleteTransactionRequest(
                        transaction_uuid=tx.transaction_uuid,
                        source_completion_key=f"sidecar-settle:{tx.transaction_uuid}",
                        raw_volume=raw_volume,
                        raw_amount=raw_amount,
                        completion_inferred=True,
                        completion_warnings=("sidecar_settle_after_hangup",),
                        publish_completion=already is None,
                    )
                )
                published = bool(newly and already is None)
                if newly and already is not None:
                    logger.info(
                        "live_fill_settle_suppressed_duplicate",
                        transaction_uuid=tx.transaction_uuid,
                        kept_uuid=already.transaction_uuid,
                    )
        except Exception as exc:
            logger.warning(
                "live_fill_finalize_failed",
                transaction_uuid=tx.transaction_uuid,
                error=str(exc),
            )
            return False
        self._finalized.add(tx.transaction_uuid)
        self.fill_book.decide(
            tx.transaction_uuid,
            raw_volume=raw_volume,
            raw_amount=raw_amount,
            is_final=True,
        )
        if published:
            logger.info(
                "live_fill_state_transition",
                source_address=dart,
                pump_id=tx.canonical_pump_id or tx.pump_id,
                nozzle_id=(
                    tx.canonical_nozzle_id if tx.canonical_nozzle_id is not None else tx.nozzle_id
                ),
                transaction_id=tx.transaction_uuid,
                previous_state=controller_state or "DISPENSING",
                new_state="COMPLETED",
                sequence=self._seq,
                session_sequence=self._tx_seq.get(tx.transaction_uuid),
                volume=raw_volume,
                amount=raw_amount,
                reason="sidecar_settle_after_hangup",
            )
        return published
