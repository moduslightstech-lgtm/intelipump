"""Publish in-progress fill totals from SQLite without touching the controller loop."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
# Stale FILLING (hang-up left the row ACTIVE) is handled by observed_at age.
_LIVE_FILL_STATES = frozenset({"FILLING", "AUTHORIZED", "NOZZLE_UP", "SUSPENDED"})
_LIVE_STATE_MAX_AGE_SECONDS = 12.0


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
    return (now - observed).total_seconds() < _LIVE_STATE_MAX_AGE_SECONDS


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
    _task: asyncio.Task[None] | None = None
    _stop: asyncio.Event = field(default_factory=asyncio.Event)
    _seq: int = 0
    _unchanged_since: dict[str, tuple[int, int, datetime]] = field(default_factory=dict)
    _finalized: set[str] = field(default_factory=set)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
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
            filling_now: dict[str, bool] = {}
            for pump_id in pump_ids:
                pump = await uow.pumps.get_by_id(pump_id)
                if pump is not None:
                    logical[pump_id] = pump.logical_pump_id
                snap = await uow.states.latest(pump_id)
                filling_now[pump_id] = _pump_is_actively_filling(snap, now)

        live_ids = {tx.transaction_uuid for tx in active}
        self._unchanged_since = {
            key: value for key, value in self._unchanged_since.items() if key in live_ids
        }
        self._finalized &= live_ids

        for tx in active:
            raw_volume = int(tx.raw_volume or 0)
            raw_amount = int(tx.raw_amount or 0)
            if raw_volume <= 0 and raw_amount <= 0:
                continue
            prev = self._unchanged_since.get(tx.transaction_uuid)
            if prev and prev[0] == raw_volume and prev[1] == raw_amount:
                if (
                    tx.transaction_uuid not in self._finalized
                    and not filling_now.get(tx.pump_id, False)
                    and (now - prev[2]).total_seconds() >= self.settle_seconds
                ):
                    if await self._finalize_settled(tx, raw_volume, raw_amount):
                        published += 1
                continue
            self._unchanged_since[tx.transaction_uuid] = (raw_volume, raw_amount, now)
            if not self.fill_book.decide(
                tx.transaction_uuid,
                raw_volume=raw_volume,
                raw_amount=raw_amount,
                is_final=False,
                now=now,
            ):
                continue
            pump_id = logical.get(tx.pump_id)
            if not pump_id:
                continue
            self._seq += 1
            envelope = build_envelope(
                event_type="FILLING_UPDATED",
                environment=self.environment,
                device_id=self.device_id,
                station_id=self.station_id,
                sequence=self._seq,
                simulated=bool(tx.simulated if tx.simulated is not None else self.simulated),
                deduplication_key=f"fill:{tx.transaction_uuid}:{raw_volume}:{raw_amount}",
                payload={
                    "transaction_uuid": tx.transaction_uuid,
                    "station_id": tx.station_id,
                    "pump_id": pump_id,
                    "nozzle_id": tx.nozzle_id,
                    "raw_unit_price": tx.raw_price,
                    "price_decimals": tx.price_decimals,
                    "raw_volume": raw_volume,
                    "volume_decimals": tx.volume_decimals,
                    "raw_amount": raw_amount,
                    "amount_decimals": tx.amount_decimals,
                    "started_at": tx.started_at.isoformat() if tx.started_at else None,
                    "final_status": "DISPENSING",
                    "environment": tx.environment,
                    "simulated": tx.simulated,
                },
                pump_id=pump_id,
                transaction_id=tx.transaction_uuid,
                occurred_at=now.isoformat(),
            )
            try:
                await self.mqtt.publish(
                    self.topics.transactions(self.station_id),
                    json.dumps(envelope.to_dict(), separators=(",", ":")).encode(),
                    qos=qos_for_event("FILLING_UPDATED"),
                    retain=False,
                )
                published += 1
                logger.info(
                    "live_fill_published",
                    transaction_uuid=tx.transaction_uuid,
                    pump_id=pump_id,
                    raw_volume=raw_volume,
                    raw_amount=raw_amount,
                )
            except (MqttNotConnectedError, MqttError) as exc:
                logger.warning("live_fill_publish_failed", error=str(exc))
                return published
        return published

    async def _finalize_settled(
        self, tx: TransactionRecord, raw_volume: int, raw_amount: int
    ) -> bool:
        """Hang-up holds DISPLAY; controller may leave the SQLite row ACTIVE.

        Complete it here so TRANSACTION_COMPLETED is queued without touching
        the Wayne loop.
        """
        try:
            async with unit_of_work(self.session_factory) as uow:
                _row, newly = await TransactionService(uow).complete(
                    CompleteTransactionRequest(
                        transaction_uuid=tx.transaction_uuid,
                        source_completion_key=f"sidecar-settle:{tx.transaction_uuid}",
                        raw_volume=raw_volume,
                        raw_amount=raw_amount,
                        completion_inferred=True,
                        completion_warnings=("sidecar_settle_after_hangup",),
                    )
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
        if newly:
            logger.info(
                "live_fill_settled_completed",
                transaction_uuid=tx.transaction_uuid,
                raw_volume=raw_volume,
                raw_amount=raw_amount,
            )
        return newly
