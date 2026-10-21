"""Durable sync_queue MQTT delivery worker."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.backoff import compute_backoff_seconds
from intelipump_fdc.cloud.delivery import DeliveryMapper
from intelipump_fdc.cloud.mqtt.base import MqttClient
from intelipump_fdc.cloud.mqtt.errors import MqttError, MqttNotConnectedError
from intelipump_fdc.persistence.sqlite_health import (
    looks_like_sqlite_corruption,
    sqlite_quick_check_factory,
)
from intelipump_fdc.persistence.unit_of_work import unit_of_work

logger = structlog.get_logger(__name__)


@dataclass
class SyncWorkerStats:
    delivered: int = 0
    failed: int = 0
    skipped_malformed: int = 0
    pruned_delivered: int = 0
    last_delivery_at: datetime | None = None
    last_prune_at: datetime | None = None
    last_integrity_at: datetime | None = None
    last_error: str | None = None
    database_degraded: bool = False


@dataclass
class SyncWorker:
    session_factory: async_sessionmaker[AsyncSession]
    mqtt: MqttClient
    mapper: DeliveryMapper
    batch_size: int = 10
    poll_interval_seconds: float = 1.0
    stale_lock_seconds: int = 60
    max_attempts: int = 20
    require_application_sale_ack: bool = False
    # Delivered outbox prune (cloud-sync only; never touches controller).
    delivered_retention_days: int = 30
    prune_batch_size: int = 500
    prune_interval_seconds: float = 300.0
    total_alert_threshold: int = 20000
    pending_alert_threshold: int = 5000
    # 0 disables periodic re-check (startup gate in cloud CLI still runs).
    integrity_interval_seconds: float = 600.0
    stats: SyncWorkerStats = field(default_factory=SyncWorkerStats)
    _task: asyncio.Task[None] | None = None
    _stop: asyncio.Event = field(default_factory=asyncio.Event)
    _last_prune_monotonic: float = field(default=0.0, repr=False)
    _last_integrity_monotonic: float = field(default=0.0, repr=False)
    _size_alert_latched: bool = field(default=False, repr=False)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="mqtt-sync-worker")

    async def stop(self, *, timeout_s: float = 5.0) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=timeout_s)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            self._task = None

    def mark_database_degraded(self, *, detail: str) -> None:
        """Fail closed on outbox publish; controller service is untouched."""
        self.stats.database_degraded = True
        self.stats.last_error = detail
        logger.error("sync_worker_database_degraded", detail=detail)

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.stats.database_degraded:
                    await asyncio.sleep(self.poll_interval_seconds)
                    continue
                if not self.mqtt.is_connected:
                    await asyncio.sleep(self.poll_interval_seconds)
                    continue
                await self._cycle()
            except Exception as exc:
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                if looks_like_sqlite_corruption(exc):
                    self.mark_database_degraded(detail=self.stats.last_error)
                else:
                    logger.warning(
                        "sync_worker_cycle_error", error=self.stats.last_error
                    )
            await asyncio.sleep(self.poll_interval_seconds)

    async def _maybe_integrity_check(self) -> None:
        interval = float(self.integrity_interval_seconds)
        if interval <= 0:
            return
        now = asyncio.get_running_loop().time()
        due = (now - self._last_integrity_monotonic) >= interval
        if not due and self._last_integrity_monotonic > 0:
            return
        self._last_integrity_monotonic = now
        try:
            ok, detail = await sqlite_quick_check_factory(self.session_factory)
            self.stats.last_integrity_at = datetime.now(UTC)
            if not ok:
                self.mark_database_degraded(detail=detail)
        except Exception as exc:
            if looks_like_sqlite_corruption(exc):
                self.mark_database_degraded(
                    detail=f"{type(exc).__name__}: {exc}"
                )
            else:
                logger.warning(
                    "sync_queue_integrity_check_error",
                    error=f"{type(exc).__name__}: {exc}",
                )

    async def _maybe_prune_and_alert(self) -> None:
        now = asyncio.get_running_loop().time()
        due = (now - self._last_prune_monotonic) >= float(self.prune_interval_seconds)
        if not due and self._last_prune_monotonic > 0:
            return
        self._last_prune_monotonic = now
        try:
            async with unit_of_work(self.session_factory) as uow:
                deleted = await uow.sync_queue.delete_delivered_older_than(
                    older_than_days=self.delivered_retention_days,
                    limit=self.prune_batch_size,
                )
                pending = await uow.sync_queue.pending_count()
                total = await uow.sync_queue.total_count()
            if deleted:
                self.stats.pruned_delivered += deleted
                self.stats.last_prune_at = datetime.now(UTC)
                logger.info(
                    "sync_queue_delivered_pruned",
                    deleted=deleted,
                    retentionDays=self.delivered_retention_days,
                    batchLimit=self.prune_batch_size,
                )
            over_total = total >= int(self.total_alert_threshold)
            over_pending = pending >= int(self.pending_alert_threshold)
            if over_total or over_pending:
                if not self._size_alert_latched:
                    self._size_alert_latched = True
                    logger.warning(
                        "sync_queue_size_alert",
                        totalCount=total,
                        pendingCount=pending,
                        totalThreshold=self.total_alert_threshold,
                        pendingThreshold=self.pending_alert_threshold,
                    )
            else:
                self._size_alert_latched = False
        except Exception as exc:
            if looks_like_sqlite_corruption(exc):
                self.mark_database_degraded(
                    detail=f"{type(exc).__name__}: {exc}"
                )
            else:
                logger.warning(
                    "sync_queue_prune_error",
                    error=f"{type(exc).__name__}: {exc}",
                )

    async def _cycle(self) -> None:
        if not self.mqtt.is_connected or self.stats.database_degraded:
            return
        await self._maybe_integrity_check()
        if self.stats.database_degraded:
            return
        await self._maybe_prune_and_alert()
        if self.stats.database_degraded:
            return
        async with unit_of_work(self.session_factory) as uow:
            await uow.sync_queue.release_stale_locks(
                older_than_seconds=self.stale_lock_seconds
            )
            batch = await uow.sync_queue.claim_batch(limit=self.batch_size)

        for record in batch:
            if not self.mapper.should_publish(record):
                async with unit_of_work(self.session_factory) as uow:
                    await uow.sync_queue.mark_delivered(record.id)
                continue
            try:
                topic, envelope, qos = self.mapper.map_record(record)
                payload = json.dumps(envelope.to_dict(), separators=(",", ":")).encode()
                env_dict = envelope.to_dict()
                nested = env_dict.get("payload") if isinstance(env_dict.get("payload"), dict) else {}
                event_id = str(
                    env_dict.get("deduplicationKey")
                    or record.deduplication_key
                    or record.id
                )
                logger.info(
                    "live_event_created",
                    eventId=event_id,
                    transactionId=env_dict.get("transactionId") or nested.get("transaction_uuid"),
                    eventType=record.event_type,
                    stationId=env_dict.get("stationId") or nested.get("station_id"),
                    pumpId=env_dict.get("pumpId") or nested.get("pumpId") or nested.get("pump_id"),
                    nozzleId=nested.get("nozzleId") or nested.get("nozzle_id"),
                    sequence=env_dict.get("sequence"),
                    mqttTopic=topic,
                    durable=True,
                    outboxId=record.id,
                )
                logger.info(
                    "live_event_outbox_inserted",
                    eventId=event_id,
                    transactionId=env_dict.get("transactionId") or nested.get("transaction_uuid"),
                    eventType=record.event_type,
                    durable=True,
                    outboxId=record.id,
                    mqttTopic=topic,
                )
                logger.info(
                    "live_event_publish_attempt",
                    eventId=event_id,
                    transactionId=env_dict.get("transactionId") or nested.get("transaction_uuid"),
                    eventType=record.event_type,
                    mqttTopic=topic,
                    qos=qos,
                    retain=False,
                    outboxId=record.id,
                )
                result = await self.mqtt.publish(topic, payload, qos=qos, retain=False)
                if qos > 0 and not result.acknowledged:
                    raise MqttError("publish not acknowledged")
                logger.info(
                    "live_event_publish_acknowledged",
                    eventId=event_id,
                    transactionId=env_dict.get("transactionId") or nested.get("transaction_uuid"),
                    eventType=record.event_type,
                    mqttTopic=topic,
                    mqttMid=result.mid,
                    acknowledged=result.acknowledged,
                    qos=qos,
                    outboxId=record.id,
                )
                async with unit_of_work(self.session_factory) as uow:
                    if self.require_application_sale_ack and str(
                        record.event_type or ""
                    ).upper() in {
                        "TRANSACTION_COMPLETED",
                        "TX_COMPLETED",
                        "SALE_COMPLETED",
                    }:
                        await uow.sync_queue.mark_awaiting_app_ack(record.id)
                        logger.info(
                            "sale_awaiting_application_ack",
                            eventId=event_id,
                            transactionId=env_dict.get("transactionId")
                            or nested.get("transaction_uuid"),
                            outboxId=record.id,
                            deduplicationKey=record.deduplication_key,
                        )
                    else:
                        await uow.sync_queue.mark_delivered(record.id)
                self.stats.delivered += 1
                self.stats.last_delivery_at = datetime.now(UTC)
            except (MqttNotConnectedError, MqttError) as exc:
                backoff = compute_backoff_seconds(record.attempt_count + 1)
                async with unit_of_work(self.session_factory) as uow:
                    await uow.sync_queue.mark_failed(
                        record.id,
                        error=str(exc),
                        backoff_seconds=backoff,
                        max_attempts=self.max_attempts,
                    )
                self.stats.failed += 1
                self.stats.last_error = str(exc)
                # Stop this cycle if disconnected; preserve remaining claimed
                # by marking failed so they retry (already done for this one).
                if isinstance(exc, MqttNotConnectedError):
                    return
            except Exception as exc:
                backoff = compute_backoff_seconds(record.attempt_count + 1)
                async with unit_of_work(self.session_factory) as uow:
                    await uow.sync_queue.mark_failed(
                        record.id,
                        error=f"malformed:{type(exc).__name__}:{exc}",
                        backoff_seconds=backoff,
                        max_attempts=self.max_attempts,
                    )
                self.stats.skipped_malformed += 1
                self.stats.failed += 1
                logger.warning(
                    "sync_record_malformed",
                    record_id=record.id,
                    error=str(exc),
                )

    async def handle_sale_ack_payload(self, payload: dict[str, Any]) -> int:
        """Apply SALE_COMMITTED application ACK to awaiting sync_queue rows.

        Conflicting amount/volume on an otherwise matching identity leaves the
        row awaiting and returns 0 (visible via unmatched / conflict logs).

        Missing or unrelated ``eventType`` values are rejected before any
        delivered mark. ``volumeLiters=0`` / ``volume_liters=0`` are preserved
        (never treated as missing via ``or`` fallback).
        """
        _ACK_EVENTS = {"SALE_COMMITTED", "SALE_ACK", "TX_COMMITTED"}
        event = str(payload.get("eventType") or payload.get("event_type") or "").upper()
        if event and event not in _ACK_EVENTS:
            nested = payload.get("payload")
            if isinstance(nested, dict):
                payload = nested
                event = str(
                    payload.get("eventType") or payload.get("event_type") or ""
                ).upper()
        if event not in _ACK_EVENTS:
            logger.warning(
                "sale_application_ack_rejected",
                reason="missing_or_unrelated_event_type",
                eventType=event or None,
            )
            return 0
        dedupe = str(
            payload.get("deduplicationKey")
            or payload.get("deduplication_key")
            or ""
        ).strip()
        tx_id = str(
            payload.get("transactionId")
            or payload.get("transaction_id")
            or payload.get("transaction_uuid")
            or ""
        ).strip()
        ack_amount = payload.get("amount")
        # Preserve explicit zero — do not use ``or`` (0 is a valid volume).
        if "volumeLiters" in payload:
            ack_volume = payload.get("volumeLiters")
        elif "volume_liters" in payload:
            ack_volume = payload.get("volume_liters")
        else:
            ack_volume = None
        matched = 0
        async with unit_of_work(self.session_factory) as uow:
            if dedupe or tx_id:
                conflict = await uow.sync_queue.ack_payload_conflicts(
                    deduplication_key=dedupe or None,
                    entity_id=tx_id or None,
                    amount=ack_amount,
                    volume_liters=ack_volume,
                )
                if conflict:
                    logger.warning(
                        "sale_application_ack_conflict",
                        deduplicationKey=dedupe or None,
                        transactionId=tx_id or None,
                        detail=conflict,
                    )
                    return 0
            if dedupe:
                matched += await uow.sync_queue.mark_delivered_by_dedupe_key(dedupe)
            if matched == 0 and tx_id:
                matched += await uow.sync_queue.mark_delivered_by_entity_id(tx_id)
        if matched:
            logger.info(
                "sale_application_ack_applied",
                deduplicationKey=dedupe or None,
                transactionId=tx_id or None,
                matched=matched,
            )
        else:
            logger.warning(
                "sale_application_ack_unmatched",
                deduplicationKey=dedupe or None,
                transactionId=tx_id or None,
            )
        return matched
