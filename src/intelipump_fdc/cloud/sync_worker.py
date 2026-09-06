"""Durable sync_queue MQTT delivery worker."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.backoff import compute_backoff_seconds
from intelipump_fdc.cloud.delivery import DeliveryMapper
from intelipump_fdc.cloud.mqtt.base import MqttClient
from intelipump_fdc.cloud.mqtt.errors import MqttError, MqttNotConnectedError
from intelipump_fdc.persistence.unit_of_work import unit_of_work

logger = structlog.get_logger(__name__)


@dataclass
class SyncWorkerStats:
    delivered: int = 0
    failed: int = 0
    skipped_malformed: int = 0
    last_delivery_at: datetime | None = None
    last_error: str | None = None


@dataclass
class SyncWorker:
    session_factory: async_sessionmaker[AsyncSession]
    mqtt: MqttClient
    mapper: DeliveryMapper
    batch_size: int = 10
    poll_interval_seconds: float = 1.0
    stale_lock_seconds: int = 60
    max_attempts: int = 20
    stats: SyncWorkerStats = field(default_factory=SyncWorkerStats)
    _task: asyncio.Task[None] | None = None
    _stop: asyncio.Event = field(default_factory=asyncio.Event)

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

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if not self.mqtt.is_connected:
                    await asyncio.sleep(self.poll_interval_seconds)
                    continue
                await self._cycle()
            except Exception as exc:
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("sync_worker_cycle_error", error=self.stats.last_error)
            await asyncio.sleep(self.poll_interval_seconds)

    async def _cycle(self) -> None:
        if not self.mqtt.is_connected:
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
                result = await self.mqtt.publish(topic, payload, qos=qos, retain=False)
                if qos > 0 and not result.acknowledged:
                    raise MqttError("publish not acknowledged")
                async with unit_of_work(self.session_factory) as uow:
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
