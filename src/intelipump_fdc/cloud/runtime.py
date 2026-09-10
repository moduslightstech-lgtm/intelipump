"""Cloud sync runtime: MQTT + heartbeat + sync worker + command intake."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.command_intake import CloudCommandIntake
from intelipump_fdc.cloud.delivery import DeliveryMapper
from intelipump_fdc.cloud.fill_stream import LiveFillStream
from intelipump_fdc.cloud.fill_throttle import FillPublishBook, FillThrottleConfig
from intelipump_fdc.cloud.heartbeat import HeartbeatService
from intelipump_fdc.cloud.messages import build_envelope
from intelipump_fdc.cloud.mqtt.base import MqttClient
from intelipump_fdc.cloud.mqtt.client import PahoMqttClient
from intelipump_fdc.cloud.mqtt.config import mqtt_config_from_settings
from intelipump_fdc.cloud.mqtt.errors import MqttError
from intelipump_fdc.cloud.mqtt.models import MqttConnectionMetadata, MqttConnectionState
from intelipump_fdc.cloud.qos import qos_for_event
from intelipump_fdc.cloud.sync_worker import SyncWorker, SyncWorkerStats
from intelipump_fdc.cloud.topics import TopicBuilder
from intelipump_fdc.controller.controller_loop import ControllerLoop
from intelipump_fdc.cloud.channel_map import safe_mappings_from_settings
from intelipump_fdc.core.config import Settings
from intelipump_fdc.persistence.unit_of_work import unit_of_work

logger = structlog.get_logger(__name__)


@dataclass
class CloudRuntime:
    """Owns MQTT lifecycle for Phase 9 cloud sync."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    mqtt: MqttClient
    topics: TopicBuilder
    fill_book: FillPublishBook
    sync_worker: SyncWorker | None = None
    heartbeat: HeartbeatService | None = None
    fill_stream: LiveFillStream | None = None
    command_intake: CloudCommandIntake | None = None
    started: bool = False
    online_published: bool = False
    payload_provider: Any = None
    controller_loop: ControllerLoop | None = None
    _stats_extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        mqtt: MqttClient | None = None,
        controller_loop: ControllerLoop | None = None,
        payload_provider: Any = None,
    ) -> CloudRuntime:
        env = (settings.mqtt.topic_environment or settings.environment).upper()
        topics = TopicBuilder(environment=env)
        cfg = mqtt_config_from_settings(
            settings.mqtt, device_id=settings.controller.device_id
        )
        client = mqtt or PahoMqttClient(cfg)
        fill_book = FillPublishBook(
            config=FillThrottleConfig(
                min_interval_seconds=settings.mqtt.fill_min_interval_seconds,
                min_volume_delta=settings.mqtt.fill_min_volume_delta,
                min_amount_delta=settings.mqtt.fill_min_amount_delta,
            )
        )
        return cls(
            settings=settings,
            session_factory=session_factory,
            mqtt=client,
            topics=topics,
            fill_book=fill_book,
            controller_loop=controller_loop,
            payload_provider=payload_provider,
        )

    @property
    def enabled(self) -> bool:
        return bool(self.settings.mqtt.enabled)

    @property
    def sync_stats(self) -> SyncWorkerStats:
        if self.sync_worker is None:
            return SyncWorkerStats()
        return self.sync_worker.stats

    def health_dict(self) -> dict[str, Any]:
        meta = self.mqtt.metadata
        pending = self._stats_extra.get("pendingOutboxCount")
        return {
            "processRunning": self.started,
            "sqliteHealthy": True,
            "mqttEnabled": self.enabled,
            "mqttConnected": self.mqtt.is_connected,
            "mqttHost": f"{meta.host}:{meta.port}" if meta.host else None,
            "mqttTls": meta.tls_enabled,
            "mqttClientId": meta.client_id,
            "mqttLastConnectedAt": (
                meta.last_connected_at.isoformat() if meta.last_connected_at else None
            ),
            "mqttLastDisconnectedAt": (
                meta.last_disconnected_at.isoformat()
                if meta.last_disconnected_at
                else None
            ),
            "mqttReconnectCount": meta.reconnect_count,
            "mqttLastError": meta.last_error,
            "heartbeatLastPublishedAt": (
                self.heartbeat.last_published_at.isoformat()
                if self.heartbeat and self.heartbeat.last_published_at
                else None
            ),
            "lastLiveEventPublishedAt": self._stats_extra.get("lastLiveEventPublishedAt"),
            "lastCompletedTransactionPublishedAt": self._stats_extra.get(
                "lastCompletedTransactionPublishedAt"
            ),
            "pendingOutboxCount": pending,
            "cloudCommandSubscriptionActive": bool(
                self.command_intake and self.command_intake.active
            ),
            "syncDeliveredCount": self.sync_stats.delivered,
            "syncFailedCount": self.sync_stats.failed,
            "connectivityStatus": (
                "ONLINE"
                if self.mqtt.is_connected
                else ("DEGRADED" if self.started else "OFFLINE")
            ),
        }

    async def start(self) -> None:
        if not self.enabled or self.started:
            return
        device_id = self.settings.controller.device_id
        station_id = self.settings.controller.station_id
        environment = self.topics.environment
        simulated = self.settings.api.simulated
        addresses = tuple(
            int(x.strip())
            for x in self.settings.api.controller_addresses.split(",")
            if x.strip()
        ) or (1, 2)
        channel_mappings = safe_mappings_from_settings(self.settings, addresses)

        offline_payload = {
            "deviceId": device_id,
            "stationId": station_id,
            "status": "OFFLINE",
            "environment": environment,
            "simulated": simulated,
        }
        offline = build_envelope(
            event_type="DEVICE_OFFLINE",
            environment=environment,
            device_id=device_id,
            station_id=station_id,
            sequence=0,
            simulated=simulated,
            deduplication_key=f"status:{device_id}:offline",
            payload=offline_payload,
        )
        self.mqtt.set_will(
            self.topics.device_status(device_id),
            json.dumps(offline.to_dict(), separators=(",", ":")),
            qos=1,
            retain=True,
        )
        self.mqtt.set_connection_handler(self._on_connection)

        mapper = DeliveryMapper(
            topics=self.topics,
            device_id=device_id,
            station_id=station_id,
            environment=environment,
            simulated=simulated,
            channel_mappings=channel_mappings,
        )
        self.sync_worker = SyncWorker(
            session_factory=self.session_factory,
            mqtt=self.mqtt,
            mapper=mapper,
            batch_size=self.settings.mqtt.sync_batch_size,
            poll_interval_seconds=self.settings.mqtt.sync_poll_interval_seconds,
            stale_lock_seconds=self.settings.mqtt.sync_stale_lock_seconds,
            max_attempts=self.settings.mqtt.sync_max_attempts,
        )
        self.heartbeat = HeartbeatService(
            mqtt=self.mqtt,
            topics=self.topics,
            device_id=device_id,
            station_id=station_id,
            environment=environment,
            simulated=simulated,
            interval_seconds=self.settings.mqtt.heartbeat_interval_seconds,
            payload_provider=self.payload_provider,
        )
        self.fill_stream = LiveFillStream(
            session_factory=self.session_factory,
            mqtt=self.mqtt,
            topics=self.topics,
            fill_book=self.fill_book,
            device_id=device_id,
            station_id=station_id,
            environment=environment,
            simulated=simulated,
            poll_interval_seconds=self.settings.mqtt.sync_poll_interval_seconds,
            channel_mappings=channel_mappings,
        )
        if self.settings.mqtt.command_subscription_enabled:
            self.command_intake = CloudCommandIntake(
                session_factory=self.session_factory,
                mqtt=self.mqtt,
                topics=self.topics,
                station_id=station_id,
                device_id=device_id,
                environment=environment,
                simulated=simulated,
                allow_lab_simulator_commands=(
                    self.settings.safety.allow_lab_simulator_commands
                ),
                controller_loop=self.controller_loop,
            )

        try:
            await self.mqtt.connect()
        except MqttError as exc:
            logger.warning("mqtt_connect_failed", error=str(exc))
            # Still start workers; they wait for connectivity.
        self.sync_worker.start()
        self.heartbeat.start()
        if self.fill_stream is not None:
            self.fill_stream.start()
        if self.command_intake is not None and self.mqtt.is_connected:
            await self.command_intake.start()
        if self.mqtt.is_connected:
            await self._publish_online()
        self.started = True
        logger.info(
            "cloud_runtime_started",
            host=self.mqtt.metadata.host,
            commands=bool(self.command_intake),
            mqtt_connected=self.mqtt.is_connected,
            note="mqtt_connected=false is possible; workers wait for reconnect",
        )

    async def stop(self) -> None:
        if not self.started:
            return
        if self.heartbeat is not None:
            await self.heartbeat.stop()
        if self.fill_stream is not None:
            await self.fill_stream.stop()
        if self.sync_worker is not None:
            await self.sync_worker.stop()
        if self.command_intake is not None:
            await self.command_intake.stop()
        try:
            await self.mqtt.disconnect()
        except Exception as exc:
            logger.warning("mqtt_disconnect_error", error=str(exc))
        self.started = False

    async def _on_connection(self, meta: MqttConnectionMetadata) -> None:
        if meta.state is MqttConnectionState.CONNECTED:
            await self._publish_online()
            if (
                self.command_intake is not None
                and not self.command_intake.active
                and self.settings.mqtt.command_subscription_enabled
            ):
                await self.command_intake.start()

    async def _publish_online(self) -> None:
        device_id = self.settings.controller.device_id
        station_id = self.settings.controller.station_id
        environment = self.topics.environment
        payload = {
            "deviceId": device_id,
            "stationId": station_id,
            "status": "ONLINE",
            "environment": environment,
            "simulated": self.settings.api.simulated,
            "timestamp": datetime.now(UTC).isoformat(),
        }
        envelope = build_envelope(
            event_type="DEVICE_ONLINE",
            environment=environment,
            device_id=device_id,
            station_id=station_id,
            sequence=1,
            simulated=self.settings.api.simulated,
            deduplication_key=f"status:{device_id}:online",
            payload=payload,
        )
        try:
            await self.mqtt.publish(
                self.topics.device_status(device_id),
                json.dumps(envelope.to_dict(), separators=(",", ":")),
                qos=qos_for_event("DEVICE_ONLINE"),
                retain=True,
            )
            self.online_published = True
        except MqttError as exc:
            logger.warning("online_status_publish_failed", error=str(exc))

    async def pending_sync_snapshot(self) -> tuple[int, float | None, int, int]:
        async with unit_of_work(self.session_factory) as uow:
            pending = await uow.sync_queue.pending_count()
            age = await uow.sync_queue.oldest_pending_age_seconds()
            delivered = await uow.sync_queue.delivered_count()
            failed = await uow.sync_queue.failed_count()
        return pending, age, delivered, failed
