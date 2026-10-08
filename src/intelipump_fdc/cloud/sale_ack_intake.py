"""Subscribe to cloud SALE_COMMITTED application ACKs for completed sales."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import structlog

from intelipump_fdc.cloud.mqtt.base import MqttClient
from intelipump_fdc.cloud.mqtt.models import MqttMessage
from intelipump_fdc.cloud.sync_worker import SyncWorker
from intelipump_fdc.cloud.topics import TopicBuilder

logger = structlog.get_logger(__name__)


@dataclass
class SaleAckIntake:
    """Promote sync_queue AWAITING_APP_ACK → DELIVERED on SALE_COMMITTED."""

    mqtt: MqttClient
    topics: TopicBuilder
    device_id: str
    sync_worker: SyncWorker
    station_id: str | None = None
    active: bool = False
    _prior_handler: Any = None

    async def start(self) -> None:
        topic = self.topics.sale_acks(self.device_id)
        # Chain with any existing handler (command intake).
        prior = getattr(self.mqtt, "_handler", None)
        self._prior_handler = prior

        async def _dispatch(message: MqttMessage) -> None:
            # Strict device topic only — never accept another device's sale-acks.
            if message.topic == topic:
                await self._on_message(message)
                return
            if self._prior_handler is not None:
                result = self._prior_handler(message)
                if hasattr(result, "__await__"):
                    await result

        self.mqtt.set_message_handler(_dispatch)
        await self.mqtt.subscribe(topic, qos=1)
        self.active = True
        logger.info("sale_ack_subscription_active", topic=topic, deviceId=self.device_id)

    async def stop(self) -> None:
        if not self.active:
            return
        topic = self.topics.sale_acks(self.device_id)
        try:
            await self.mqtt.unsubscribe(topic)
        except Exception as exc:
            logger.warning("sale_ack_unsubscribe_failed", error=str(exc))
        if self._prior_handler is not None:
            self.mqtt.set_message_handler(self._prior_handler)
        self.active = False

    async def _on_message(self, message: MqttMessage) -> None:
        try:
            raw = json.loads(message.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("sale_ack_invalid_json", error=str(exc))
            return
        if not isinstance(raw, dict):
            return
        ack_device = str(
            raw.get("deviceId") or raw.get("device_id") or ""
        ).strip()
        if ack_device and ack_device != self.device_id:
            logger.warning(
                "sale_ack_wrong_device_ignored",
                expected=self.device_id,
                got=ack_device,
            )
            return
        ack_station = str(
            raw.get("stationId") or raw.get("station_id") or ""
        ).strip()
        if (
            self.station_id
            and ack_station
            and ack_station != self.station_id
        ):
            logger.warning(
                "sale_ack_wrong_station_ignored",
                expected=self.station_id,
                got=ack_station,
            )
            return
        await self.sync_worker.handle_sale_ack_payload(raw)
