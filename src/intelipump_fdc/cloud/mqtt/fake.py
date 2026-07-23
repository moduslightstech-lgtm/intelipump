"""In-memory fake MQTT client for unit tests."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any, ClassVar

from intelipump_fdc.cloud.mqtt.base import ConnectionHandler, MessageHandler, MqttClient
from intelipump_fdc.cloud.mqtt.errors import MqttNotConnectedError, MqttPublishError
from intelipump_fdc.cloud.mqtt.models import (
    MqttConnectionMetadata,
    MqttConnectionState,
    MqttMessage,
    MqttPublishResult,
)


class FakeMqttClient(MqttClient):
    """Process-local fake broker for deterministic tests."""

    _registry: ClassVar[dict[str, list[FakeMqttClient]]] = {}

    def __init__(self, *, host: str = "fake", client_id: str = "fake-client") -> None:
        self._meta = MqttConnectionMetadata(host=host, client_id=client_id)
        self._handler: MessageHandler | None = None
        self._conn_handler: ConnectionHandler | None = None
        self._subs: set[str] = set()
        self.published: list[MqttMessage] = []
        self._will: MqttMessage | None = None
        self._mid = 0
        self.fail_next_publish = False
        self.auto_ack = True

    @property
    def is_connected(self) -> bool:
        return self._meta.state is MqttConnectionState.CONNECTED

    @property
    def metadata(self) -> MqttConnectionMetadata:
        return self._meta

    def set_message_handler(self, handler: MessageHandler | None) -> None:
        self._handler = handler

    def set_connection_handler(self, handler: ConnectionHandler | None) -> None:
        self._conn_handler = handler

    def set_will(
        self, topic: str, payload: bytes | str, *, qos: int = 1, retain: bool = True
    ) -> None:
        data = payload.encode() if isinstance(payload, str) else payload
        self._will = MqttMessage(topic=topic, payload=data, qos=qos, retain=retain)

    async def connect(self) -> None:
        self._meta.state = MqttConnectionState.CONNECTING
        key = self._meta.host
        FakeMqttClient._registry.setdefault(key, []).append(self)
        self._meta.state = MqttConnectionState.CONNECTED
        self._meta.last_connected_at = datetime.now(UTC)
        self._meta.last_error = None
        if self._conn_handler:
            result = self._conn_handler(self._meta)
            if asyncio.iscoroutine(result):
                await result

    async def disconnect(self) -> None:
        key = self._meta.host
        peers = FakeMqttClient._registry.get(key, [])
        if self in peers:
            peers.remove(self)
        was = self.is_connected
        self._meta.state = MqttConnectionState.DISCONNECTED
        self._meta.last_disconnected_at = datetime.now(UTC)
        if was and self._will is not None:
            await self._fanout(self._will)
        if self._conn_handler:
            result = self._conn_handler(self._meta)
            if asyncio.iscoroutine(result):
                await result

    async def publish(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: int = 0,
        retain: bool = False,
    ) -> MqttPublishResult:
        if not self.is_connected:
            raise MqttNotConnectedError("not connected")
        if self.fail_next_publish:
            self.fail_next_publish = False
            raise MqttPublishError("forced publish failure")
        data = payload.encode() if isinstance(payload, str) else payload
        msg = MqttMessage(topic=topic, payload=data, qos=qos, retain=retain)
        self.published.append(msg)
        self._mid += 1
        await self._fanout(msg)
        return MqttPublishResult(
            topic=topic, acknowledged=self.auto_ack or qos == 0, mid=self._mid
        )

    async def subscribe(self, topic: str, *, qos: int = 1) -> None:
        del qos
        if not self.is_connected:
            raise MqttNotConnectedError("not connected")
        self._subs.add(topic)

    async def unsubscribe(self, topic: str) -> None:
        self._subs.discard(topic)

    async def inject(self, topic: str, payload: dict[str, Any] | bytes | str) -> None:
        if isinstance(payload, dict):
            data = json.dumps(payload).encode()
        elif isinstance(payload, str):
            data = payload.encode()
        else:
            data = payload
        await self._deliver(MqttMessage(topic=topic, payload=data, qos=1))

    async def _fanout(self, msg: MqttMessage) -> None:
        for peer in list(FakeMqttClient._registry.get(self._meta.host, [])):
            if peer is self:
                continue
            if any(_topic_match(sub, msg.topic) for sub in peer._subs):
                await peer._deliver(msg)

    async def _deliver(self, msg: MqttMessage) -> None:
        if self._handler is None:
            return
        result = self._handler(msg)
        if asyncio.iscoroutine(result):
            await result


def _topic_match(subscription: str, topic: str) -> bool:
    if subscription == topic:
        return True
    if subscription.endswith("/#"):
        prefix = subscription[:-2]
        return topic == prefix or topic.startswith(prefix + "/")
    if "+" in subscription:
        sp = subscription.split("/")
        tp = topic.split("/")
        if len(sp) != len(tp):
            return False
        return all(a == "+" or a == b for a, b in zip(sp, tp, strict=True))
    return False
