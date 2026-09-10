"""Paho-MQTT async wrapper (isolated behind MqttClient)."""

from __future__ import annotations

import asyncio
import ssl
from datetime import UTC, datetime
from typing import Any

import structlog

from intelipump_fdc.cloud.mqtt.base import ConnectionHandler, MessageHandler, MqttClient
from intelipump_fdc.cloud.mqtt.config import MqttClientConfig
from intelipump_fdc.cloud.mqtt.errors import MqttError, MqttNotConnectedError, MqttPublishError
from intelipump_fdc.cloud.mqtt.models import (
    MqttConnectionMetadata,
    MqttConnectionState,
    MqttMessage,
    MqttPublishResult,
)

logger = structlog.get_logger(__name__)


class PahoMqttClient(MqttClient):
    """Async facade over paho-mqtt (network I/O off the controller loop)."""

    def __init__(self, config: MqttClientConfig) -> None:
        self._config = config
        self._meta = MqttConnectionMetadata(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            tls_enabled=config.tls_enabled,
        )
        self._handler: MessageHandler | None = None
        self._conn_handler: ConnectionHandler | None = None
        self._client: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connected = asyncio.Event()
        self._will: tuple[str, bytes, int, bool] | None = None
        self._subscriptions: dict[str, int] = {}
        self._pending_acks: dict[int, asyncio.Future[bool]] = {}

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
        self._will = (topic, data, qos, retain)

    async def connect(self) -> None:
        import paho.mqtt.client as mqtt

        self._loop = asyncio.get_running_loop()
        self._meta.state = MqttConnectionState.CONNECTING
        logger.info(
            "mqtt_connecting",
            host=self._config.host,
            port=self._config.port,
            tls=self._config.tls_enabled,
            client_id=self._config.client_id,
            keepalive=self._config.keepalive_seconds,
        )
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,  # type: ignore[attr-defined]
            client_id=self._config.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=self._config.clean_session,
        )
        if self._config.username:
            # Password may be None for username-only brokers.
            client.username_pw_set(self._config.username, self._config.password)
        if self._config.tls_enabled:
            context = ssl.create_default_context(
                cafile=self._config.ca_file if self._config.ca_file else None
            )
            if self._config.client_cert and self._config.client_key:
                context.load_cert_chain(
                    self._config.client_cert, self._config.client_key
                )
            client.tls_set_context(context)
        if self._will is not None:
            topic, payload, qos, retain = self._will
            client.will_set(topic, payload=payload, qos=qos, retain=retain)

        client.reconnect_delay_set(
            min_delay=max(1, int(self._config.reconnect_min_delay_seconds)),
            max_delay=max(1, int(self._config.reconnect_max_delay_seconds)),
        )
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.on_publish = self._on_publish
        self._client = client
        client.connect_async(
            self._config.host,
            self._config.port,
            keepalive=self._config.keepalive_seconds,
        )
        client.loop_start()
        try:
            await asyncio.wait_for(
                self._connected.wait(), timeout=self._config.connect_timeout_seconds
            )
        except TimeoutError as exc:
            # Leave loop_start running — paho will keep reconnecting. Do not
            # imply cloud_runtime_started means MQTT is up.
            self._meta.last_error = "connect_timeout"
            self._meta.state = MqttConnectionState.DISCONNECTED
            logger.warning(
                "mqtt_reconnect_scheduled",
                host=self._config.host,
                port=self._config.port,
                reason="connect_timeout",
                min_delay=self._config.reconnect_min_delay_seconds,
                max_delay=self._config.reconnect_max_delay_seconds,
            )
            raise MqttError("MQTT connect timeout") from exc

    async def disconnect(self) -> None:
        if self._client is not None:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None
        self._connected.clear()
        self._meta.state = MqttConnectionState.DISCONNECTED
        self._meta.last_disconnected_at = datetime.now(UTC)

    async def publish(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: int = 0,
        retain: bool = False,
    ) -> MqttPublishResult:
        if not self.is_connected or self._client is None:
            raise MqttNotConnectedError("MQTT not connected")
        data = payload.encode() if isinstance(payload, str) else payload
        info = self._client.publish(topic, payload=data, qos=qos, retain=retain)
        if info.rc != 0:
            raise MqttPublishError(f"publish rc={info.rc}")
        acknowledged = True
        if qos > 0:
            fut: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
            self._pending_acks[info.mid] = fut
            try:
                acknowledged = await asyncio.wait_for(fut, timeout=10.0)
            except TimeoutError as exc:
                self._pending_acks.pop(info.mid, None)
                raise MqttPublishError("publish ack timeout") from exc
        return MqttPublishResult(topic=topic, acknowledged=acknowledged, mid=info.mid)

    async def subscribe(self, topic: str, *, qos: int = 1) -> None:
        if not self.is_connected or self._client is None:
            raise MqttNotConnectedError("MQTT not connected")
        self._client.subscribe(topic, qos=qos)
        self._subscriptions[topic] = qos

    async def unsubscribe(self, topic: str) -> None:
        if self._client is not None:
            self._client.unsubscribe(topic)
        self._subscriptions.pop(topic, None)

    def _on_connect(
        self,
        client: Any,
        _userdata: Any,
        _flags: Any,
        reason_code: Any,
        _props: Any = None,
    ) -> None:
        rc = int(getattr(reason_code, "value", reason_code))
        if rc == 0:
            was_reconnect = self._meta.reconnect_count > 0 or (
                self._meta.last_disconnected_at is not None
            )
            self._meta.state = MqttConnectionState.CONNECTED
            self._meta.last_connected_at = datetime.now(UTC)
            self._meta.last_error = None
            logger.info(
                "mqtt_reconnected" if was_reconnect else "mqtt_connected",
                host=self._config.host,
                port=self._config.port,
                tls=self._config.tls_enabled,
                client_id=self._config.client_id,
                reconnect_count=self._meta.reconnect_count,
            )
            if self._loop:
                self._loop.call_soon_threadsafe(self._connected.set)
                for topic, qos in list(self._subscriptions.items()):
                    client.subscribe(topic, qos=qos)
                if self._conn_handler:
                    self._loop.call_soon_threadsafe(
                        lambda: asyncio.create_task(self._notify_conn())
                    )
        else:
            self._meta.last_error = f"connect_rc={rc}"
            self._meta.state = MqttConnectionState.DISCONNECTED
            logger.warning(
                "mqtt_reconnect_scheduled",
                host=self._config.host,
                port=self._config.port,
                reason=f"connect_rc={rc}",
                min_delay=self._config.reconnect_min_delay_seconds,
                max_delay=self._config.reconnect_max_delay_seconds,
            )

    def _on_disconnect(
        self,
        _client: Any,
        _userdata: Any,
        _flags: Any,
        reason_code: Any,
        _props: Any = None,
    ) -> None:
        reason = f"disconnect_rc={getattr(reason_code, 'value', reason_code)}"
        self._meta.state = MqttConnectionState.DISCONNECTED
        self._meta.last_disconnected_at = datetime.now(UTC)
        self._meta.reconnect_count += 1
        self._meta.last_error = reason
        logger.warning(
            "mqtt_disconnected",
            host=self._config.host,
            port=self._config.port,
            client_id=self._config.client_id,
            reason=reason,
            reconnect_count=self._meta.reconnect_count,
        )
        logger.info(
            "mqtt_reconnect_scheduled",
            host=self._config.host,
            port=self._config.port,
            reason=reason,
            min_delay=self._config.reconnect_min_delay_seconds,
            max_delay=self._config.reconnect_max_delay_seconds,
        )
        if self._loop:
            self._loop.call_soon_threadsafe(self._connected.clear)
            if self._conn_handler:
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(self._notify_conn())
                )

    def _on_message(self, _client: Any, _userdata: Any, msg: Any) -> None:
        if self._handler is None or self._loop is None:
            return
        message = MqttMessage(
            topic=msg.topic,
            payload=bytes(msg.payload),
            qos=int(msg.qos),
            retain=bool(msg.retain),
        )

        async def _dispatch() -> None:
            assert self._handler is not None
            result = self._handler(message)
            if asyncio.iscoroutine(result):
                await result

        self._loop.call_soon_threadsafe(lambda: asyncio.create_task(_dispatch()))

    def _on_publish(self, _client: Any, _userdata: Any, mid: int, *_args: Any) -> None:
        fut = self._pending_acks.pop(mid, None)
        if fut is not None and not fut.done() and self._loop is not None:
            self._loop.call_soon_threadsafe(fut.set_result, True)

    async def _notify_conn(self) -> None:
        if self._conn_handler is None:
            return
        result = self._conn_handler(self._meta)
        if asyncio.iscoroutine(result):
            await result
