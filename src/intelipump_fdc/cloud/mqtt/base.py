"""Abstract MQTT client."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Self

from intelipump_fdc.cloud.mqtt.models import (
    MqttConnectionMetadata,
    MqttMessage,
    MqttPublishResult,
)

MessageHandler = Callable[[MqttMessage], Awaitable[None] | None]
ConnectionHandler = Callable[[MqttConnectionMetadata], Awaitable[None] | None]


class MqttClient(ABC):
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def publish(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: int = 0,
        retain: bool = False,
    ) -> MqttPublishResult: ...

    @abstractmethod
    async def subscribe(self, topic: str, *, qos: int = 1) -> None: ...

    @abstractmethod
    async def unsubscribe(self, topic: str) -> None: ...

    @property
    @abstractmethod
    def is_connected(self) -> bool: ...

    @property
    @abstractmethod
    def metadata(self) -> MqttConnectionMetadata: ...

    @abstractmethod
    def set_message_handler(self, handler: MessageHandler | None) -> None: ...

    @abstractmethod
    def set_connection_handler(self, handler: ConnectionHandler | None) -> None: ...

    @abstractmethod
    def set_will(
        self, topic: str, payload: bytes | str, *, qos: int = 1, retain: bool = True
    ) -> None: ...

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.disconnect()
