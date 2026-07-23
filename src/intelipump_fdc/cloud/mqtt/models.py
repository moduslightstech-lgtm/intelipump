"""MQTT models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class MqttConnectionState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"


@dataclass(frozen=True, slots=True)
class MqttMessage:
    topic: str
    payload: bytes
    qos: int = 0
    retain: bool = False
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MqttPublishResult:
    topic: str
    acknowledged: bool
    mid: int | None = None


@dataclass
class MqttConnectionMetadata:
    state: MqttConnectionState = MqttConnectionState.DISCONNECTED
    host: str = ""
    port: int = 1883
    client_id: str = ""
    last_connected_at: datetime | None = None
    last_disconnected_at: datetime | None = None
    last_error: str | None = None
    reconnect_count: int = 0
    tls_enabled: bool = False
