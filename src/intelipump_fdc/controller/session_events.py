"""Typed in-memory controller events (Phase 6+; optional persistence subscribers)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from intelipump_fdc.domain.pump_state import PumpState


class ControllerEventType(StrEnum):
    FRAME_SENT = "FRAME_SENT"
    FRAME_RECEIVED = "FRAME_RECEIVED"
    FRAME_REJECTED = "FRAME_REJECTED"
    POLL_SENT = "POLL_SENT"
    DATA_RECEIVED = "DATA_RECEIVED"
    EOT_RECEIVED = "EOT_RECEIVED"
    ACK_SENT = "ACK_SENT"
    NAK_RECEIVED = "NAK_RECEIVED"
    RESPONSE_TIMEOUT = "RESPONSE_TIMEOUT"
    PUMP_CONNECTED = "PUMP_CONNECTED"
    PUMP_DISCONNECTED = "PUMP_DISCONNECTED"
    STATE_CHANGED = "STATE_CHANGED"
    APPLICATION_TRANSACTION_DECODED = "APPLICATION_TRANSACTION_DECODED"


@dataclass(frozen=True, slots=True)
class ControllerEvent:
    type: ControllerEventType
    address: int | None = None
    timestamp: datetime | None = None
    detail: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


EventSubscriber = Callable[["ControllerEvent"], None]


class EventBus:
    """In-memory fan-out for diagnostics and persistence subscribers."""

    def __init__(self) -> None:
        self._events: list[ControllerEvent] = []
        self._subscribers: list[EventSubscriber] = []

    def add_subscriber(self, subscriber: EventSubscriber) -> None:
        self._subscribers.append(subscriber)

    def remove_subscriber(self, subscriber: EventSubscriber) -> None:
        self._subscribers = [s for s in self._subscribers if s is not subscriber]

    def publish(self, event: ControllerEvent) -> None:
        self._events.append(event)
        for subscriber in self._subscribers:
            subscriber(event)

    def clear(self) -> None:
        self._events.clear()

    @property
    def events(self) -> tuple[ControllerEvent, ...]:
        return tuple(self._events)

    def of_type(self, event_type: ControllerEventType) -> tuple[ControllerEvent, ...]:
        return tuple(e for e in self._events if e.type is event_type)


@dataclass(frozen=True, slots=True)
class StateChangeNote:
    address: int
    previous: PumpState | None
    current: PumpState
