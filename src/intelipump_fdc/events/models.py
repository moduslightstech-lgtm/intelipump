"""Live event models for the in-process broker."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class LiveEventType(StrEnum):
    CONNECTED = "CONNECTED"
    HEARTBEAT = "HEARTBEAT"
    PUMP_CONNECTED = "PUMP_CONNECTED"
    PUMP_DISCONNECTED = "PUMP_DISCONNECTED"
    PUMP_STATE_CHANGED = "PUMP_STATE_CHANGED"
    NOZZLE_LIFTED = "NOZZLE_LIFTED"
    AUTHORIZATION_CONFIRMED = "AUTHORIZATION_CONFIRMED"
    FILLING_STARTED = "FILLING_STARTED"
    FILLING_UPDATED = "FILLING_UPDATED"
    FILLING_COMPLETED = "FILLING_COMPLETED"
    TRANSACTION_CREATED = "TRANSACTION_CREATED"
    TRANSACTION_COMPLETED = "TRANSACTION_COMPLETED"
    ALARM_RAISED = "ALARM_RAISED"
    ALARM_CLEARED = "ALARM_CLEARED"
    PROTOCOL_ERROR = "PROTOCOL_ERROR"
    PERSISTENCE_WARNING = "PERSISTENCE_WARNING"
    CONTROLLER_RECOVERED = "CONTROLLER_RECOVERED"
    COMMAND_EVALUATED = "COMMAND_EVALUATED"
    SUBSCRIBER_DROPPED = "SUBSCRIBER_DROPPED"


CRITICAL_LIVE_EVENTS: frozenset[LiveEventType] = frozenset(
    {
        LiveEventType.TRANSACTION_COMPLETED,
        LiveEventType.ALARM_RAISED,
        LiveEventType.FILLING_COMPLETED,
        LiveEventType.CONTROLLER_RECOVERED,
    }
)


@dataclass(frozen=True, slots=True)
class LiveEvent:
    event_id: str
    event_type: LiveEventType
    timestamp: datetime
    station_id: str
    environment: str
    simulated: bool
    sequence: int
    pump_id: str | None = None
    transaction_id: str | None = None
    correlation_id: str | None = None
    severity: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    state_version: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "station_id": self.station_id,
            "pump_id": self.pump_id,
            "transaction_id": self.transaction_id,
            "correlation_id": self.correlation_id,
            "environment": self.environment,
            "simulated": self.simulated,
            "severity": self.severity,
            "payload": self.payload,
            "sequence": self.sequence,
            "state_version": self.state_version,
        }


@dataclass(frozen=True, slots=True)
class EventFilter:
    station_id: str | None = None
    pump_id: str | None = None
    event_types: frozenset[LiveEventType] | None = None
    severity: str | None = None
    simulated: bool | None = None

    def matches(self, event: LiveEvent) -> bool:
        if self.station_id is not None and event.station_id != self.station_id:
            return False
        if self.pump_id is not None and event.pump_id != self.pump_id:
            return False
        if self.event_types is not None and event.event_type not in self.event_types:
            return False
        if self.severity is not None and event.severity != self.severity:
            return False
        return not (self.simulated is not None and event.simulated is not self.simulated)
