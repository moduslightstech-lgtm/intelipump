"""QoS selection policy (documented; business exactly-once via dedupe keys)."""

from __future__ import annotations

_QOS: dict[str, int] = {
    "HEARTBEAT": 0,
    "DEVICE_ONLINE": 1,
    "DEVICE_OFFLINE": 1,
    "STATE_CHANGED": 1,
    "PUMP_STATE_CHANGED": 1,
    "FILLING_UPDATED": 0,
    "FILLING_STARTED": 1,
    "FILLING_COMPLETED": 1,
    "TRANSACTION_STARTED": 1,
    "TRANSACTION_COMPLETED": 1,
    "POSSIBLE_UNINTENDED_FLOW": 1,
    "CANCELLED_NO_SALE": 1,
    "ALARM_ACTIVE": 1,
    "ALARM_RAISED": 1,
    "ALARM_CLEARED": 1,
    "AUDIT_EVENT": 1,
    "COMMAND_RESULT": 1,
    "COMMAND_INTAKE": 1,
}


def qos_for_event(event_type: str, *, default: int = 1) -> int:
    return _QOS.get(event_type, default)
