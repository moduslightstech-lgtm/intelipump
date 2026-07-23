"""Wayne pump status codes (application layer only).

Source: WAYNE EUROPE - Protocol Specification Dart Pump Interface Revision 2.11,
page 20 (DC1 Pump status).

This module does NOT implement the normalized InteliPump state machine or
command eligibility. It only maps documented Wayne status bytes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class WaynePumpStatus(IntEnum):
    """DC1 STATUS byte values (Pump Interface Rev 2.11, page 20)."""

    PUMP_NOT_PROGRAMMED = 0
    RESET = 1
    AUTHORIZED = 2
    FILLING = 4
    FILLING_COMPLETED = 5
    MAX_AMOUNT_VOLUME_REACHED = 6
    SWITCHED_OFF = 7
    SUSPENDED = 8


_STATUS_DESCRIPTIONS: dict[int, str] = {
    WaynePumpStatus.PUMP_NOT_PROGRAMMED: "PUMP NOT PROGRAMMED",
    WaynePumpStatus.RESET: "RESET",
    WaynePumpStatus.AUTHORIZED: "AUTHORIZED",
    WaynePumpStatus.FILLING: "FILLING",
    WaynePumpStatus.FILLING_COMPLETED: "FILLING COMPLETED",
    WaynePumpStatus.MAX_AMOUNT_VOLUME_REACHED: "MAX AMOUNT/VOLUME REACHED",
    WaynePumpStatus.SWITCHED_OFF: "SWITCHED OFF",
    WaynePumpStatus.SUSPENDED: "SUSPENDED",
}


@dataclass(frozen=True, slots=True)
class WayneStatusInfo:
    raw_code: int
    known: bool
    name: str | None
    description: str


def describe_wayne_status(raw_code: int) -> WayneStatusInfo:
    """Map a raw DC1 status byte to a documented description when known."""
    if raw_code in _STATUS_DESCRIPTIONS:
        status = WaynePumpStatus(raw_code)
        return WayneStatusInfo(
            raw_code=raw_code,
            known=True,
            name=status.name,
            description=_STATUS_DESCRIPTIONS[raw_code],
        )
    return WayneStatusInfo(
        raw_code=raw_code,
        known=False,
        name=None,
        description=f"UNKNOWN_STATUS_0x{raw_code:02X}",
    )
