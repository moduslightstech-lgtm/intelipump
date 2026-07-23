"""Normalized InteliPump pump states."""

from __future__ import annotations

from enum import StrEnum


class PumpState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    DISCOVERING = "DISCOVERING"
    NOT_PROGRAMMED = "NOT_PROGRAMMED"
    RESET = "RESET"
    READY = "READY"
    NOZZLE_UP = "NOZZLE_UP"
    AUTHORIZED = "AUTHORIZED"
    FILLING = "FILLING"
    FILLING_COMPLETE = "FILLING_COMPLETE"
    SUSPENDED = "SUSPENDED"
    LIMIT_REACHED = "LIMIT_REACHED"
    FAULTED = "FAULTED"
    MAINTENANCE = "MAINTENANCE"


OPERATIONAL_STATES: frozenset[PumpState] = frozenset(
    {
        PumpState.DISCOVERING,
        PumpState.NOT_PROGRAMMED,
        PumpState.RESET,
        PumpState.READY,
        PumpState.NOZZLE_UP,
        PumpState.AUTHORIZED,
        PumpState.FILLING,
        PumpState.FILLING_COMPLETE,
        PumpState.SUSPENDED,
        PumpState.LIMIT_REACHED,
        PumpState.FAULTED,
        PumpState.MAINTENANCE,
    }
)
