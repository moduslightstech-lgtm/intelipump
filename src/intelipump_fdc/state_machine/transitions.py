"""Explicit normalized pump state transition table.

Undocumented transitions are rejected. Same-state no-ops are handled in the
machine for repeated observations.
"""

from __future__ import annotations

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import OPERATIONAL_STATES, PumpState

# (from_state, event) -> to_state
TRANSITION_TABLE: dict[tuple[PumpState, PumpEvent], PumpState] = {
    (PumpState.DISCONNECTED, PumpEvent.COMMUNICATION_STARTED): PumpState.DISCOVERING,
    (PumpState.DISCOVERING, PumpEvent.CONFIGURATION_MISSING): PumpState.NOT_PROGRAMMED,
    (PumpState.DISCOVERING, PumpEvent.RESET_OBSERVED): PumpState.RESET,
    (PumpState.DISCOVERING, PumpEvent.READY_OBSERVED): PumpState.READY,
    (PumpState.NOT_PROGRAMMED, PumpEvent.RESET_OBSERVED): PumpState.RESET,
    (PumpState.NOT_PROGRAMMED, PumpEvent.READY_OBSERVED): PumpState.READY,
    (PumpState.RESET, PumpEvent.READY_OBSERVED): PumpState.READY,
    (PumpState.RESET, PumpEvent.CONFIGURATION_MISSING): PumpState.NOT_PROGRAMMED,
    (PumpState.READY, PumpEvent.NOZZLE_LIFTED): PumpState.NOZZLE_UP,
    (PumpState.READY, PumpEvent.RESET_OBSERVED): PumpState.RESET,
    (PumpState.NOZZLE_UP, PumpEvent.AUTHORIZATION_CONFIRMED): PumpState.AUTHORIZED,
    (PumpState.NOZZLE_UP, PumpEvent.NOZZLE_RETURNED): PumpState.READY,
    (PumpState.NOZZLE_UP, PumpEvent.FILLING_STARTED): PumpState.FILLING,
    (PumpState.AUTHORIZED, PumpEvent.FILLING_STARTED): PumpState.FILLING,
    (PumpState.AUTHORIZED, PumpEvent.NOZZLE_RETURNED): PumpState.READY,
    (PumpState.FILLING, PumpEvent.FILLING_UPDATED): PumpState.FILLING,
    (PumpState.FILLING, PumpEvent.FILLING_COMPLETED): PumpState.FILLING_COMPLETE,
    (PumpState.FILLING, PumpEvent.SUSPENDED_OBSERVED): PumpState.SUSPENDED,
    (PumpState.FILLING, PumpEvent.LIMIT_REACHED): PumpState.LIMIT_REACHED,
    (PumpState.SUSPENDED, PumpEvent.RESUMED_OBSERVED): PumpState.FILLING,
    (PumpState.SUSPENDED, PumpEvent.FILLING_COMPLETED): PumpState.FILLING_COMPLETE,
    (PumpState.SUSPENDED, PumpEvent.LIMIT_REACHED): PumpState.LIMIT_REACHED,
    (PumpState.LIMIT_REACHED, PumpEvent.FILLING_COMPLETED): PumpState.FILLING_COMPLETE,
    (PumpState.LIMIT_REACHED, PumpEvent.RESET_OBSERVED): PumpState.RESET,
    (PumpState.FILLING_COMPLETE, PumpEvent.RESET_OBSERVED): PumpState.RESET,
    (PumpState.FAULTED, PumpEvent.FAULT_CLEARED): PumpState.DISCOVERING,
    (PumpState.MAINTENANCE, PumpEvent.MAINTENANCE_EXITED): PumpState.DISCOVERING,
}

# PUMP_DISCOVERED: DISCOVERING -> RESET (default recovery target)
TRANSITION_TABLE[(PumpState.DISCOVERING, PumpEvent.PUMP_DISCOVERED)] = PumpState.RESET

# COMMUNICATION_LOST from every state including DISCONNECTED (stays / enters DISCONNECTED)
for state in PumpState:
    TRANSITION_TABLE[(state, PumpEvent.COMMUNICATION_LOST)] = PumpState.DISCONNECTED

# FAULT_OBSERVED from every operational state
for state in OPERATIONAL_STATES:
    TRANSITION_TABLE[(state, PumpEvent.FAULT_OBSERVED)] = PumpState.FAULTED

# MAINTENANCE_ENTERED from any state except already in MAINTENANCE (handled as no-op)
for state in PumpState:
    if state is not PumpState.MAINTENANCE:
        TRANSITION_TABLE[(state, PumpEvent.MAINTENANCE_ENTERED)] = PumpState.MAINTENANCE

# Events that are valid no-ops when already in the target state
SAME_STATE_NOOP_EVENTS: frozenset[tuple[PumpState, PumpEvent]] = frozenset(
    {
        (PumpState.READY, PumpEvent.READY_OBSERVED),
        (PumpState.RESET, PumpEvent.RESET_OBSERVED),
        (PumpState.FILLING, PumpEvent.FILLING_UPDATED),
        (PumpState.FILLING, PumpEvent.FILLING_STARTED),
        (PumpState.AUTHORIZED, PumpEvent.AUTHORIZATION_CONFIRMED),
        (PumpState.NOZZLE_UP, PumpEvent.NOZZLE_LIFTED),
        (PumpState.FILLING_COMPLETE, PumpEvent.FILLING_COMPLETED),
        (PumpState.SUSPENDED, PumpEvent.SUSPENDED_OBSERVED),
        (PumpState.LIMIT_REACHED, PumpEvent.LIMIT_REACHED),
        (PumpState.FAULTED, PumpEvent.FAULT_OBSERVED),
        (PumpState.DISCONNECTED, PumpEvent.COMMUNICATION_LOST),
        (PumpState.DISCOVERING, PumpEvent.COMMUNICATION_STARTED),
        (PumpState.MAINTENANCE, PumpEvent.MAINTENANCE_ENTERED),
        (PumpState.NOT_PROGRAMMED, PumpEvent.CONFIGURATION_MISSING),
    }
)


def lookup_transition(state: PumpState, event: PumpEvent) -> PumpState | None:
    return TRANSITION_TABLE.get((state, event))


def is_same_state_noop(state: PumpState, event: PumpEvent) -> bool:
    return (state, event) in SAME_STATE_NOOP_EVENTS
