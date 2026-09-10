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
    # After restart, live DC1 may show AUTHORIZED/FILLING before RESET/READY.
    # Accept these so persistence can open an ACTIVE sale (never auto-authorize).
    (PumpState.DISCOVERING, PumpEvent.AUTHORIZATION_CONFIRMED): PumpState.AUTHORIZED,
    (PumpState.DISCOVERING, PumpEvent.FILLING_STARTED): PumpState.FILLING,
    (PumpState.DISCOVERING, PumpEvent.NOZZLE_LIFTED): PumpState.NOZZLE_UP,
    (PumpState.NOT_PROGRAMMED, PumpEvent.RESET_OBSERVED): PumpState.RESET,
    (PumpState.NOT_PROGRAMMED, PumpEvent.READY_OBSERVED): PumpState.READY,
    # Documented Wayne CD5 price-accept path (DC1 0 → 5): not READY.
    (PumpState.NOT_PROGRAMMED, PumpEvent.FILLING_COMPLETED): PumpState.FILLING_COMPLETE,
    (PumpState.RESET, PumpEvent.READY_OBSERVED): PumpState.READY,
    (PumpState.RESET, PumpEvent.CONFIGURATION_MISSING): PumpState.NOT_PROGRAMMED,
    # Protocol-complete: AUTHORIZE may precede nozzle lift (Wayne RESET).
    (PumpState.RESET, PumpEvent.AUTHORIZATION_CONFIRMED): PumpState.AUTHORIZED,
    (PumpState.RESET, PumpEvent.FILLING_STARTED): PumpState.FILLING,
    (PumpState.READY, PumpEvent.NOZZLE_LIFTED): PumpState.NOZZLE_UP,
    # Repeated DC1 RESET while application READY is not a demotion.
    (PumpState.READY, PumpEvent.RESET_OBSERVED): PumpState.READY,
    (PumpState.READY, PumpEvent.AUTHORIZATION_CONFIRMED): PumpState.AUTHORIZED,
    (PumpState.NOZZLE_UP, PumpEvent.AUTHORIZATION_CONFIRMED): PumpState.AUTHORIZED,
    # Return before authorize → idle/reset-derived (READY only via predicate).
    (PumpState.NOZZLE_UP, PumpEvent.NOZZLE_RETURNED): PumpState.RESET,
    (PumpState.NOZZLE_UP, PumpEvent.FILLING_STARTED): PumpState.FILLING,
    (PumpState.AUTHORIZED, PumpEvent.FILLING_STARTED): PumpState.FILLING,
    # Cancel auth with no dispense; READY only if readiness edge follows.
    (PumpState.AUTHORIZED, PumpEvent.NOZZLE_RETURNED): PumpState.RESET,
    (PumpState.AUTHORIZED, PumpEvent.NOZZLE_LIFTED): PumpState.AUTHORIZED,
    (PumpState.FILLING, PumpEvent.FILLING_UPDATED): PumpState.FILLING,
    (PumpState.FILLING, PumpEvent.FILLING_COMPLETED): PumpState.FILLING_COMPLETE,
    # Hang-up: leave FILLING; await/confirm DC1 FILLING_COMPLETED.
    (PumpState.FILLING, PumpEvent.NOZZLE_RETURNED): PumpState.FILLING_COMPLETE,
    (PumpState.FILLING, PumpEvent.SUSPENDED_OBSERVED): PumpState.SUSPENDED,
    (PumpState.FILLING, PumpEvent.LIMIT_REACHED): PumpState.LIMIT_REACHED,
    (PumpState.SUSPENDED, PumpEvent.RESUMED_OBSERVED): PumpState.FILLING,
    (PumpState.SUSPENDED, PumpEvent.FILLING_COMPLETED): PumpState.FILLING_COMPLETE,
    (PumpState.SUSPENDED, PumpEvent.NOZZLE_RETURNED): PumpState.FILLING_COMPLETE,
    (PumpState.SUSPENDED, PumpEvent.LIMIT_REACHED): PumpState.LIMIT_REACHED,
    (PumpState.LIMIT_REACHED, PumpEvent.FILLING_COMPLETED): PumpState.FILLING_COMPLETE,
    (PumpState.LIMIT_REACHED, PumpEvent.NOZZLE_RETURNED): PumpState.FILLING_COMPLETE,
    (PumpState.LIMIT_REACHED, PumpEvent.RESET_OBSERVED): PumpState.RESET,
    (PumpState.FILLING_COMPLETE, PumpEvent.RESET_OBSERVED): PumpState.RESET,
    # Next sale may authorize/fill before RESET lands in the SM.
    (PumpState.FILLING_COMPLETE, PumpEvent.AUTHORIZATION_CONFIRMED): PumpState.AUTHORIZED,
    (PumpState.FILLING_COMPLETE, PumpEvent.FILLING_STARTED): PumpState.FILLING,
    (PumpState.FILLING_COMPLETE, PumpEvent.NOZZLE_LIFTED): PumpState.NOZZLE_UP,
    (PumpState.FAULTED, PumpEvent.FAULT_CLEARED): PumpState.DISCOVERING,
    (PumpState.MAINTENANCE, PumpEvent.MAINTENANCE_EXITED): PumpState.DISCOVERING,
}

TRANSITION_TABLE[(PumpState.DISCOVERING, PumpEvent.PUMP_DISCOVERED)] = PumpState.RESET

for state in PumpState:
    TRANSITION_TABLE[(state, PumpEvent.COMMUNICATION_LOST)] = PumpState.DISCONNECTED

for state in OPERATIONAL_STATES:
    TRANSITION_TABLE[(state, PumpEvent.FAULT_OBSERVED)] = PumpState.FAULTED

for state in PumpState:
    if state is not PumpState.MAINTENANCE:
        TRANSITION_TABLE[(state, PumpEvent.MAINTENANCE_ENTERED)] = PumpState.MAINTENANCE

# SWITCHED_OFF: explicit offline/disabled — disconnect path (not READY/RESET).
for state in OPERATIONAL_STATES:
    TRANSITION_TABLE[(state, PumpEvent.SWITCHED_OFF_OBSERVED)] = PumpState.DISCONNECTED

SAME_STATE_NOOP_EVENTS: frozenset[tuple[PumpState, PumpEvent]] = frozenset(
    {
        (PumpState.READY, PumpEvent.READY_OBSERVED),
        (PumpState.READY, PumpEvent.RESET_OBSERVED),
        (PumpState.RESET, PumpEvent.RESET_OBSERVED),
        (PumpState.FILLING, PumpEvent.FILLING_UPDATED),
        (PumpState.FILLING, PumpEvent.FILLING_STARTED),
        (PumpState.AUTHORIZED, PumpEvent.AUTHORIZATION_CONFIRMED),
        (PumpState.AUTHORIZED, PumpEvent.NOZZLE_LIFTED),
        (PumpState.NOZZLE_UP, PumpEvent.NOZZLE_LIFTED),
        (PumpState.NOZZLE_UP, PumpEvent.NOZZLE_SELECTION_CHANGED),
        (PumpState.AUTHORIZED, PumpEvent.NOZZLE_SELECTION_CHANGED),
        (PumpState.FILLING, PumpEvent.NOZZLE_SELECTION_CHANGED),
        (PumpState.FILLING_COMPLETE, PumpEvent.FILLING_COMPLETED),
        (PumpState.FILLING_COMPLETE, PumpEvent.NOZZLE_STATUS_OBSERVED),
        (PumpState.SUSPENDED, PumpEvent.SUSPENDED_OBSERVED),
        (PumpState.LIMIT_REACHED, PumpEvent.LIMIT_REACHED),
        (PumpState.FAULTED, PumpEvent.FAULT_OBSERVED),
        (PumpState.DISCONNECTED, PumpEvent.COMMUNICATION_LOST),
        (PumpState.DISCONNECTED, PumpEvent.SWITCHED_OFF_OBSERVED),
        (PumpState.DISCOVERING, PumpEvent.COMMUNICATION_STARTED),
        (PumpState.MAINTENANCE, PumpEvent.MAINTENANCE_ENTERED),
        (PumpState.NOT_PROGRAMMED, PumpEvent.CONFIGURATION_MISSING),
        (PumpState.READY, PumpEvent.NOZZLE_STATUS_OBSERVED),
        (PumpState.RESET, PumpEvent.NOZZLE_STATUS_OBSERVED),
        (PumpState.NOZZLE_UP, PumpEvent.NOZZLE_STATUS_OBSERVED),
        (PumpState.AUTHORIZED, PumpEvent.NOZZLE_STATUS_OBSERVED),
        (PumpState.FILLING, PumpEvent.NOZZLE_STATUS_OBSERVED),
        (PumpState.SUSPENDED, PumpEvent.NOZZLE_STATUS_OBSERVED),
        (PumpState.LIMIT_REACHED, PumpEvent.NOZZLE_STATUS_OBSERVED),
    }
)


def lookup_transition(state: PumpState, event: PumpEvent) -> PumpState | None:
    return TRANSITION_TABLE.get((state, event))


def is_same_state_noop(state: PumpState, event: PumpEvent) -> bool:
    return (state, event) in SAME_STATE_NOOP_EVENTS
