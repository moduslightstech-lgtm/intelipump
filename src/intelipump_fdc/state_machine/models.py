"""Pump context and transition result models."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.errors import TransitionSeverity


@dataclass(frozen=True, slots=True)
class ObservationRef:
    """Reference to the observation that produced an event."""

    source_frame_raw_hex: str | None = None
    transaction_id: int | None = None
    transaction_type: str | None = None
    raw_wayne_status: int | None = None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PumpContext:
    pump_id: str
    dart_address: int
    current_state: PumpState = PumpState.DISCONNECTED
    previous_state: PumpState | None = None
    selected_nozzle: int | None = None
    active_transaction_id: str | None = None
    last_observation_at: datetime | None = None
    last_transition_at: datetime | None = None
    last_raw_wayne_status: int | None = None
    communication_healthy: bool = False
    price_verified: bool = False
    fault_code: int | None = None
    warnings: tuple[str, ...] = ()
    state_version: int = 0
    completed_evidence_keys: frozenset[str] = field(default_factory=frozenset)
    last_source_frame_hex: str | None = None

    def with_updates(self, **kwargs: Any) -> PumpContext:
        return replace(self, **kwargs)


@dataclass(frozen=True, slots=True)
class TransitionResult:
    accepted: bool
    context: PumpContext
    current_state: PumpState
    previous_state: PumpState | None
    attempted_event: PumpEvent
    reason: str
    severity: TransitionSeverity
    noop: bool = False
    observation: ObservationRef | None = None
    warnings: tuple[str, ...] = ()
