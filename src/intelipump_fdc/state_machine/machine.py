"""Deterministic pump state machine (pure; no I/O)."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.errors import TransitionSeverity
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext, TransitionResult
from intelipump_fdc.state_machine.transitions import is_same_state_noop, lookup_transition

if TYPE_CHECKING:
    from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation


class PumpStateMachine:
    """Apply normalized events to a pump context."""

    def __init__(self, context: PumpContext) -> None:
        self._context = context

    @property
    def context(self) -> PumpContext:
        return self._context

    def apply_mapped(
        self,
        mapped: MappedWayneObservation,
        *,
        observed_at: datetime | None = None,
        active_transaction_id: str | None = None,
        price_verified: bool | None = None,
        fault_code: int | None = None,
        allow_implicit_authorize_to_filling: bool | None = None,
    ) -> TransitionResult:
        """Apply a mapper result (convenience for integration paths)."""
        return self.apply(
            mapped.event,
            observation=mapped.observation,
            observed_at=observed_at,
            selected_nozzle=mapped.selected_nozzle,
            active_transaction_id=active_transaction_id,
            raw_wayne_status=mapped.raw_wayne_status,
            price_verified=price_verified,
            fault_code=fault_code
            if fault_code is not None
            else mapped.raw_wayne_status
            if mapped.event is PumpEvent.FAULT_OBSERVED
            else None,
            completion_evidence_key=mapped.completion_evidence_key,
            allow_implicit_authorize_to_filling=(
                allow_implicit_authorize_to_filling
                if allow_implicit_authorize_to_filling is not None
                else mapped.allow_implicit_authorize_to_filling
            ),
        )

    def replace_context(self, context: PumpContext) -> None:
        """Replace context (simulator sync / reconciliation helpers)."""
        self._context = context

    def apply(
        self,
        event: PumpEvent,
        *,
        observation: ObservationRef | None = None,
        observed_at: datetime | None = None,
        selected_nozzle: int | None = None,
        active_transaction_id: str | None = None,
        raw_wayne_status: int | None = None,
        price_verified: bool | None = None,
        fault_code: int | None = None,
        completion_evidence_key: str | None = None,
        allow_implicit_authorize_to_filling: bool = False,
    ) -> TransitionResult:
        ctx = self._context
        warnings: list[str] = list(ctx.warnings)

        # Stale observation: do not roll state backward.
        if (
            observed_at is not None
            and ctx.last_observation_at is not None
            and observed_at < ctx.last_observation_at
        ):
            warn = (
                f"Stale observation ignored for event {event.value}: "
                f"observed_at={observed_at.isoformat()} < "
                f"last_observation_at={ctx.last_observation_at.isoformat()}"
            )
            new_ctx = ctx.with_updates(warnings=tuple([*warnings, warn]))
            self._context = new_ctx
            return TransitionResult(
                accepted=False,
                context=new_ctx,
                current_state=ctx.current_state,
                previous_state=ctx.previous_state,
                attempted_event=event,
                reason="stale_observation",
                severity=TransitionSeverity.WARNING,
                observation=observation,
                warnings=(warn,),
            )

        # Identical source-frame replay (no-op accept).
        if (
            observation is not None
            and observation.source_frame_raw_hex is not None
            and observation.source_frame_raw_hex == ctx.last_source_frame_hex
        ):
            warn = "Identical source-frame reference; treated as duplicate observation"
            new_ctx = ctx.with_updates(
                last_observation_at=observed_at or ctx.last_observation_at,
                warnings=tuple([*warnings, warn]),
            )
            self._context = new_ctx
            return TransitionResult(
                accepted=True,
                context=new_ctx,
                current_state=ctx.current_state,
                previous_state=ctx.previous_state,
                attempted_event=event,
                reason="duplicate_source_frame",
                severity=TransitionSeverity.INFO,
                noop=True,
                observation=observation,
                warnings=(warn,),
            )

        # Duplicate completion evidence.
        if (
            event is PumpEvent.FILLING_COMPLETED
            and completion_evidence_key is not None
            and completion_evidence_key in ctx.completed_evidence_keys
        ):
            warn = (
                f"Duplicate completion evidence ignored: {completion_evidence_key}"
            )
            new_ctx = ctx.with_updates(
                last_observation_at=observed_at or ctx.last_observation_at,
                warnings=tuple([*warnings, warn]),
            )
            self._context = new_ctx
            return TransitionResult(
                accepted=True,
                context=new_ctx,
                current_state=ctx.current_state,
                previous_state=ctx.previous_state,
                attempted_event=event,
                reason="duplicate_completion_evidence",
                severity=TransitionSeverity.WARNING,
                noop=True,
                observation=observation,
                warnings=(warn,),
            )

        # Unknown observation never changes state.
        if event is PumpEvent.UNKNOWN_OBSERVATION:
            warn = "Unknown observation preserved; state unchanged"
            new_ctx = ctx.with_updates(
                last_observation_at=observed_at or ctx.last_observation_at,
                last_raw_wayne_status=(
                    raw_wayne_status
                    if raw_wayne_status is not None
                    else ctx.last_raw_wayne_status
                ),
                last_source_frame_hex=(
                    observation.source_frame_raw_hex
                    if observation and observation.source_frame_raw_hex
                    else ctx.last_source_frame_hex
                ),
                warnings=tuple([*warnings, warn]),
            )
            # Meaningful context change (raw status) may bump version.
            if (
                raw_wayne_status is not None
                and raw_wayne_status != ctx.last_raw_wayne_status
            ):
                new_ctx = new_ctx.with_updates(state_version=ctx.state_version + 1)
            self._context = new_ctx
            return TransitionResult(
                accepted=True,
                context=new_ctx,
                current_state=ctx.current_state,
                previous_state=ctx.previous_state,
                attempted_event=event,
                reason="unknown_observation_preserved",
                severity=TransitionSeverity.WARNING,
                noop=True,
                observation=observation,
                warnings=(warn,),
            )

        # Special guard: NOZZLE_UP + FILLING_STARTED requires explicit allowance.
        if (
            ctx.current_state is PumpState.NOZZLE_UP
            and event is PumpEvent.FILLING_STARTED
            and not allow_implicit_authorize_to_filling
        ):
            reason = (
                "NOZZLE_UP + FILLING_STARTED rejected without evidence of "
                "implicit authorization (set allow_implicit_authorize_to_filling)"
            )
            new_ctx = ctx.with_updates(warnings=tuple([*warnings, reason]))
            self._context = new_ctx
            return TransitionResult(
                accepted=False,
                context=new_ctx,
                current_state=ctx.current_state,
                previous_state=ctx.previous_state,
                attempted_event=event,
                reason="implicit_authorize_not_supported",
                severity=TransitionSeverity.WARNING,
                observation=observation,
                warnings=(reason,),
            )

        # Same-state no-op (e.g. repeated READY).
        if is_same_state_noop(ctx.current_state, event):
            new_ctx = self._apply_context_fields(
                ctx,
                observed_at=observed_at,
                selected_nozzle=selected_nozzle,
                active_transaction_id=active_transaction_id,
                raw_wayne_status=raw_wayne_status,
                price_verified=price_verified,
                fault_code=fault_code,
                observation=observation,
                completion_evidence_key=None,
                bump_version_on_context=True,
            )
            if event is PumpEvent.COMMUNICATION_LOST:
                new_ctx = new_ctx.with_updates(communication_healthy=False)
            elif event is PumpEvent.COMMUNICATION_STARTED:
                new_ctx = new_ctx.with_updates(communication_healthy=True)
            self._context = new_ctx
            return TransitionResult(
                accepted=True,
                context=new_ctx,
                current_state=ctx.current_state,
                previous_state=ctx.previous_state,
                attempted_event=event,
                reason="same_state_noop",
                severity=TransitionSeverity.INFO,
                noop=True,
                observation=observation,
            )

        target = lookup_transition(ctx.current_state, event)
        if target is None:
            reason = (
                f"Invalid transition: {ctx.current_state.value} + {event.value}"
            )
            new_ctx = ctx.with_updates(warnings=tuple([*warnings, reason]))
            self._context = new_ctx
            return TransitionResult(
                accepted=False,
                context=new_ctx,
                current_state=ctx.current_state,
                previous_state=ctx.previous_state,
                attempted_event=event,
                reason="invalid_transition",
                severity=TransitionSeverity.ERROR,
                observation=observation,
                warnings=(reason,),
            )

        # FAULT_CLEARED recovers to DISCOVERING (table) — never to FILLING.
        completed_keys = ctx.completed_evidence_keys
        if (
            event is PumpEvent.FILLING_COMPLETED
            and completion_evidence_key is not None
        ):
            completed_keys = frozenset({*completed_keys, completion_evidence_key})

        comm_healthy = ctx.communication_healthy
        if event is PumpEvent.COMMUNICATION_STARTED:
            comm_healthy = True
        elif event is PumpEvent.COMMUNICATION_LOST:
            comm_healthy = False

        clear_fault = event is PumpEvent.FAULT_CLEARED
        new_fault = None if clear_fault else (
            fault_code if fault_code is not None else ctx.fault_code
        )
        if event is PumpEvent.FAULT_OBSERVED and fault_code is not None:
            new_fault = fault_code

        new_ctx = ctx.with_updates(
            previous_state=ctx.current_state,
            current_state=target,
            state_version=ctx.state_version + 1,
            last_transition_at=observed_at or ctx.last_transition_at,
            last_observation_at=observed_at or ctx.last_observation_at,
            communication_healthy=comm_healthy,
            selected_nozzle=(
                selected_nozzle if selected_nozzle is not None else ctx.selected_nozzle
            ),
            active_transaction_id=(
                active_transaction_id
                if active_transaction_id is not None
                else ctx.active_transaction_id
            ),
            last_raw_wayne_status=(
                raw_wayne_status
                if raw_wayne_status is not None
                else ctx.last_raw_wayne_status
            ),
            price_verified=(
                price_verified if price_verified is not None else ctx.price_verified
            ),
            fault_code=new_fault,
            completed_evidence_keys=completed_keys,
            last_source_frame_hex=(
                observation.source_frame_raw_hex
                if observation and observation.source_frame_raw_hex
                else ctx.last_source_frame_hex
            ),
            warnings=tuple(warnings),
        )
        # Clear active transaction on reset/disconnect paths (keep on completion
        # for investigation until an explicit reset).
        if (
            target
            in {
                PumpState.RESET,
                PumpState.READY,
                PumpState.DISCONNECTED,
                PumpState.DISCOVERING,
            }
            and event
            in {
                PumpEvent.RESET_OBSERVED,
                PumpEvent.COMMUNICATION_LOST,
                PumpEvent.FAULT_CLEARED,
                PumpEvent.MAINTENANCE_EXITED,
            }
        ):
            new_ctx = new_ctx.with_updates(active_transaction_id=None)

        self._context = new_ctx
        return TransitionResult(
            accepted=True,
            context=new_ctx,
            current_state=target,
            previous_state=ctx.current_state,
            attempted_event=event,
            reason="transition_applied",
            severity=TransitionSeverity.INFO,
            observation=observation,
        )

    def _apply_context_fields(
        self,
        ctx: PumpContext,
        *,
        observed_at: datetime | None,
        selected_nozzle: int | None,
        active_transaction_id: str | None,
        raw_wayne_status: int | None,
        price_verified: bool | None,
        fault_code: int | None,
        observation: ObservationRef | None,
        completion_evidence_key: str | None,
        bump_version_on_context: bool,
    ) -> PumpContext:
        meaningful = False
        updates: dict[str, object] = {
            "last_observation_at": observed_at or ctx.last_observation_at,
        }
        if selected_nozzle is not None and selected_nozzle != ctx.selected_nozzle:
            updates["selected_nozzle"] = selected_nozzle
            meaningful = True
        if (
            active_transaction_id is not None
            and active_transaction_id != ctx.active_transaction_id
        ):
            updates["active_transaction_id"] = active_transaction_id
            meaningful = True
        if (
            raw_wayne_status is not None
            and raw_wayne_status != ctx.last_raw_wayne_status
        ):
            updates["last_raw_wayne_status"] = raw_wayne_status
            meaningful = True
        if price_verified is not None and price_verified != ctx.price_verified:
            updates["price_verified"] = price_verified
            meaningful = True
        if fault_code is not None and fault_code != ctx.fault_code:
            updates["fault_code"] = fault_code
            meaningful = True
        if observation and observation.source_frame_raw_hex:
            updates["last_source_frame_hex"] = observation.source_frame_raw_hex
        if completion_evidence_key is not None:
            updates["completed_evidence_keys"] = frozenset(
                {*ctx.completed_evidence_keys, completion_evidence_key}
            )
            meaningful = True
        if bump_version_on_context and meaningful:
            updates["state_version"] = ctx.state_version + 1
        return ctx.with_updates(**updates)
