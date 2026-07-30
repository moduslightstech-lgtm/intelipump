"""Deterministic pump state machine (pure; no I/O)."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.errors import TransitionSeverity
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext, TransitionResult
from intelipump_fdc.state_machine.readiness import can_derive_ready
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
        dispensed_volume_raw: int | None = None,
    ) -> TransitionResult:
        """Apply a mapper result (convenience for integration paths)."""
        return self.apply(
            mapped.event,
            observation=mapped.observation,
            observed_at=observed_at,
            selected_nozzle=mapped.selected_nozzle,
            logical_nozzle_raw=mapped.logical_nozzle_raw,
            nozzle_out=mapped.nozzle_out,
            nozio_raw=mapped.nozio_raw,
            filling_price_raw=mapped.filling_price_raw,
            active_transaction_id=active_transaction_id,
            raw_wayne_status=mapped.raw_wayne_status,
            price_verified=price_verified,
            fault_code=fault_code
            if fault_code is not None
            else mapped.raw_wayne_status
            if mapped.event is PumpEvent.FAULT_OBSERVED
            else None,
            completion_evidence_key=mapped.completion_evidence_key,
            awaiting_filling_complete=mapped.awaiting_filling_complete,
            completion_inferred=mapped.completion_inferred,
            dispensed_volume_raw=dispensed_volume_raw,
            allow_implicit_authorize_to_filling=(
                allow_implicit_authorize_to_filling
                if allow_implicit_authorize_to_filling is not None
                else mapped.allow_implicit_authorize_to_filling
            ),
            filling_inferred_from_dc2=mapped.filling_inferred_from_dc2,
            mapped_warnings=mapped.warnings,
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
        logical_nozzle_raw: int | None = None,
        nozzle_out: bool | None = None,
        nozio_raw: int | None = None,
        filling_price_raw: int | None = None,
        active_transaction_id: str | None = None,
        raw_wayne_status: int | None = None,
        price_verified: bool | None = None,
        fault_code: int | None = None,
        completion_evidence_key: str | None = None,
        awaiting_filling_complete: bool | None = None,
        completion_inferred: bool = False,
        dispensed_volume_raw: int | None = None,
        allow_implicit_authorize_to_filling: bool = False,
        filling_inferred_from_dc2: bool = False,
        mapped_warnings: tuple[str, ...] = (),
    ) -> TransitionResult:
        ctx = self._context
        warnings: list[str] = list(ctx.warnings)
        warnings.extend(mapped_warnings)

        # DC2 supporting evidence must not decrease volume mid-fueling.
        if (
            dispensed_volume_raw is not None
            and ctx.dispensed_volume_raw is not None
            and dispensed_volume_raw < ctx.dispensed_volume_raw
            and ctx.current_state
            in {PumpState.FILLING, PumpState.SUSPENDED, PumpState.LIMIT_REACHED}
        ):
            warnings.append(
                "Decreasing DC2 volume ignored: "
                f"{dispensed_volume_raw} < {ctx.dispensed_volume_raw}"
            )
            dispensed_volume_raw = None

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

        # Per-frame+tx duplicate: multi-tx DATA shares frame hex but each
        # TRANS must apply. Identity is frame|tid|ttype.
        obs_identity = None
        if observation is not None and observation.source_frame_raw_hex:
            obs_identity = (
                f"{observation.source_frame_raw_hex}|"
                f"{observation.transaction_id}|"
                f"{observation.transaction_type}"
            )
            if (
                obs_identity == ctx.last_source_frame_hex
                and event
                not in {
                    PumpEvent.NOZZLE_STATUS_OBSERVED,
                    PumpEvent.NOZZLE_SELECTION_CHANGED,
                    PumpEvent.FILLING_UPDATED,
                }
            ):
                warn = (
                    "Identical source-frame+transaction reference; "
                    "treated as duplicate observation"
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
                    reason="duplicate_source_frame",
                    severity=TransitionSeverity.INFO,
                    noop=True,
                    observation=observation,
                    warnings=(warn,),
                )
        # Duplicate completion evidence (idempotent sale close).
        if (
            event in {PumpEvent.FILLING_COMPLETED, PumpEvent.NOZZLE_RETURNED}
            and completion_evidence_key is not None
            and completion_evidence_key in ctx.completed_evidence_keys
            and ctx.current_state
            in {PumpState.FILLING_COMPLETE, PumpState.LIMIT_REACHED}
        ):
            warn = (
                f"Duplicate completion evidence ignored: {completion_evidence_key}"
            )
            new_ctx = self._apply_context_fields(
                ctx,
                observed_at=observed_at,
                selected_nozzle=selected_nozzle,
                logical_nozzle_raw=logical_nozzle_raw,
                nozzle_out=nozzle_out,
                nozio_raw=nozio_raw,
                filling_price_raw=filling_price_raw,
                active_transaction_id=active_transaction_id,
                raw_wayne_status=raw_wayne_status,
                price_verified=price_verified,
                fault_code=fault_code,
                observation=observation,
                completion_evidence_key=None,
                awaiting_filling_complete=False,
                completion_inferred=False,
                dispensed_volume_raw=dispensed_volume_raw,
                bump_version_on_context=False,
            )
            new_ctx = new_ctx.with_updates(warnings=tuple([*warnings, warn]))
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

        # Unknown observation never changes state (and must not rewrite DC1).
        if event is PumpEvent.UNKNOWN_OBSERVATION:
            warn = "Unknown observation preserved; state unchanged"
            new_ctx = self._apply_context_fields(
                ctx,
                observed_at=observed_at,
                selected_nozzle=None,  # do not update nozzle from ambiguous
                logical_nozzle_raw=None,
                nozzle_out=None,
                nozio_raw=None,
                filling_price_raw=None,
                active_transaction_id=active_transaction_id,
                raw_wayne_status=None,
                price_verified=None,
                fault_code=fault_code,
                observation=observation,
                completion_evidence_key=None,
                awaiting_filling_complete=None,
                completion_inferred=False,
                dispensed_volume_raw=dispensed_volume_raw,
                bump_version_on_context=bool(observation and observation.source_frame_raw_hex),
            )
            new_ctx = new_ctx.with_updates(warnings=tuple([*warnings, warn]))
            self._context = self._finalize_readiness(new_ctx)
            return TransitionResult(
                accepted=True,
                context=self._context,
                current_state=self._context.current_state,
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

        if filling_inferred_from_dc2:
            warnings.append(
                "FILLING inferred from DC2 while DC1 unavailable; audit marked"
            )

        # Same-state no-op (e.g. repeated READY / nozzle status).
        if is_same_state_noop(ctx.current_state, event):
            new_ctx = self._apply_context_fields(
                ctx,
                observed_at=observed_at,
                selected_nozzle=selected_nozzle,
                logical_nozzle_raw=logical_nozzle_raw,
                nozzle_out=nozzle_out,
                nozio_raw=nozio_raw,
                filling_price_raw=filling_price_raw,
                active_transaction_id=active_transaction_id,
                raw_wayne_status=raw_wayne_status,
                price_verified=price_verified,
                fault_code=fault_code,
                observation=observation,
                completion_evidence_key=None,
                awaiting_filling_complete=awaiting_filling_complete,
                completion_inferred=completion_inferred,
                dispensed_volume_raw=dispensed_volume_raw,
                bump_version_on_context=True,
            )
            if event is PumpEvent.COMMUNICATION_LOST:
                new_ctx = new_ctx.with_updates(communication_healthy=False)
            elif event is PumpEvent.COMMUNICATION_STARTED:
                new_ctx = new_ctx.with_updates(communication_healthy=True)
            if event is PumpEvent.FILLING_STARTED and new_ctx.fueling_session_uuid is None:
                new_ctx = new_ctx.with_updates(fueling_session_uuid=str(uuid4()))
            new_ctx = self._finalize_readiness(new_ctx)
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
                warnings=tuple(warnings) if warnings else (),
            )

        target = lookup_transition(ctx.current_state, event)
        if target is None:
            reason = (
                f"Invalid transition: {ctx.current_state.value} + {event.value}"
            )
            # Still refresh diagnostic fields when possible.
            new_ctx = self._apply_context_fields(
                ctx,
                observed_at=observed_at,
                selected_nozzle=selected_nozzle,
                logical_nozzle_raw=logical_nozzle_raw,
                nozzle_out=nozzle_out,
                nozio_raw=nozio_raw,
                filling_price_raw=filling_price_raw,
                active_transaction_id=None,
                raw_wayne_status=raw_wayne_status,
                price_verified=None,
                fault_code=None,
                observation=observation,
                completion_evidence_key=None,
                awaiting_filling_complete=None,
                completion_inferred=False,
                dispensed_volume_raw=dispensed_volume_raw,
                bump_version_on_context=False,
            )
            new_ctx = new_ctx.with_updates(warnings=tuple([*warnings, reason]))
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

        completed_keys = ctx.completed_evidence_keys
        if (
            event in {PumpEvent.FILLING_COMPLETED, PumpEvent.NOZZLE_RETURNED}
            and target is PumpState.FILLING_COMPLETE
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

        session_uuid = ctx.fueling_session_uuid
        if event is PumpEvent.FILLING_STARTED and session_uuid is None:
            session_uuid = str(uuid4())

        # Entering READY implies holstered nozzle + DC1 RESET + healthy link.
        if target is PumpState.READY:
            from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus

            if nozzle_out is None:
                nozzle_out = False
            if raw_wayne_status is None:
                raw_wayne_status = int(WaynePumpStatus.RESET)
            comm_healthy = True

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
            logical_nozzle_raw=(
                logical_nozzle_raw
                if logical_nozzle_raw is not None
                else ctx.logical_nozzle_raw
            ),
            nozzle_out=nozzle_out if nozzle_out is not None else ctx.nozzle_out,
            last_nozio_raw=nozio_raw if nozio_raw is not None else ctx.last_nozio_raw,
            last_filling_price_raw=(
                filling_price_raw
                if filling_price_raw is not None
                else ctx.last_filling_price_raw
            ),
            active_transaction_id=(
                active_transaction_id
                if active_transaction_id is not None
                else ctx.active_transaction_id
            ),
            fueling_session_uuid=session_uuid,
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
            awaiting_filling_complete=(
                awaiting_filling_complete
                if awaiting_filling_complete is not None
                else (
                    True
                    if event is PumpEvent.NOZZLE_RETURNED
                    and target is PumpState.FILLING_COMPLETE
                    else ctx.awaiting_filling_complete
                )
            ),
            completion_inferred=completion_inferred or ctx.completion_inferred,
            dispensed_volume_raw=(
                dispensed_volume_raw
                if dispensed_volume_raw is not None
                else ctx.dispensed_volume_raw
            ),
            last_source_frame_hex=(
                obs_identity
                if obs_identity is not None
                else (
                    observation.source_frame_raw_hex
                    if observation and observation.source_frame_raw_hex
                    else ctx.last_source_frame_hex
                )
            ),
            warnings=tuple(warnings),
        )
        if event is PumpEvent.FILLING_COMPLETED:
            new_ctx = new_ctx.with_updates(awaiting_filling_complete=False)

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
                PumpEvent.SWITCHED_OFF_OBSERVED,
                PumpEvent.NOZZLE_RETURNED,
            }
        ):
            new_ctx = new_ctx.with_updates(
                active_transaction_id=None,
                fueling_session_uuid=None,
                has_unresolved_transaction=False,
            )

        new_ctx = self._finalize_readiness(new_ctx)
        self._context = new_ctx
        return TransitionResult(
            accepted=True,
            context=new_ctx,
            current_state=new_ctx.current_state,
            previous_state=ctx.current_state,
            attempted_event=event,
            reason="transition_applied",
            severity=TransitionSeverity.INFO,
            observation=observation,
            warnings=tuple(warnings) if warnings else (),
        )

    def _finalize_readiness(self, ctx: PumpContext) -> PumpContext:
        """Track readiness edge; drop READY only when gates clearly fail.

        Nozzle OUT must not demote READY→RESET here: leave READY via
        NOZZLE_LIFTED → NOZZLE_UP. Syncing physical nozzle_out before the
        mapped lift edge would otherwise race and collapse READY to RESET.
        """
        from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus

        ready_now = can_derive_ready(ctx)
        # was_ready_derivable tracks claimed READY, not merely gate satisfaction,
        # so RESET+gates can still edge-trigger READY_OBSERVED.
        claimed = ctx.current_state is PumpState.READY and ready_now
        updates: dict[str, object] = {"was_ready_derivable": claimed}
        if ctx.current_state is PumpState.READY and not ready_now:
            clearly_false = (
                ctx.fault_code is not None
                or not ctx.communication_healthy
                or ctx.active_transaction_id is not None
                or ctx.has_unresolved_transaction
                or (
                    ctx.last_raw_wayne_status is not None
                    and ctx.last_raw_wayne_status != int(WaynePumpStatus.RESET)
                )
            )
            if clearly_false:
                updates["current_state"] = PumpState.RESET
                updates["previous_state"] = PumpState.READY
                updates["state_version"] = ctx.state_version + 1
                updates["was_ready_derivable"] = False
                updates["warnings"] = tuple(
                    [
                        *ctx.warnings,
                        "READY revoked: readiness predicate became false",
                    ]
                )
        return ctx.with_updates(**updates)

    def _apply_context_fields(
        self,
        ctx: PumpContext,
        *,
        observed_at: datetime | None,
        selected_nozzle: int | None,
        logical_nozzle_raw: int | None,
        nozzle_out: bool | None,
        nozio_raw: int | None,
        filling_price_raw: int | None,
        active_transaction_id: str | None,
        raw_wayne_status: int | None,
        price_verified: bool | None,
        fault_code: int | None,
        observation: ObservationRef | None,
        completion_evidence_key: str | None,
        awaiting_filling_complete: bool | None,
        completion_inferred: bool,
        dispensed_volume_raw: int | None,
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
            logical_nozzle_raw is not None
            and logical_nozzle_raw != ctx.logical_nozzle_raw
        ):
            updates["logical_nozzle_raw"] = logical_nozzle_raw
            meaningful = True
        if nozzle_out is not None and nozzle_out != ctx.nozzle_out:
            updates["nozzle_out"] = nozzle_out
            meaningful = True
        if nozio_raw is not None and nozio_raw != ctx.last_nozio_raw:
            updates["last_nozio_raw"] = nozio_raw
            meaningful = True
        if (
            filling_price_raw is not None
            and filling_price_raw != ctx.last_filling_price_raw
        ):
            updates["last_filling_price_raw"] = filling_price_raw
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
        if (
            dispensed_volume_raw is not None
            and dispensed_volume_raw != ctx.dispensed_volume_raw
        ):
            prior = ctx.dispensed_volume_raw
            if (
                prior is not None
                and dispensed_volume_raw < prior
                and ctx.current_state
                in {PumpState.FILLING, PumpState.SUSPENDED, PumpState.LIMIT_REACHED}
            ):
                # Non-decreasing volume during an active fueling session.
                # Keep prior; warning is emitted by apply() before this call.
                pass
            else:
                updates["dispensed_volume_raw"] = dispensed_volume_raw
                meaningful = True
        if awaiting_filling_complete is not None:
            updates["awaiting_filling_complete"] = awaiting_filling_complete
            meaningful = True
        if completion_inferred:
            updates["completion_inferred"] = True
            meaningful = True
        if observation and observation.source_frame_raw_hex:
            identity = (
                f"{observation.source_frame_raw_hex}|"
                f"{observation.transaction_id}|"
                f"{observation.transaction_type}"
            )
            updates["last_source_frame_hex"] = identity
        if completion_evidence_key is not None:
            updates["completed_evidence_keys"] = frozenset(
                {*ctx.completed_evidence_keys, completion_evidence_key}
            )
            meaningful = True
        if bump_version_on_context and meaningful:
            updates["state_version"] = ctx.state_version + 1
        return ctx.with_updates(**updates)
