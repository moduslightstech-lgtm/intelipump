"""Map Phase 3 decoded Wayne application models to normalized pump events.

Inferences are documented on each result. Ambiguous CD1/DC1 and CD3/DC3
records do not force lifecycle state changes unless the caller resolves
direction with explicit context (not TRANS-ID collisions).

NOZIO lift/return events are edge-triggered against MapperContext.nozzle_out.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.constants import (
    DecodeStatus,
    MessageDirection,
    TransactionType,
)
from intelipump_fdc.protocol.dart.application.models import ApplicationTransaction
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.readiness import can_derive_ready, readiness_blockers


@dataclass(frozen=True, slots=True)
class MapperContext:
    """Optional caller context to resolve direction without TRANS collisions."""

    current_state: PumpState | None = None
    previous_wayne_status: int | None = None
    # Explicit resolution — never inferred from TRANS alone.
    resolve_as_dc1: bool = False
    resolve_as_cd1: bool = False
    resolve_as_dc3: bool = False
    resolve_as_cd3: bool = False
    outstanding_request_was_cd1_status: bool = False
    bus_direction: MessageDirection | None = None
    # Edge / READY inputs (mirrors PumpContext fields).
    nozzle_out: bool | None = None
    selected_nozzle: int | None = None
    communication_healthy: bool = False
    active_transaction_id: str | None = None
    has_unresolved_transaction: bool = False
    fault_code: int | None = None
    last_raw_wayne_status: int | None = None
    dispensed_volume_raw: int | None = None
    was_ready_derivable: bool = False

    @classmethod
    def from_pump_context(
        cls,
        context: PumpContext,
        *,
        resolve_as_dc1: bool = False,
        resolve_as_dc3: bool = False,
        resolve_as_cd1: bool = False,
        resolve_as_cd3: bool = False,
        outstanding_request_was_cd1_status: bool = False,
        bus_direction: MessageDirection | None = None,
    ) -> MapperContext:
        return cls(
            current_state=context.current_state,
            previous_wayne_status=context.last_raw_wayne_status,
            last_raw_wayne_status=context.last_raw_wayne_status,
            resolve_as_dc1=resolve_as_dc1,
            resolve_as_dc3=resolve_as_dc3,
            resolve_as_cd1=resolve_as_cd1,
            resolve_as_cd3=resolve_as_cd3,
            outstanding_request_was_cd1_status=outstanding_request_was_cd1_status,
            bus_direction=bus_direction,
            nozzle_out=context.nozzle_out,
            selected_nozzle=context.selected_nozzle,
            communication_healthy=context.communication_healthy,
            active_transaction_id=context.active_transaction_id,
            has_unresolved_transaction=context.has_unresolved_transaction,
            fault_code=context.fault_code,
            dispensed_volume_raw=context.dispensed_volume_raw,
            was_ready_derivable=context.was_ready_derivable,
        )


@dataclass(frozen=True, slots=True)
class MappedWayneObservation:
    event: PumpEvent
    observation: ObservationRef
    raw_wayne_status: int | None = None
    selected_nozzle: int | None = None
    logical_nozzle_raw: int | None = None
    nozzle_out: bool | None = None
    nozio_raw: int | None = None
    filling_price_raw: int | None = None
    completion_evidence_key: str | None = None
    awaiting_filling_complete: bool | None = None
    completion_inferred: bool = False
    allow_implicit_authorize_to_filling: bool = False
    filling_inferred_from_dc2: bool = False
    inferences: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


def map_wayne_observation(
    tx: ApplicationTransaction,
    *,
    context: MapperContext | None = None,
) -> MappedWayneObservation:
    """Map one decoded application transaction to a normalized event."""
    ctx = context or MapperContext()
    obs = ObservationRef(
        source_frame_raw_hex=tx.source_frame_raw_hex,
        transaction_id=tx.transaction_id,
        transaction_type=tx.transaction_type.value,
        raw_wayne_status=_extract_raw_status(tx),
        notes=tuple(tx.warnings),
    )

    if tx.transaction_type is TransactionType.AMBIGUOUS_CD1_OR_DC1:
        return _map_ambiguous_cd1_dc1(tx, obs, ctx)

    if tx.transaction_type is TransactionType.DC1_PUMP_STATUS:
        return _map_dc1(tx, obs, ctx)

    if tx.transaction_type in {
        TransactionType.DC3_NOZZLE_STATUS_PRICE,
        TransactionType.AMBIGUOUS_CD3_OR_DC3,
    }:
        return _map_dc3(tx, obs, ctx)

    if tx.transaction_type is TransactionType.DC2_FILLED_VOLUME_AMOUNT:
        return _map_dc2(tx, obs, ctx)

    if tx.transaction_type is TransactionType.DC5_ALARM:
        body = tx.decoded_body or {}
        fault = body.get("alarm_code")
        fault_code = int(fault) if isinstance(fault, int) else None
        return MappedWayneObservation(
            event=PumpEvent.FAULT_OBSERVED,
            observation=ObservationRef(
                source_frame_raw_hex=obs.source_frame_raw_hex,
                transaction_id=obs.transaction_id,
                transaction_type=obs.transaction_type,
                raw_wayne_status=fault_code,
                notes=obs.notes,
            ),
            raw_wayne_status=fault_code,
            inferences=("DC5 alarm maps to FAULT_OBSERVED.",),
        )

    if tx.transaction_type is TransactionType.DC9_PUMP_IDENTITY:
        return MappedWayneObservation(
            event=PumpEvent.PUMP_DISCOVERED,
            observation=obs,
            inferences=("DC9 pump identity maps to PUMP_DISCOVERED.",),
        )

    return MappedWayneObservation(
        event=PumpEvent.UNKNOWN_OBSERVATION,
        observation=obs,
        warnings=(
            f"No lifecycle mapping for {tx.transaction_type.value}; "
            "preserved as UNKNOWN_OBSERVATION.",
        ),
        inferences=(
            "Non-lifecycle or master-directed transaction does not change "
            "normalized pump state.",
        ),
    )


def map_wayne_status_code(
    status_code: int,
    *,
    previous_wayne_status: int | None = None,
    current_state: PumpState | None = None,
    source_frame_raw_hex: str | None = None,
    transaction_id: int | None = 0x01,
) -> MappedWayneObservation:
    """Map a resolved DC1 status byte (caller already proved direction)."""
    obs = ObservationRef(
        source_frame_raw_hex=source_frame_raw_hex,
        transaction_id=transaction_id,
        transaction_type=TransactionType.DC1_PUMP_STATUS.value,
        raw_wayne_status=status_code,
    )
    return _status_code_to_event(
        status_code,
        obs=obs,
        previous_wayne_status=previous_wayne_status,
        current_state=current_state,
        inferences=("Caller-resolved DC1 status byte.",),
    )


def _extract_raw_status(tx: ApplicationTransaction) -> int | None:
    body = tx.decoded_body
    if not body:
        return None
    raw_code = body.get("raw_code")
    if isinstance(raw_code, int):
        return raw_code
    dc1 = body.get("dc1_pump_status")
    if isinstance(dc1, dict):
        dc1_code = dc1.get("raw_code")
        if isinstance(dc1_code, int):
            return dc1_code
        # describe_wayne embeds name only in ambiguous body; status field for DC1
    status = body.get("status")
    if isinstance(status, int):
        return status
    return None


def _map_ambiguous_cd1_dc1(
    tx: ApplicationTransaction,
    obs: ObservationRef,
    ctx: MapperContext,
) -> MappedWayneObservation:
    direction = ctx.bus_direction or tx.direction
    resolve_dc1 = (
        ctx.resolve_as_dc1
        or ctx.outstanding_request_was_cd1_status
        or direction is MessageDirection.SLAVE_TO_MASTER
    )
    resolve_cd1 = ctx.resolve_as_cd1 or direction is MessageDirection.MASTER_TO_SLAVE

    if resolve_cd1 and not resolve_dc1:
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            raw_wayne_status=obs.raw_wayne_status,
            warnings=(
                "Resolved as CD1 command (master→slave); commands are not "
                "pump-state observations.",
            ),
            inferences=(
                "INFERENCE: direction/context selected CD1; no lifecycle event.",
            ),
        )

    if resolve_dc1 and not resolve_cd1:
        code = obs.raw_wayne_status
        if code is None:
            return MappedWayneObservation(
                event=PumpEvent.UNKNOWN_OBSERVATION,
                observation=obs,
                warnings=("DC1 resolution requested but raw status missing.",),
            )
        return _status_code_to_event(
            code,
            obs=obs,
            previous_wayne_status=ctx.previous_wayne_status,
            current_state=ctx.current_state,
            inferences=(
                "INFERENCE: ambiguous CD1/DC1 resolved as DC1 via caller "
                "direction/outstanding-request context (not TRANS collision).",
            ),
        )

    return MappedWayneObservation(
        event=PumpEvent.UNKNOWN_OBSERVATION,
        observation=obs,
        raw_wayne_status=obs.raw_wayne_status,
        warnings=(
            "Ambiguous CD1/DC1: direction/context insufficient; "
            "both interpretations preserved in decoded body; "
            "no state change forced.",
        ),
        inferences=(
            "TRANS 0x01 LNG=1 wire collision (Pump Interface pp. 13 and 20).",
        ),
    )


def _map_dc1(
    tx: ApplicationTransaction,
    obs: ObservationRef,
    ctx: MapperContext,
) -> MappedWayneObservation:
    code = obs.raw_wayne_status
    if code is None and tx.decoded_body and isinstance(tx.decoded_body.get("status"), int):
        code = tx.decoded_body["status"]
    if code is None:
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            warnings=("DC1 without status byte.",),
        )
    return _status_code_to_event(
        code,
        obs=obs,
        previous_wayne_status=ctx.previous_wayne_status,
        current_state=ctx.current_state,
        inferences=("Decoded DC1_PUMP_STATUS.",),
    )


def _map_dc2(
    tx: ApplicationTransaction,
    obs: ObservationRef,
    ctx: MapperContext,
) -> MappedWayneObservation:
    """DC2 is supporting volume/amount evidence; FILLING is primarily DC1."""
    body = tx.decoded_body or {}
    volume = body.get("volume") if isinstance(body.get("volume"), dict) else {}
    vol_raw = volume.get("raw_scaled") if isinstance(volume, dict) else None
    inferences = (
        "INFERENCE: TRANS 0x02 LNG=8 decoded as DC2 (VOL+AMO); "
        "maps to FILLING_UPDATED as supporting evidence. "
        "FILLING entry remains DC1-driven.",
    )
    warnings: tuple[str, ...] = ()
    # After AUTHORIZE the pump often emits DC2 before the next DC1 FILLING.
    # Infer FILLING_STARTED so the SM opens a sale and live ticks are not dropped.
    # Do not infer from a retained COMPLETED face (status 5) or while already FILLING.
    if (
        ctx.current_state in {PumpState.AUTHORIZED, PumpState.NOZZLE_UP}
        and isinstance(vol_raw, int)
        and vol_raw > 0
        and ctx.previous_wayne_status
        not in {
            int(WaynePumpStatus.FILLING),
            int(WaynePumpStatus.FILLING_COMPLETED),
        }
    ):
        return MappedWayneObservation(
            event=PumpEvent.FILLING_STARTED,
            observation=obs,
            filling_inferred_from_dc2=True,
            warnings=(
                "DC1 FILLING not yet observed; inferring FILLING_STARTED from DC2 "
                "volume while AUTHORIZED/NOZZLE_UP (marked inferred).",
            ),
            inferences=inferences,
        )
    return MappedWayneObservation(
        event=PumpEvent.FILLING_UPDATED,
        observation=obs,
        inferences=inferences,
        warnings=warnings,
    )


def _probe_ready_context(
    ctx: MapperContext,
    *,
    nozzle_out: bool,
    wayne_status: int | None = None,
) -> PumpContext:
    """Build a transient PumpContext for readiness checks."""
    return PumpContext(
        pump_id="mapper",
        dart_address=0,
        current_state=ctx.current_state or PumpState.RESET,
        last_raw_wayne_status=(
            wayne_status
            if wayne_status is not None
            else ctx.last_raw_wayne_status
            if ctx.last_raw_wayne_status is not None
            else ctx.previous_wayne_status
        ),
        nozzle_out=nozzle_out,
        communication_healthy=ctx.communication_healthy,
        active_transaction_id=ctx.active_transaction_id,
        has_unresolved_transaction=ctx.has_unresolved_transaction,
        fault_code=ctx.fault_code,
        was_ready_derivable=ctx.was_ready_derivable,
    )


def _map_dc3(
    tx: ApplicationTransaction,
    obs: ObservationRef,
    ctx: MapperContext,
) -> MappedWayneObservation:
    """DC3 requires proven pump→controller direction; nozzle edges only."""
    if ctx.resolve_as_cd3 or (
        (ctx.bus_direction or tx.direction) is MessageDirection.MASTER_TO_SLAVE
        and not ctx.resolve_as_dc3
    ):
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            warnings=(
                "Resolved as CD3 preset volume (master→slave); not a "
                "pump-state observation; nozzle/price not updated.",
            ),
            inferences=("INFERENCE: caller/direction selected CD3 interpretation.",),
        )

    direction = ctx.bus_direction or tx.direction
    resolved_dc3 = ctx.resolve_as_dc3 or (
        tx.transaction_type is TransactionType.DC3_NOZZLE_STATUS_PRICE
        and tx.decode_status is DecodeStatus.DECODED
        and direction is MessageDirection.SLAVE_TO_MASTER
    )
    if not resolved_dc3:
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            warnings=(
                "Ambiguous CD3/DC3 (TRANS 0x03 LNG=4): direction/context "
                "insufficient; nozzle/price fields are not used to force "
                "state or update nozzle context.",
            ),
            inferences=(
                "Pump Interface pp. 14 and 21 wire collision; "
                "passive ambiguous decode is not proof of direction.",
            ),
        )

    body = tx.decoded_body or {}
    nozzle_out = body.get("nozzle_out")
    selected = body.get("selected_logical_nozzle")
    selected_nozzle = int(selected) if isinstance(selected, int) else None
    logical_raw = body.get("logical_nozzle_raw")
    if not isinstance(logical_raw, int):
        logical_raw = selected_nozzle if selected_nozzle is not None else 0
    nozio_raw = body.get("nozio_raw")
    nozio_raw_i = int(nozio_raw) if isinstance(nozio_raw, int) else None
    price = body.get("price") if isinstance(body.get("price"), dict) else {}
    price_raw = price.get("raw_scaled") if isinstance(price, dict) else None
    price_raw_i = int(price_raw) if isinstance(price_raw, int) else None

    base_kwargs: dict[str, Any] = dict(
        observation=obs,
        selected_nozzle=selected_nozzle,
        logical_nozzle_raw=logical_raw if isinstance(logical_raw, int) else None,
        nozzle_out=nozzle_out if isinstance(nozzle_out, bool) else None,
        nozio_raw=nozio_raw_i,
        filling_price_raw=price_raw_i,
    )

    if not isinstance(nozzle_out, bool):
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            warnings=("DC3 body missing nozzle_out; preserved as unknown.",),
            **base_kwargs,
        )

    previous = ctx.nozzle_out
    # First observation: treat as edge from unknown → current.
    if previous is None:
        if nozzle_out:
            return MappedWayneObservation(
                event=PumpEvent.NOZZLE_LIFTED,
                inferences=(
                    "INFERENCE: first resolved DC3 with nozzle OUT → NOZZLE_LIFTED.",
                ),
                **base_kwargs,
            )
        return _maybe_ready_or_status(
            ctx,
            base_kwargs=base_kwargs,
            idle_event=PumpEvent.NOZZLE_STATUS_OBSERVED,
            idle_inference=(
                "INFERENCE: first resolved DC3 with nozzle IN; no lift edge."
            ),
        )

    # Edge: IN → OUT
    if previous is False and nozzle_out is True:
        return MappedWayneObservation(
            event=PumpEvent.NOZZLE_LIFTED,
            inferences=(
                "INFERENCE: NOZIO edge IN→OUT → NOZZLE_LIFTED.",
            ),
            **base_kwargs,
        )

    # Edge: OUT → IN
    if previous is True and nozzle_out is False:
        return _map_nozzle_return_edge(ctx, base_kwargs=base_kwargs)

    # Steady OUT: selection change vs status-only
    if previous is True and nozzle_out is True:
        if (
            selected_nozzle is not None
            and ctx.selected_nozzle is not None
            and selected_nozzle != ctx.selected_nozzle
        ):
            return MappedWayneObservation(
                event=PumpEvent.NOZZLE_SELECTION_CHANGED,
                inferences=(
                    "INFERENCE: nozzle remained OUT; selected logical nozzle "
                    f"changed {ctx.selected_nozzle}→{selected_nozzle}; "
                    "not a second NOZZLE_LIFTED.",
                ),
                **base_kwargs,
            )
        return MappedWayneObservation(
            event=PumpEvent.NOZZLE_STATUS_OBSERVED,
            inferences=(
                "INFERENCE: repeated DC3 nozzle OUT; no lift edge; "
                "context (NOZIO/price/nozzle) updated only.",
            ),
            **base_kwargs,
        )

    # Steady IN
    return _maybe_ready_or_status(
        ctx,
        base_kwargs=base_kwargs,
        idle_event=PumpEvent.NOZZLE_STATUS_OBSERVED,
        idle_inference=(
            "INFERENCE: repeated DC3 nozzle IN; no return edge; "
            "context updated only."
        ),
    )


def _map_nozzle_return_edge(
    ctx: MapperContext,
    *,
    base_kwargs: dict[str, Any],
) -> MappedWayneObservation:
    state = ctx.current_state
    if state in {PumpState.FILLING, PumpState.SUSPENDED}:
        frame = base_kwargs["observation"].source_frame_raw_hex or ""
        return MappedWayneObservation(
            event=PumpEvent.NOZZLE_RETURNED,
            awaiting_filling_complete=True,
            completion_evidence_key=f"nozzle_return_pending:{frame}",
            inferences=(
                "INFERENCE: NOZIO edge OUT→IN during "
                f"{state.value} → NOZZLE_RETURNED; expect DC1 "
                "FILLING_COMPLETED (Rev 2.11). Awaiting confirmation.",
            ),
            warnings=(
                "Nozzle hang-up observed; FILLING_COMPLETE pending DC1 "
                "confirmation (or inferred completion with audit).",
            ),
            **base_kwargs,
        )

    if state is PumpState.LIMIT_REACHED:
        frame = base_kwargs["observation"].source_frame_raw_hex or ""
        return MappedWayneObservation(
            event=PumpEvent.NOZZLE_RETURNED,
            awaiting_filling_complete=True,
            completion_evidence_key=f"limit_nozzle_return:{frame}",
            inferences=(
                "INFERENCE: nozzle return after LIMIT_REACHED → expect "
                "FILLING_COMPLETE; capture final DC2; completion idempotent.",
            ),
            **base_kwargs,
        )

    if state is PumpState.AUTHORIZED:
        volume = ctx.dispensed_volume_raw or 0
        if volume <= 0:
            return MappedWayneObservation(
                event=PumpEvent.NOZZLE_RETURNED,
                inferences=(
                    "INFERENCE: AUTHORIZED + nozzle return with no dispense → "
                    "cancel authorization (no completed paid sale).",
                ),
                warnings=("authorization_cancelled_no_dispense",),
                **base_kwargs,
            )

    # Idle / NOZZLE_UP / others: READY only on false→true readiness edge.
    return _maybe_ready_or_status(
        ctx,
        base_kwargs=base_kwargs,
        idle_event=PumpEvent.NOZZLE_RETURNED,
        idle_inference=(
            "INFERENCE: NOZIO edge OUT→IN → NOZZLE_RETURNED "
            "(idle/reset-derived unless READY gates pass)."
        ),
    )


def _maybe_ready_or_status(
    ctx: MapperContext,
    *,
    base_kwargs: dict[str, Any],
    idle_event: PumpEvent,
    idle_inference: str,
) -> MappedWayneObservation:
    probe = _probe_ready_context(ctx, nozzle_out=False)
    ready_now = can_derive_ready(probe)
    if ready_now and not ctx.was_ready_derivable:
        return MappedWayneObservation(
            event=PumpEvent.READY_OBSERVED,
            inferences=(
                "INFERENCE: application READY derived (Wayne has no READY). "
                "Gates: DC1=RESET, nozzle IN, healthy, no unresolved txn/fault. "
                "Emitted on readiness false→true edge only.",
            ),
            **base_kwargs,
        )
    if not ready_now and ctx.was_ready_derivable:
        blockers = readiness_blockers(probe)
        return MappedWayneObservation(
            event=idle_event,
            warnings=(
                "READY no longer derivable: " + ",".join(blockers),
            ),
            inferences=(idle_inference,),
            **base_kwargs,
        )
    return MappedWayneObservation(
        event=idle_event,
        inferences=(idle_inference,),
        **base_kwargs,
    )


def _status_code_to_event(
    status_code: int,
    *,
    obs: ObservationRef,
    previous_wayne_status: int | None,
    current_state: PumpState | None,
    inferences: tuple[str, ...],
) -> MappedWayneObservation:
    """Map Wayne DC1 STATUS bytes (Pump Interface Rev 2.11, page 20).

    Documented codes: 0,1,2,4,5,6,7,8. There is no status 3 in Rev 2.11.
    """
    try:
        status = WaynePumpStatus(status_code)
    except ValueError:
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            raw_wayne_status=status_code,
            warnings=(f"Unknown Wayne status 0x{status_code:02X}.",),
            inferences=inferences,
        )

    completion_key: str | None = None
    if status is WaynePumpStatus.FILLING_COMPLETED:
        frame = obs.source_frame_raw_hex or ""
        completion_key = f"complete:{frame}:{status_code}"

    if status is WaynePumpStatus.PUMP_NOT_PROGRAMMED:
        return MappedWayneObservation(
            event=PumpEvent.CONFIGURATION_MISSING,
            observation=obs,
            raw_wayne_status=status_code,
            inferences=(*inferences, "DC1 STATUS=0 → CONFIGURATION_MISSING."),
        )

    if status is WaynePumpStatus.RESET:
        return MappedWayneObservation(
            event=PumpEvent.RESET_OBSERVED,
            observation=obs,
            raw_wayne_status=status_code,
            inferences=(*inferences, "DC1 STATUS=1 RESET → RESET_OBSERVED."),
        )

    if status is WaynePumpStatus.AUTHORIZED:
        return MappedWayneObservation(
            event=PumpEvent.AUTHORIZATION_CONFIRMED,
            observation=obs,
            raw_wayne_status=status_code,
            inferences=(*inferences, "DC1 STATUS=2 → AUTHORIZATION_CONFIRMED."),
        )

    if status is WaynePumpStatus.FILLING:
        if previous_wayne_status == WaynePumpStatus.SUSPENDED:
            return MappedWayneObservation(
                event=PumpEvent.RESUMED_OBSERVED,
                observation=obs,
                raw_wayne_status=status_code,
                inferences=(
                    *inferences,
                    "INFERENCE: prior SUSPENDED + live FILLING → RESUMED_OBSERVED.",
                ),
            )
        # Only treat as update when already in FILLING normalized state.
        # Do not use previous_wayne_status alone — the simulator may pre-set
        # intended DC1 before mapping.
        if current_state is PumpState.FILLING:
            return MappedWayneObservation(
                event=PumpEvent.FILLING_UPDATED,
                observation=obs,
                raw_wayne_status=status_code,
                inferences=(
                    *inferences,
                    "Already filling; DC1 FILLING → FILLING_UPDATED "
                    "(idempotent; no new transaction).",
                ),
            )
        return MappedWayneObservation(
            event=PumpEvent.FILLING_STARTED,
            observation=obs,
            raw_wayne_status=status_code,
            allow_implicit_authorize_to_filling=False,
            inferences=(
                *inferences,
                "DC1 STATUS=4 FILLING → FILLING_STARTED (primary filling signal).",
            ),
        )

    if status is WaynePumpStatus.FILLING_COMPLETED:
        return MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=obs,
            raw_wayne_status=status_code,
            completion_evidence_key=completion_key,
            awaiting_filling_complete=False,
            inferences=(*inferences, "DC1 STATUS=5 → FILLING_COMPLETED."),
        )

    if status is WaynePumpStatus.MAX_AMOUNT_VOLUME_REACHED:
        return MappedWayneObservation(
            event=PumpEvent.LIMIT_REACHED,
            observation=obs,
            raw_wayne_status=status_code,
            inferences=(*inferences, "DC1 STATUS=6 → LIMIT_REACHED."),
        )

    if status is WaynePumpStatus.SWITCHED_OFF:
        return MappedWayneObservation(
            event=PumpEvent.SWITCHED_OFF_OBSERVED,
            observation=obs,
            raw_wayne_status=status_code,
            warnings=(
                "Wayne SWITCHED_OFF is not READY/RESET; preserved as "
                "SWITCHED_OFF_OBSERVED (offline/disabled).",
            ),
            inferences=(*inferences, "DC1 STATUS=7 → SWITCHED_OFF_OBSERVED."),
        )

    if status is WaynePumpStatus.SUSPENDED:
        return MappedWayneObservation(
            event=PumpEvent.SUSPENDED_OBSERVED,
            observation=obs,
            raw_wayne_status=status_code,
            inferences=(*inferences, "DC1 STATUS=8 → SUSPENDED_OBSERVED."),
        )

    return MappedWayneObservation(
        event=PumpEvent.UNKNOWN_OBSERVATION,
        observation=obs,
        raw_wayne_status=status_code,
        warnings=(f"Unhandled WaynePumpStatus {status.name}.",),
        inferences=inferences,
    )
