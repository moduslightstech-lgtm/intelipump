"""Map Phase 3 decoded Wayne application models to normalized pump events.

Inferences are documented on each result. Ambiguous CD1/DC1 and CD3/DC3
records do not force lifecycle state changes unless the caller resolves
direction with explicit context (not TRANS-ID collisions).
"""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.constants import (
    DecodeStatus,
    MessageDirection,
    TransactionType,
)
from intelipump_fdc.protocol.dart.application.models import ApplicationTransaction
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.models import ObservationRef


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


@dataclass(frozen=True, slots=True)
class MappedWayneObservation:
    event: PumpEvent
    observation: ObservationRef
    raw_wayne_status: int | None = None
    selected_nozzle: int | None = None
    completion_evidence_key: str | None = None
    allow_implicit_authorize_to_filling: bool = False
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

    if tx.transaction_type is TransactionType.DC3_NOZZLE_STATUS_PRICE:
        return _map_dc3(tx, obs, ctx)

    if tx.transaction_type is TransactionType.DC2_FILLED_VOLUME_AMOUNT:
        return MappedWayneObservation(
            event=PumpEvent.FILLING_UPDATED,
            observation=obs,
            inferences=(
                "INFERENCE: TRANS 0x02 LNG=8 decoded as DC2 (VOL+AMO); "
                "maps to FILLING_UPDATED for live volume/amount progress.",
            ),
        )

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

    # Master-side or non-lifecycle observations: preserve, do not force state.
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


def _map_dc3(
    tx: ApplicationTransaction,
    obs: ObservationRef,
    ctx: MapperContext,
) -> MappedWayneObservation:
    """DC3 is PARTIAL due to CD3 collision unless caller resolves it."""
    if ctx.resolve_as_cd3:
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            warnings=(
                "Resolved as CD3 preset volume (master→slave); not a "
                "pump-state observation.",
            ),
            inferences=("INFERENCE: caller selected CD3 interpretation.",),
        )

    # Phase-3 prefer-DC3 decode is PARTIAL and not proof of direction.
    # Require explicit resolve_as_dc3 (or DECODED + SLAVE_TO_MASTER).
    direction = ctx.bus_direction or tx.direction
    resolved_dc3 = ctx.resolve_as_dc3 or (
        tx.decode_status is DecodeStatus.DECODED
        and direction is MessageDirection.SLAVE_TO_MASTER
    )
    if not resolved_dc3:
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            warnings=(
                "Ambiguous CD3/DC3 (TRANS 0x03 LNG=4): direction/context "
                "insufficient; nozzle/price fields are not used to force "
                "state without resolve_as_dc3=True.",
            ),
            inferences=(
                "Pump Interface pp. 14 and 21 wire collision; "
                "passive prefer-DC3 is not proof of direction.",
            ),
        )

    body = tx.decoded_body or {}
    nozzle_out = body.get("nozzle_out")
    selected = body.get("selected_logical_nozzle")
    selected_nozzle = int(selected) if isinstance(selected, int) else None

    if not isinstance(nozzle_out, bool):
        return MappedWayneObservation(
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            selected_nozzle=selected_nozzle,
            warnings=("DC3 body missing nozzle_out; preserved as unknown.",),
        )

    if nozzle_out:
        return MappedWayneObservation(
            event=PumpEvent.NOZZLE_LIFTED,
            observation=obs,
            selected_nozzle=selected_nozzle,
            inferences=(
                "INFERENCE: resolved DC3 NOZIO bit0x10 set → NOZZLE_LIFTED. "
                "Wayne has no dedicated READY status; nozzle-out drives "
                "NOZZLE_UP path.",
            ),
        )

    # Nozzle in / holstered.
    if ctx.current_state in {
        PumpState.NOZZLE_UP,
        PumpState.AUTHORIZED,
        PumpState.FILLING,
        PumpState.SUSPENDED,
    }:
        return MappedWayneObservation(
            event=PumpEvent.NOZZLE_RETURNED,
            observation=obs,
            selected_nozzle=selected_nozzle,
            inferences=(
                "INFERENCE: resolved DC3 nozzle_out=false while operational "
                "nozzle/fueling state → NOZZLE_RETURNED.",
            ),
        )

    return MappedWayneObservation(
        event=PumpEvent.READY_OBSERVED,
        observation=obs,
        selected_nozzle=selected_nozzle,
        inferences=(
            "INFERENCE: Wayne DC1 has no READY code. Resolved DC3 with "
            "nozzle_out=false is mapped to READY_OBSERVED (idle/holstered).",
        ),
    )


def _status_code_to_event(
    status_code: int,
    *,
    obs: ObservationRef,
    previous_wayne_status: int | None,
    current_state: PumpState | None,
    inferences: tuple[str, ...],
) -> MappedWayneObservation:
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
        if current_state is PumpState.FILLING or previous_wayne_status == (
            WaynePumpStatus.FILLING
        ):
            return MappedWayneObservation(
                event=PumpEvent.FILLING_UPDATED,
                observation=obs,
                raw_wayne_status=status_code,
                inferences=(
                    *inferences,
                    "Already filling; DC1 FILLING → FILLING_UPDATED.",
                ),
            )
        return MappedWayneObservation(
            event=PumpEvent.FILLING_STARTED,
            observation=obs,
            raw_wayne_status=status_code,
            # Implicit auth path only when evidence exists (caller may enable).
            allow_implicit_authorize_to_filling=False,
            inferences=(
                *inferences,
                "DC1 STATUS=4 FILLING → FILLING_STARTED. "
                "NOZZLE_UP→FILLING still requires machine flag for implicit auth.",
            ),
        )

    if status is WaynePumpStatus.FILLING_COMPLETED:
        return MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=obs,
            raw_wayne_status=status_code,
            completion_evidence_key=completion_key,
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
            event=PumpEvent.UNKNOWN_OBSERVATION,
            observation=obs,
            raw_wayne_status=status_code,
            warnings=(
                "Wayne SWITCHED_OFF has no dedicated normalized event; "
                "preserved as UNKNOWN_OBSERVATION (not forced to MAINTENANCE).",
            ),
            inferences=inferences,
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
