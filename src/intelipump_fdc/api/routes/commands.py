"""Command evaluation and LAB-only simulator command submission."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Request

from intelipump_fdc.api.dependencies import get_app_state
from intelipump_fdc.api.errors import ApiError
from intelipump_fdc.api.middleware import get_correlation_id
from intelipump_fdc.api.models.responses import (
    CommandEvaluateRequest,
    CommandEvaluateResponse,
    LabCommandRequest,
    LabCommandResponse,
)
from intelipump_fdc.api.state import AppState
from intelipump_fdc.controller.outbound import OutboundQueueFullError, OutboundRejectedError
from intelipump_fdc.controller.session_models import IdempotencyClass, OutboundDataItem
from intelipump_fdc.domain.pump_command import NON_IDEMPOTENT_COMMANDS, PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.events.models import LiveEventType
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.simulator.encoding import encode_cd1_command
from intelipump_fdc.state_machine.guards import evaluate_command_eligibility
from intelipump_fdc.state_machine.models import PumpContext

router = APIRouter(tags=["commands"])

_CD1_MAP: dict[PumpCommand, PumpControlCommand] = {
    PumpCommand.READ_STATUS: PumpControlCommand.RETURN_STATUS,
    PumpCommand.RESET: PumpControlCommand.RESET,
    PumpCommand.AUTHORIZE: PumpControlCommand.AUTHORIZE,
    PumpCommand.STOP: PumpControlCommand.STOP,
}


async def _resolve_pump_context(
    state: AppState, pump_id: str
) -> tuple[str, int, PumpContext]:
    async with unit_of_work(state.session_factory) as uow:
        pump = await uow.pumps.get_by_id(pump_id)
        if pump is None:
            for p in await uow.pumps.list_for_station(
                state.settings.controller.station_id
            ):
                if p.logical_pump_id == pump_id or str(p.dart_address) == pump_id:
                    pump = p
                    break
        if pump is None:
            raise ApiError(
                code="PUMP_NOT_FOUND",
                message=f"Pump was not found: {pump_id}",
                status_code=404,
            )
        snap = await uow.states.latest(pump.id)
        if state.controller_loop is not None:
            session = state.controller_loop.sessions.get(pump.dart_address)
            if session is not None:
                return pump.id, pump.dart_address, session.machine.context
        try:
            current = PumpState(snap.normalized_state) if snap else PumpState.DISCONNECTED
        except ValueError:
            current = PumpState.DISCONNECTED
        prev = None
        if snap and snap.previous_state:
            try:
                prev = PumpState(snap.previous_state)
            except ValueError:
                prev = None
        ctx = PumpContext(
            pump_id=pump.logical_pump_id,
            dart_address=pump.dart_address,
            current_state=current,
            previous_state=prev,
            selected_nozzle=snap.selected_nozzle if snap else None,
            active_transaction_id=snap.active_transaction_id if snap else None,
            communication_healthy=bool(snap and snap.communication_healthy),
            last_raw_wayne_status=snap.raw_wayne_status if snap else None,
            state_version=snap.state_version if snap else 0,
        )
        return pump.id, pump.dart_address, ctx


@router.post(
    "/pumps/{pump_id}/commands/evaluate",
    response_model=CommandEvaluateResponse,
)
async def evaluate_command(
    pump_id: str,
    body: CommandEvaluateRequest,
    request: Request,
    state: AppState = Depends(get_app_state),
) -> CommandEvaluateResponse:
    if state.shutting_down or not state.metrics.accepting_commands:
        raise ApiError(
            code="CONTROLLER_UNAVAILABLE",
            message="Controller is shutting down",
            status_code=503,
        )
    correlation_id = body.correlation_id or get_correlation_id(request)
    try:
        command = PumpCommand(body.command_type)
    except ValueError as exc:
        raise ApiError(
            code="VALIDATION_ERROR",
            message=f"Unknown command_type: {body.command_type}",
            status_code=422,
        ) from exc

    pump_db_id, _address, ctx = await _resolve_pump_context(state, pump_id)
    if body.nozzle_id is not None:
        ctx = ctx.with_updates(selected_nozzle=body.nozzle_id)

    preset = body.raw_preset_value
    result = evaluate_command_eligibility(
        command,
        ctx,
        preset_value=preset,
        physical_enable_present=False,
        active_commands_enabled=state.settings.safety.active_commands_enabled,
    )

    # Always persist evaluation / rejection for AUTHORIZE and non-eligible cmds.
    audit_id: str | None = None
    persisted = False
    async with unit_of_work(state.session_factory) as uow:
        status = "ELIGIBLE" if result.eligible else "REJECTED"
        await uow.commands.create(
            correlation_id=correlation_id,
            station_id=state.settings.controller.station_id,
            pump_id=pump_db_id,
            command_type=command.value,
            status=status,
            idempotency_class=(
                "NON_IDEMPOTENT"
                if command in NON_IDEMPOTENT_COMMANDS
                else "IDEMPOTENT"
            ),
            simulator_only=body.simulator_only,
            expires_at=body.expires_at,
            completed_at=datetime.now(UTC),
            request_payload={
                "nozzle_id": body.nozzle_id,
                "raw_preset_value": body.raw_preset_value,
                "preset_decimals": body.preset_decimals,
            },
            result_payload={"eligible": result.eligible},
            blocking_reasons=result.blocking_reasons,
        )
        audit = await uow.audit.append(
            actor="api",
            source="command_evaluate",
            action=f"COMMAND_EVALUATE:{command.value}",
            station_id=state.settings.controller.station_id,
            pump_id=pump_db_id,
            previous_state=ctx.current_state.value,
            resulting_state=ctx.current_state.value,
            result=status,
            correlation_id=correlation_id,
            details={
                "blocking_reasons": list(result.blocking_reasons),
                "warnings": list(result.warnings),
            },
        )
        audit_id = audit.id
        persisted = True

    state.broker.publish_typed(
        LiveEventType.COMMAND_EVALUATED,
        station_id=state.settings.controller.station_id,
        environment=state.settings.environment,
        simulated=state.settings.api.simulated,
        pump_id=ctx.pump_id,
        correlation_id=correlation_id,
        payload={
            "eligible": result.eligible,
            "command": command.value,
            "blocking_reasons": list(result.blocking_reasons),
        },
    )

    return CommandEvaluateResponse(
        eligible=result.eligible,
        current_state=result.current_state.value,
        blocking_reasons=list(result.blocking_reasons),
        warnings=list(result.warnings),
        requires_physical_enable=result.requires_physical_enable,
        requires_active_commands_enabled=result.requires_active_commands_enabled,
        command_persisted=persisted,
        audit_record_id=audit_id,
        correlation_id=correlation_id,
    )


@router.post(
    "/lab/pumps/{pump_id}/commands",
    response_model=LabCommandResponse,
)
async def lab_submit_command(
    pump_id: str,
    body: LabCommandRequest,
    request: Request,
    state: AppState = Depends(get_app_state),
) -> LabCommandResponse:
    """Enqueue LAB simulator-only commands on virtual/memory transport only."""
    correlation_id = body.correlation_id or get_correlation_id(request)
    reasons: list[str] = []

    if state.settings.environment.upper() != "LAB":
        reasons.append("environment_not_LAB")
    if not body.simulator_only:
        reasons.append("simulator_only_required")
    if not state.settings.safety.allow_lab_simulator_commands:
        reasons.append("allow_lab_simulator_commands_disabled")
    if state.settings.controller.mode.value == "LISTEN_ONLY" and body.command_type not in {
        PumpCommand.READ_STATUS.value,
        PumpCommand.READ_TOTALS.value,
    } and not state.settings.safety.allow_lab_simulator_commands:
        reasons.append("LISTEN_ONLY_blocks_active_commands")

    if state.controller_loop is None:
        reasons.append("controller_loop_not_attached")
    else:
        meta = state.controller_loop.runtime.transport.metadata
        if not meta.is_virtual_or_memory:
            reasons.append("transport_not_virtual_or_memory")

    try:
        command = PumpCommand(body.command_type)
    except ValueError as exc:
        raise ApiError(
            code="VALIDATION_ERROR",
            message=f"Unknown command_type: {body.command_type}",
            status_code=422,
        ) from exc

    if command not in _CD1_MAP:
        reasons.append(f"command_{command.value}_not_supported_for_lab_enqueue")

    if reasons:
        raise ApiError(
            code="SIMULATOR_ONLY_RESTRICTION",
            message="LAB simulator command rejected",
            status_code=403,
            details={"blocking_reasons": reasons},
        )

    assert state.controller_loop is not None
    _pump_db_id, address, _ctx = await _resolve_pump_context(state, pump_id)
    wire = encode_cd1_command(_CD1_MAP[command])
    item = OutboundDataItem.create(
        address=address,
        application_payload=wire,
        command_type=command,
        simulator_only=True,
        idempotency=(
            IdempotencyClass.NON_IDEMPOTENT
            if command in NON_IDEMPOTENT_COMMANDS
            else IdempotencyClass.IDEMPOTENT
        ),
    )
    from dataclasses import replace

    lab_safety = replace(
        state.controller_loop.runtime.safety,
        allow_lab_simulator_commands=True,
    )
    try:
        state.controller_loop.runtime.outbound.enqueue(item, lab_safety)
    except OutboundRejectedError as exc:
        raise ApiError(
            code="COMMAND_BLOCKED",
            message="Outbound safety rejected command",
            status_code=403,
            details={"blocking_reasons": list(exc.reasons)},
        ) from exc
    except OutboundQueueFullError as exc:
        raise ApiError(
            code="CONTROLLER_UNAVAILABLE",
            message="Outbound queue full",
            status_code=503,
        ) from exc

    async with unit_of_work(state.session_factory) as uow:
        await uow.commands.create(
            correlation_id=correlation_id,
            station_id=state.settings.controller.station_id,
            pump_id=_pump_db_id,
            command_type=command.value,
            status="QUEUED",
            idempotency_class=(
                "NON_IDEMPOTENT"
                if command in NON_IDEMPOTENT_COMMANDS
                else "IDEMPOTENT"
            ),
            simulator_only=True,
            expires_at=body.expires_at,
            request_payload={"address": address, "lab": True},
        )
        await uow.audit.append(
            actor="api",
            source="lab_command",
            action=f"LAB_COMMAND_QUEUED:{command.value}",
            station_id=state.settings.controller.station_id,
            pump_id=_pump_db_id,
            result="QUEUED",
            correlation_id=correlation_id,
            details={
                "transport_kind": state.controller_loop.runtime.transport.metadata.kind
            },
        )

    return LabCommandResponse(
        accepted=True,
        correlation_id=correlation_id,
        command_type=command.value,
        queued=True,
    )
