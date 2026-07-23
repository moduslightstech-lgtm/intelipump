"""Inbound cloud command intake (validate, evaluate, optionally LAB execute)."""

from __future__ import annotations

import contextlib
import json
from datetime import UTC, datetime
from typing import Any

import structlog
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.messages import build_envelope
from intelipump_fdc.cloud.mqtt.base import MqttClient
from intelipump_fdc.cloud.mqtt.models import MqttMessage
from intelipump_fdc.cloud.qos import qos_for_event
from intelipump_fdc.cloud.schemas import CloudCommandInbound
from intelipump_fdc.cloud.topics import TopicBuilder
from intelipump_fdc.controller.controller_loop import ControllerLoop
from intelipump_fdc.controller.outbound import OutboundQueueFullError, OutboundRejectedError
from intelipump_fdc.controller.session_models import IdempotencyClass, OutboundDataItem
from intelipump_fdc.domain.pump_command import NON_IDEMPOTENT_COMMANDS, PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.simulator.encoding import encode_cd1_command
from intelipump_fdc.state_machine.guards import evaluate_command_eligibility
from intelipump_fdc.state_machine.models import PumpContext

logger = structlog.get_logger(__name__)

_CD1_MAP: dict[PumpCommand, PumpControlCommand] = {
    PumpCommand.READ_STATUS: PumpControlCommand.RETURN_STATUS,
    PumpCommand.RESET: PumpControlCommand.RESET,
    PumpCommand.AUTHORIZE: PumpControlCommand.AUTHORIZE,
    PumpCommand.STOP: PumpControlCommand.STOP,
}


class CloudCommandIntake:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        mqtt: MqttClient,
        topics: TopicBuilder,
        station_id: str,
        device_id: str,
        environment: str,
        simulated: bool,
        allow_lab_simulator_commands: bool,
        controller_loop: ControllerLoop | None = None,
    ) -> None:
        self._factory = session_factory
        self._mqtt = mqtt
        self._topics = topics
        self._station_id = station_id
        self._device_id = device_id
        self._environment = environment.upper()
        self._simulated = simulated
        self._allow_lab = allow_lab_simulator_commands
        self._loop = controller_loop
        self._seen: set[str] = set()
        self.active = False
        self._seq = 0

    async def start(self) -> None:
        topic = self._topics.commands(self._station_id)
        self._mqtt.set_message_handler(self._on_message)
        await self._mqtt.subscribe(topic, qos=1)
        self.active = True

    async def stop(self) -> None:
        self.active = False
        with contextlib.suppress(Exception):
            await self._mqtt.unsubscribe(self._topics.commands(self._station_id))

    async def _on_message(self, message: MqttMessage) -> None:
        if not self.active:
            return
        try:
            raw = json.loads(message.payload.decode())
            cmd = CloudCommandInbound.model_validate(raw)
        except (json.JSONDecodeError, ValidationError, UnicodeDecodeError) as exc:
            logger.warning("cloud_command_invalid", error=str(exc))
            return
        await self.handle_command(cmd)

    async def handle_command(self, cmd: CloudCommandInbound) -> dict[str, Any]:
        reasons: list[str] = []
        if cmd.stationId != self._station_id:
            reasons.append("wrong_station")
        if cmd.environment.upper() != self._environment:
            reasons.append("wrong_environment")
        if cmd.schemaVersion != "1.0":
            reasons.append("unsupported_schema_version")
        if self._environment != "LAB" and cmd.simulatorOnly:
            reasons.append("simulator_only_requires_LAB")
        now = datetime.now(UTC)
        expires = cmd.expiresAt
        if expires is not None:
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=UTC)
            if expires <= now:
                reasons.append("expired")
        if cmd.correlationId in self._seen:
            reasons.append("duplicate_correlation_id")

        async with unit_of_work(self._factory) as uow:
            existing = await uow.commands.get(cmd.correlationId)
            if existing is not None:
                reasons.append("duplicate_persisted_command")

        executed = False
        execution_status = "NOT_EXECUTED"
        warnings: list[str] = []
        current_state = "UNKNOWN"
        resulting_state: str | None = None
        eligible = False

        try:
            command = PumpCommand(cmd.commandType)
        except ValueError:
            reasons.append("unknown_command_type")
            command = None

        pump_db_id: str | None = None
        ctx: PumpContext | None = None
        if not reasons and command is not None:
            async with unit_of_work(self._factory) as uow:
                pumps = await uow.pumps.list_for_station(self._station_id)
                pump = next(
                    (
                        p
                        for p in pumps
                        if p.logical_pump_id == cmd.pumpId
                        or p.id == cmd.pumpId
                        or str(p.dart_address) == cmd.pumpId
                    ),
                    None,
                )
                if pump is None:
                    reasons.append("pump_not_found")
                else:
                    pump_db_id = pump.id
                    snap = await uow.states.latest(pump.id)
                    if self._loop is not None:
                        session = self._loop.sessions.get(pump.dart_address)
                        if session is not None:
                            ctx = session.machine.context
                    if ctx is None:
                        try:
                            state = (
                                PumpState(snap.normalized_state)
                                if snap
                                else PumpState.DISCONNECTED
                            )
                        except ValueError:
                            state = PumpState.DISCONNECTED
                        ctx = PumpContext(
                            pump_id=pump.logical_pump_id,
                            dart_address=pump.dart_address,
                            current_state=state,
                            communication_healthy=bool(
                                snap and snap.communication_healthy
                            ),
                            state_version=snap.state_version if snap else 0,
                        )
                    result = evaluate_command_eligibility(
                        command,
                        ctx,
                        physical_enable_present=False,
                        active_commands_enabled=False,
                    )
                    eligible = result.eligible
                    current_state = result.current_state.value
                    warnings = list(result.warnings)
                    if not result.eligible:
                        reasons.extend(result.blocking_reasons)

        # Phase 9: production active commands never execute.
        if command is not None and command in NON_IDEMPOTENT_COMMANDS:
            if not (
                self._environment == "LAB"
                and cmd.simulatorOnly
                and self._allow_lab
                and self._loop is not None
                and self._loop.runtime.transport.metadata.is_virtual_or_memory
            ):
                execution_status = "EVALUATED_ONLY"
            elif not reasons and command in _CD1_MAP:
                # LAB simulator-only enqueue
                from dataclasses import replace

                item = OutboundDataItem.create(
                    address=ctx.dart_address if ctx else 1,
                    application_payload=encode_cd1_command(_CD1_MAP[command]),
                    command_type=command,
                    simulator_only=True,
                    idempotency=IdempotencyClass.NON_IDEMPOTENT,
                )
                safety = replace(
                    self._loop.runtime.safety, allow_lab_simulator_commands=True
                )
                try:
                    self._loop.runtime.outbound.enqueue(item, safety)
                    executed = True
                    execution_status = "QUEUED"
                    resulting_state = current_state
                except (OutboundRejectedError, OutboundQueueFullError) as exc:
                    reasons.append(f"enqueue_failed:{exc}")
                    execution_status = "ENQUEUE_FAILED"
            else:
                execution_status = "EVALUATED_ONLY"
        elif command in {PumpCommand.READ_STATUS, PumpCommand.READ_TOTALS}:
            if (
                self._environment == "LAB"
                and cmd.simulatorOnly
                and self._allow_lab
                and self._loop is not None
                and self._loop.runtime.transport.metadata.is_virtual_or_memory
                and not reasons
                and command in _CD1_MAP
                and ctx is not None
            ):
                from dataclasses import replace

                item = OutboundDataItem.create(
                    address=ctx.dart_address,
                    application_payload=encode_cd1_command(_CD1_MAP[command]),
                    command_type=command,
                    simulator_only=True,
                    idempotency=IdempotencyClass.IDEMPOTENT,
                )
                safety = replace(
                    self._loop.runtime.safety, allow_lab_simulator_commands=True
                )
                try:
                    self._loop.runtime.outbound.enqueue(item, safety)
                    executed = True
                    execution_status = "QUEUED"
                    resulting_state = current_state
                except (OutboundRejectedError, OutboundQueueFullError) as exc:
                    reasons.append(f"enqueue_failed:{exc}")
                    execution_status = "ENQUEUE_FAILED"
            else:
                execution_status = "EVALUATED_ONLY"
        else:
            execution_status = "EVALUATED_ONLY"

        accepted = not reasons or (
            eligible
            and execution_status in {"QUEUED", "EVALUATED_ONLY"}
            and "expired" not in reasons
            and "wrong_station" not in reasons
            and "duplicate_correlation_id" not in reasons
            and "duplicate_persisted_command" not in reasons
        )
        # Stricter: reject on validation failures
        rejected = any(
            r in reasons
            for r in (
                "wrong_station",
                "wrong_environment",
                "unsupported_schema_version",
                "expired",
                "duplicate_correlation_id",
                "duplicate_persisted_command",
                "unknown_command_type",
                "pump_not_found",
            )
        )
        if rejected:
            accepted = False
            execution_status = "REJECTED"

        self._seen.add(cmd.correlationId)

        async with unit_of_work(self._factory) as uow:
            if await uow.commands.get(cmd.correlationId) is None:
                await uow.commands.create(
                    correlation_id=cmd.correlationId,
                    station_id=self._station_id,
                    pump_id=pump_db_id,
                    command_type=cmd.commandType,
                    status=execution_status,
                    idempotency_class=(
                        "NON_IDEMPOTENT"
                        if command in NON_IDEMPOTENT_COMMANDS
                        else "IDEMPOTENT"
                    ),
                    simulator_only=cmd.simulatorOnly,
                    expires_at=cmd.expiresAt,
                    completed_at=datetime.now(UTC),
                    request_payload={
                        "commandId": cmd.commandId,
                        "requestedBy": cmd.requestedBy,
                        "payload": cmd.payload,
                    },
                    result_payload={
                        "accepted": accepted,
                        "executed": executed,
                        "execution_status": execution_status,
                    },
                    blocking_reasons=tuple(reasons),
                )
            await uow.audit.append(
                actor=cmd.requestedBy or "cloud",
                source="mqtt_command",
                action=f"CLOUD_COMMAND:{cmd.commandType}",
                station_id=self._station_id,
                pump_id=pump_db_id,
                previous_state=current_state,
                resulting_state=resulting_state or current_state,
                result=execution_status,
                correlation_id=cmd.correlationId,
                details={"blocking_reasons": reasons, "executed": executed},
            )

        result_payload = {
            "commandId": cmd.commandId,
            "correlationId": cmd.correlationId,
            "accepted": accepted,
            "evaluated": True,
            "executed": executed,
            "executionStatus": execution_status,
            "blockingReasons": reasons,
            "warnings": warnings,
            "currentPumpState": current_state,
            "resultingState": resulting_state,
            "environment": self._environment,
            "simulated": self._simulated or cmd.simulatorOnly,
            "timestamp": datetime.now(UTC).isoformat(),
        }
        self._seq += 1
        envelope = build_envelope(
            event_type="COMMAND_RESULT",
            environment=self._environment,
            device_id=self._device_id,
            station_id=self._station_id,
            sequence=self._seq,
            simulated=self._simulated or cmd.simulatorOnly,
            deduplication_key=f"cmd-result:{cmd.correlationId}",
            payload=result_payload,
            pump_id=cmd.pumpId,
            correlation_id=cmd.correlationId,
        )
        if self._mqtt.is_connected:
            topic = self._topics.command_result(self._station_id, cmd.correlationId)
            await self._mqtt.publish(
                topic,
                json.dumps(envelope.to_dict(), separators=(",", ":")),
                qos=qos_for_event("COMMAND_RESULT"),
            )
        return result_payload
