"""Inbound cloud command intake (validate, evaluate, optionally LAB execute)."""

from __future__ import annotations

import asyncio
import contextlib
import os
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
from intelipump_fdc.cloud.set_price_ownership import local_set_price_pump_ids
from intelipump_fdc.cloud.set_price_request import (
    SetPriceRequest,
    ack_set_price_outcome,
    list_set_price_outcomes,
    parse_prices_from_payload,
    pending_request_blocks_outcome_ack,
    write_set_price_request,
)
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
from intelipump_fdc.cloud.mqtt.errors import MqttError, MqttNotConnectedError, MqttPublishError
from intelipump_fdc.cloud.meter_result_publisher import MeterResultPublisher
from intelipump_fdc.controller.meter_read_request import (
    MeterReadRequest,
    write_meter_read_request,
)
from intelipump_fdc.core.config import MeterReadingSettings
from intelipump_fdc.services import meter_reading as meter_svc

logger = structlog.get_logger(__name__)

_CD1_MAP: dict[PumpCommand, PumpControlCommand] = {
    PumpCommand.READ_STATUS: PumpControlCommand.RETURN_STATUS,
    PumpCommand.RESET: PumpControlCommand.RESET,
    PumpCommand.AUTHORIZE: PumpControlCommand.AUTHORIZE,
    PumpCommand.STOP: PumpControlCommand.STOP,
}


# Back-compat for tests that import the old private name.
def _local_set_price_pump_ids(*, device_id: str, pumps: list[Any]) -> set[str]:
    return local_set_price_pump_ids(device_id=device_id, pumps=pumps)


def _normalize_cloud_environment(value: str) -> str:
    env = (value or "").strip().upper()
    if env in {"PROD", "PRODUCTION"}:
        return "PRODUCTION"
    return env


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
        allow_production_remote_set_price: bool = False,
        meter_reading_settings: MeterReadingSettings | None = None,
        channel_mappings: dict[int, Any] | None = None,
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
        self._allow_production_set_price = bool(allow_production_remote_set_price)
        self._meter_settings = meter_reading_settings or MeterReadingSettings()
        self._channel_mappings = channel_mappings or {}
        self._meter_results = MeterResultPublisher(
            session_factory=session_factory,
            station_id=station_id,
            device_id=device_id,
            meter_settings=self._meter_settings,
        )
        self._seen: set[str] = set()
        self.active = False
        self._seq = 0
        self._outcome_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        topic = self._topics.commands(self._station_id)
        self._mqtt.set_message_handler(self._on_message)
        await self._mqtt.subscribe(topic, qos=1)
        self.active = True
        if self._outcome_task is None or self._outcome_task.done():
            self._outcome_task = asyncio.create_task(
                self._outcome_loop(), name="set-price-outcome-publisher"
            )

    async def stop(self) -> None:
        self.active = False
        if self._outcome_task is not None:
            self._outcome_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._outcome_task
            self._outcome_task = None
        with contextlib.suppress(Exception):
            await self._mqtt.unsubscribe(self._topics.commands(self._station_id))

    async def _outcome_loop(self) -> None:
        while self.active:
            try:
                await self.publish_pending_set_price_outcomes()
            except Exception:
                logger.exception("set_price_outcome_publish_failed")
            try:
                await self._meter_results.publish_new_results()
            except Exception:
                logger.exception("meter_result_publish_failed")
            await asyncio.sleep(1.0)

    async def publish_pending_set_price_outcomes(self) -> int:
        """Publish retained CD5 outcomes; ACK-delete only after MQTT delivery.

        Multiple correlations stay on disk concurrently — a publish failure
        leaves that outcome (and later ones) for the next loop tick.

        While the matching set-price *request* still exists, do not publish or
        ACK-delete the outcome: that file is the only completion marker that
        lets a crash after outcome-write clear the leftover request without
        re-applying CD5.
        """
        published = 0
        for outcome in list_set_price_outcomes():
            if not self._mqtt.is_connected:
                break
            if pending_request_blocks_outcome_ack(outcome.correlation_id):
                logger.info(
                    "set_price_outcome_ack_deferred_request_still_present",
                    correlationId=outcome.correlation_id,
                    pumpId=outcome.pump_id,
                )
                continue
            pump_id = outcome.pump_id
            station_id = outcome.station_id or self._station_id
            result_payload = {
                "commandId": outcome.command_id,
                "correlationId": outcome.correlation_id,
                "accepted": outcome.accepted,
                "evaluated": True,
                "executed": outcome.accepted
                and outcome.execution_status
                in {"PRICE_CONFIRMED", "PRICE_PARTIAL"},
                "executionStatus": outcome.execution_status,
                "blockingReasons": (
                    [outcome.detail] if outcome.detail and not outcome.accepted else []
                ),
                "warnings": [],
                "unitPriceRaw": outcome.unit_price_raw,
                "appliedAddresses": list(outcome.applied_addresses),
                "gaveUpAddresses": list(outcome.gave_up_addresses),
                "deferredAddresses": list(outcome.deferred_addresses),
                "unverifiedAddresses": list(outcome.unverified_addresses),
                "environment": self._environment,
                "simulated": self._simulated,
                "timestamp": datetime.now(UTC).isoformat(),
                "detail": outcome.detail,
            }
            self._seq += 1
            envelope = build_envelope(
                event_type="COMMAND_RESULT",
                environment=self._environment,
                device_id=self._device_id,
                station_id=station_id,
                sequence=self._seq,
                simulated=self._simulated,
                deduplication_key=(
                    f"cmd-result-final:{outcome.correlation_id}:"
                    f"{outcome.execution_status}"
                ),
                payload=result_payload,
                pump_id=pump_id,
                correlation_id=outcome.correlation_id,
            )
            topic = self._topics.command_result(station_id, outcome.correlation_id)
            try:
                result = await self._mqtt.publish(
                    topic,
                    json.dumps(envelope.to_dict(), separators=(",", ":")),
                    qos=qos_for_event("COMMAND_RESULT"),
                )
            except (MqttNotConnectedError, MqttPublishError, MqttError) as exc:
                logger.warning(
                    "set_price_final_result_publish_failed_retained",
                    correlationId=outcome.correlation_id,
                    pumpId=pump_id,
                    stationId=station_id,
                    error=str(exc),
                )
                break
            if not getattr(result, "acknowledged", True):
                logger.warning(
                    "set_price_final_result_unacked_retained",
                    correlationId=outcome.correlation_id,
                    pumpId=pump_id,
                    stationId=station_id,
                )
                break
            # Re-check after PUBACK: controller may still hold the request.
            if pending_request_blocks_outcome_ack(outcome.correlation_id):
                logger.info(
                    "set_price_outcome_ack_deferred_request_still_present",
                    correlationId=outcome.correlation_id,
                    pumpId=pump_id,
                    note="after_mqtt_ack",
                )
                continue
            ack_set_price_outcome(outcome.correlation_id)
            published += 1
            logger.info(
                "set_price_final_result_published",
                correlationId=outcome.correlation_id,
                pumpId=pump_id,
                stationId=station_id,
                executionStatus=outcome.execution_status,
                accepted=outcome.accepted,
            )
        return published

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
        cmd_env = _normalize_cloud_environment(cmd.environment)
        self_env = _normalize_cloud_environment(self._environment)
        if cmd_env != self_env:
            reasons.append("wrong_environment")
        if cmd.schemaVersion != "1.0":
            reasons.append("unsupported_schema_version")
        if self_env != "LAB" and cmd.simulatorOnly:
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
        meter_result_extra: dict[str, Any] | None = None

        try:
            command = PumpCommand(cmd.commandType)
        except ValueError:
            reasons.append("unknown_command_type")
            command = None

        # Production SET_PRICE is a file bridge to the RS-485 controller — do not
        # require a local pump catalog row or Phase-4 physical-enable eligibility.
        production_set_price = (
            command is PumpCommand.SET_PRICE and self._allow_production_set_price
        )
        # Dashboard READ_METER resolves DART address from channel map + nozzleId;
        # one-Pi-per-pump stations often have no local SQLite pump catalog row.
        hardware_read_meter = command is PumpCommand.READ_METER

        pump_db_id: str | None = None
        ctx: PumpContext | None = None
        if (
            not reasons
            and command is not None
            and not production_set_price
            and not hardware_read_meter
        ):
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
        elif production_set_price and not reasons:
            current_state = "IDLE"
            # One Pi per physical pump: only queue CD5 when pumpId is ours.
            # Previously we fell back to pumps[0] and still wrote the request,
            # so an AGO SET_PRICE (pump-8) was applied on every PMS Pi too.
            async with unit_of_work(self._factory) as uow:
                pumps = await uow.pumps.list_for_station(self._station_id)
                local_ids = local_set_price_pump_ids(
                    device_id=self._device_id,
                    pumps=pumps,
                )
                # Require pumpId in the device-owned set. Do not accept merely
                # because a station-wide SQLite row exists for that logical id
                # (PMS All-price was CD5'd on AGO when SQLite listed every pump).
                if cmd.pumpId not in local_ids:
                    reasons.append("set_price_not_for_this_device")
                    eligible = False
                    logger.info(
                        "set_price_ignored_other_pump",
                        stationId=self._station_id,
                        deviceId=self._device_id,
                        commandPumpId=cmd.pumpId,
                        localPumpIds=sorted(local_ids),
                        unitPriceRaw=(cmd.payload or {}).get("unitPriceRaw"),
                        correlationId=cmd.correlationId,
                    )
                else:
                    eligible = True
                    logger.info(
                        "set_price_accepted_for_device",
                        stationId=self._station_id,
                        deviceId=self._device_id,
                        commandPumpId=cmd.pumpId,
                        localPumpIds=sorted(local_ids),
                        unitPriceRaw=(cmd.payload or {}).get("unitPriceRaw"),
                        correlationId=cmd.correlationId,
                    )
                    pump = next(
                        (
                            p
                            for p in pumps
                            if p.logical_pump_id == cmd.pumpId
                            or p.id == cmd.pumpId
                            or (
                                str(p.dart_address) == cmd.pumpId
                                and p.logical_pump_id in local_ids
                            )
                        ),
                        None,
                    )
                    if pump is not None:
                        pump_db_id = pump.id

        # Phase 9: production active commands never execute — except SET_PRICE
        # when this sidecar is explicitly confirmed as a production sole-
        # controller price bridge (writes a file the RS-485 controller applies).
        if production_set_price:
            if cmd.simulatorOnly:
                reasons.append("production_set_price_rejects_simulator_only")
                execution_status = "REJECTED"
            elif reasons:
                execution_status = "REJECTED"
            else:
                try:
                    unit_price, prices = parse_prices_from_payload(cmd.payload)
                    from intelipump_fdc.cloud.set_price_request import (
                        supersede_set_price_pending_verifies,
                    )

                    # A newer SET_PRICE supersedes late verification of older
                    # SENT_UNVERIFIED correlations on this device.
                    supersede_set_price_pending_verifies(
                        except_correlation_id=cmd.correlationId
                    )
                    path = write_set_price_request(
                        SetPriceRequest(
                            correlation_id=cmd.correlationId,
                            command_id=cmd.commandId,
                            unit_price_raw=unit_price,
                            prices_raw=prices,
                            requested_by=cmd.requestedBy,
                            pump_id=cmd.pumpId,
                        )
                    )
                    # Queued only — not applied on the pump yet (CD5 still pending).
                    executed = False
                    execution_status = "PENDING_CONTROLLER"
                    resulting_state = current_state
                    logger.info(
                        "set_price_queued_for_controller",
                        correlationId=cmd.correlationId,
                        unitPriceRaw=unit_price,
                        path=str(path),
                        stationId=self._station_id,
                        pumpId=cmd.pumpId,
                    )
                except ValueError as exc:
                    reasons.append(f"invalid_set_price_payload:{exc}")
                    execution_status = "REJECTED"
                except OSError as exc:
                    reasons.append(f"set_price_write_failed:{exc}")
                    execution_status = "ENQUEUE_FAILED"
        elif command is PumpCommand.READ_METER and not any(
            r in reasons
            for r in (
                "wrong_station",
                "wrong_environment",
                "expired",
                "duplicate_correlation_id",
                "duplicate_persisted_command",
                "unknown_command_type",
            )
        ):
            # Additive meter reconciliation: default UNSUPPORTED (never invent zeros).
            # Optional auto-CD101 uses the existing outbound queue only — no new serial.
            nozzle_id = meter_svc.nozzle_from_payload(cmd.payload)
            dart_address = None
            if ctx is not None:
                dart_address = int(ctx.dart_address)
            elif isinstance((cmd.payload or {}).get("dartAddress"), int):
                dart_address = int(cmd.payload["dartAddress"])
            rate_limited = meter_svc.is_rate_limited(
                station_id=self._station_id,
                pump_id=cmd.pumpId,
                nozzle_id=nozzle_id,
                min_interval_seconds=self._meter_settings.min_interval_seconds,
            )
            pending_count = 0
            async with unit_of_work(self._factory) as uow:
                pending_count = await uow.meter_readings.count_pending(
                    station_id=self._station_id, pump_id=cmd.pumpId
                )
            state_for_gate = ctx.current_state if ctx is not None else current_state
            execution_status, meter_error_code, meter_message = meter_svc.decide_read_meter(
                settings=self._meter_settings,
                current_state=state_for_gate,
                pending_count=pending_count,
                rate_limited=rate_limited,
            )
            meter_svc.mark_read_attempt(
                station_id=self._station_id,
                pump_id=cmd.pumpId,
                nozzle_id=nozzle_id,
            )
            # Hardware path: file bridge to sole controller (no second serial).
            # LAB auto_cd101: optional virtual outbound enqueue.
            if execution_status == "PENDING_CONTROLLER" and self._meter_settings.hardware_cd101:
                allowed_dev = (self._meter_settings.allowed_device_id or "").strip()
                if allowed_dev and allowed_dev != self._device_id.strip():
                    execution_status = "UNSUPPORTED"
                    meter_error_code = "METER_DEVICE_NOT_ALLOWLISTED"
                    meter_message = "HARDWARE_CD101 device allowlist mismatch"
                else:
                    if dart_address is None:
                        dart_address = meter_svc.dart_address_for_nozzle(
                            self._channel_mappings, nozzle_id
                        )
                    if dart_address is None and ctx is not None:
                        dart_address = int(ctx.dart_address)
                    allowed_addrs = self._meter_settings.allowed_address_set()
                    if dart_address is None:
                        execution_status = "UNSUPPORTED"
                        meter_error_code = "METER_ADDRESS_UNRESOLVED"
                        meter_message = (
                            "Could not resolve DART address for nozzle; "
                            "pass dartAddress or fix channel map"
                        )
                    elif allowed_addrs and int(dart_address) not in allowed_addrs:
                        execution_status = "UNSUPPORTED"
                        meter_error_code = "METER_ADDRESS_NOT_ALLOWLISTED"
                        meter_message = f"address {dart_address} not allowlisted"
                    else:
                        try:
                            write_meter_read_request(
                                MeterReadRequest(
                                    correlation_id=cmd.correlationId,
                                    dart_address=int(dart_address),
                                    counter_select=int(
                                        self._meter_settings.counter_select
                                    ),
                                    requested_by=cmd.requestedBy,
                                    nozzle_hint=nozzle_id,
                                    notes="dashboard-read-now",
                                )
                            )
                            executed = False
                            resulting_state = current_state
                            meter_error_code = "METER_READ_QUEUED_HARDWARE"
                            meter_message = (
                                "READ_METER accepted; CD101 pending on controller "
                                "outbound (file bridge)"
                            )
                            logger.info(
                                "meter_read_request_file_written",
                                correlationId=cmd.correlationId,
                                pumpId=cmd.pumpId,
                                nozzleId=nozzle_id,
                                address=dart_address,
                            )
                        except FileExistsError as exc:
                            execution_status = "RATE_LIMITED"
                            meter_error_code = "METER_REQUEST_BRIDGE_BUSY"
                            meter_message = str(exc)
                        except OSError as exc:
                            execution_status = "ENQUEUE_FAILED"
                            meter_error_code = "METER_REQUEST_WRITE_FAILED"
                            meter_message = str(exc)
            elif execution_status == "PENDING_CONTROLLER":
                can_lab_enqueue = (
                    self_env == "LAB"
                    and self._allow_lab
                    and self._loop is not None
                    and ctx is not None
                    and self._loop.runtime.transport.metadata.is_virtual_or_memory
                )
                if not can_lab_enqueue:
                    execution_status = "UNSUPPORTED"
                    meter_error_code = "METER_READ_UNSUPPORTED"
                    meter_message = (
                        "CD101 meter capture not enabled "
                        "(set HARDWARE_CD101 or LAB AUTO_CD101); "
                        "reporting unsupported (no invented zero)."
                    )
                else:
                    item = OutboundDataItem.create(
                        address=ctx.dart_address,
                        application_payload=meter_svc.build_cd101_outbound_payload(),
                        command_type=PumpCommand.READ_METER,
                        simulator_only=True,
                        idempotency=IdempotencyClass.IDEMPOTENT,
                    )
                    from dataclasses import replace

                    safety = replace(
                        self._loop.runtime.safety, allow_lab_simulator_commands=True
                    )
                    try:
                        self._loop.runtime.outbound.enqueue(item, safety)
                        executed = True
                        resulting_state = current_state
                        logger.info(
                            "meter_read_cd101_queued",
                            correlationId=cmd.correlationId,
                            pumpId=cmd.pumpId,
                            nozzleId=nozzle_id,
                            address=ctx.dart_address,
                        )
                    except (OutboundRejectedError, OutboundQueueFullError) as exc:
                        reasons.append(f"enqueue_failed:{exc}")
                        execution_status = "UNSUPPORTED"
                        meter_error_code = "METER_READ_ENQUEUE_FAILED"
                        meter_message = (
                            f"CD101 enqueue failed; reporting unsupported: {exc}"
                        )
            # Persist local unsupported/pending evidence + durable cloud delivery.
            meter_event = (
                "METER_READING"
                if execution_status == "PENDING_CONTROLLER"
                else "METER_READING_UNSUPPORTED"
            )
            meter_status = (
                "PENDING_CONTROLLER"
                if execution_status == "PENDING_CONTROLLER"
                else "UNSUPPORTED"
            )
            meter_payload = meter_svc.build_unsupported_payload(
                station_id=self._station_id,
                device_id=self._device_id,
                pump_id=cmd.pumpId,
                nozzle_id=nozzle_id,
                dart_address=dart_address,
                correlation_id=cmd.correlationId,
                reason=meter_message,
                error_code=meter_error_code,
                flags={
                    "automatic_cd101": (
                        "gated_on" if self._meter_settings.auto_cd101 else "off"
                    ),
                    "execution_status": execution_status,
                },
            )
            if execution_status == "PENDING_CONTROLLER":
                meter_payload["status"] = "PENDING_CONTROLLER"
                meter_payload["errorCode"] = meter_error_code
                meter_payload["errorMessage"] = meter_message
            meter_dedupe = f"meter:{self._station_id}:{cmd.correlationId}:{nozzle_id}"
            async with unit_of_work(self._factory) as uow:
                await uow.meter_readings.create(
                    station_id=self._station_id,
                    device_id=self._device_id,
                    pump_id=cmd.pumpId,
                    nozzle_id=nozzle_id,
                    dart_address=dart_address,
                    source="READ_NOW",
                    status=meter_status,
                    deduplication_key=meter_dedupe,
                    correlation_id=cmd.correlationId,
                    requested_at=datetime.now(UTC),
                    raw_evidence=meter_payload.get("rawEvidence"),
                    software_version=meter_svc.SOFTWARE_VERSION,
                    flags=meter_payload.get("flags"),
                    error_code=meter_error_code,
                    error_message=meter_message,
                )
                await uow.sync_queue.enqueue_checked(
                    entity_type="meter_reading",
                    entity_id=cmd.correlationId,
                    event_type=meter_event,
                    payload=meter_payload,
                    deduplication_key=meter_dedupe,
                )
            warnings = list(warnings) + [meter_message]
            resulting_state = resulting_state or current_state
            meter_result_extra = {
                "nozzleId": nozzle_id,
                "meterStatus": meter_status,
                "meterEvent": meter_event,
                "meterMessage": meter_message,
                "errorCode": meter_error_code,
            }
        elif command is not None and command in NON_IDEMPOTENT_COMMANDS:
            if not (
                self_env == "LAB"
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
                self_env == "LAB"
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
            and execution_status
            in {
                "QUEUED",
                "QUEUED_FOR_CONTROLLER",
                "PENDING_CONTROLLER",
                "EVALUATED_ONLY",
                "UNSUPPORTED",
                "RATE_LIMITED",
                "DEFERRED",
            }
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
        elif command is PumpCommand.READ_METER and execution_status in {
            "UNSUPPORTED",
            "RATE_LIMITED",
            "DEFERRED",
            "PENDING_CONTROLLER",
        }:
            # Controller answered read-only meter request; soft eligibility
            # warnings must not hide the unsupported/deferred outcome.
            accepted = True

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
        if meter_result_extra:
            result_payload.update(meter_result_extra)
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
