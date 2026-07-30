"""Virtual Wayne fueling-position model (no real hardware)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal
from uuid import uuid4

from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.bcd import decode_packed_bcd
from intelipump_fdc.protocol.dart.application.constants import (
    MessageDirection,
    PumpControlCommand,
)
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.simulator.clock import SimulatedClock
from intelipump_fdc.simulator.config import PumpConfig, SimulatorConfig
from intelipump_fdc.simulator.encoding import (
    encode_dc1_status,
    encode_dc2_volume_amount,
    encode_dc3_nozzle_price,
    encode_dc5_alarm,
    encode_dc9_identity,
    encode_dc101_totals,
)
from intelipump_fdc.simulator.faults import ProtocolFault, ProtocolFaultKind
from intelipump_fdc.simulator.models import OutboundApplicationTransaction, PumpSnapshot
from intelipump_fdc.state_machine.guards import evaluate_command_eligibility
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import PumpContext
from intelipump_fdc.state_machine.price_verification import verify_dc3_filling_price
from intelipump_fdc.state_machine.wayne_mapper import MapperContext, map_wayne_observation


@dataclass
class SimulatedPump:
    """One simulated fueling position with line/app/state behavior."""

    config: PumpConfig
    clock: SimulatedClock
    simulator_config: SimulatorConfig
    pending_outbound: deque[OutboundApplicationTransaction] = field(
        default_factory=deque
    )
    prices_raw: dict[int, int] = field(default_factory=dict)
    volume_raw: int = 0
    amount_raw: int = 0
    total_volume_raw: int = 0
    total_amount_raw: int = 0
    selected_nozzle: int | None = None
    nozzle_out: bool = False
    price_verified: bool = False
    wayne_status: WaynePumpStatus = WaynePumpStatus.RESET
    communication_enabled: bool = False
    fault_code: int | None = None
    last_command: PumpCommand | None = None
    active_transaction_id: str | None = None
    tx_sequence: int = 0
    expected_controller_sequence: int = 0
    last_accepted_controller_sequence: int | None = None
    awaiting_ack_sequence: int | None = None
    data_pending_since_ms: int | None = None
    filling_active: bool = False
    suspended: bool = False
    last_fill_tick_ms: int | None = None
    preset_volume_raw: int | None = None
    preset_amount_raw: int | None = None
    last_outbound_wire: bytes | None = None
    _machine: PumpStateMachine = field(init=False, repr=False)
    _notes: list[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        for n in range(1, self.config.nozzle_count + 1):
            self.prices_raw.setdefault(n, self.config.default_price_raw)
        self._machine = PumpStateMachine(
            PumpContext(
                pump_id=self.config.pump_id,
                dart_address=self.config.dart_address,
                current_state=PumpState.DISCONNECTED,
                communication_healthy=False,
                price_verified=False,
            )
        )

    @property
    def normalized_state(self) -> PumpState:
        return self._machine.context.current_state

    @property
    def context(self) -> PumpContext:
        return self._machine.context

    def snapshot(self) -> PumpSnapshot:
        return PumpSnapshot(
            pump_id=self.config.pump_id,
            dart_address=self.config.dart_address,
            normalized_state=self.normalized_state,
            wayne_status=self.wayne_status,
            selected_nozzle=self.selected_nozzle,
            prices_raw=dict(self.prices_raw),
            price_verified=self.price_verified,
            volume_raw=self.volume_raw,
            amount_raw=self.amount_raw,
            total_volume_raw=self.total_volume_raw,
            total_amount_raw=self.total_amount_raw,
            active_transaction_id=self.active_transaction_id,
            tx_sequence=self.tx_sequence,
            expected_controller_sequence=self.expected_controller_sequence,
            pending_outbound_count=len(self.pending_outbound),
            communication_enabled=self.communication_enabled,
            fault_code=self.fault_code,
            last_command=self.last_command,
            nozzle_out=self.nozzle_out,
            suspended=self.suspended,
            filling_active=self.filling_active,
            clock_ms=self.clock.now(),
        )

    def reset_protocol_sequences(self) -> None:
        """Protocol restart: TX# initiated to 0."""
        self.tx_sequence = 0
        self.expected_controller_sequence = 0
        self.last_accepted_controller_sequence = None
        self.awaiting_ack_sequence = None
        self.data_pending_since_ms = None
        self._notes.append("protocol sequences reset to 0")

    def enable_communication(self) -> None:
        self.communication_enabled = True
        self._apply_event(PumpEvent.COMMUNICATION_STARTED)
        self._notes.append("communication enabled")

    def disable_communication(self) -> None:
        self.communication_enabled = False
        self.filling_active = False
        self._apply_event(PumpEvent.COMMUNICATION_LOST)
        self.pending_outbound.clear()
        self.awaiting_ack_sequence = None
        self.data_pending_since_ms = None

    def cold_start_to_ready(self) -> None:
        """High-level cold start: DISCONNECTED → … → READY via DC1/DC3 mapper."""
        if not self.communication_enabled:
            self.enable_communication()
        self.wayne_status = WaynePumpStatus.RESET
        self.nozzle_out = False
        self.selected_nozzle = None
        self.price_verified = False
        # Production path: emit DC1+DC3 and map (no synthetic READY_OBSERVED).
        self.apply_queued_status_observations()

    def restart_pump(self, *, preserve_totals: bool = True) -> None:
        """Simulate pump electronic restart (never auto-authorize)."""
        was_filling = self.filling_active or self.normalized_state in {
            PumpState.FILLING,
            PumpState.SUSPENDED,
            PumpState.AUTHORIZED,
        }
        self.filling_active = False
        self.suspended = False
        self.nozzle_out = False
        self.volume_raw = 0
        self.amount_raw = 0
        self.preset_amount_raw = None
        self.preset_volume_raw = None
        self.fault_code = None
        self.active_transaction_id = None
        self.price_verified = False
        self.selected_nozzle = None
        self.pending_outbound.clear()
        self.reset_protocol_sequences()
        if not preserve_totals:
            self.total_volume_raw = 0
            self.total_amount_raw = 0
        if self.communication_enabled:
            self._machine = PumpStateMachine(
                PumpContext(
                    pump_id=self.config.pump_id,
                    dart_address=self.config.dart_address,
                    current_state=PumpState.DISCOVERING,
                    communication_healthy=True,
                    price_verified=False,
                    nozzle_out=False,
                    warnings=(
                        ("restart during filling; recovered to DISCOVERING",)
                        if was_filling
                        else ("pump restart during idle",)
                    ),
                )
            )
            self.wayne_status = WaynePumpStatus.RESET
            self.apply_queued_status_observations()
        else:
            self._machine = PumpStateMachine(
                PumpContext(
                    pump_id=self.config.pump_id,
                    dart_address=self.config.dart_address,
                    current_state=PumpState.DISCONNECTED,
                    communication_healthy=False,
                )
            )

    # --- Physical / local simulator actions ---------------------------------

    def lift_nozzle(self, nozzle: int = 1) -> list[ProtocolFault]:
        if nozzle not in self.prices_raw:
            return [
                ProtocolFault(
                    ProtocolFaultKind.APPLICATION_REJECTED,
                    f"unknown nozzle {nozzle}",
                    address=self.config.dart_address,
                    at_ms=self.clock.now(),
                )
            ]
        # Preserve prior holster state for mapper edge detection (IN→OUT).
        previous_out = self.context.nozzle_out
        if previous_out is None:
            previous_out = False
        self.selected_nozzle = nozzle
        self.nozzle_out = True
        self._refresh_price_verification()
        self._sync_context_fields()
        self._machine.replace_context(
            self.context.with_updates(nozzle_out=previous_out)
        )
        if self.normalized_state is PumpState.AUTHORIZED:
            self.apply_queued_status_observations(preserve_context_nozzle=True)
            self.filling_active = True
            self.suspended = False
            self.last_fill_tick_ms = self.clock.now()
            self.wayne_status = WaynePumpStatus.FILLING
            self.apply_queued_status_observations()
            self._queue_fill_data()
            return []
        self.apply_queued_status_observations(preserve_context_nozzle=True)
        return []

    def return_nozzle(self) -> None:
        previous_out = self.context.nozzle_out
        if previous_out is None:
            previous_out = True
        self.nozzle_out = False
        self._sync_context_fields()
        self._machine.replace_context(
            self.context.with_updates(nozzle_out=previous_out)
        )
        if self.normalized_state in {
            PumpState.FILLING,
            PumpState.SUSPENDED,
            PumpState.LIMIT_REACHED,
        }:
            self.apply_queued_status_observations(preserve_context_nozzle=True)
            self.filling_active = False
            self.suspended = False
            self.wayne_status = WaynePumpStatus.FILLING_COMPLETED
            self.apply_queued_status_observations()
            self._queue_fill_data()
            return
        self.apply_queued_status_observations(preserve_context_nozzle=True)

    def inject_fault(self, code: int = 1) -> None:
        self.fault_code = code
        self.filling_active = False
        self.suspended = False
        self.wayne_status = WaynePumpStatus.RESET
        self._apply_event(PumpEvent.FAULT_OBSERVED, fault_code=code)
        self._queue(encode_dc5_alarm(code), "DC5_ALARM")

    def clear_fault(self) -> None:
        self.fault_code = None
        self._apply_event(PumpEvent.FAULT_CLEARED)
        self.wayne_status = WaynePumpStatus.RESET
        self.nozzle_out = False
        self.price_verified = False
        self.apply_queued_status_observations()

    # --- Application command handling (controller DATA) ---------------------

    def handle_application_payload(self, payload: bytes) -> list[ProtocolFault]:
        """Process one or more TRANS+LNG+DATA units from controller."""
        faults: list[ProtocolFault] = []
        offset = 0
        while offset + 2 <= len(payload):
            trans = payload[offset]
            lng = payload[offset + 1]
            end = offset + 2 + lng
            if end > len(payload):
                faults.append(
                    ProtocolFault(
                        ProtocolFaultKind.MALFORMED_FRAME,
                        "truncated application transaction",
                        address=self.config.dart_address,
                        at_ms=self.clock.now(),
                    )
                )
                break
            data = payload[offset + 2 : end]
            faults.extend(self._handle_one_tx(trans, data))
            offset = end
        return faults

    def _handle_one_tx(self, trans: int, data: bytes) -> list[ProtocolFault]:
        if trans == 0x01 and len(data) == 1:
            return self._handle_cd1(data[0])
        if trans == 0x03 and len(data) == 4:
            return self._handle_cd3_preset_volume(data)
        if trans == 0x04 and len(data) == 4:
            return self._handle_cd4_preset_amount(data)
        if trans == 0x05 and len(data) >= 3 and len(data) % 3 == 0:
            return self._handle_cd5_price(data)
        if trans == 0x65 and len(data) == 1:
            self._queue(
                encode_dc101_totals(
                    counter_select=data[0],
                    total_value_raw=self.total_amount_raw,
                    total_meter1_raw=self.total_volume_raw,
                    total_meter2_raw=0,
                ),
                "DC101_TOTALS",
            )
            return []
        return [
            ProtocolFault(
                ProtocolFaultKind.APPLICATION_REJECTED,
                f"unsupported controller TRANS=0x{trans:02X} LNG={len(data)}",
                address=self.config.dart_address,
                at_ms=self.clock.now(),
            )
        ]

    def _handle_cd1(self, dcc: int) -> list[ProtocolFault]:
        try:
            command = PumpControlCommand(dcc)
        except ValueError:
            return [
                ProtocolFault(
                    ProtocolFaultKind.APPLICATION_REJECTED,
                    f"unknown CD1 DCC=0x{dcc:02X}",
                    address=self.config.dart_address,
                    at_ms=self.clock.now(),
                )
            ]

        if command is PumpControlCommand.RETURN_STATUS:
            self._queue(encode_dc1_status(int(self.wayne_status)), "DC1_STATUS")
            self._queue(self._encode_dc3(), "DC3_NOZZLE_PRICE")
            return []
        if command is PumpControlCommand.RETURN_FILLING_INFORMATION:
            self._queue(
                encode_dc2_volume_amount(
                    volume_raw=self.volume_raw, amount_raw=self.amount_raw
                ),
                "DC2_FILL",
            )
            return []
        if command is PumpControlCommand.RETURN_PUMP_IDENTITY:
            self._queue(encode_dc9_identity(), "DC9_IDENTITY")
            return []
        if command is PumpControlCommand.RETURN_PUMP_PARAMETERS:
            # Not fully modeled; acknowledge by status only.
            self._queue(encode_dc1_status(int(self.wayne_status)), "DC1_STATUS")
            return []

        mapping = {
            PumpControlCommand.RESET: PumpCommand.RESET,
            PumpControlCommand.AUTHORIZE: PumpCommand.AUTHORIZE,
            PumpControlCommand.STOP: PumpCommand.STOP,
            PumpControlCommand.SUSPEND_FUELLING_POINT: PumpCommand.SUSPEND,
            PumpControlCommand.RESUME_FUELLING_POINT: PumpCommand.RESUME,
        }
        if command not in mapping:
            return [
                ProtocolFault(
                    ProtocolFaultKind.APPLICATION_REJECTED,
                    f"CD1 {command.name} not implemented in simulator",
                    address=self.config.dart_address,
                    at_ms=self.clock.now(),
                )
            ]
        return self._execute_guarded(mapping[command])

    def _handle_cd3_preset_volume(self, data: bytes) -> list[ProtocolFault]:
        volume_raw = decode_packed_bcd(data)
        return self._execute_guarded(
            PumpCommand.PRESET_VOLUME, preset_value=volume_raw, volume_raw=volume_raw
        )

    def _handle_cd4_preset_amount(self, data: bytes) -> list[ProtocolFault]:
        amount_raw = decode_packed_bcd(data)
        return self._execute_guarded(
            PumpCommand.PRESET_AMOUNT, preset_value=amount_raw, amount_raw=amount_raw
        )

    def _handle_cd5_price(self, data: bytes) -> list[ProtocolFault]:
        # Single or multi 3-byte price chunks; nozzle = 1-based ordinal.
        # Documented DART flow after a complete accepted CD5 on a zeroized pump:
        # PUMP_NOT_PROGRAMMED → FILLING_COMPLETED (not READY). Transition once
        # after all nozzle prices in this transaction are applied.
        was_not_programmed = self.wayne_status is WaynePumpStatus.PUMP_NOT_PROGRAMMED
        faults: list[ProtocolFault] = []
        for i in range(0, len(data), 3):
            nozzle = (i // 3) + 1
            price_raw = decode_packed_bcd(data[i : i + 3])
            result = self._execute_guarded(
                PumpCommand.SET_PRICE, price_raw=price_raw, nozzle=nozzle
            )
            faults.extend(result)
        if not faults and was_not_programmed:
            # CD5 programs prices without a selected nozzle; treat a complete
            # multi-nozzle accept as verified for the documented 0→5 path.
            self.price_verified = all(
                n in self.prices_raw
                for n in range(1, self.config.nozzle_count + 1)
            )
            self._sync_context_fields()
            if self.price_verified:
                self.wayne_status = WaynePumpStatus.FILLING_COMPLETED
                self._apply_event(
                    PumpEvent.FILLING_COMPLETED,
                    raw_wayne_status=int(self.wayne_status),
                    price_verified=True,
                )
                self.queue_status_and_nozzle()
        return faults

    def _execute_guarded(
        self,
        command: PumpCommand,
        *,
        preset_value: int | None = None,
        volume_raw: int | None = None,
        amount_raw: int | None = None,
        price_raw: int | None = None,
        nozzle: int | None = None,
    ) -> list[ProtocolFault]:
        self._sync_context_fields()
        eligibility = evaluate_command_eligibility(
            command,
            self.context,
            preset_value=preset_value,
            physical_enable_present=self.simulator_config.simulator_physical_enable,
            active_commands_enabled=self.simulator_config.simulator_active_commands_enabled,
        )
        self.last_command = command
        if not eligibility.eligible:
            return [
                ProtocolFault(
                    ProtocolFaultKind.INELIGIBLE_COMMAND,
                    f"{command.value} blocked: {','.join(eligibility.blocking_reasons)}",
                    address=self.config.dart_address,
                    at_ms=self.clock.now(),
                )
            ]

        if command is PumpCommand.SET_PRICE:
            assert price_raw is not None and nozzle is not None
            self.prices_raw[nozzle] = price_raw
            # CD5 master write programs prices; DC3 verify only when a nozzle
            # is selected. With no selection, mark verified once every nozzle
            # has an accepted programmed price and decimals are known.
            if self.selected_nozzle is not None:
                self._refresh_price_verification()
            else:
                decimals_known = self.config.filling.price_decimals is not None
                all_programmed = all(
                    n in self.prices_raw
                    for n in range(1, self.config.nozzle_count + 1)
                )
                self.price_verified = decimals_known and all_programmed
            self._sync_context_fields()
            self.queue_status_and_nozzle()
            return []

        if command is PumpCommand.PRESET_VOLUME:
            assert volume_raw is not None
            self.preset_volume_raw = volume_raw
            return []

        if command is PumpCommand.PRESET_AMOUNT:
            assert amount_raw is not None
            self.preset_amount_raw = amount_raw
            return []

        if command is PumpCommand.AUTHORIZE:
            self.active_transaction_id = str(uuid4())
            self.volume_raw = 0
            self.amount_raw = 0
            self.wayne_status = WaynePumpStatus.AUTHORIZED
            self._apply_event(
                PumpEvent.AUTHORIZATION_CONFIRMED,
                active_transaction_id=self.active_transaction_id,
                raw_wayne_status=int(self.wayne_status),
            )
            # Wayne may authorize before lift; start FILLING only once nozzle is OUT.
            if self.nozzle_out:
                self.filling_active = True
                self.suspended = False
                self.last_fill_tick_ms = self.clock.now()
                self.wayne_status = WaynePumpStatus.FILLING
                self._apply_event(
                    PumpEvent.FILLING_STARTED,
                    raw_wayne_status=int(self.wayne_status),
                )
                self.queue_status_and_nozzle()
                self._queue_fill_data()
            else:
                self.queue_status_and_nozzle()
            return []

        if command is PumpCommand.STOP:
            self._complete_filling(reason="stop")
            return []

        if command is PumpCommand.SUSPEND:
            self.suspended = True
            self.wayne_status = WaynePumpStatus.SUSPENDED
            self._apply_event(
                PumpEvent.SUSPENDED_OBSERVED,
                raw_wayne_status=int(self.wayne_status),
            )
            self.queue_status_and_nozzle()
            return []

        if command is PumpCommand.RESUME:
            self.suspended = False
            self.wayne_status = WaynePumpStatus.FILLING
            self.last_fill_tick_ms = self.clock.now()
            self._apply_event(
                PumpEvent.RESUMED_OBSERVED,
                raw_wayne_status=int(self.wayne_status),
            )
            self.queue_status_and_nozzle()
            return []

        if command is PumpCommand.RESET:
            self.filling_active = False
            self.suspended = False
            self.volume_raw = 0
            self.amount_raw = 0
            self.preset_amount_raw = None
            self.preset_volume_raw = None
            self.active_transaction_id = None
            self.price_verified = False
            self.wayne_status = WaynePumpStatus.RESET
            # RESET clears display/amount/volume/preset; READY only via mapper.
            self.apply_queued_status_observations()
            return []

        return []

    def apply_queued_status_observations(
        self, *, preserve_context_nozzle: bool = False
    ) -> None:
        """Encode DC1+DC3 and apply through production decoder/mapper.

        This is the simulator stand-in for pump→controller DATA so READY and
        nozzle edges use the same path as real hardware observations.

        When ``preserve_context_nozzle`` is True, keep the current context
        ``nozzle_out`` (prior edge state) instead of overwriting it from the
        physical sim flag before mapping.
        """
        payload = encode_dc1_status(int(self.wayne_status)) + self._encode_dc3()
        self.queue_status_and_nozzle()
        bundle = decode_data_payload(
            payload,
            pump_address=self.config.dart_address,
            source_frame_raw_hex=f"sim:{self.clock.now()}:{payload.hex()}",
            price_decimals=self.config.filling.price_decimals,
        )
        prior_noz = self.context.nozzle_out
        self._sync_context_fields()
        if preserve_context_nozzle:
            self._machine.replace_context(
                self.context.with_updates(nozzle_out=prior_noz)
            )
        for tx in bundle.transactions:
            mapped = map_wayne_observation(
                tx,
                context=MapperContext.from_pump_context(
                    self.context,
                    resolve_as_dc1=True,
                    resolve_as_dc3=True,
                    bus_direction=MessageDirection.SLAVE_TO_MASTER,
                ),
            )
            self._machine.apply_mapped(
                mapped,
                dispensed_volume_raw=self.volume_raw,
                price_verified=self.price_verified,
            )
            self._sync_from_machine()

    def _refresh_price_verification(self) -> None:
        nozzle = self.selected_nozzle
        if nozzle is None:
            self.price_verified = False
            return
        price_raw = self.prices_raw.get(nozzle)
        result = verify_dc3_filling_price(
            received_price_raw=price_raw,
            selected_nozzle=nozzle,
            configured_prices_raw=self.prices_raw,
            price_decimals=self.config.filling.price_decimals,
            price_bcd_valid=True,
        )
        # Full verify when decimals known; partial raw match is not verified.
        self.price_verified = result.verified
        if result.partial and not result.verified:
            self._notes.append(f"price partially verified: {result.reasons}")

    def _sync_from_machine(self) -> None:
        ctx = self.context
        if ctx.nozzle_out is not None:
            self.nozzle_out = ctx.nozzle_out
        if ctx.selected_nozzle is not None:
            self.selected_nozzle = ctx.selected_nozzle
        self.price_verified = ctx.price_verified
        self.active_transaction_id = ctx.active_transaction_id
        self.fault_code = ctx.fault_code
        # Wayne status remains simulator-authoritative (encoded into DC1).

    # --- Filling engine -----------------------------------------------------

    def on_time_advance(self) -> None:
        """Advance filling deterministically; never double-apply the same tick."""
        if not self.filling_active or self.suspended:
            return
        if self.normalized_state not in {PumpState.FILLING, PumpState.AUTHORIZED}:
            return
        now = self.clock.now()
        if self.last_fill_tick_ms is None:
            self.last_fill_tick_ms = now
            return
        elapsed = now - self.last_fill_tick_ms
        interval = self.config.filling.update_interval_ms
        if elapsed < interval:
            return
        # Apply whole intervals only (idempotent vs duplicate advance of 0).
        steps = elapsed // interval
        self.last_fill_tick_ms += steps * interval
        for _ in range(steps):
            if not self.filling_active or self.suspended:
                break
            self._fill_one_interval()

    def _fill_one_interval(self) -> None:
        cfg = self.config.filling
        # volume increment: flow_rate * (interval_ms/1000) * 10^decimals
        seconds = Decimal(cfg.update_interval_ms) / Decimal(1000)
        liters = cfg.flow_rate_liters_per_second * seconds
        scale = Decimal(10) ** cfg.volume_decimals
        delta = int((liters * scale).to_integral_value(rounding=ROUND_DOWN))
        if delta <= 0:
            delta = 1  # ensure progress at very low rates in tests
        new_volume = self.volume_raw + delta

        # Apply presets / natural complete.
        limit_volume = cfg.preset_volume_raw
        if self.preset_volume_raw is not None:
            limit_volume = self.preset_volume_raw
        if limit_volume is None:
            limit_volume = cfg.natural_complete_volume_raw

        hit_limit = False
        if new_volume >= limit_volume:
            new_volume = limit_volume
            hit_limit = True

        self.volume_raw = new_volume
        self.amount_raw = self._amount_from_volume(self.volume_raw)

        if self.preset_amount_raw is not None and self.amount_raw >= self.preset_amount_raw:
            self.amount_raw = self.preset_amount_raw
            # Recompute volume to match amount floor (stable, non-decreasing amount).
            hit_limit = True

        self._apply_event(
            PumpEvent.FILLING_UPDATED,
            raw_wayne_status=int(WaynePumpStatus.FILLING),
        )
        self._queue_fill_data()

        if hit_limit:
            if (
                self.preset_amount_raw is not None
                or self.preset_volume_raw is not None
                or cfg.preset_amount_raw is not None
                or cfg.preset_volume_raw is not None
            ):
                self.wayne_status = WaynePumpStatus.MAX_AMOUNT_VOLUME_REACHED
                self._apply_event(
                    PumpEvent.LIMIT_REACHED,
                    raw_wayne_status=int(self.wayne_status),
                )
            self._complete_filling(reason="limit_or_natural")

    def _amount_from_volume(self, volume_raw: int) -> int:
        """amount_raw = volume * price / 10^volume_decimals (Decimal math)."""
        cfg = self.config.filling
        nozzle = self.selected_nozzle or 1
        price = Decimal(self.prices_raw.get(nozzle, self.config.default_price_raw))
        volume = Decimal(volume_raw) / (Decimal(10) ** cfg.volume_decimals)
        # price_raw already scaled by price_decimals; amount uses amount_decimals.
        unit = price / (Decimal(10) ** cfg.price_decimals)
        amount = volume * unit
        amount_scaled = (amount * (Decimal(10) ** cfg.amount_decimals)).to_integral_value(
            rounding=ROUND_DOWN
        )
        return int(amount_scaled)

    def _complete_filling(self, *, reason: str) -> None:
        if not self.filling_active and self.normalized_state not in {
            PumpState.FILLING,
            PumpState.SUSPENDED,
            PumpState.AUTHORIZED,
            PumpState.LIMIT_REACHED,
        }:
            return
        self.filling_active = False
        self.suspended = False
        self.total_volume_raw += self.volume_raw
        self.total_amount_raw += self.amount_raw
        self.wayne_status = WaynePumpStatus.FILLING_COMPLETED
        evidence = f"complete:{self.active_transaction_id}:{self.volume_raw}:{self.amount_raw}"
        self._apply_event(
            PumpEvent.FILLING_COMPLETED,
            raw_wayne_status=int(self.wayne_status),
            completion_evidence_key=evidence,
        )
        self._notes.append(f"filling completed ({reason})")
        self.queue_status_and_nozzle()
        self._queue_fill_data()

    # --- Outbound helpers ---------------------------------------------------

    def queue_status_and_nozzle(self) -> None:
        self._queue(encode_dc1_status(int(self.wayne_status)), "DC1_STATUS")
        self._queue(self._encode_dc3(), "DC3_NOZZLE_PRICE")

    def _queue_fill_data(self) -> None:
        self._queue(
            encode_dc2_volume_amount(
                volume_raw=self.volume_raw, amount_raw=self.amount_raw
            ),
            "DC2_FILL",
        )

    def _encode_dc3(self) -> bytes:
        nozzle = self.selected_nozzle or 1
        price = self.prices_raw.get(nozzle, self.config.default_price_raw)
        return encode_dc3_nozzle_price(
            price_raw=price,
            logical_nozzle=nozzle,
            nozzle_out=self.nozzle_out,
        )

    def _queue(self, payload: bytes, label: str) -> None:
        self.pending_outbound.append(
            OutboundApplicationTransaction(payload=payload, label=label)
        )

    def pop_pending_payload(self) -> bytes | None:
        if not self.pending_outbound:
            return None
        item = self.pending_outbound.popleft()
        return item.payload

    def has_pending(self) -> bool:
        return bool(self.pending_outbound)

    def _sync_context_fields(self) -> None:
        ctx = self.context.with_updates(
            selected_nozzle=self.selected_nozzle,
            nozzle_out=self.nozzle_out,
            price_verified=self.price_verified,
            communication_healthy=self.communication_enabled,
            active_transaction_id=self.active_transaction_id,
            fault_code=self.fault_code,
            last_raw_wayne_status=int(self.wayne_status),
            dispensed_volume_raw=self.volume_raw,
            has_unresolved_transaction=self.active_transaction_id is not None
            and self.normalized_state
            in {
                PumpState.FILLING,
                PumpState.SUSPENDED,
                PumpState.LIMIT_REACHED,
                PumpState.FILLING_COMPLETE,
            },
        )
        self._machine.replace_context(ctx)

    def _apply_event(self, event: PumpEvent, **kwargs: object) -> None:
        self._sync_context_fields()
        self._machine.apply(event, **kwargs)  # type: ignore[arg-type]
        # Mirror communication / price from machine context after apply.
        self.price_verified = self.context.price_verified
        if event is PumpEvent.COMMUNICATION_LOST:
            self.communication_enabled = False
        elif event is PumpEvent.COMMUNICATION_STARTED:
            self.communication_enabled = True
