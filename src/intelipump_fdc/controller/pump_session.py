"""Per-address DART protocol session (controller side)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import structlog

from intelipump_fdc.controller.comm_health import HealthThresholds, HealthTransitionLog
from intelipump_fdc.controller.sale_lifecycle import SaleLifecycle
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.controller.session_models import (
    CommunicationHealth,
    NozzlePosition,
    ObservedStatus,
    PumpSessionState,
)
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.constants import MessageDirection
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_ack, build_poll
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.simulator.config import SequencePolicy, next_sequence
from intelipump_fdc.state_machine.completion_timeout import (
    DEFAULT_AWAITING_FILLING_COMPLETE_TIMEOUT,
    DEFAULT_DC2_STABILITY_WINDOW,
    evaluate_awaiting_filling_complete_timeout,
)
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import PumpContext
from intelipump_fdc.state_machine.reconciliation import (
    LiveObservationSummary,
    reconcile_after_restart,
)
from intelipump_fdc.state_machine.wayne_mapper import (
    MappedWayneObservation,
    MapperContext,
    map_wayne_observation,
)

logger = structlog.get_logger(__name__)

# Cleared when a valid EOT/DATA proves the link is healthy again.
# Historical counters (timeout_count, etc.) are never decremented.
_TRANSIENT_COMMUNICATION_ERRORS = frozenset({"response_timeout"})


class PumpSession:
    """Track one DART address: sequences, health, and normalized state."""

    def __init__(
        self,
        *,
        address: int,
        pump_id: str,
        events: EventBus,
        sequence_policy: SequencePolicy = SequencePolicy.SPEC_F_TO_1,
        thresholds: HealthThresholds | None = None,
        transitions: HealthTransitionLog | None = None,
        awaiting_filling_complete_timeout: timedelta = (
            DEFAULT_AWAITING_FILLING_COMPLETE_TIMEOUT
        ),
        dc2_stability_window: timedelta = DEFAULT_DC2_STABILITY_WINDOW,
        soft_rx_sequence: bool = True,
    ) -> None:
        self.state = PumpSessionState(address=address, pump_id=pump_id)
        self.events = events
        self.sequence_policy = sequence_policy
        self.thresholds = thresholds or HealthThresholds()
        self.transitions = transitions
        self.awaiting_filling_complete_timeout = awaiting_filling_complete_timeout
        self.dc2_stability_window = dc2_stability_window
        self.soft_rx_sequence = soft_rx_sequence
        self.machine = PumpStateMachine(
            PumpContext(
                pump_id=pump_id,
                dart_address=address,
                current_state=PumpState.DISCONNECTED,
                communication_healthy=False,
            )
        )
        self._applied_completion_keys: set[str] = set()
        self._needs_restart_reconcile = False
        self._persisted_for_reconcile: PumpContext | None = None
        self._await_started_at: datetime | None = None
        self._dc2_last_changed_at: datetime | None = None
        self._last_dc2_volume: int | None = None
        self._insufficient_evidence_warned = False
        self._inferred_completion_key: str | None = None
        # True only after FILLING is observed in this process (not retained COMPLETED).
        self._filling_seen_this_boot = False
        self._startup_baseline_fingerprint: str | None = None

    @property
    def address(self) -> int:
        return self.state.address

    def seed_recovered_context(self, context: PumpContext) -> None:
        """Apply recovery snapshot; require live reconcile before healthy claims."""
        seeded = context.with_updates(communication_healthy=False)
        # Re-derive awaiting when snapshot is FILLING_COMPLETE with open sale.
        if (
            seeded.current_state is PumpState.FILLING_COMPLETE
            and seeded.active_transaction_id is not None
            and not seeded.completion_inferred
        ):
            seeded = seeded.with_updates(
                awaiting_filling_complete=True,
                has_unresolved_transaction=True,
            )
        elif seeded.active_transaction_id is not None:
            seeded = seeded.with_updates(has_unresolved_transaction=True)
        self.machine = PumpStateMachine(seeded)
        self.state.last_state = seeded.current_state
        self._persisted_for_reconcile = seeded
        self._needs_restart_reconcile = True
        self._await_started_at = (
            seeded.last_observation_at if seeded.awaiting_filling_complete else None
        )
        self._insufficient_evidence_warned = False
        if seeded.dispensed_volume_raw is not None:
            self._last_dc2_volume = seeded.dispensed_volume_raw
            self._dc2_last_changed_at = seeded.last_observation_at

    def tick_awaiting_completion(self, *, now: datetime | None = None) -> None:
        """Evaluate hang-up completion timeout (poll-loop driven; idempotent)."""
        now = now or datetime.now(UTC)
        ctx = self.machine.context
        if not ctx.awaiting_filling_complete:
            self._await_started_at = None
            self._insufficient_evidence_warned = False
            return
        if self._await_started_at is None:
            self._await_started_at = ctx.last_observation_at or now

        decision = evaluate_awaiting_filling_complete_timeout(
            ctx,
            now=now,
            timeout=self.awaiting_filling_complete_timeout,
            await_started_at=self._await_started_at,
            dc2_last_changed_at=self._dc2_last_changed_at,
            dc2_stability_window=self.dc2_stability_window,
        )
        if decision.insufficient_evidence:
            if not self._insufficient_evidence_warned:
                self._insufficient_evidence_warned = True
                self._publish_reconciliation_warning(
                    warnings=decision.warnings,
                    inferences=decision.inferences,
                    context=ctx,
                )
            return
        if not decision.should_infer or decision.event is None:
            return

        tx_id = ctx.active_transaction_id or "unknown"
        key = self._inferred_completion_key or f"inferred:{tx_id}:hangup-timeout"
        if key in self._applied_completion_keys:
            return
        self._inferred_completion_key = key
        before = ctx.current_state
        result = self.machine.apply(
            PumpEvent.FILLING_COMPLETED,
            observed_at=now,
            completion_evidence_key=key,
            completion_inferred=True,
            awaiting_filling_complete=False,
            nozzle_out=ctx.nozzle_out,
            dispensed_volume_raw=ctx.dispensed_volume_raw,
            active_transaction_id=ctx.active_transaction_id,
        )
        if result.accepted:
            self._applied_completion_keys.add(key)
            self._await_started_at = None
            self._insufficient_evidence_warned = False
            warnings = tuple([*decision.warnings, *result.warnings])
            self._publish_state_changed(
                before=before,
                after=result.context.current_state,
                event_name=PumpEvent.FILLING_COMPLETED.value,
                context=result.context.with_updates(warnings=warnings),
                completion_evidence_key=key,
            )

    def _clear_transient_communication_error(self) -> None:
        """Clear stale transient link errors after a valid EOT/DATA recovery.

        Does not clear persistent protocol/configuration faults (CRC, sequence,
        address mismatch, unexpected control, NAK). Does not alter counters.
        """
        if self.state.last_error in _TRANSIENT_COMMUNICATION_ERRORS:
            self.state.last_error = None
        self.state.last_transient_error = None

    def _mark_valid_response(self, *, kind: str, capture_mono: float | None = None) -> None:
        now_mono = capture_mono if capture_mono is not None else time.monotonic()
        self.state.last_valid_response_mono = now_mono
        self.state.last_valid_frame_time = now_mono
        self.state.last_response_at = datetime.now(UTC)
        self.state.consecutive_timeouts = 0
        self.state.consecutive_protocol_faults = 0
        self.state.missed_bus_responses = 0
        self.state.communication_online = True
        if kind == "EOT":
            self.state.last_valid_eot_mono = now_mono
            self.state.stats.short_bus_response_count += 1
        elif kind == "DATA":
            self.state.last_valid_data_mono = now_mono
            self.state.stale_application_data = False
        elif kind == "SHORT_BUS":
            self.state.stats.short_bus_response_count += 1
        self.evaluate_synchronized()

    def note_short_bus_response(self, *, capture_mono: float | None = None) -> None:
        """Recognized short bus response (EOT/ACK/NAK) keeps link online."""
        self._mark_valid_response(kind="SHORT_BUS", capture_mono=capture_mono)
        self._set_communication(CommunicationHealth.HEALTHY)
        self._clear_transient_communication_error()

    def note_missed_bus_response(self) -> None:
        """No correlated response in the poll window (empty ≠ temporary Empty)."""
        self.state.missed_bus_responses += 1
        self.on_timeout()

    def note_command_tx(self, *, tx_mono: float) -> None:
        self.state.last_command_time = tx_mono
        self.state.pending_exchange = True
        self.state.pending_command_events.clear()

    def note_link_ack(self, *, ack_mono: float, sequence: int | None = None) -> None:
        self.state.last_ack_time = ack_mono
        self.state.pending_exchange = False
        if sequence is not None:
            self.state.last_rx_ack_sequence = sequence

    def should_skip_reset(self) -> bool:
        """Skip redundant RESET when already RESET and synchronized."""
        return (
            self.state.observed_status is ObservedStatus.RESET
            and self.state.state_synchronized
            and not self.state.pending_exchange
        )

    def evaluate_synchronized(self) -> bool:
        """Sync requires recent valid correlated response + known DC1 + NOZIO.

        Bus activity alone (EOT) is not sufficient.
        """
        known_dc1 = self.state.observed_status is not ObservedStatus.UNKNOWN
        known_nozio = self.state.nozzle_position is not NozzlePosition.UNKNOWN
        recent = self.state.last_valid_data_mono is not None
        unresolved = self.state.pending_exchange
        synced = bool(
            self.state.communication_online
            and known_dc1
            and known_nozio
            and recent
            and not unresolved
        )
        self.state.state_synchronized = synced
        return synced

    def status_observed_after(
        self, expected: ObservedStatus, *, not_before_mono: float
    ) -> bool:
        if self.state.observed_status is not expected:
            return False
        if self.state.last_status_time is None:
            return False
        return self.state.last_status_time > not_before_mono

    def _record_persistent_fault(self, code: str) -> None:
        self.state.last_persistent_fault = code
        self.state.last_error = code
        self.state.consecutive_protocol_faults += 1
        if (
            self.state.consecutive_protocol_faults
            >= self.thresholds.faulted_after_protocol_errors
        ):
            self._set_communication(CommunicationHealth.FAULTED)

    @property
    def wire_address(self) -> int:
        """Captured-profile on-wire ADR for this logical pump side."""
        return encode_wire_address(self.address)

    def build_poll(self) -> bytes:
        self.state.stats.poll_count += 1
        self.state.last_poll_at = datetime.now(UTC)
        self.state.last_poll_mono = time.monotonic()
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.POLL_SENT,
                address=self.address,
                timestamp=self.state.last_poll_at,
            )
        )
        return build_poll(self.address)

    def on_timeout(
        self,
        *,
        max_consecutive: int | None = None,
        degraded_after: int | None = None,
        disconnected_after: int | None = None,
    ) -> None:
        degraded = (
            degraded_after
            if degraded_after is not None
            else self.thresholds.degraded_after_timeouts
        )
        disconnected = (
            disconnected_after
            if disconnected_after is not None
            else (
                max_consecutive
                if max_consecutive is not None
                else self.thresholds.disconnected_after_timeouts
            )
        )
        self.state.stats.timeout_count += 1
        self.state.consecutive_timeouts += 1
        self.state.last_error = "response_timeout"
        self.state.last_transient_error = "response_timeout"
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.RESPONSE_TIMEOUT,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail=f"consecutive={self.state.consecutive_timeouts}",
            )
        )
        if self.state.consecutive_timeouts >= disconnected:
            self._set_communication(CommunicationHealth.DISCONNECTED)
            self._apply_sm(PumpEvent.COMMUNICATION_LOST)
            self._arm_reconnect_reconcile()
        elif self.state.consecutive_timeouts >= degraded:
            self._set_communication(CommunicationHealth.DEGRADED)

    def mark_serial_lost(self) -> None:
        """Mark pump DISCONNECTED due to serial port loss (not a protocol timeout)."""
        self._set_communication(CommunicationHealth.DISCONNECTED)
        if self.machine.context.current_state is not PumpState.DISCONNECTED:
            self._apply_sm(PumpEvent.COMMUNICATION_LOST)
        self._arm_reconnect_reconcile()

    def _arm_reconnect_reconcile(self) -> None:
        ctx = self.machine.context
        if ctx.active_transaction_id is not None or ctx.has_unresolved_transaction:
            self._persisted_for_reconcile = ctx
            self._needs_restart_reconcile = True

    def handle_response_frame(
        self,
        frame: DartLineFrame,
        *,
        capture_mono: float | None = None,
    ) -> bytes | None:
        """Process a pump response. Return optional ACK frame bytes to send."""
        self.state.last_raw_frame = frame.raw_frame
        self.state.last_response_at = datetime.now(UTC)
        if capture_mono is not None:
            self.state.last_valid_frame_time = capture_mono
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.FRAME_RECEIVED,
                address=self.address,
                timestamp=self.state.last_response_at,
                detail=frame.control_type.value,
                payload={"raw_hex": frame.raw_frame.hex(" ")},
            )
        )

        if frame.address != self.wire_address:
            self.state.stats.address_mismatch_count += 1
            self._record_persistent_fault("address_mismatch")
            return None

        if frame.control_type is ControlType.EOT:
            return self._on_eot(frame, capture_mono=capture_mono)
        if frame.control_type is ControlType.DATA:
            return self._on_data(frame, capture_mono=capture_mono)
        if frame.control_type is ControlType.NAK:
            return self._on_nak(frame)
        if frame.control_type is ControlType.ACK:
            # Short ACK observed on poll bus — link alive, not application DATA.
            self.note_short_bus_response(capture_mono=capture_mono)
            self.state.last_rx_ack_sequence = frame.sequence
            self.state.last_ack_time = (
                capture_mono if capture_mono is not None else time.monotonic()
            )
            return None
        self._record_persistent_fault(f"unexpected_control_{frame.control_type.value}")
        return None

    def _on_eot(
        self, frame: DartLineFrame, *, capture_mono: float | None = None
    ) -> bytes | None:
        del frame
        self.state.stats.eot_count += 1
        self.state.last_valid_frame = self.state.last_raw_frame
        self._mark_valid_response(kind="EOT", capture_mono=capture_mono)
        self._set_communication(CommunicationHealth.HEALTHY)
        self._clear_transient_communication_error()
        if self.machine.context.current_state is PumpState.DISCONNECTED:
            self._apply_sm(PumpEvent.COMMUNICATION_STARTED)
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.EOT_RECEIVED,
                address=self.address,
                timestamp=datetime.now(UTC),
            )
        )
        return None

    def _on_nak(self, frame: DartLineFrame) -> bytes | None:
        self.state.stats.nak_count += 1
        self.state.stats.retry_count += 1
        self._record_persistent_fault(f"nak_seq_{frame.sequence}")
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.NAK_RECEIVED,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail=f"seq={frame.sequence}",
            )
        )
        return None

    def _on_data(
        self, frame: DartLineFrame, *, capture_mono: float | None = None
    ) -> bytes | None:
        if frame.crc_valid is False:
            self.state.stats.crc_error_count += 1
            self._record_persistent_fault("invalid_crc")
            self.events.publish(
                ControllerEvent(
                    type=ControllerEventType.FRAME_REJECTED,
                    address=self.address,
                    timestamp=datetime.now(UTC),
                    detail="invalid_crc",
                )
            )
            # Do not decode, do not mutate state, do not ACK.
            return None

        # Duplicate DATA: ACK, do not re-apply application events.
        if (
            self.state.last_accepted_rx_sequence is not None
            and frame.sequence == self.state.last_accepted_rx_sequence
        ):
            self.state.stats.duplicate_count += 1
            self._mark_valid_response(kind="DATA", capture_mono=capture_mono)
            self._set_communication(CommunicationHealth.HEALTHY)
            self._clear_transient_communication_error()
            ack = build_ack(self.wire_address, frame.sequence)
            self.state.stats.ack_sent_count += 1
            self.events.publish(
                ControllerEvent(
                    type=ControllerEventType.ACK_SENT,
                    address=self.address,
                    timestamp=datetime.now(UTC),
                    detail="duplicate",
                )
            )
            return ack

        if frame.sequence != self.state.expected_rx_sequence:
            self.state.stats.sequence_error_count += 1
            detail = (
                f"seq_mismatch expected={self.state.expected_rx_sequence} "
                f"got={frame.sequence}"
            )
            if not self.soft_rx_sequence:
                self._record_persistent_fault(detail)
                self.events.publish(
                    ControllerEvent(
                        type=ControllerEventType.FRAME_REJECTED,
                        address=self.address,
                        timestamp=datetime.now(UTC),
                        detail=detail,
                    )
                )
                # Strict mode: do not apply; no ACK for unexpected seq from pump.
                return None
            # Soft mode: ACK CRC-valid DATA by frame seq and resync expected.
            self.state.stats.seq_resync_count += 1
            self.state.last_transient_error = detail
            self.events.publish(
                ControllerEvent(
                    type=ControllerEventType.FRAME_REJECTED,
                    address=self.address,
                    timestamp=datetime.now(UTC),
                    detail=f"seq_resync:{detail}",
                )
            )

        self.state.stats.data_count += 1
        self.state.last_valid_frame = frame.raw_frame
        self._mark_valid_response(kind="DATA", capture_mono=capture_mono)
        self._set_communication(CommunicationHealth.HEALTHY)
        self._clear_transient_communication_error()
        if self.machine.context.current_state is PumpState.DISCONNECTED:
            self._apply_sm(PumpEvent.COMMUNICATION_STARTED)

        self._decode_and_apply(frame, capture_mono=capture_mono)

        self.state.last_accepted_rx_sequence = frame.sequence
        self.state.expected_rx_sequence = next_sequence(
            frame.sequence, self.sequence_policy
        )

        ack = build_ack(self.wire_address, frame.sequence)
        self.state.stats.ack_sent_count += 1
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.DATA_RECEIVED,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail=f"seq={frame.sequence}",
            )
        )
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.ACK_SENT,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail=f"seq={frame.sequence}",
            )
        )
        return ack

    def _decode_and_apply(
        self, frame: DartLineFrame, *, capture_mono: float | None = None
    ) -> None:
        # Controller-owned poll/session RX is always pump→controller.
        bundle = decode_data_payload(
            frame.payload,
            pump_address=self.address,
            line_sequence=frame.sequence,
            source_frame_raw_hex=frame.raw_frame.hex(" "),
        )
        obs_mono = capture_mono if capture_mono is not None else time.monotonic()
        for tx in bundle.transactions:
            decoded = tx.decoded_body or {}
            volume = decoded.get("volume") if isinstance(decoded, dict) else None
            amount = decoded.get("amount") if isinstance(decoded, dict) else None
            price = decoded.get("price") if isinstance(decoded, dict) else None
            raw_volume = (
                volume.get("raw_scaled") if isinstance(volume, dict) else None
            )
            raw_amount = (
                amount.get("raw_scaled") if isinstance(amount, dict) else None
            )
            self.events.publish(
                ControllerEvent(
                    type=ControllerEventType.APPLICATION_TRANSACTION_DECODED,
                    address=self.address,
                    timestamp=datetime.now(UTC),
                    detail=tx.transaction_type.value,
                    payload={
                        "decode_status": tx.decode_status.value,
                        "direction": tx.direction.value,
                        "transaction_type": tx.transaction_type.value,
                        "transaction_id": tx.transaction_id,
                        "source_frame_ref": tx.source_frame_raw_hex,
                        "raw_volume": raw_volume,
                        "volume_decimals": (
                            volume.get("decimals") if isinstance(volume, dict) else None
                        ),
                        "raw_amount": raw_amount,
                        "amount_decimals": (
                            amount.get("decimals") if isinstance(amount, dict) else None
                        ),
                        "raw_price": (
                            price.get("raw_scaled") if isinstance(price, dict) else None
                        ),
                        "price_decimals": (
                            price.get("decimals") if isinstance(price, dict) else None
                        ),
                    },
                )
            )
            if self.state.pending_exchange:
                self.state.pending_command_events.append(tx.transaction_type.value)
            if isinstance(raw_volume, int):
                self._note_dc2_volume(raw_volume, at=datetime.now(UTC))
                self.state.filled_volume_raw = max(self.state.filled_volume_raw, raw_volume)
            if isinstance(raw_amount, int):
                self.state.filled_amount_raw = max(self.state.filled_amount_raw, raw_amount)
            self.state.sale_evidence.note_dc2(
                volume_raw=raw_volume if isinstance(raw_volume, int) else None,
                amount_raw=raw_amount if isinstance(raw_amount, int) else None,
            )
            ctx = self.machine.context
            mapped = map_wayne_observation(
                tx,
                context=MapperContext.from_pump_context(
                    ctx,
                    resolve_as_dc1=True,
                    resolve_as_dc3=True,
                    bus_direction=MessageDirection.SLAVE_TO_MASTER,
                ),
            )
            self._update_observed_from_mapped(mapped, capture_mono=obs_mono)
            if self._needs_restart_reconcile:
                self._reconcile_then_apply(
                    mapped,
                    raw_volume=raw_volume if isinstance(raw_volume, int) else None,
                )
            else:
                self._apply_mapped(
                    mapped,
                    dispensed_volume_raw=(
                        raw_volume if isinstance(raw_volume, int) else None
                    ),
                )

    def _update_observed_from_mapped(
        self, mapped: MappedWayneObservation, *, capture_mono: float
    ) -> None:
        if mapped.raw_wayne_status is not None:
            try:
                status = WaynePumpStatus(mapped.raw_wayne_status)
                status_map = {
                    WaynePumpStatus.PUMP_NOT_PROGRAMMED: ObservedStatus.NOT_PROGRAMMED,
                    WaynePumpStatus.RESET: ObservedStatus.RESET,
                    WaynePumpStatus.AUTHORIZED: ObservedStatus.AUTHORIZED,
                    WaynePumpStatus.FILLING: ObservedStatus.FILLING,
                    WaynePumpStatus.FILLING_COMPLETED: ObservedStatus.FILLING_COMPLETED,
                    WaynePumpStatus.MAX_AMOUNT_VOLUME_REACHED: (
                        ObservedStatus.MAX_AMOUNT_VOLUME_REACHED
                    ),
                    WaynePumpStatus.SWITCHED_OFF: ObservedStatus.SWITCHED_OFF,
                    WaynePumpStatus.SUSPENDED: ObservedStatus.SUSPENDED,
                }
                observed = status_map.get(status)
                if observed is not None:
                    self.state.observed_status = observed
                self.state.last_status_time = capture_mono
                if status is WaynePumpStatus.FILLING:
                    self._filling_seen_this_boot = True
                    self.state.sale_evidence.note_filling()
                    self.state.sale_lifecycle = SaleLifecycle.FILLING
                elif status is WaynePumpStatus.AUTHORIZED:
                    self.state.sale_evidence.note_authorized(
                        application_confirmed=True
                    )
                    self.state.sale_lifecycle = SaleLifecycle.AUTHORIZED
                elif status is WaynePumpStatus.FILLING_COMPLETED:
                    self.state.sale_evidence.filling_completed_observed = True
                elif status is WaynePumpStatus.RESET:
                    if self.state.sale_lifecycle in {
                        SaleLifecycle.ABORTED_NO_DELIVERY,
                        SaleLifecycle.ABORTED,
                        SaleLifecycle.FILLING_COMPLETED,
                        SaleLifecycle.CLOSED,
                    }:
                        self.state.sale_lifecycle = SaleLifecycle.IDLE
                        self.state.sale_evidence.reset_attempt()
                        self.state.filled_volume_raw = 0
                        self.state.filled_amount_raw = 0
            except ValueError:
                pass
        if mapped.nozzle_out is not None:
            new_pos = NozzlePosition.OUT if mapped.nozzle_out else NozzlePosition.IN
            prev = self.state.nozzle_position
            # Edge-only NOZIO events are handled by mapper; still update observed.
            if prev is NozzlePosition.UNKNOWN:
                self.state.nozzle_position = new_pos
            elif prev is not new_pos:
                self.state.nozzle_position = new_pos
                if new_pos is NozzlePosition.OUT:
                    self.state.sale_evidence.note_nozzle_out()
                    self.state.sale_lifecycle = SaleLifecycle.NOZZLE_LIFTED
                elif new_pos is NozzlePosition.IN:
                    ev = self.state.sale_evidence
                    if ev.lifecycle in {
                        SaleLifecycle.NOZZLE_LIFTED,
                        SaleLifecycle.AUTHORIZED,
                    } and not ev.has_positive_delivery:
                        self.state.sale_evidence.note_nozzle_in_zero_delivery()
                        self.state.sale_lifecycle = SaleLifecycle.ABORTED_NO_DELIVERY
            self.state.last_nozio_time = capture_mono
            if mapped.logical_nozzle_raw is not None:
                self.state.logical_nozzle = mapped.logical_nozzle_raw
            elif mapped.selected_nozzle is not None:
                self.state.logical_nozzle = mapped.selected_nozzle
        if mapped.filling_price_raw is not None:
            self.state.unit_price_raw = mapped.filling_price_raw
        self.state.sale_lifecycle = self.state.sale_evidence.lifecycle
        self.evaluate_synchronized()

    def _note_dc2_volume(self, raw_volume: int, *, at: datetime) -> None:
        if self._last_dc2_volume != raw_volume:
            self._last_dc2_volume = raw_volume
            self._dc2_last_changed_at = at

    def _reconcile_then_apply(
        self,
        mapped: MappedWayneObservation,
        *,
        raw_volume: int | None,
    ) -> None:
        persisted = self._persisted_for_reconcile or self.machine.context
        observed_at = datetime.now(UTC)
        hint: PumpState | None = None
        if mapped.event in {
            PumpEvent.FILLING_STARTED,
            PumpEvent.FILLING_UPDATED,
        }:
            hint = PumpState.FILLING
        elif mapped.event is PumpEvent.FILLING_COMPLETED:
            hint = PumpState.FILLING_COMPLETE
        elif mapped.event is PumpEvent.RESET_OBSERVED:
            hint = PumpState.RESET
        elif mapped.event is PumpEvent.LIMIT_REACHED:
            hint = PumpState.LIMIT_REACHED
        live = LiveObservationSummary(
            communication_healthy=True,
            wayne_status=mapped.raw_wayne_status,
            normalized_hint=hint,
            observed_at=observed_at,
            selected_nozzle=mapped.selected_nozzle,
            nozzle_out=mapped.nozzle_out,
            dispensed_volume_raw=(
                raw_volume
                if isinstance(raw_volume, int)
                else persisted.dispensed_volume_raw
            ),
            raw_source_hex=(
                mapped.observation.source_frame_raw_hex
                if mapped.observation
                else None
            ),
        )
        result = reconcile_after_restart(
            persisted,
            live,
            had_active_transaction_persisted=persisted.active_transaction_id
            is not None
            or persisted.has_unresolved_transaction,
        )
        before = self.machine.context.current_state
        self.machine = PumpStateMachine(result.context)
        self.state.last_state = result.context.current_state
        self._needs_restart_reconcile = False
        self._persisted_for_reconcile = None
        if result.warnings:
            self._publish_reconciliation_warning(
                warnings=result.warnings,
                inferences=(),
                context=result.context,
            )
        if before is not result.context.current_state:
            finalize_unresolved = (
                result.recovered_state is PumpState.FILLING_COMPLETE
                and result.preserved_unresolved_transaction_id
            )
            retained_complete = (
                result.recovered_state is PumpState.FILLING_COMPLETE
                and not result.preserved_unresolved_transaction_id
            )
            self._publish_state_changed(
                before=before,
                after=result.context.current_state,
                event_name="RESTART_RECONCILED",
                context=result.context,
                completion_evidence_key=(
                    f"restart-complete:{result.preserved_unresolved_transaction_id}"
                    if finalize_unresolved
                    else None
                ),
            )
            if retained_complete:
                self._publish_startup_baseline(mapped=mapped, context=result.context)
        elif (
            result.recovered_state is PumpState.FILLING_COMPLETE
            and result.preserved_unresolved_transaction_id
        ):
            # Case B: already FILLING_COMPLETE in memory; still finalize once.
            key = f"restart-complete:{result.preserved_unresolved_transaction_id}"
            self._publish_state_changed(
                before=before,
                after=result.context.current_state,
                event_name=PumpEvent.FILLING_COMPLETED.value,
                context=result.context.with_updates(awaiting_filling_complete=False),
                completion_evidence_key=key,
            )
        elif (
            result.recovered_state is PumpState.FILLING_COMPLETE
            and not result.preserved_unresolved_transaction_id
        ):
            # Retained completed face after restart/reconnect — baseline only.
            self._publish_startup_baseline(mapped=mapped, context=result.context)
        else:
            # Continue applying this observation onto the reconciled context
            # (e.g. DC2 volume after FILLING restore) without duplicating txs.
            self._apply_mapped(mapped)

    def _publish_startup_baseline(
        self,
        *,
        mapped: MappedWayneObservation,
        context: PumpContext,
    ) -> None:
        """Record the dispenser face totals as already-observed; never publish."""
        vol = (
            context.dispensed_volume_raw
            if isinstance(context.dispensed_volume_raw, int)
            else self.state.filled_volume_raw
        )
        amt = self.state.filled_amount_raw
        # Retained COMPLETED face is not a new sale — clear in-memory evidence
        # so later repeated COMPLETED frames cannot synthesize filling_observed.
        self.state.sale_evidence.reset_attempt()
        self.state.sale_lifecycle = SaleLifecycle.IDLE
        self._filling_seen_this_boot = False
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail="startup_baseline",
                payload={
                    "event": "STARTUP_BASELINE_OBSERVED",
                    "previous_state": context.current_state.value,
                    "normalized_state": context.current_state.value,
                    "state_version": context.state_version,
                    "selected_nozzle": mapped.selected_nozzle or context.selected_nozzle,
                    "active_transaction_id": None,
                    "communication_healthy": context.communication_healthy,
                    "raw_wayne_status": context.last_raw_wayne_status,
                    "source_frame_ref": context.last_source_frame_hex,
                    "completion_evidence_key": None,
                    "awaiting_filling_complete": False,
                    "completion_inferred": False,
                    "dispensed_volume_raw": vol,
                    "filled_volume_raw": vol if isinstance(vol, int) else 0,
                    "filled_amount_raw": amt if isinstance(amt, int) else 0,
                    "filling_price_raw": mapped.filling_price_raw,
                    "nozzle_out": context.nozzle_out,
                    "has_unresolved_transaction": False,
                    "startup_baseline": True,
                    "may_publish_sale": False,
                    "sale_lifecycle": SaleLifecycle.IDLE.value,
                    "warnings": [
                        "startup_baseline: retained completed sale observed; not published"
                    ],
                },
            )
        )

    def _apply_mapped(
        self,
        mapped: MappedWayneObservation,
        *,
        dispensed_volume_raw: int | None = None,
    ) -> None:
        if (
            mapped.completion_evidence_key
            and mapped.completion_evidence_key in self._applied_completion_keys
        ):
            return
        if mapped.event in {
            PumpEvent.FILLING_STARTED,
            PumpEvent.FILLING_UPDATED,
        }:
            self._filling_seen_this_boot = True
            self.state.sale_evidence.note_filling()
            # New FILLING_STARTED must not keep max() peaks from a prior sale
            # face. Do not wipe meter progress that already arrived while
            # AUTHORIZED (DC2 often precedes the FILLING DC1).
            if mapped.event is PumpEvent.FILLING_STARTED:
                prior = self.machine.context.current_state
                if prior not in {
                    PumpState.AUTHORIZED,
                    PumpState.FILLING,
                    PumpState.NOZZLE_UP,
                    PumpState.SUSPENDED,
                }:
                    self.state.filled_volume_raw = 0
                    self.state.filled_amount_raw = 0
                    self.state.sale_evidence.reset_attempt()
                self.state.sale_evidence.note_filling()
                self.state.sale_lifecycle = SaleLifecycle.FILLING
        # Gate sale finalize: FILLING_COMPLETED without valid evidence → no sale.
        if mapped.event is PumpEvent.FILLING_COMPLETED:
            ctx0 = self.machine.context
            # Credit filling only from a real FILLING observation this boot —
            # never from a retained FILLING_COMPLETE face after restart.
            if (
                self._filling_seen_this_boot
                or ctx0.current_state is PumpState.FILLING
                or ctx0.previous_state is PumpState.FILLING
            ):
                self.state.sale_evidence.filling_observed = True
            # Prefer live DC2 peaks. Fall back to SM dispensed_volume_raw only when
            # live peaks are empty (timeout finalize without a recent DC2 tick).
            # Never max() a stale SM volume onto a non-zero live peak (42 vs 19).
            live_vol = int(self.state.filled_volume_raw or 0)
            live_amt = int(self.state.filled_amount_raw or 0)
            stale_sm = ctx0.dispensed_volume_raw
            if live_vol > 0 or live_amt > 0:
                self.state.sale_evidence.note_dc2(
                    volume_raw=live_vol if live_vol > 0 else None,
                    amount_raw=live_amt if live_amt > 0 else None,
                )
                if (
                    isinstance(stale_sm, int)
                    and stale_sm > 0
                    and live_vol > 0
                    and stale_sm != live_vol
                ):
                    logger.warning(
                        "sale_totals_inconsistent",
                        address=self.address,
                        reason="stale_sm_volume_ignored",
                        liveVolumeMinorUnits=live_vol,
                        staleSmVolumeMinorUnits=stale_sm,
                        liveAmountMinorUnits=live_amt,
                        unitPriceMinorUnits=self.state.unit_price_raw,
                    )
            elif isinstance(stale_sm, int) and stale_sm > 0:
                self.state.sale_evidence.note_dc2(
                    volume_raw=stale_sm,
                    amount_raw=max(live_amt, 1) if live_amt <= 0 else live_amt,
                )
                self.state.filled_volume_raw = stale_sm
            price = self.state.unit_price_raw
            if (
                isinstance(price, int)
                and price > 0
                and int(self.state.filled_volume_raw or 0) > 0
                and int(self.state.filled_amount_raw or 0) > 0
            ):
                # Face price is ₦/L integer (1175). Volume/amount minor units are
                # 2 dp: amount_raw = volume_raw * price_raw
                # (e.g. 19 * 1175 = 22325 → ₦223.25 for 0.19 L).
                fv = int(self.state.filled_volume_raw)
                fa = int(self.state.filled_amount_raw)
                expected_amt = fv * price
                tol = max(price // 100, 1)
                if abs(fa - expected_amt) > tol:
                    logger.warning(
                        "sale_totals_inconsistent",
                        address=self.address,
                        reason="amount_volume_price_mismatch",
                        volumeMinorUnits=fv,
                        amountMinorUnits=fa,
                        unitPriceMinorUnits=price,
                        expectedAmountMinorUnits=expected_amt,
                    )
            may_sale, reason = self.state.sale_evidence.evaluate_filling_completed()
            self.state.sale_lifecycle = self.state.sale_evidence.lifecycle
            if not may_sale:
                # Cold start / reconnect with retained COMPLETED face: baseline it.
                if (
                    reason == "filling_completed_without_filling"
                    and self.state.sale_evidence.has_positive_delivery
                    and not self._filling_seen_this_boot
                ):
                    self._publish_startup_baseline(mapped=mapped, context=ctx0)
                    # Observe status in the SM without a publishable completion key.
                    mapped = MappedWayneObservation(
                        event=mapped.event,
                        observation=mapped.observation,
                        raw_wayne_status=mapped.raw_wayne_status,
                        selected_nozzle=mapped.selected_nozzle,
                        logical_nozzle_raw=mapped.logical_nozzle_raw,
                        nozzle_out=mapped.nozzle_out,
                        nozio_raw=mapped.nozio_raw,
                        filling_price_raw=mapped.filling_price_raw,
                        completion_evidence_key=None,
                        awaiting_filling_complete=False,
                        completion_inferred=mapped.completion_inferred,
                        allow_implicit_authorize_to_filling=(
                            mapped.allow_implicit_authorize_to_filling
                        ),
                        filling_inferred_from_dc2=mapped.filling_inferred_from_dc2,
                        inferences=mapped.inferences,
                        warnings=(
                            *mapped.warnings,
                            "startup_baseline: retained completed sale; not published",
                        ),
                    )
                else:
                    self._publish_state_changed(
                        before=self.machine.context.current_state,
                        after=self.machine.context.current_state,
                        event_name="SALE_SUPPRESSED",
                        context=self.machine.context.with_updates(
                            warnings=(
                                *self.machine.context.warnings,
                                f"sale_suppressed:{reason}",
                            )
                        ),
                        completion_evidence_key=None,
                    )
                    # Still allow SM to observe status, but strip completion key so
                    # persistence does not finalize a paid sale.
                    mapped = MappedWayneObservation(
                        event=mapped.event,
                        observation=mapped.observation,
                        raw_wayne_status=mapped.raw_wayne_status,
                        selected_nozzle=mapped.selected_nozzle,
                        logical_nozzle_raw=mapped.logical_nozzle_raw,
                        nozzle_out=mapped.nozzle_out,
                        nozio_raw=mapped.nozio_raw,
                        filling_price_raw=mapped.filling_price_raw,
                        completion_evidence_key=None,
                        awaiting_filling_complete=False,
                        completion_inferred=mapped.completion_inferred,
                        allow_implicit_authorize_to_filling=(
                            mapped.allow_implicit_authorize_to_filling
                        ),
                        filling_inferred_from_dc2=mapped.filling_inferred_from_dc2,
                        inferences=mapped.inferences,
                        warnings=(*mapped.warnings, f"sale_suppressed:{reason}"),
                    )
        before_ctx = self.machine.context
        before = before_ctx.current_state
        was_awaiting = before_ctx.awaiting_filling_complete
        sm_volume = dispensed_volume_raw
        if sm_volume is None and mapped.event in {
            PumpEvent.FILLING_UPDATED,
            PumpEvent.FILLING_STARTED,
            PumpEvent.FILLING_COMPLETED,
        }:
            sm_volume = (
                int(self.state.filled_volume_raw)
                if self.state.filled_volume_raw
                else None
            )
        result = self.machine.apply_mapped(
            mapped, dispensed_volume_raw=sm_volume
        )
        after_ctx = result.context
        after = after_ctx.current_state
        self.state.last_state = after

        if after_ctx.awaiting_filling_complete and not was_awaiting:
            self._await_started_at = after_ctx.last_observation_at or datetime.now(UTC)
            self._insufficient_evidence_warned = False
        if was_awaiting and not after_ctx.awaiting_filling_complete:
            self._await_started_at = None
            self._insufficient_evidence_warned = False

        # FILLING_COMPLETE + FILLING_COMPLETED is a same-state noop in the SM,
        # but still finalizes a hang-up await and must publish once.
        cleared_await = was_awaiting and not after_ctx.awaiting_filling_complete
        finalized = (
            mapped.event is PumpEvent.FILLING_COMPLETED
            and mapped.completion_evidence_key is not None
            and result.accepted
            and (not result.noop or cleared_await or mapped.completion_inferred)
        )
        hangup_await = (
            after_ctx.awaiting_filling_complete
            and not was_awaiting
            and mapped.event is PumpEvent.NOZZLE_RETURNED
        )
        if mapped.completion_evidence_key and result.accepted and (
            not result.noop or finalized
        ):
            self._applied_completion_keys.add(mapped.completion_evidence_key)
        if before is not after or finalized or hangup_await:
            self._publish_state_changed(
                before=before,
                after=after,
                event_name=mapped.event.value,
                context=after_ctx,
                completion_evidence_key=mapped.completion_evidence_key,
            )

    def _apply_sm(self, event: PumpEvent) -> None:
        before = self.machine.context.current_state
        result = self.machine.apply(event)
        after = result.context.current_state
        self.state.last_state = after
        if before is not after:
            self._publish_state_changed(
                before=before,
                after=after,
                event_name=event.value,
                context=result.context,
                completion_evidence_key=None,
            )

    def _publish_state_changed(
        self,
        *,
        before: PumpState,
        after: PumpState,
        event_name: str,
        context: PumpContext,
        completion_evidence_key: str | None,
    ) -> None:
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail=f"{before.value}->{after.value}",
                payload={
                    "event": event_name,
                    "previous_state": before.value,
                    "normalized_state": after.value,
                    "state_version": context.state_version,
                    "selected_nozzle": context.selected_nozzle,
                    "active_transaction_id": context.active_transaction_id,
                    "communication_healthy": context.communication_healthy,
                    "raw_wayne_status": context.last_raw_wayne_status,
                    "source_frame_ref": context.last_source_frame_hex,
                    "completion_evidence_key": completion_evidence_key,
                    "awaiting_filling_complete": context.awaiting_filling_complete,
                    "completion_inferred": context.completion_inferred,
                    "dispensed_volume_raw": context.dispensed_volume_raw,
                    "nozzle_out": context.nozzle_out,
                    "has_unresolved_transaction": context.has_unresolved_transaction,
                    "warnings": list(context.warnings[-8:]),
                    "sale_lifecycle": self.state.sale_lifecycle.value,
                    "may_publish_sale": (
                        completion_evidence_key is not None
                        and (
                            (
                                self.state.sale_evidence.lifecycle
                                is SaleLifecycle.FILLING_COMPLETED
                                and self.state.sale_evidence.has_positive_delivery
                                and not self.state.sale_evidence.aborted
                            )
                            or (
                                isinstance(completion_evidence_key, str)
                                and completion_evidence_key.startswith(
                                    "restart-complete:"
                                )
                                and context.active_transaction_id is not None
                            )
                        )
                    ),
                    "filled_volume_raw": self.state.filled_volume_raw,
                    "filled_amount_raw": self.state.filled_amount_raw,
                },
            )
        )

    def _publish_reconciliation_warning(
        self,
        *,
        warnings: tuple[str, ...],
        inferences: tuple[str, ...],
        context: PumpContext,
    ) -> None:
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail="reconciliation_warning",
                payload={
                    "event": "RECONCILIATION_WARNING",
                    "previous_state": context.current_state.value,
                    "normalized_state": context.current_state.value,
                    "state_version": context.state_version,
                    "selected_nozzle": context.selected_nozzle,
                    "active_transaction_id": context.active_transaction_id,
                    "communication_healthy": context.communication_healthy,
                    "raw_wayne_status": context.last_raw_wayne_status,
                    "source_frame_ref": context.last_source_frame_hex,
                    "completion_evidence_key": None,
                    "awaiting_filling_complete": context.awaiting_filling_complete,
                    "completion_inferred": context.completion_inferred,
                    "audit_only": True,
                    "warnings": list(warnings),
                    "inferences": list(inferences),
                },
            )
        )

    def _set_communication(self, health: CommunicationHealth) -> None:
        prev = self.state.communication
        self.state.communication = health
        if prev is health:
            return
        if self.transitions is not None:
            if health is CommunicationHealth.DEGRADED:
                self.transitions.emit(
                    "pump_communication_degraded",
                    pump_address=self.address,
                    previous_state=prev.value,
                    new_state=health.value,
                    consecutive_timeouts=self.state.consecutive_timeouts,
                    cumulative_timeouts=self.state.stats.timeout_count,
                )
            elif health is CommunicationHealth.DISCONNECTED:
                self.transitions.emit(
                    "pump_disconnected",
                    pump_address=self.address,
                    previous_state=prev.value,
                    new_state=health.value,
                    consecutive_timeouts=self.state.consecutive_timeouts,
                    cumulative_timeouts=self.state.stats.timeout_count,
                )
            elif health is CommunicationHealth.HEALTHY and prev in {
                CommunicationHealth.DEGRADED,
                CommunicationHealth.DISCONNECTED,
                CommunicationHealth.FAULTED,
                CommunicationHealth.UNKNOWN,
            }:
                self.transitions.emit(
                    "pump_communication_recovered",
                    pump_address=self.address,
                    previous_state=prev.value,
                    new_state=health.value,
                    consecutive_timeouts=self.state.consecutive_timeouts,
                    cumulative_timeouts=self.state.stats.timeout_count,
                )
            elif health is CommunicationHealth.FAULTED:
                self.transitions.emit(
                    "persistent_protocol_fault",
                    pump_address=self.address,
                    previous_state=prev.value,
                    new_state=health.value,
                    error=self.state.last_persistent_fault,
                    consecutive_timeouts=self.state.consecutive_timeouts,
                    cumulative_timeouts=self.state.stats.timeout_count,
                )
        if health is CommunicationHealth.HEALTHY and prev in {
            CommunicationHealth.UNKNOWN,
            CommunicationHealth.DISCONNECTED,
            CommunicationHealth.DEGRADED,
            CommunicationHealth.FAULTED,
        }:
            self.events.publish(
                ControllerEvent(
                    type=ControllerEventType.PUMP_CONNECTED,
                    address=self.address,
                    timestamp=datetime.now(UTC),
                )
            )
        if health is CommunicationHealth.DISCONNECTED:
            self.events.publish(
                ControllerEvent(
                    type=ControllerEventType.PUMP_DISCONNECTED,
                    address=self.address,
                    timestamp=datetime.now(UTC),
                )
            )
