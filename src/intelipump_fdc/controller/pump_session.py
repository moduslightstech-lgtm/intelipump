"""Per-address DART protocol session (controller side)."""

from __future__ import annotations

from datetime import UTC, datetime

from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.controller.session_models import (
    CommunicationHealth,
    PumpSessionState,
)
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_ack, build_poll
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.simulator.config import SequencePolicy, next_sequence
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.wayne_mapper import (
    MappedWayneObservation,
    MapperContext,
    map_wayne_observation,
)

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
    ) -> None:
        self.state = PumpSessionState(address=address, pump_id=pump_id)
        self.events = events
        self.sequence_policy = sequence_policy
        self.machine = PumpStateMachine(
            PumpContext(
                pump_id=pump_id,
                dart_address=address,
                current_state=PumpState.DISCONNECTED,
                communication_healthy=False,
            )
        )
        self._applied_completion_keys: set[str] = set()

    @property
    def address(self) -> int:
        return self.state.address

    def _clear_transient_communication_error(self) -> None:
        """Clear stale transient link errors after a valid EOT/DATA recovery.

        Does not clear persistent protocol/configuration faults (CRC, sequence,
        address mismatch, unexpected control, NAK). Does not alter counters.
        """
        if self.state.last_error in _TRANSIENT_COMMUNICATION_ERRORS:
            self.state.last_error = None

    def build_poll(self) -> bytes:
        self.state.stats.poll_count += 1
        self.state.last_poll_at = datetime.now(UTC)
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.POLL_SENT,
                address=self.address,
                timestamp=self.state.last_poll_at,
            )
        )
        return build_poll(self.address)

    def on_timeout(self, *, max_consecutive: int) -> None:
        self.state.stats.timeout_count += 1
        self.state.consecutive_timeouts += 1
        self.state.last_error = "response_timeout"
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.RESPONSE_TIMEOUT,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail=f"consecutive={self.state.consecutive_timeouts}",
            )
        )
        if self.state.consecutive_timeouts >= max_consecutive:
            self._set_communication(CommunicationHealth.DISCONNECTED)
            self._apply_sm(PumpEvent.COMMUNICATION_LOST)
        elif self.state.consecutive_timeouts >= 2:
            self._set_communication(CommunicationHealth.DEGRADED)

    def handle_response_frame(self, frame: DartLineFrame) -> bytes | None:
        """Process a pump response. Return optional ACK frame bytes to send."""
        self.state.last_raw_frame = frame.raw_frame
        self.state.last_response_at = datetime.now(UTC)
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.FRAME_RECEIVED,
                address=self.address,
                timestamp=self.state.last_response_at,
                detail=frame.control_type.value,
                payload={"raw_hex": frame.raw_frame.hex(" ")},
            )
        )

        if frame.address != self.address:
            self.state.last_error = "address_mismatch"
            return None

        if frame.control_type is ControlType.EOT:
            return self._on_eot(frame)
        if frame.control_type is ControlType.DATA:
            return self._on_data(frame)
        if frame.control_type is ControlType.NAK:
            return self._on_nak(frame)
        self.state.last_error = f"unexpected_control_{frame.control_type.value}"
        return None

    def _on_eot(self, frame: DartLineFrame) -> bytes | None:
        del frame
        self.state.stats.eot_count += 1
        self.state.consecutive_timeouts = 0
        self.state.last_valid_frame = self.state.last_raw_frame
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
        self.state.last_error = f"nak_seq_{frame.sequence}"
        self.events.publish(
            ControllerEvent(
                type=ControllerEventType.NAK_RECEIVED,
                address=self.address,
                timestamp=datetime.now(UTC),
                detail=f"seq={frame.sequence}",
            )
        )
        return None

    def _on_data(self, frame: DartLineFrame) -> bytes | None:
        if frame.crc_valid is False:
            self.state.stats.crc_error_count += 1
            self.state.last_error = "invalid_crc"
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
            self.state.consecutive_timeouts = 0
            self._set_communication(CommunicationHealth.HEALTHY)
            self._clear_transient_communication_error()
            ack = build_ack(self.address, frame.sequence)
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
            self.state.last_error = (
                f"seq_mismatch expected={self.state.expected_rx_sequence} "
                f"got={frame.sequence}"
            )
            self.events.publish(
                ControllerEvent(
                    type=ControllerEventType.FRAME_REJECTED,
                    address=self.address,
                    timestamp=datetime.now(UTC),
                    detail=self.state.last_error,
                )
            )
            # Documented response: do not apply; no ACK for unexpected seq from pump.
            return None

        self.state.stats.data_count += 1
        self.state.consecutive_timeouts = 0
        self.state.last_valid_frame = frame.raw_frame
        self._set_communication(CommunicationHealth.HEALTHY)
        self._clear_transient_communication_error()
        if self.machine.context.current_state is PumpState.DISCONNECTED:
            self._apply_sm(PumpEvent.COMMUNICATION_STARTED)

        self._decode_and_apply(frame)

        self.state.last_accepted_rx_sequence = frame.sequence
        self.state.expected_rx_sequence = next_sequence(
            frame.sequence, self.sequence_policy
        )

        ack = build_ack(self.address, frame.sequence)
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

    def _decode_and_apply(self, frame: DartLineFrame) -> None:
        bundle = decode_data_payload(
            frame.payload,
            pump_address=self.address,
            line_sequence=frame.sequence,
            source_frame_raw_hex=frame.raw_frame.hex(" "),
        )
        for tx in bundle.transactions:
            decoded = tx.decoded_body or {}
            volume = decoded.get("volume") if isinstance(decoded, dict) else None
            amount = decoded.get("amount") if isinstance(decoded, dict) else None
            price = decoded.get("price") if isinstance(decoded, dict) else None
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
                        "raw_volume": (
                            volume.get("raw_scaled")
                            if isinstance(volume, dict)
                            else None
                        ),
                        "volume_decimals": (
                            volume.get("decimals") if isinstance(volume, dict) else None
                        ),
                        "raw_amount": (
                            amount.get("raw_scaled")
                            if isinstance(amount, dict)
                            else None
                        ),
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
            mapped = map_wayne_observation(
                tx,
                context=MapperContext(
                    current_state=self.machine.context.current_state,
                    previous_wayne_status=self.machine.context.last_raw_wayne_status,
                    # Controller received slave→master DATA: resolve DC1/DC3.
                    resolve_as_dc1=tx.transaction_id == 0x01,
                    resolve_as_dc3=tx.transaction_id == 0x03,
                    bus_direction=tx.direction,
                ),
            )
            self._apply_mapped(mapped)

    def _apply_mapped(self, mapped: MappedWayneObservation) -> None:
        if (
            mapped.completion_evidence_key
            and mapped.completion_evidence_key in self._applied_completion_keys
        ):
            return
        before = self.machine.context.current_state
        result = self.machine.apply(
            mapped.event,
            observation=mapped.observation
            or ObservationRef(
                source_frame_raw_hex=None,
                transaction_type=None,
            ),
            selected_nozzle=mapped.selected_nozzle,
            raw_wayne_status=mapped.raw_wayne_status,
            completion_evidence_key=mapped.completion_evidence_key,
            allow_implicit_authorize_to_filling=mapped.allow_implicit_authorize_to_filling,
        )
        if mapped.completion_evidence_key and result.accepted and not result.noop:
            self._applied_completion_keys.add(mapped.completion_evidence_key)
        after = result.context.current_state
        self.state.last_state = after
        if before is not after:
            self._publish_state_changed(
                before=before,
                after=after,
                event_name=mapped.event.value,
                context=result.context,
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
                },
            )
        )

    def _set_communication(self, health: CommunicationHealth) -> None:
        prev = self.state.communication
        self.state.communication = health
        if prev is health:
            return
        if health is CommunicationHealth.HEALTHY and prev in {
            CommunicationHealth.UNKNOWN,
            CommunicationHealth.DISCONNECTED,
            CommunicationHealth.DEGRADED,
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
