"""Controller session and outbound-queue models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import uuid4

from intelipump_fdc.controller.sale_lifecycle import SaleEvidence, SaleLifecycle
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState


class CommunicationHealth(StrEnum):
    UNKNOWN = "UNKNOWN"
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    DISCONNECTED = "DISCONNECTED"
    FAULTED = "FAULTED"


class IdempotencyClass(StrEnum):
    IDEMPOTENT = "IDEMPOTENT"
    NON_IDEMPOTENT = "NON_IDEMPOTENT"


class NozzlePosition(StrEnum):
    UNKNOWN = "UNKNOWN"
    IN = "IN"
    OUT = "OUT"


class ObservedStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    NOT_PROGRAMMED = "NOT_PROGRAMMED"
    RESET = "RESET"
    AUTHORIZED = "AUTHORIZED"
    FILLING = "FILLING"
    FILLING_COMPLETED = "FILLING_COMPLETED"
    MAX_AMOUNT_VOLUME_REACHED = "MAX_AMOUNT_VOLUME_REACHED"
    SWITCHED_OFF = "SWITCHED_OFF"
    SUSPENDED = "SUSPENDED"


@dataclass(frozen=True, slots=True)
class OutboundDataItem:
    """Queued controller→pump DATA (LAB / simulator-only in Phase 6)."""

    correlation_id: str
    address: int
    application_payload: bytes
    created_at: datetime
    expires_at: datetime
    idempotency: IdempotencyClass
    max_retries: int
    simulator_only: bool
    command_type: PumpCommand
    sequence: int | None = None  # assigned at send time; reused on retry
    attempts: int = 0
    # Optional application-confirm expectation (DC1 name after TX time).
    expect_status_after_tx: str | None = None

    @staticmethod
    def create(
        *,
        address: int,
        application_payload: bytes,
        command_type: PumpCommand,
        simulator_only: bool,
        idempotency: IdempotencyClass,
        ttl_ms: int = 5_000,
        max_retries: int = 2,
        expect_status_after_tx: str | None = None,
        sequence: int | None = None,
        attempts: int = 0,
    ) -> OutboundDataItem:
        now = datetime.now(UTC)
        return OutboundDataItem(
            correlation_id=str(uuid4()),
            address=address,
            application_payload=bytes(application_payload),
            created_at=now,
            expires_at=now + timedelta(milliseconds=ttl_ms),
            idempotency=idempotency,
            max_retries=max_retries,
            simulator_only=simulator_only,
            command_type=command_type,
            sequence=sequence,
            attempts=attempts,
            expect_status_after_tx=expect_status_after_tx,
        )

    def is_expired(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(UTC)
        return current >= self.expires_at

    def with_attempt(self, *, sequence: int, attempts: int) -> OutboundDataItem:
        return OutboundDataItem(
            correlation_id=self.correlation_id,
            address=self.address,
            application_payload=self.application_payload,
            created_at=self.created_at,
            expires_at=self.expires_at,
            idempotency=self.idempotency,
            max_retries=self.max_retries,
            simulator_only=self.simulator_only,
            command_type=self.command_type,
            sequence=sequence,
            attempts=attempts,
            expect_status_after_tx=self.expect_status_after_tx,
        )


@dataclass
class PumpSessionStats:
    poll_count: int = 0
    eot_count: int = 0
    data_count: int = 0
    ack_sent_count: int = 0
    nak_count: int = 0
    timeout_count: int = 0
    crc_error_count: int = 0
    duplicate_count: int = 0
    sequence_error_count: int = 0
    address_mismatch_count: int = 0
    retry_count: int = 0
    stale_frame_count: int = 0
    short_bus_response_count: int = 0
    seq_resync_count: int = 0


@dataclass
class PumpSessionState:
    address: int
    pump_id: str
    tx_sequence: int = 0  # next controller DATA TX#
    expected_rx_sequence: int = 0  # next expected pump DATA TX#
    last_accepted_rx_sequence: int | None = None
    last_poll_at: datetime | None = None
    last_response_at: datetime | None = None
    last_poll_mono: float | None = None
    last_valid_response_mono: float | None = None
    last_valid_eot_mono: float | None = None
    last_valid_data_mono: float | None = None
    communication: CommunicationHealth = CommunicationHealth.UNKNOWN
    consecutive_timeouts: int = 0
    consecutive_protocol_faults: int = 0
    last_valid_frame: bytes | None = None
    last_raw_frame: bytes | None = None
    last_error: str | None = None
    last_transient_error: str | None = None
    last_persistent_fault: str | None = None
    pending_ack_for_seq: int | None = None
    last_state: PumpState = PumpState.DISCONNECTED
    stats: PumpSessionStats = field(default_factory=PumpSessionStats)
    # --- Per-address observed state (online ≠ synchronized) ---
    communication_online: bool = False
    state_synchronized: bool = False
    observed_status: ObservedStatus = ObservedStatus.UNKNOWN
    nozzle_position: NozzlePosition = NozzlePosition.UNKNOWN
    logical_nozzle: int | None = None
    filled_volume_raw: int = 0
    filled_amount_raw: int = 0
    unit_price_raw: int | None = None
    last_valid_frame_time: float | None = None
    last_status_time: float | None = None
    last_nozio_time: float | None = None
    last_command_time: float | None = None
    last_ack_time: float | None = None
    last_rx_ack_sequence: int | None = None
    missed_bus_responses: int = 0
    stale_application_data: bool = False
    pending_exchange: bool = False
    sale_lifecycle: SaleLifecycle = SaleLifecycle.IDLE
    sale_evidence: SaleEvidence = field(default_factory=SaleEvidence)
    # Events preserved while waiting for command ACK (non-ACK DATA handled).
    pending_command_events: list[str] = field(default_factory=list)
