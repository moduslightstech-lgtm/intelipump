"""Controller session and outbound-queue models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import uuid4

from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState


class CommunicationHealth(StrEnum):
    UNKNOWN = "UNKNOWN"
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    DISCONNECTED = "DISCONNECTED"


class IdempotencyClass(StrEnum):
    IDEMPOTENT = "IDEMPOTENT"
    NON_IDEMPOTENT = "NON_IDEMPOTENT"


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
    sequence: int | None = None  # assigned at send time
    attempts: int = 0

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
        )

    def is_expired(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(UTC)
        return current >= self.expires_at


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
    retry_count: int = 0


@dataclass
class PumpSessionState:
    address: int
    pump_id: str
    tx_sequence: int = 0  # next controller DATA TX#
    expected_rx_sequence: int = 0  # next expected pump DATA TX#
    last_accepted_rx_sequence: int | None = None
    last_poll_at: datetime | None = None
    last_response_at: datetime | None = None
    communication: CommunicationHealth = CommunicationHealth.UNKNOWN
    consecutive_timeouts: int = 0
    last_valid_frame: bytes | None = None
    last_raw_frame: bytes | None = None
    last_error: str | None = None
    pending_ack_for_seq: int | None = None
    last_state: PumpState = PumpState.DISCONNECTED
    stats: PumpSessionStats = field(default_factory=PumpSessionStats)
