"""Typed DTOs returned by repositories (no ORM leakage)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class PumpRecord:
    id: str
    station_id: str
    logical_pump_id: str
    dart_address: int
    enabled: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class StateSnapshotRecord:
    id: str
    pump_id: str
    normalized_state: str
    previous_state: str | None
    selected_nozzle: int | None
    active_transaction_id: str | None
    communication_healthy: bool
    raw_wayne_status: int | None
    source_frame_ref: str | None
    state_version: int
    observed_at: datetime | None
    persisted_at: datetime


@dataclass(frozen=True, slots=True)
class TransactionRecord:
    id: str
    transaction_uuid: str
    station_id: str
    pump_id: str
    nozzle_id: int | None
    status: str
    raw_price: int | None
    price_decimals: int | None
    raw_volume: int
    volume_decimals: int | None
    raw_amount: int
    amount_decimals: int | None
    started_at: datetime | None
    completed_at: datetime | None
    closed_at: datetime | None
    source_completion_key: str | None
    simulated: bool
    environment: str
    created_at: datetime
    updated_at: datetime
    canonical_pump_id: str | None = None
    canonical_nozzle_id: str | None = None
    source_identifier: str | None = None


@dataclass(frozen=True, slots=True)
class TransactionEventRecord:
    id: str
    transaction_id: str
    event_type: str
    event_key: str
    raw_payload: dict[str, Any] | None
    source_frame_ref: str | None
    observed_at: datetime | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class CommandRecord:
    correlation_id: str
    station_id: str
    pump_id: str | None
    command_type: str
    status: str
    idempotency_class: str
    simulator_only: bool
    requested_at: datetime
    expires_at: datetime | None
    completed_at: datetime | None
    request_payload: dict[str, Any] | None
    result_payload: dict[str, Any] | None
    blocking_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CommandAttemptRecord:
    id: str
    correlation_id: str
    attempt_number: int
    sequence_number: int | None
    outcome: str
    error_code: str | None
    raw_frame: str | None
    attempted_at: datetime


@dataclass(frozen=True, slots=True)
class AlarmRecord:
    id: str
    station_id: str
    pump_id: str | None
    severity: str
    alarm_type: str
    message: str
    active: bool
    first_seen_at: datetime
    last_seen_at: datetime
    cleared_at: datetime | None
    source_key: str


@dataclass(frozen=True, slots=True)
class AuditRecord:
    id: str
    correlation_id: str | None
    actor: str
    source: str
    action: str
    station_id: str
    pump_id: str | None
    previous_state: str | None
    resulting_state: str | None
    result: str
    details: dict[str, Any] | None
    created_at: datetime
    previous_hash: str
    record_hash: str


@dataclass(frozen=True, slots=True)
class SyncQueueRecord:
    id: str
    entity_type: str
    entity_id: str
    event_type: str
    payload: dict[str, Any]
    status: str
    attempt_count: int
    available_at: datetime
    locked_at: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    deduplication_key: str
