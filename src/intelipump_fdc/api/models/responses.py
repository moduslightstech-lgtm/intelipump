"""API response / request models (independent of ORM)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from intelipump_fdc.core.config import ControllerMode


def format_scaled(raw: int | None, decimals: int | None) -> str | None:
    if raw is None or decimals is None:
        return None
    value = Decimal(raw).scaleb(-decimals)
    return format(value, "f")


class PageMeta(BaseModel):
    page: int
    page_size: int
    total: int
    total_pages: int


class RecoverySummary(BaseModel):
    schema_version: int
    pumps_restored: list[str]
    unresolved_transactions: list[str]
    commands_expired: list[str]
    commands_needing_reconciliation: list[str]
    queue_locks_released: int
    warnings: list[str]


class ControllerHealthResponse(BaseModel):
    status: str
    environment: str
    mode: ControllerMode
    device_id: str
    station_id: str
    active_commands_enabled: bool
    physical_enable_detected: bool
    controller_loop_running: bool
    transport_open: bool | None
    database_status: str
    schema_version: int | None
    persistence_queue_depth: int | None
    pending_sync_count: int | None
    unresolved_transaction_count: int | None
    configured_pump_count: int
    healthy_pump_count: int
    degraded_pump_count: int
    disconnected_pump_count: int
    last_recovery_report: RecoverySummary | None = None
    uptime_seconds: float
    timestamp: datetime
    warnings: list[str] = Field(default_factory=list)
    # Phase 9 MQTT / cloud health (no secrets)
    mqtt_enabled: bool = False
    mqtt_connected: bool = False
    mqtt_host: str | None = None
    mqtt_last_connected_at: datetime | None = None
    mqtt_last_disconnected_at: datetime | None = None
    mqtt_reconnect_count: int = 0
    mqtt_last_error: str | None = None
    sync_oldest_pending_age_seconds: float | None = None
    sync_delivered_count: int = 0
    sync_failed_count: int = 0
    heartbeat_last_published_at: datetime | None = None
    cloud_command_subscription_active: bool = False


class ControllerMetricsResponse(BaseModel):
    poll_count: int
    data_count: int
    eot_count: int
    crc_error_count: int
    timeout_count: int
    nak_count: int
    duplicate_count: int
    persistence_queue_depth: int | None
    dropped_live_event_count: int
    active_sse_subscribers: int
    active_websocket_subscribers: int
    database_latency_ms_last: float | None
    database_latency_ms_max: float | None
    uptime_seconds: float
    mqtt_enabled: bool = False
    mqtt_connected: bool = False
    mqtt_host: str | None = None
    mqtt_reconnect_count: int = 0
    pending_sync_count: int | None = None
    sync_delivered_count: int = 0
    sync_failed_count: int = 0
    heartbeat_last_published_at: datetime | None = None
    cloud_command_subscription_active: bool = False


class PumpStatusResponse(BaseModel):
    logical_pump_id: str
    pump_db_id: str
    dart_address: int
    enabled: bool
    normalized_state: str
    previous_state: str | None
    communication_health: str
    selected_nozzle: int | None
    active_transaction_id: str | None
    price_verified: bool
    last_wayne_status: int | None
    state_version: int
    last_observed_at: datetime | None
    last_transition_at: datetime | None
    timeout_count: int
    retry_count: int
    last_protocol_error: str | None
    simulated: bool
    environment: str


class PumpTotalsResponse(BaseModel):
    logical_pump_id: str
    raw_volume: int | None = None
    volume_decimals: int | None = None
    volume_formatted: str | None = None
    raw_amount: int | None = None
    amount_decimals: int | None = None
    amount_formatted: str | None = None
    notes: list[str] = Field(default_factory=list)


class TransactionResponse(BaseModel):
    id: str
    transaction_uuid: str
    station_id: str
    pump_id: str
    nozzle_id: int | None
    status: str
    raw_price: int | None
    price_decimals: int | None
    price_formatted: str | None
    raw_volume: int
    volume_decimals: int | None
    volume_formatted: str | None
    raw_amount: int
    amount_decimals: int | None
    amount_formatted: str | None
    started_at: datetime | None
    completed_at: datetime | None
    closed_at: datetime | None
    source_completion_key: str | None
    environment: str
    simulated: bool
    event_count: int = 0


class TransactionEventResponse(BaseModel):
    id: str
    transaction_id: str
    event_type: str
    event_key: str
    raw_payload: dict[str, Any] | None
    source_frame_ref: str | None
    observed_at: datetime | None
    created_at: datetime


class TransactionListResponse(BaseModel):
    items: list[TransactionResponse]
    page: PageMeta


class AlarmResponse(BaseModel):
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


class AlarmListResponse(BaseModel):
    items: list[AlarmResponse]
    page: PageMeta


class AuditResponse(BaseModel):
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
    created_at: datetime
    previous_hash: str
    record_hash: str


class AuditListResponse(BaseModel):
    items: list[AuditResponse]
    page: PageMeta


class AuditVerifyResponse(BaseModel):
    valid: bool
    records_checked: int
    first_invalid_record_id: str | None
    genesis: str
    reason: str | None = None


class CommandEvaluateRequest(BaseModel):
    command_type: str
    nozzle_id: int | None = None
    raw_preset_value: int | None = None
    preset_decimals: int | None = None
    correlation_id: str | None = None
    simulator_only: bool = True
    expires_at: datetime | None = None


class CommandEvaluateResponse(BaseModel):
    eligible: bool
    current_state: str
    blocking_reasons: list[str]
    warnings: list[str]
    requires_physical_enable: bool
    requires_active_commands_enabled: bool
    command_persisted: bool
    audit_record_id: str | None
    correlation_id: str


class LabCommandRequest(BaseModel):
    command_type: str
    simulator_only: bool = True
    correlation_id: str | None = None
    nozzle_id: int | None = None
    expires_at: datetime | None = None


class LabCommandResponse(BaseModel):
    accepted: bool
    correlation_id: str
    command_type: str
    queued: bool
    blocking_reasons: list[str] = Field(default_factory=list)
