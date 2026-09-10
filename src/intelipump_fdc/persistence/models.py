"""SQLAlchemy ORM models for Phase 7 persistence (internal to persistence layer)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

SCHEMA_VERSION = 3
AUDIT_GENESIS_HASH = "GENESIS_V1"

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _uuid() -> str:
    return str(uuid4())


class ControllerMetadataRow(Base):
    __tablename__ = "controller_metadata"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class PumpRow(Base):
    __tablename__ = "pumps"
    __table_args__ = (
        UniqueConstraint("station_id", "logical_pump_id", name="uq_pumps_station_logical"),
        UniqueConstraint("station_id", "dart_address", name="uq_pumps_station_address"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    station_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    logical_pump_id: Mapped[str] = mapped_column(String(64), nullable=False)
    dart_address: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class PumpStateSnapshotRow(Base):
    __tablename__ = "pump_state_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    pump_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pumps.id"), nullable=False, index=True
    )
    normalized_state: Mapped[str] = mapped_column(String(64), nullable=False)
    previous_state: Mapped[str | None] = mapped_column(String(64), nullable=True)
    selected_nozzle: Mapped[int | None] = mapped_column(Integer, nullable=True)
    active_transaction_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    communication_healthy: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    raw_wayne_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_frame_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class TransactionRow(Base):
    __tablename__ = "transactions"
    __table_args__ = (
        UniqueConstraint("source_completion_key", name="uq_transactions_completion_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    transaction_uuid: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    station_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    pump_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pumps.id"), nullable=False, index=True
    )
    nozzle_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    canonical_pump_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    canonical_nozzle_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_identifier: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    raw_price: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price_decimals: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_volume: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    volume_decimals: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_amount: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    amount_decimals: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_completion_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    simulated: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    environment: Mapped[str] = mapped_column(String(32), nullable=False, default="LAB")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    events: Mapped[list[TransactionEventRow]] = relationship(back_populates="transaction")


class TransactionEventRow(Base):
    __tablename__ = "transaction_events"
    __table_args__ = (
        UniqueConstraint(
            "transaction_id", "event_key", name="uq_transaction_events_tx_key"
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    transaction_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("transactions.id"), nullable=False, index=True
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_key: Mapped[str] = mapped_column(String(256), nullable=False)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    source_frame_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    transaction: Mapped[TransactionRow] = relationship(back_populates="events")


class CommandRow(Base):
    __tablename__ = "commands"

    correlation_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    station_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    pump_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("pumps.id"), nullable=True, index=True
    )
    command_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    idempotency_class: Mapped[str] = mapped_column(String(32), nullable=False)
    simulator_only: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    request_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    result_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    blocking_reasons: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)


class CommandAttemptRow(Base):
    __tablename__ = "command_attempts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    correlation_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("commands.correlation_id"), nullable=False, index=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    sequence_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_frame: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AlarmRow(Base):
    __tablename__ = "alarms"
    __table_args__ = (
        UniqueConstraint("station_id", "source_key", name="uq_alarms_station_source"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    station_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    pump_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("pumps.id"), nullable=True, index=True
    )
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    alarm_type: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    cleared_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_key: Mapped[str] = mapped_column(String(256), nullable=False)


class AuditLogRow(Base):
    __tablename__ = "audit_log"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    correlation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    station_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    pump_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    previous_state: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resulting_state: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result: Mapped[str] = mapped_column(String(32), nullable=False)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    previous_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    record_hash: Mapped[str] = mapped_column(String(128), nullable=False, index=True)


class SyncQueueRow(Base):
    __tablename__ = "sync_queue"
    __table_args__ = (
        UniqueConstraint("deduplication_key", name="uq_sync_queue_dedupe"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    deduplication_key: Mapped[str] = mapped_column(String(256), nullable=False)


class ConfigurationVersionRow(Base):
    __tablename__ = "configuration_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    version: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    checksum: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class NozzleSaleBaselineRow(Base):
    """Persisted per-nozzle completed-sale baseline so restarts do not republish."""

    __tablename__ = "nozzle_sale_baselines"
    __table_args__ = (
        UniqueConstraint(
            "station_id",
            "dart_address",
            "nozzle_id",
            name="uq_nozzle_sale_baselines_scope",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    station_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    pump_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    dart_address: Mapped[int] = mapped_column(Integer, nullable=False)
    nozzle_id: Mapped[int] = mapped_column(Integer, nullable=False)
    last_completed_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_published_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_transaction_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_raw_volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_raw_amount: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    initialized: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
