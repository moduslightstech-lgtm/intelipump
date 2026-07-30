"""Transaction service DTOs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class BeginTransactionRequest:
    station_id: str
    pump_db_id: str
    transaction_uuid: str
    nozzle_id: int | None
    raw_price: int | None
    price_decimals: int | None
    volume_decimals: int | None
    amount_decimals: int | None
    simulated: bool
    environment: str
    started_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class FillingUpdateRequest:
    transaction_uuid: str
    raw_volume: int
    raw_amount: int
    raw_price: int | None = None
    event_key: str | None = None
    source_frame_ref: str | None = None


@dataclass(frozen=True, slots=True)
class CompleteTransactionRequest:
    transaction_uuid: str
    source_completion_key: str
    raw_volume: int
    raw_amount: int
    source_frame_ref: str | None = None
    completed_at: datetime | None = None
    completion_inferred: bool = False
    completion_warnings: tuple[str, ...] = ()
