from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from intelipump_fdc.core.config import ControllerMode


class ControllerHealth(BaseModel):
    status: str
    environment: str
    mode: ControllerMode
    device_id: str
    station_id: str
    active_commands_enabled: bool
    timestamp: datetime
    database_status: str = "UNKNOWN"
    schema_version: int | None = None
    persistence_queue_depth: int | None = None
    unresolved_transaction_count: int | None = None
    pending_sync_count: int | None = None
    recovery_warnings: list[str] = Field(default_factory=list)
