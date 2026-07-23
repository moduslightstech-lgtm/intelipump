"""Shared application state for Phase 8 API lifespan."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.runtime import CloudRuntime
from intelipump_fdc.controller.controller_loop import ControllerLoop
from intelipump_fdc.core.config import Settings
from intelipump_fdc.events.broker import EventBroker
from intelipump_fdc.services.persistence_worker import PersistenceWorker
from intelipump_fdc.services.recovery_service import RecoveryReport


@dataclass
class MetricsState:
    poll_count: int = 0
    data_count: int = 0
    eot_count: int = 0
    crc_error_count: int = 0
    timeout_count: int = 0
    nak_count: int = 0
    duplicate_count: int = 0
    database_latency_ms_last: float | None = None
    database_latency_ms_max: float | None = None
    accepting_commands: bool = True

    def observe_db_latency(self, ms: float) -> None:
        self.database_latency_ms_last = ms
        if self.database_latency_ms_max is None or ms > self.database_latency_ms_max:
            self.database_latency_ms_max = ms


@dataclass
class AppState:
    settings: Settings
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    broker: EventBroker
    started_at: datetime
    recovery_report: RecoveryReport | None = None
    worker: PersistenceWorker | None = None
    controller_loop: ControllerLoop | None = None
    controller_task: asyncio.Task[None] | None = None
    pump_id_by_address: dict[int, str] = field(default_factory=dict)
    metrics: MetricsState = field(default_factory=MetricsState)
    shutting_down: bool = False
    cloud: CloudRuntime | None = None

    @property
    def uptime_seconds(self) -> float:
        return (datetime.now(UTC) - self.started_at).total_seconds()

    def recovery_summary(self) -> dict[str, Any] | None:
        if self.recovery_report is None:
            return None
        r = self.recovery_report
        return {
            "schema_version": r.schema_version,
            "pumps_restored": list(r.pumps_restored),
            "unresolved_transactions": list(r.unresolved_transactions),
            "commands_expired": list(r.commands_expired),
            "commands_needing_reconciliation": list(
                r.commands_needing_reconciliation
            ),
            "queue_locks_released": r.queue_locks_released,
            "warnings": list(r.warnings)[:20],
        }
