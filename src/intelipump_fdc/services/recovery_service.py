"""Startup recovery service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from intelipump_fdc.domain.pump_command import NON_IDEMPOTENT_COMMANDS, PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.database import configure_sqlite_pragmas
from intelipump_fdc.persistence.dto import CommandRecord
from intelipump_fdc.persistence.migrations import get_schema_version, init_schema
from intelipump_fdc.persistence.unit_of_work import UnitOfWork, unit_of_work
from intelipump_fdc.state_machine.models import PumpContext


@dataclass
class RecoveryReport:
    schema_version: int
    pumps_restored: list[str] = field(default_factory=list)
    unresolved_transactions: list[str] = field(default_factory=list)
    commands_expired: list[str] = field(default_factory=list)
    commands_needing_reconciliation: list[str] = field(default_factory=list)
    queue_locks_released: int = 0
    warnings: list[str] = field(default_factory=list)
    pump_contexts: dict[str, PumpContext] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "pumps_restored": list(self.pumps_restored),
            "unresolved_transactions": list(self.unresolved_transactions),
            "commands_expired": list(self.commands_expired),
            "commands_needing_reconciliation": list(
                self.commands_needing_reconciliation
            ),
            "queue_locks_released": self.queue_locks_released,
            "warnings": list(self.warnings),
            "pump_contexts": {
                k: {
                    "pump_id": v.pump_id,
                    "dart_address": v.dart_address,
                    "current_state": v.current_state.value,
                    "previous_state": (
                        v.previous_state.value if v.previous_state else None
                    ),
                    "active_transaction_id": v.active_transaction_id,
                    "communication_healthy": v.communication_healthy,
                    "state_version": v.state_version,
                }
                for k, v in self.pump_contexts.items()
            },
        }


class RecoveryService:
    def __init__(
        self,
        engine: AsyncEngine,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        station_id: str,
        environment: str = "LAB",
    ) -> None:
        self._engine = engine
        self._factory = session_factory
        self._station_id = station_id
        self._environment = environment

    async def ensure_pumps(
        self, addresses: tuple[int, ...]
    ) -> dict[int, str]:
        """Upsert pump rows for configured DART addresses. Returns address→db id."""
        mapping: dict[int, str] = {}
        async with unit_of_work(self._factory) as uow:
            for address in addresses:
                logical = f"pump-{address}"
                pump = await uow.pumps.upsert(
                    station_id=self._station_id,
                    logical_pump_id=logical,
                    dart_address=address,
                    enabled=True,
                )
                mapping[address] = pump.id
        return mapping

    async def recover(self) -> RecoveryReport:
        await configure_sqlite_pragmas(self._engine)
        schema = await init_schema(self._engine)
        report = RecoveryReport(schema_version=schema)

        async with unit_of_work(self._factory) as uow:
            report.queue_locks_released = await uow.sync_queue.release_stale_locks(
                older_than_seconds=60
            )
            pumps = await uow.pumps.list_for_station(self._station_id)
            for pump in pumps:
                report.pumps_restored.append(pump.logical_pump_id)
                snap = await uow.states.latest(pump.id)
                if snap is None:
                    ctx = PumpContext(
                        pump_id=pump.logical_pump_id,
                        dart_address=pump.dart_address,
                        current_state=PumpState.DISCONNECTED,
                        communication_healthy=False,
                    )
                else:
                    try:
                        state = PumpState(snap.normalized_state)
                    except ValueError:
                        state = PumpState.DISCOVERING
                        report.warnings.append(
                            f"unknown persisted state {snap.normalized_state} "
                            f"for {pump.logical_pump_id}"
                        )
                    prev = None
                    if snap.previous_state:
                        try:
                            prev = PumpState(snap.previous_state)
                        except ValueError:
                            prev = None
                    ctx = PumpContext(
                        pump_id=pump.logical_pump_id,
                        dart_address=pump.dart_address,
                        current_state=state,
                        previous_state=prev,
                        selected_nozzle=snap.selected_nozzle,
                        active_transaction_id=snap.active_transaction_id,
                        communication_healthy=False,  # require live observation
                        last_raw_wayne_status=snap.raw_wayne_status,
                        state_version=snap.state_version,
                        last_source_frame_hex=snap.source_frame_ref,
                    )
                    if snap.communication_healthy:
                        report.warnings.append(
                            f"{pump.logical_pump_id}: persisted communication "
                            "was healthy; restart forces unhealthy until live data"
                        )
                report.pump_contexts[pump.logical_pump_id] = ctx

            unresolved = await uow.transactions.list_unresolved(
                station_id=self._station_id
            )
            for tx in unresolved:
                report.unresolved_transactions.append(tx.transaction_uuid)
                report.warnings.append(
                    f"unresolved transaction preserved: {tx.transaction_uuid}"
                )

            pending = await uow.commands.list_pending_or_in_progress(
                station_id=self._station_id
            )
            now = datetime.now(UTC)
            for cmd in pending:
                await self._reconcile_command(uow, cmd, now, report)

            _ = await get_schema_version(uow.session)

        report.warnings.append("Never automatically authorize after restart.")
        report.warnings.append(
            "Non-idempotent pending commands were not replayed."
        )
        return report

    async def _reconcile_command(
        self,
        uow: UnitOfWork,
        cmd: CommandRecord,
        now: datetime,
        report: RecoveryReport,
    ) -> None:
        expired = False
        if cmd.expires_at is not None:
            expires = cmd.expires_at
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=UTC)
            expired = expires <= now
        try:
            command_type = PumpCommand(cmd.command_type)
        except ValueError:
            command_type = None

        if expired:
            await uow.commands.update_status(
                cmd.correlation_id,
                status="EXPIRED",
                completed_at=now,
                result_payload={"reason": "expired_on_restart"},
            )
            report.commands_expired.append(cmd.correlation_id)
            return

        if command_type in NON_IDEMPOTENT_COMMANDS or (
            command_type is not None
            and command_type
            not in {PumpCommand.READ_STATUS, PumpCommand.READ_TOTALS}
        ):
            await uow.commands.update_status(
                cmd.correlation_id,
                status="NEEDS_RECONCILIATION",
                completed_at=now,
                result_payload={
                    "reason": "non_idempotent_not_replayed_after_restart"
                },
            )
            report.commands_needing_reconciliation.append(cmd.correlation_id)
            report.warnings.append(
                f"command {cmd.command_type} {cmd.correlation_id} → "
                "NEEDS_RECONCILIATION (not replayed)"
            )
            return

        await uow.commands.update_status(
            cmd.correlation_id,
            status="EXPIRED",
            completed_at=now,
            result_payload={"reason": "read_not_auto_replayed"},
        )
        report.commands_expired.append(cmd.correlation_id)


def format_recovery_report(report: RecoveryReport) -> str:
    """Human-readable recovery report for CLI."""
    lines = [
        f"schema_version={report.schema_version}",
        f"pumps_restored={report.pumps_restored}",
        f"unresolved_transactions={report.unresolved_transactions}",
        f"commands_expired={report.commands_expired}",
        f"commands_needing_reconciliation={report.commands_needing_reconciliation}",
        f"queue_locks_released={report.queue_locks_released}",
    ]
    for warning in report.warnings:
        lines.append(f"warning: {warning}")
    return "\n".join(lines)
