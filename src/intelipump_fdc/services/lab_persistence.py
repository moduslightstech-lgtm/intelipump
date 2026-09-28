"""LAB persistence wiring for controller runtime."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.channel_map import ChannelMapping, safe_mappings_from_settings
from intelipump_fdc.controller.controller_loop import ControllerLoop
from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.events.broker import EventBroker
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.services.persist_recovery import PersistRecoveryStore
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.services.persistence_worker import (
    PersistFlushIncompleteError,
    PersistenceWorker,
)
from intelipump_fdc.services.recovery_service import RecoveryReport, RecoveryService
from intelipump_fdc.state_machine.models import PumpContext

logger = logging.getLogger(__name__)


def _persist_recovery_path(database_url: str) -> Path:
    """Place recovery JSONL beside the SQLite file when possible."""
    if database_url.startswith("sqlite"):
        raw = ""
        if database_url.startswith("sqlite+aiosqlite:////"):
            raw = "/" + database_url.removeprefix("sqlite+aiosqlite:////")
        elif database_url.startswith("sqlite+aiosqlite:///"):
            raw = database_url.removeprefix("sqlite+aiosqlite:///")
        elif ":///" in database_url:
            raw = database_url.split(":///", 1)[-1]
        if raw and raw != ":memory:":
            path = Path(unquote(raw)).expanduser()
            return path.with_name(path.stem + ".persist_recovery.jsonl")
    return Path("intelipump.persist_recovery.jsonl")


@dataclass
class PersistenceRuntime:
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    worker: PersistenceWorker
    bridge: PersistenceBridge
    recovery: RecoveryReport
    pump_id_by_address: dict[int, str]

    async def shutdown(self, *, flush_timeout_s: float = 5.0) -> None:
        """Detach bridge, flush CRITICAL sales, dispose engine.

        Bounded failure policy (storage unavailable at shutdown):
        - Always dispose the engine (no resource leak).
        - If flush leaves sales only in ``_retained_sales``, log
          ``SALE_DURABILITY_NOT_SAFE`` and re-raise ``PersistFlushIncompleteError``.
        - Callers must not treat a raised error as a durable/safe shutdown.
        """
        self.bridge.detach()
        flush_error: PersistFlushIncompleteError | None = None
        try:
            await self.worker.stop(flush=True, timeout_s=flush_timeout_s)
        except PersistFlushIncompleteError as exc:
            flush_error = exc
            logger.error("%s", exc.operator_message())
        finally:
            await dispose_engine(self.engine)
        if flush_error is not None:
            raise flush_error


async def start_persistence(
    *,
    database_url: str,
    station_id: str,
    environment: str,
    addresses: tuple[int, ...],
    events: EventBus,
    simulated: bool = True,
    worker_maxsize: int = 256,
    live_broker: EventBroker | None = None,
    channel_map: dict[int, ChannelMapping] | None = None,
) -> PersistenceRuntime:
    engine = create_engine(database_url)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    recovery_svc = RecoveryService(
        engine, factory, station_id=station_id, environment=environment
    )
    pump_map = await recovery_svc.ensure_pumps(addresses)
    report = await recovery_svc.recover()
    recovery_store = PersistRecoveryStore(_persist_recovery_path(database_url))
    worker = PersistenceWorker(
        maxsize=worker_maxsize,
        recovery_store=recovery_store,
    )
    worker.start()
    mapping = channel_map
    if mapping is None:
        mapping = safe_mappings_from_settings(get_settings(), addresses)
    mqtt_pump = {a: mapping[a].pump_id for a in addresses if a in mapping}
    mqtt_nozzle = {a: mapping[a].nozzle_id for a in addresses if a in mapping}
    mqtt_source = {a: mapping[a].source_identifier for a in addresses if a in mapping}
    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=station_id,
        environment=environment,
        simulated=simulated,
        worker=worker,
        pump_id_by_address=pump_map,
        logical_by_address={a: f"pump-{a}" for a in addresses},
        events=events,
        live_broker=live_broker,
        mqtt_pump_by_address=mqtt_pump or None,
        mqtt_nozzle_by_address=mqtt_nozzle or None,
        mqtt_source_by_address=mqtt_source or None,
    )
    bridge.attach()
    restored = worker.recover_pending()
    if restored:
        report.warnings.append(
            f"restored_{restored}_pending_critical_persist_jobs_from_durable_store"
        )
    return PersistenceRuntime(
        engine=engine,
        session_factory=factory,
        worker=worker,
        bridge=bridge,
        recovery=report,
        pump_id_by_address=pump_map,
    )


def apply_recovered_contexts(
    loop: ControllerLoop, contexts: dict[str, PumpContext]
) -> None:
    """Seed session state machines from recovery without claiming live health.

    Arms restart reconciliation so the first live DC1/DC2/DC3 observation
    runs ``reconcile_after_restart`` (never auto-authorizes).
    """
    by_address = {ctx.dart_address: ctx for ctx in contexts.values()}
    for address, session in loop.sessions.items():
        ctx = by_address.get(address)
        if ctx is None:
            continue
        session.seed_recovered_context(ctx)
