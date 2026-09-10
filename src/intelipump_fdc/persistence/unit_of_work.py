"""Unit of work aggregating repositories under one session/transaction."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.persistence.repositories.alarms import AlarmRepository
from intelipump_fdc.persistence.repositories.audit import AuditRepository
from intelipump_fdc.persistence.repositories.commands import CommandRepository
from intelipump_fdc.persistence.repositories.nozzle_baselines import NozzleSaleBaselineRepository
from intelipump_fdc.persistence.repositories.pumps import PumpRepository
from intelipump_fdc.persistence.repositories.states import StateRepository
from intelipump_fdc.persistence.repositories.sync_queue import SyncQueueRepository
from intelipump_fdc.persistence.repositories.transactions import TransactionRepository


@dataclass
class UnitOfWork:
    session: AsyncSession
    pumps: PumpRepository
    states: StateRepository
    transactions: TransactionRepository
    commands: CommandRepository
    alarms: AlarmRepository
    audit: AuditRepository
    sync_queue: SyncQueueRepository
    nozzle_baselines: NozzleSaleBaselineRepository


@asynccontextmanager
async def unit_of_work(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[UnitOfWork]:
    session = factory()
    try:
        async with session.begin():
            yield UnitOfWork(
                session=session,
                pumps=PumpRepository(session),
                states=StateRepository(session),
                transactions=TransactionRepository(session),
                commands=CommandRepository(session),
                alarms=AlarmRepository(session),
                audit=AuditRepository(session),
                sync_queue=SyncQueueRepository(session),
                nozzle_baselines=NozzleSaleBaselineRepository(session),
            )
    finally:
        await session.close()
