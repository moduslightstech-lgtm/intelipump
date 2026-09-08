"""Versioned schema initializer (simple migrations for Phase 7)."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from intelipump_fdc.persistence.errors import SchemaError
from intelipump_fdc.persistence.models import (
    SCHEMA_VERSION,
    Base,
    ControllerMetadataRow,
)

SCHEMA_VERSION_KEY = "schema_version"


_TX_HIERARCHY_COLUMNS = (
    ("canonical_pump_id", "VARCHAR(64)"),
    ("canonical_nozzle_id", "VARCHAR(64)"),
    ("source_identifier", "VARCHAR(64)"),
)


async def _ensure_transaction_hierarchy_columns(conn) -> None:
    """Additive SQLite columns for canonical pump/nozzle ids (idempotent)."""
    if conn.dialect.name != "sqlite":
        return
    result = await conn.execute(text("PRAGMA table_info(transactions)"))
    existing = {row[1] for row in result.fetchall()}
    if not existing:
        return
    for name, ddl in _TX_HIERARCHY_COLUMNS:
        if name not in existing:
            await conn.execute(text(f"ALTER TABLE transactions ADD COLUMN {name} {ddl}"))


async def init_schema(engine: AsyncEngine) -> int:
    """Create tables if needed and record schema version. Returns version."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _ensure_transaction_hierarchy_columns(conn)

    async with AsyncSession(engine, expire_on_commit=False) as session, session.begin():
        result = await session.execute(
            select(ControllerMetadataRow).where(
                ControllerMetadataRow.key == SCHEMA_VERSION_KEY
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            session.add(
                ControllerMetadataRow(
                    key=SCHEMA_VERSION_KEY,
                    value=str(SCHEMA_VERSION),
                    updated_at=datetime.now(UTC),
                )
            )
            return SCHEMA_VERSION
        try:
            current = int(row.value)
        except ValueError as exc:
            raise SchemaError(f"invalid schema_version: {row.value}") from exc
        if current > SCHEMA_VERSION:
            raise SchemaError(
                f"database schema {current} newer than code {SCHEMA_VERSION}"
            )
        if current < SCHEMA_VERSION:
            # Future: apply stepwise migrations here.
            row.value = str(SCHEMA_VERSION)
            row.updated_at = datetime.now(UTC)
        return SCHEMA_VERSION


async def get_schema_version(session: AsyncSession) -> int:
    result = await session.execute(
        select(ControllerMetadataRow).where(ControllerMetadataRow.key == SCHEMA_VERSION_KEY)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return 0
    return int(row.value)


async def reset_lab_database(engine: AsyncEngine, *, environment: str) -> None:
    """Drop and recreate schema. LAB only."""
    if environment.upper() != "LAB":
        raise SchemaError("reset_lab_database only permitted in LAB")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await init_schema(engine)


async def verify_sqlite_pragmas(engine: AsyncEngine) -> dict[str, str]:
    """Return journal_mode and foreign_keys for diagnostics."""
    out: dict[str, str] = {}
    if engine.dialect.name != "sqlite":
        return out
    async with engine.connect() as conn:
        jm = await conn.execute(text("PRAGMA journal_mode"))
        out["journal_mode"] = str(jm.scalar())
        fk = await conn.execute(text("PRAGMA foreign_keys"))
        out["foreign_keys"] = str(fk.scalar())
    return out
