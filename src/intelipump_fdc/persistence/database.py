"""Async SQLAlchemy engine and session factory (no connect at import)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from intelipump_fdc.persistence.errors import PersistenceError


def ensure_sqlite_parent_dir(database_url: str) -> None:
    """Create parent directory for file-backed SQLite URLs."""
    if not database_url.startswith("sqlite"):
        return
    if ":memory:" in database_url:
        return
    # sqlite+aiosqlite:///./data/x.db or sqlite+aiosqlite:////abs/path.db
    raw = database_url.split("://", 1)[-1]
    if raw.startswith("//"):
        path_part = raw[1:] if raw.startswith("///") else raw
        if path_part.startswith("//"):
            path_part = path_part[1:]
    else:
        path_part = raw
    if path_part.startswith("/./"):
        path_part = path_part[1:]
    if path_part in {":memory:", ""} or path_part.startswith("file:"):
        return
    path = Path(path_part)
    if path.parent and str(path.parent) not in {".", ""}:
        path.parent.mkdir(parents=True, exist_ok=True)


def _register_sqlite_connect_pragmas(engine: AsyncEngine) -> None:
    """Apply WAL / FK / busy_timeout on every new SQLite connection."""

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection: object, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()


def create_engine(database_url: str, *, echo: bool = False) -> AsyncEngine:
    """Create an async engine. Does not open a connection until first use."""
    ensure_sqlite_parent_dir(database_url)
    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["timeout"] = 30.0
    engine = create_async_engine(
        database_url,
        echo=echo,
        connect_args=connect_args,
    )
    if database_url.startswith("sqlite"):
        _register_sqlite_connect_pragmas(engine)
    return engine


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def configure_sqlite_pragmas(engine: AsyncEngine) -> None:
    """Ensure WAL/FK pragmas by opening a connection (also via connect hook)."""
    if engine.dialect.name != "sqlite":
        return
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        await conn.exec_driver_sql("PRAGMA foreign_keys=ON")
        await conn.exec_driver_sql("PRAGMA busy_timeout=30000")
        await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Explicit transaction boundary: commit on success, rollback on error."""
    session = factory()
    try:
        async with session.begin():
            yield session
    except Exception:
        raise
    finally:
        await session.close()


async def dispose_engine(engine: AsyncEngine) -> None:
    await engine.dispose()


def parse_sqlite_path(database_url: str) -> Path | None:
    if not database_url.startswith("sqlite"):
        return None
    if ":memory:" in database_url:
        return None
    parsed = urlparse(database_url.replace("sqlite+aiosqlite", "sqlite", 1))
    path = parsed.path
    if path.startswith("//"):
        path = path[1:]
    if path.startswith("/./"):
        path = path[1:]
    if not path:
        raise PersistenceError(f"cannot parse sqlite path from {database_url}")
    return Path(path)
