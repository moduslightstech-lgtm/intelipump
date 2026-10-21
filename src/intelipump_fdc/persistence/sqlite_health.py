"""SQLite health helpers for cloud-sync (never block the RS-485 controller)."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


def _interpret_quick_check_rows(rows: list[object]) -> tuple[bool, str]:
    if not rows:
        return False, "quick_check returned no rows"
    messages: list[str] = []
    for row in rows:
        if row is None:
            continue
        try:
            # SQLAlchemy Row / tuple / list — prefer first column.
            val: object = row[0]  # type: ignore[index]
        except (TypeError, KeyError, IndexError):
            val = row
        messages.append(str(val))
    if len(messages) == 1 and messages[0].lower() == "ok":
        return True, "ok"
    detail = "; ".join(messages)[:500]
    return False, detail or "quick_check failed"


async def sqlite_quick_check(engine: AsyncEngine) -> tuple[bool, str]:
    """Return (ok, detail) from ``PRAGMA quick_check``.

    Non-SQLite engines always report ok. Used at cloud-sync startup so a
    corrupt outbox DB can degrade MQTT delivery without touching
    ``intelipump.service``.
    """
    if engine.dialect.name != "sqlite":
        return True, "ok"
    async with engine.connect() as conn:
        result = await conn.exec_driver_sql("PRAGMA quick_check")
        rows = list(result.fetchall())
    return _interpret_quick_check_rows(rows)


async def sqlite_quick_check_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[bool, str]:
    """Same as ``sqlite_quick_check`` via a session factory (SyncWorker path)."""
    async with session_factory() as session:
        bind = session.get_bind()
        if bind is not None and getattr(bind.dialect, "name", None) != "sqlite":
            return True, "ok"
        result = await session.execute(text("PRAGMA quick_check"))
        rows = list(result.fetchall())
    return _interpret_quick_check_rows(rows)


def looks_like_sqlite_corruption(exc: BaseException) -> bool:
    """True when an exception message indicates a corrupt SQLite file."""
    text_s = f"{type(exc).__name__}: {exc}".lower()
    needles = (
        "database disk image is malformed",
        "file is not a database",
        "corrupt",
        "malformed",
    )
    return any(n in text_s for n in needles)
