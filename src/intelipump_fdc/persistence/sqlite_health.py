"""SQLite health helpers for cloud-sync (never block the RS-485 controller)."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine


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
        rows = result.fetchall()
    if not rows:
        return False, "quick_check returned no rows"
    messages = [str(r[0]) for r in rows if r and r[0] is not None]
    if len(messages) == 1 and messages[0].lower() == "ok":
        return True, "ok"
    detail = "; ".join(messages)[:500]
    return False, detail or "quick_check failed"


def looks_like_sqlite_corruption(exc: BaseException) -> bool:
    """True when an exception message indicates a corrupt SQLite file."""
    text = f"{type(exc).__name__}: {exc}".lower()
    needles = (
        "database disk image is malformed",
        "file is not a database",
        "corrupt",
        "malformed",
    )
    return any(n in text for n in needles)
