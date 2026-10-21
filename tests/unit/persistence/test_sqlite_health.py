"""SQLite quick_check / corruption helpers."""

from __future__ import annotations

import pytest

from intelipump_fdc.persistence.sqlite_health import (
    looks_like_sqlite_corruption,
    sqlite_quick_check,
)


@pytest.mark.asyncio
async def test_sqlite_quick_check_ok(engine_factory: tuple) -> None:
    engine, _factory = engine_factory
    ok, detail = await sqlite_quick_check(engine)
    assert ok is True
    assert detail == "ok"


@pytest.mark.asyncio
async def test_sqlite_quick_check_factory_ok(engine_factory: tuple) -> None:
    from intelipump_fdc.persistence.sqlite_health import sqlite_quick_check_factory

    _engine, factory = engine_factory
    ok, detail = await sqlite_quick_check_factory(factory)
    assert ok is True
    assert detail == "ok"


def test_looks_like_sqlite_corruption() -> None:
    assert looks_like_sqlite_corruption(
        Exception("database disk image is malformed")
    )
    assert looks_like_sqlite_corruption(Exception("file is not a database"))
    assert not looks_like_sqlite_corruption(Exception("database is locked"))
