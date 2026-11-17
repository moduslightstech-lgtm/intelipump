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


@pytest.mark.asyncio
async def test_sqlite_quick_check_swallows_malformed() -> None:
    """Corrupt PRAGMA must return (False, …), never raise (cloud-sync stays up)."""

    class _BoomConn:
        async def __aenter__(self) -> object:
            raise Exception("database disk image is malformed")

        async def __aexit__(self, *args: object) -> None:
            return None

    class _FakeDialect:
        name = "sqlite"

    class _FakeEngine:
        dialect = _FakeDialect()

        def connect(self) -> _BoomConn:
            return _BoomConn()

    ok, detail = await sqlite_quick_check(_FakeEngine())  # type: ignore[arg-type]
    assert ok is False
    assert "malformed" in detail.lower()
