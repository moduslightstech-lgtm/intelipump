"""API test fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from intelipump_fdc.api.app import create_app
from intelipump_fdc.core.config import get_settings


@pytest.fixture
def api_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    get_settings.cache_clear()
    db = tmp_path / "api.db"
    monkeypatch.setenv("INTELIPUMP_DATABASE__URL", f"sqlite+aiosqlite:///{db}")
    monkeypatch.setenv("INTELIPUMP_ENVIRONMENT", "LAB")
    monkeypatch.setenv("INTELIPUMP_CONTROLLER__STATION_ID", "InteliPump-US-Lab")
    app = create_app()
    with TestClient(app) as client:
        yield client
    get_settings.cache_clear()
