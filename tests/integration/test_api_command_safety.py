"""Command safety integration tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from intelipump_fdc.api.app import create_app
from intelipump_fdc.core.config import get_settings


def test_no_production_command_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv(
        "INTELIPUMP_DATABASE__URL", f"sqlite+aiosqlite:///{tmp_path / 'c.db'}"
    )
    with TestClient(create_app()) as client:
        # General production command path must not exist
        r = client.post("/api/v1/pumps/pump-1/commands", json={"command_type": "AUTHORIZE"})
        assert r.status_code == 404
        # Evaluate exists but does not execute
        pumps = client.get("/api/v1/pumps").json()
        r2 = client.post(
            f"/api/v1/pumps/{pumps[0]['logical_pump_id']}/commands/evaluate",
            json={"command_type": "SET_PRICE", "raw_preset_value": 1},
        )
        assert r2.status_code == 200
        assert r2.json()["eligible"] is False
    get_settings.cache_clear()
