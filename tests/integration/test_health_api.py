from fastapi.testclient import TestClient

from intelipump_fdc.api.app import create_app
from intelipump_fdc.core.config import get_settings


def test_health_endpoint(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    get_settings.cache_clear()
    monkeypatch.setenv(
        "INTELIPUMP_DATABASE__URL", f"sqlite+aiosqlite:///{tmp_path / 'h.db'}"
    )
    monkeypatch.setenv("INTELIPUMP_CONTROLLER__MODE", "LISTEN_ONLY")
    with TestClient(create_app()) as client:
        response = client.get("/api/v1/controller/health")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] in {"ONLINE", "DEGRADED"}
        assert payload["mode"] == "LISTEN_ONLY"
        assert payload["active_commands_enabled"] is False
        assert payload["database_status"] == "OK"
    get_settings.cache_clear()
