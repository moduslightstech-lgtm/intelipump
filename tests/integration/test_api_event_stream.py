"""SSE/WebSocket and command safety integration tests."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from intelipump_fdc.api.app import create_app
from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.events.models import LiveEventType
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.protocol.dart.transport.serial import SerialConfig, SerialTransport


@pytest.fixture
def app_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    get_settings.cache_clear()
    monkeypatch.setenv(
        "INTELIPUMP_DATABASE__URL", f"sqlite+aiosqlite:///{tmp_path / 's.db'}"
    )
    monkeypatch.setenv("INTELIPUMP_SAFETY__ALLOW_LAB_SIMULATOR_COMMANDS", "true")
    app = create_app()
    with TestClient(app) as client:
        yield client, app
    get_settings.cache_clear()


def test_sse_connection_and_keepalive(app_client) -> None:  # type: ignore[no-untyped-def]
    client, app = app_client
    state = app.state.app_state

    async def _probe() -> str:
        sub = await state.broker.subscribe(kind="sse")
        state.broker.publish_typed(
            LiveEventType.PUMP_STATE_CHANGED,
            station_id=state.settings.controller.station_id,
            environment="LAB",
            simulated=True,
            pump_id="pump-1",
        )
        item = await asyncio.wait_for(sub.queue.get(), timeout=1.0)
        await state.broker.unsubscribe(sub.subscription_id)
        assert item is not None
        return item.event_type.value

    assert asyncio.run(_probe()) == LiveEventType.PUMP_STATE_CHANGED.value
    assert client.get("/api/v1/controller/health").status_code == 200
    # Endpoint is registered on the app router (smoke via OpenAPI).
    assert "/api/v1/events/stream" in client.app.openapi()["paths"]


def test_websocket_connection(app_client) -> None:  # type: ignore[no-untyped-def]
    client, _app = app_client
    with client.websocket_connect("/api/v1/events/ws") as ws:
        msg = ws.receive_json()
        assert msg["event_type"] == LiveEventType.CONNECTED.value


def test_lab_command_rejected_on_physical_transport(
    app_client,  # type: ignore[no-untyped-def]
) -> None:
    client, app = app_client
    state = app.state.app_state
    # Attach a "physical" serial transport (non-/tmp path)
    transport = SerialTransport(
        SerialConfig(device="/dev/ttyUSB0")  # not opened; metadata kind physical
    )
    runtime = ControllerRuntime(
        transport=transport,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(addresses=(1,)),
    )
    state.controller_loop = ControllerLoop(runtime)
    pumps = client.get("/api/v1/pumps").json()
    pid = pumps[0]["logical_pump_id"]
    r = client.post(
        f"/api/v1/lab/pumps/{pid}/commands",
        json={"command_type": "READ_STATUS", "simulator_only": True},
    )
    assert r.status_code == 403
    assert "transport_not_virtual_or_memory" in str(r.json()["error"]["details"])


def test_lab_command_allowed_on_memory_transport(
    app_client,  # type: ignore[no-untyped-def]
) -> None:
    client, app = app_client
    state = app.state.app_state
    ctrl_t, _sim_t = create_memory_transport_pair()
    from dataclasses import replace

    safety = replace(default_lab_safety(), allow_lab_simulator_commands=True)
    runtime = ControllerRuntime(
        transport=ctrl_t,
        safety=safety,
        config=PollSchedulerConfig(addresses=(1, 2)),
    )
    state.controller_loop = ControllerLoop(runtime)
    pumps = client.get("/api/v1/pumps").json()
    pid = pumps[0]["logical_pump_id"]
    r = client.post(
        f"/api/v1/lab/pumps/{pid}/commands",
        json={"command_type": "READ_STATUS", "simulator_only": True},
    )
    assert r.status_code == 200
    assert r.json()["queued"] is True


def test_listen_only_blocks_field_authorize_evaluate(app_client) -> None:  # type: ignore[no-untyped-def]
    client, _app = app_client
    pumps = client.get("/api/v1/pumps").json()
    pid = pumps[0]["logical_pump_id"]
    r = client.post(
        f"/api/v1/pumps/{pid}/commands/evaluate",
        json={"command_type": "AUTHORIZE", "simulator_only": False},
    )
    assert r.status_code == 200
    assert r.json()["eligible"] is False
    assert r.json()["requires_active_commands_enabled"] is True or r.json()[
        "blocking_reasons"
    ]
