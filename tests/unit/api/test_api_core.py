"""API health, correlation, errors, pumps, transactions."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from intelipump_fdc.api.state import AppState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService


def test_health_response(api_client: TestClient) -> None:
    r = api_client.get("/api/v1/controller/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] in {"ONLINE", "DEGRADED", "STARTING"}
    assert body["mode"] == "LISTEN_ONLY"
    assert body["active_commands_enabled"] is False
    assert body["database_status"] == "OK"
    assert body["schema_version"] == 3
    assert "X-Correlation-ID" in r.headers


def test_correlation_id_generation_and_propagation(api_client: TestClient) -> None:
    r = api_client.get("/api/v1/controller/health")
    assert r.headers["X-Correlation-ID"]
    custom = "lab-corr-123"
    r2 = api_client.get(
        "/api/v1/controller/health", headers={"X-Correlation-ID": custom}
    )
    assert r2.headers["X-Correlation-ID"] == custom


def test_malformed_correlation_id_rejected(api_client: TestClient) -> None:
    r = api_client.get(
        "/api/v1/controller/health",
        headers={"X-Correlation-ID": "bad id with spaces!!!"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "INVALID_CORRELATION_ID"


def test_consistent_error_format_pump_not_found(api_client: TestClient) -> None:
    r = api_client.get("/api/v1/pumps/does-not-exist")
    assert r.status_code == 404
    err = r.json()["error"]
    assert err["code"] == "PUMP_NOT_FOUND"
    assert "correlation_id" in err


def test_pump_list_and_details(api_client: TestClient) -> None:
    r = api_client.get("/api/v1/pumps")
    assert r.status_code == 200
    pumps = r.json()
    assert len(pumps) >= 2
    pid = pumps[0]["logical_pump_id"]
    r2 = api_client.get(f"/api/v1/pumps/{pid}")
    assert r2.status_code == 200
    assert r2.json()["logical_pump_id"] == pid
    assert "normalized_state" in r2.json()


def test_metrics_endpoint(api_client: TestClient) -> None:
    r = api_client.get("/api/v1/controller/metrics")
    assert r.status_code == 200
    body = r.json()
    assert "poll_count" in body
    assert "active_sse_subscribers" in body
    assert body["mqtt_enabled"] is False
    assert "mqtt_host" in body
    assert "password" not in json.dumps(body).lower()


def test_health_mqtt_fields_default_disabled(api_client: TestClient) -> None:
    r = api_client.get("/api/v1/controller/health")
    body = r.json()
    assert body["mqtt_enabled"] is False
    assert body["mqtt_connected"] is False
    assert body["cloud_command_subscription_active"] is False
    assert "sync_delivered_count" in body


@pytest.mark.asyncio
async def test_transaction_pagination_filters_and_raw_values(
    api_client: TestClient,
) -> None:
    state: AppState = api_client.app.state.app_state
    async with unit_of_work(state.session_factory) as uow:
        pump = (await uow.pumps.list_for_station(state.settings.controller.station_id))[0]
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=state.settings.controller.station_id,
                pump_db_id=pump.id,
                transaction_uuid="api-tx-1",
                nozzle_id=1,
                raw_price=12345,
                price_decimals=None,
                volume_decimals=3,
                amount_decimals=None,
                simulated=True,
                environment="LAB",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="api-tx-1",
                source_completion_key="api-key-1",
                raw_volume=1000,
                raw_amount=2000,
            )
        )

    r = api_client.get(
        "/api/v1/transactions",
        params={"status": "COMPLETED", "page": 1, "page_size": 10},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["page"]["total"] >= 1
    item = next(i for i in body["items"] if i["transaction_uuid"] == "api-tx-1")
    assert item["raw_volume"] == 1000
    assert item["raw_price"] == 12345
    assert item["price_formatted"] is None  # decimals unknown
    assert item["volume_formatted"] == "1.000"

    r2 = api_client.get(f"/api/v1/transactions/{item['id']}")
    assert r2.status_code == 200
    ev = api_client.get(f"/api/v1/transactions/{item['id']}/events")
    assert ev.status_code == 200
    assert len(ev.json()) >= 1


def test_transaction_not_found(api_client: TestClient) -> None:
    r = api_client.get(f"/api/v1/transactions/{uuid4()}")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "TRANSACTION_NOT_FOUND"


def test_command_evaluation_blocked_and_persisted(api_client: TestClient) -> None:
    pumps = api_client.get("/api/v1/pumps").json()
    pid = pumps[0]["logical_pump_id"]
    corr = "eval-auth-1"
    r = api_client.post(
        f"/api/v1/pumps/{pid}/commands/evaluate",
        headers={"X-Correlation-ID": corr},
        json={
            "command_type": "AUTHORIZE",
            "simulator_only": True,
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["eligible"] is False
    assert body["command_persisted"] is True
    assert body["correlation_id"] == corr
    assert body["audit_record_id"]

    audit = api_client.get("/api/v1/audit", params={"correlation_id": corr})
    assert audit.status_code == 200
    assert audit.json()["page"]["total"] >= 1

    verify = api_client.get("/api/v1/audit/verify")
    assert verify.status_code == 200
    assert verify.json()["valid"] is True
    assert verify.json()["genesis"] == "GENESIS_V1"


def test_lab_command_rejected_without_controller(api_client: TestClient) -> None:
    pumps = api_client.get("/api/v1/pumps").json()
    pid = pumps[0]["logical_pump_id"]
    r = api_client.post(
        f"/api/v1/lab/pumps/{pid}/commands",
        json={"command_type": "READ_STATUS", "simulator_only": True},
    )
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "SIMULATOR_ONLY_RESTRICTION"


def test_alarms_list_empty(api_client: TestClient) -> None:
    r = api_client.get("/api/v1/alarms")
    assert r.status_code == 200
    assert "items" in r.json()
