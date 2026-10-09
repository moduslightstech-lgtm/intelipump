"""Hang-up / LIMIT with face volume must COMPLETE even if may_publish_sale=false.

Regression for SAO pump-2 2026-10-09: live sessions left ACTIVE forever because
PersistenceBridge suppressed on may_publish_sale=false while filled_volume>0
and active_transaction_id was null on the SM payload.
"""

from __future__ import annotations

import pytest

from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.services.persistence_worker import PersistenceWorker
from intelipump_fdc.services.transaction_models import BeginTransactionRequest
from intelipump_fdc.services.transaction_service import TransactionService

STATION = "SAO-Redeemed-Station-1"


def _bridge(factory, pump_id: str) -> PersistenceBridge:
    return PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="PROD",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_id},
        logical_by_address={2: "pump-2"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-2"},
        mqtt_nozzle_by_address={2: "nozzle-1"},
        mqtt_source_by_address={2: "pump-2-n1"},
    )


def _seed_verified(bridge: PersistenceBridge, *, address: int, pump_id: str) -> None:
    can_pump = bridge._mqtt_pump_by_address.get(
        address, bridge._logical_by_address.get(address, pump_id)
    )
    can_nozzle = bridge._mqtt_nozzle_by_address.get(address) or "nozzle-1"
    bridge._verified.note_nozzle_lifted(
        pump_id=can_pump, nozzle_id=can_nozzle, dart_address=address
    )
    bridge._verified.note_authorized(
        pump_id=can_pump,
        nozzle_id=can_nozzle,
        dart_address=address,
        baseline_volume_raw=0,
    )
    bridge._verified.note_dc1_state(
        pump_id=can_pump,
        nozzle_id=can_nozzle,
        dc1_state="FILLING",
        dart_address=address,
    )
    bridge._verified.note_volume(
        pump_id=can_pump,
        nozzle_id=can_nozzle,
        volume_raw=5,
        dart_address=address,
    )


@pytest.mark.asyncio
async def test_hangup_completes_active_when_may_publish_false(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
        await TransactionService(uow).begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-zombie-active",
                nozzle_id=1,
                canonical_pump_id="pump-2",
                canonical_nozzle_id="nozzle-1",
                source_identifier="pump-2-n1",
                raw_price=1355,
                price_decimals=0,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="PROD",
            )
        )

    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-zombie-active"
    _seed_verified(bridge, address=2, pump_id="pump-2")

    # Progress the open ACTIVE (as live DC2 would).
    await bridge._handle_app_decoded(
        {
            "address": 2,
            "is_dc2": True,
            "payload": {
                "raw_volume": 3690,
                "raw_amount": 5000000,
                "volume_decimals": 2,
                "amount_decimals": 2,
                "raw_price": 1355,
                "price_decimals": 0,
                "selected_nozzle": 1,
            },
        }
    )

    # Hang-up / LIMIT style STATE_CHANGED: may_publish false, SM tx id null —
    # must still COMPLETE the mapped ACTIVE row.
    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "FILLING->LIMIT_REACHED",
            "payload": {
                "event": "LIMIT_REACHED",
                "normalized_state": PumpState.LIMIT_REACHED.value,
                "previous_state": PumpState.FILLING.value,
                "active_transaction_id": None,
                "selected_nozzle": 1,
                "communication_healthy": True,
                "state_version": 12,
                "may_publish_sale": False,
                "sale_lifecycle": "FILLING",
                "filled_volume_raw": 3690,
                "filled_amount_raw": 5000000,
                "filling_price_raw": 1355,
                "unit_price_raw": 1355,
                "filling_seen_this_boot": True,
                "filling_observed": True,
                "completion_evidence_key": "complete:frame:6",
                "awaiting_filling_complete": False,
            },
        }
    )

    async with unit_of_work(factory) as uow:
        row = await uow.transactions.get_by_uuid("tx-zombie-active")
        assert row is not None
        assert row.status == "COMPLETED"
        assert row.raw_volume == 3690
        assert row.raw_amount == 5000000
        open_rows = await uow.transactions.list_unresolved(station_id=STATION)
        assert open_rows == ()
