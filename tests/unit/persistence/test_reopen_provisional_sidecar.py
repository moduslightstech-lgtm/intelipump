"""Same-UUID reopen is limited to provisional sidecar-settle sessions."""

from __future__ import annotations

import pytest

from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
    FillingUpdateRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService

STATION = "InteliPump-US-Lab"


async def _seed_completed(
    factory,
    *,
    uuid: str,
    key: str,
    volume: int,
    amount: int,
    pump_logical: str,
    dart: int,
) -> None:
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id=pump_logical, dart_address=dart
        )
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump.id,
                transaction_uuid=uuid,
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid=uuid,
                raw_volume=volume,
                raw_amount=amount,
                event_key=f"fill:{uuid}:{volume}:{amount}",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid=uuid,
                source_completion_key=key,
                raw_volume=volume,
                raw_amount=amount,
                completion_inferred=key.startswith("sidecar-settle:"),
            )
        )


@pytest.mark.asyncio
async def test_reopen_allows_sidecar_settle_only(engine_factory: tuple) -> None:
    _engine, factory = engine_factory

    await _seed_completed(
        factory,
        uuid="tx-prov",
        key="sidecar-settle:tx-prov",
        volume=100,
        amount=117500,
        pump_logical="pump-1",
        dart=1,
    )
    async with unit_of_work(factory) as uow:
        reopened = await uow.transactions.reopen_provisional_sidecar(
            "tx-prov", raw_volume=110, raw_amount=129250
        )
        assert reopened is not None
        assert reopened.status == "ACTIVE"
        assert reopened.source_completion_key is None
        assert reopened.raw_volume == 110

    await _seed_completed(
        factory,
        uuid="tx-verified",
        key="complete:tx-verified",
        volume=200,
        amount=235000,
        pump_logical="pump-2",
        dart=2,
    )
    async with unit_of_work(factory) as uow:
        refused = await uow.transactions.reopen_provisional_sidecar(
            "tx-verified", raw_volume=210, raw_amount=246750
        )
        assert refused is None
        kept = await uow.transactions.get_by_uuid("tx-verified")
        assert kept is not None
        assert kept.status == "COMPLETED"
        assert kept.source_completion_key == "complete:tx-verified"
        assert kept.raw_volume == 200
