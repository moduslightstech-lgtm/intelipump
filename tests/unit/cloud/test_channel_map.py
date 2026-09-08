"""Config-driven DART address → physical pump / nozzle mapping."""

from __future__ import annotations

from intelipump_fdc.cloud.channel_map import (
    US_LAB_CHANNEL_MAP,
    enrich_transaction_payload,
    parse_channel_map,
)


def test_default_keeps_legacy_logical_pump_ids():
    mapping = parse_channel_map(None, (1, 2))
    assert mapping[1].pump_id == "pump-1"
    assert mapping[1].nozzle_id == "nozzle-1"
    assert mapping[2].pump_id == "pump-2"
    assert mapping[2].source_identifier == "pump-2"


def test_us_lab_two_channels_one_physical_pump():
    mapping = parse_channel_map(US_LAB_CHANNEL_MAP, (1, 2))
    assert mapping[1].pump_id == "pump-1"
    assert mapping[1].nozzle_id == "nozzle-1"
    assert mapping[2].pump_id == "pump-1"
    assert mapping[2].nozzle_id == "nozzle-2"
    assert mapping[2].source_identifier == "pump-2"


def test_enrich_legacy_queue_row_address_2():
    mapping = parse_channel_map(US_LAB_CHANNEL_MAP, (1, 2))
    out = enrich_transaction_payload(
        {
            "transaction_uuid": "tx-1",
            "pump_id": "pump-2",
            "nozzle_id": 1,
            "raw_amount": 30000,
        },
        mapping,
    )
    assert out["pumpId"] == "pump-1"
    assert out["nozzleId"] == "nozzle-2"
    assert out["sourceIdentifier"] == "pump-2"
    assert out["wayneNozzleIndex"] == 1


def test_enrich_does_not_discard_unmapped_sale():
    mapping = parse_channel_map(US_LAB_CHANNEL_MAP, (1, 2))
    out = enrich_transaction_payload({"pump_id": "pump-99", "amount": 100}, mapping)
    assert out["pump_id"] == "pump-99"
    assert out["amount"] == 100
    assert "nozzleId" not in out
