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


def test_enrich_quarantines_canonical_identity_mismatch():
    mapping = parse_channel_map(US_LAB_CHANNEL_MAP, (1, 2))
    out = enrich_transaction_payload(
        {
            "transaction_uuid": "tx-lock",
            "sourceIdentifier": "pump-2",
            "canonical_pump_id": "pump-1",
            "canonical_nozzle_id": "nozzle-1",
            "pump_id": "pump-1",
        },
        mapping,
    )
    assert out.get("identityQuarantined") is True
    assert out["canonical_nozzle_id"] == "nozzle-1"


def test_enrich_keeps_locked_canonical_identity():
    mapping = parse_channel_map(US_LAB_CHANNEL_MAP, (1, 2))
    out = enrich_transaction_payload(
        {
            "transaction_uuid": "tx-ok",
            "sourceIdentifier": "pump-2",
            "canonical_pump_id": "pump-1",
            "canonical_nozzle_id": "nozzle-2",
        },
        mapping,
    )
    assert out.get("identityQuarantined") is not True
    assert out["pumpId"] == "pump-1"
    assert out["nozzleId"] == "nozzle-2"


def test_safe_mappings_keeps_us_lab_on_load_failure(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from intelipump_fdc.cloud.channel_map import safe_mappings_from_settings

    bad = tmp_path / "broken.json"
    bad.write_text("{not-json", encoding="utf-8")
    settings = SimpleNamespace(
        channel_map_path=str(bad),
        channel_map=None,
        channel_map_allow_missing=True,
        controller=SimpleNamespace(station_id="InteliPump-US-Lab"),
    )
    mapping = safe_mappings_from_settings(settings, (1, 2))
    assert mapping[2].pump_id == "pump-1"
    assert mapping[2].nozzle_id == "nozzle-2"
    assert mapping[2].source_identifier == "pump-2"
    assert mapping[1].pump_id == "pump-1"
    assert mapping[1].nozzle_id == "nozzle-1"


def test_default_address_2_is_not_physical_nozzle_2():
    """Document the incorrect default that US Lab must override."""
    mapping = parse_channel_map(None, (1, 2))
    assert mapping[2].pump_id == "pump-2"
    assert mapping[2].nozzle_id == "nozzle-1"


def test_duplicate_nozzle_mapping_rejected():
    import pytest
    from intelipump_fdc.cloud.channel_map import DuplicateChannelMappingError

    with pytest.raises(DuplicateChannelMappingError):
        parse_channel_map(
            {
                "1": {"pump_id": "pump-1", "nozzle_id": "nozzle-1"},
                "2": {"pump_id": "pump-1", "nozzle_id": "nozzle-1"},
            },
            (1, 2),
        )

def test_missing_channel_map_path_fails_without_allow_missing():
    from types import SimpleNamespace

    import pytest

    from intelipump_fdc.cloud.channel_map import (
        ChannelMapMissingError,
        mappings_from_settings,
    )

    settings = SimpleNamespace(
        channel_map_path="/no/such/channel_map.us-lab.json",
        channel_map=None,
        channel_map_allow_missing=False,
        controller=SimpleNamespace(station_id="InteliPump-US-Lab"),
    )
    with pytest.raises(ChannelMapMissingError):
        mappings_from_settings(settings, (1, 2))


def test_missing_channel_map_path_uses_us_lab_embedded_when_allowed():
    from types import SimpleNamespace

    from intelipump_fdc.cloud.channel_map import mappings_from_settings

    settings = SimpleNamespace(
        channel_map_path="/no/such/channel_map.us-lab.json",
        channel_map=None,
        channel_map_allow_missing=True,
        controller=SimpleNamespace(station_id="InteliPump-US-Lab"),
    )
    mapping = mappings_from_settings(settings, (1, 2))
    assert mapping[2].pump_id == "pump-1"
    assert mapping[2].nozzle_id == "nozzle-2"


def test_source_address_1_and_2_map_to_distinct_nozzles():
    mapping = parse_channel_map(US_LAB_CHANNEL_MAP, (1, 2))
    assert mapping[1].nozzle_id == "nozzle-1"
    assert mapping[2].nozzle_id == "nozzle-2"
    assert mapping[1].pump_id == mapping[2].pump_id == "pump-1"
