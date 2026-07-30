"""Unit tests for documented DC3 NOZIO bit masks."""

from __future__ import annotations

import pytest

from intelipump_fdc.protocol.dart.application.constants import TransactionType
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.nozio import (
    NOZIO_LOGICAL_NOZZLE_MASK,
    NOZIO_POSITION_MASK,
    NOZIO_RESERVED_MASK,
    decode_nozio,
)


@pytest.mark.parametrize(
    ("raw", "logical_raw", "selected", "out"),
    [
        (0x01, 1, 1, False),
        (0x11, 1, 1, True),
        (0x02, 2, 2, False),
        (0x12, 2, 2, True),
        (0x10, 0, None, True),
        (0x00, 0, None, False),
        (0x07, 7, 7, False),
        (0x17, 7, 7, True),
    ],
)
def test_documented_nozio_examples(
    raw: int, logical_raw: int, selected: int | None, out: bool
) -> None:
    nozio = raw
    logical_nozzle = nozio & 0x0F
    nozzle_out = bool(nozio & 0x10)
    assert logical_nozzle == logical_raw
    assert nozzle_out is out
    dec = decode_nozio(raw)
    assert dec.logical_nozzle_raw == logical_raw
    assert dec.selected_logical_nozzle == selected
    assert dec.nozzle_out is out
    assert dec.nozzle_position == ("OUT" if out else "IN")
    assert dec.reserved_bits == 0
    ev = dec.to_evidence_dict()
    assert ev["logicalNozzleRaw"] == logical_raw
    assert ev["selectedLogicalNozzle"] == selected
    assert ev["positionMask"] == "0x10"


def test_nozio_masks_are_documented_values() -> None:
    assert NOZIO_LOGICAL_NOZZLE_MASK == 0x0F
    assert NOZIO_POSITION_MASK == 0x10
    assert NOZIO_RESERVED_MASK == 0xE0


def test_reserved_bits_warn_and_lower_confidence() -> None:
    dec = decode_nozio(0xA1)  # reserved 0xA0 + nozzle 1 IN
    assert dec.selected_logical_nozzle == 1
    assert dec.nozzle_out is False
    assert dec.reserved_bits == 0xA0
    assert dec.decoder_confidence == "MEDIUM_RESERVED_BITS_SET"
    assert any("reserved bits" in w for w in dec.warnings)
    assert dec.to_evidence_dict()["reservedBits"] == "0xA0"


def test_dc3_payload_includes_nozio_evidence() -> None:
    bundle = decode_data_payload(bytes.fromhex("03 04 00 11 75 11"), price_decimals=2)
    tx = bundle.transactions[0]
    assert tx.transaction_type is TransactionType.AMBIGUOUS_CD3_OR_DC3
    assert tx.decoded_body is not None
    assert tx.decoded_body["selected_logical_nozzle"] == 1
    assert tx.decoded_body["logical_nozzle_raw"] == 1
    assert tx.decoded_body["nozzle_out"] is True
    nozio = tx.decoded_body["nozio"]
    assert nozio["nozioRawHex"] == "11"
    assert nozio["selectedLogicalNozzle"] == 1
