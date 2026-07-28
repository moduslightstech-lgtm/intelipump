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
    ("raw", "logical", "out"),
    [
        (0x01, 1, False),
        (0x11, 1, True),
        (0x02, 2, False),
        (0x12, 2, True),
        (0x07, 7, False),
        (0x17, 7, True),
    ],
)
def test_documented_nozio_examples(raw: int, logical: int, out: bool) -> None:
    dec = decode_nozio(raw)
    assert dec.selected_logical_nozzle == logical
    assert dec.nozzle_out is out
    assert dec.nozzle_position == ("OUT" if out else "IN")
    assert dec.reserved_bits == 0
    assert dec.decoder_confidence == "HIGH_DOCUMENTED_MASKS"
    ev = dec.to_evidence_dict()
    assert ev["nozioRawHex"] == f"{raw:02X}"
    assert ev["nozioBinary"] == format(raw, "08b")
    assert ev["logicalNozzleMask"] == "0x0F"
    assert ev["positionMask"] == "0x10"
    assert ev["reservedBits"] == "0x00"
    assert ev["selectedLogicalNozzle"] == logical
    assert ev["nozzlePosition"] == ("OUT" if out else "IN")
    assert "page 21" in ev["documentationSource"]
    assert ev["decoderConfidence"] == "HIGH_DOCUMENTED_MASKS"


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
    assert tx.transaction_type is TransactionType.DC3_NOZZLE_STATUS_PRICE
    assert tx.decoded_body is not None
    assert tx.decoded_body["selected_logical_nozzle"] == 1
    assert tx.decoded_body["nozzle_out"] is True
    nozio = tx.decoded_body["nozio"]
    assert nozio["nozioRawHex"] == "11"
    assert nozio["nozioBinary"] == "00010001"
    assert nozio["nozzlePosition"] == "OUT"
    assert nozio["decoderConfidence"] == "HIGH_DOCUMENTED_MASKS"
