from __future__ import annotations

import pytest

from intelipump_fdc.protocol.dart.application.bcd import BcdError
from intelipump_fdc.protocol.dart.application.constants import (
    DecodeStatus,
    MessageDirection,
    TransactionType,
)
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.fields import decode_scaled_bcd
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus


def test_unknown_transaction_id_preserved() -> None:
    bundle = decode_data_payload(bytes.fromhex("FF 02 AA BB"))
    assert len(bundle.transactions) == 1
    tx = bundle.transactions[0]
    assert tx.transaction_type is TransactionType.UNKNOWN
    assert tx.decode_status is DecodeStatus.UNKNOWN
    assert tx.raw_payload == bytes.fromhex("AA BB")
    assert tx.raw_transaction == bytes.fromhex("FF 02 AA BB")


def test_invalid_bcd_in_dc2() -> None:
    # VOL contains nibble 0xA
    payload = bytes.fromhex("02 08 00 00 00 0A 00 00 00 00")
    bundle = decode_data_payload(payload)
    tx = bundle.transactions[0]
    assert tx.transaction_type is TransactionType.DC2_FILLED_VOLUME_AMOUNT
    assert tx.decode_status is DecodeStatus.MALFORMED
    assert tx.raw_payload == bytes.fromhex("00 00 00 0A 00 00 00 00")


def test_reserved_or_raw_bytes_preserved_on_partial() -> None:
    bundle = decode_data_payload(bytes.fromhex("01 01 05"))
    tx = bundle.transactions[0]
    assert tx.transaction_type is TransactionType.AMBIGUOUS_CD1_OR_DC1
    assert tx.decode_status is DecodeStatus.PARTIAL
    assert tx.direction is MessageDirection.UNKNOWN
    assert tx.raw_payload == b"\x05"
    assert tx.decoded_body is not None
    assert tx.decoded_body["dc1_pump_status"]["name"] == WaynePumpStatus.FILLING_COMPLETED.name


def test_price_field_cd5() -> None:
    bundle = decode_data_payload(
        bytes.fromhex("05 03 00 11 75"),
        price_decimals=2,
    )
    tx = bundle.transactions[0]
    assert tx.transaction_type is TransactionType.CD5_PRICE_UPDATE
    assert tx.decode_status is DecodeStatus.DECODED
    assert tx.direction is MessageDirection.MASTER_TO_SLAVE
    assert tx.decoded_body is not None
    price = tx.decoded_body["prices"][0]["price"]
    assert price["raw_scaled"] == 1175
    assert price["decimals"] == 2
    assert price["value"] == "11.75"


def test_amount_and_volume_dc2_without_assumed_decimals() -> None:
    bundle = decode_data_payload(bytes.fromhex("02 08 00 00 00 12 00 00 34 56"))
    tx = bundle.transactions[0]
    assert tx.transaction_type is TransactionType.DC2_FILLED_VOLUME_AMOUNT
    assert tx.decode_status is DecodeStatus.DECODED
    assert tx.decoded_body is not None
    assert tx.decoded_body["volume"]["raw_scaled"] == 12
    assert tx.decoded_body["volume"]["value"] is None
    assert tx.decoded_body["amount"]["raw_scaled"] == 3456
    assert tx.decoded_body["amount"]["value"] is None


def test_amount_and_volume_with_explicit_decimals() -> None:
    bundle = decode_data_payload(
        bytes.fromhex("02 08 00 00 00 12 00 00 34 56"),
        volume_decimals=2,
        amount_decimals=2,
    )
    tx = bundle.transactions[0]
    assert tx.decoded_body is not None
    assert tx.decoded_body["volume"]["value"] == "0.12"
    assert tx.decoded_body["amount"]["value"] == "34.56"


def test_payload_with_literal_10_and_fa_after_unescape() -> None:
    # After line unescaping, application DATA may contain literal 0x10 / 0xFA.
    mixed = bytes.fromhex("FF 04 10 FA 01 02")
    assert mixed[2] == 0x10 and mixed[3] == 0xFA
    bundle = decode_data_payload(mixed)
    tx = bundle.transactions[0]
    assert tx.raw_payload == bytes.fromhex("10 FA 01 02")
    rebuilt = b"".join(t.raw_transaction for t in bundle.transactions) + bundle.trailing_bytes
    assert rebuilt == mixed


def test_scaled_bcd_rejects_invalid() -> None:
    with pytest.raises(BcdError):
        decode_scaled_bcd(b"\x1a")


def test_dc3_nozzle_and_price() -> None:
    bundle = decode_data_payload(bytes.fromhex("03 04 00 11 75 11"), price_decimals=2)
    tx = bundle.transactions[0]
    assert tx.transaction_type is TransactionType.DC3_NOZZLE_STATUS_PRICE
    assert tx.decoded_body is not None
    assert tx.decoded_body["price"]["value"] == "11.75"
    assert tx.decoded_body["selected_logical_nozzle"] == 1
    assert tx.decoded_body["nozzle_out"] is True
