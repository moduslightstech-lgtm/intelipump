"""DC1/DC2/DC3/unknown transaction parsing for capture analysis."""

from __future__ import annotations

from tools.passive_dart_capture.dart_parser import DC1_STATUS_LABELS, parse_data_payload


def test_dc1_status_labels() -> None:
    expected = {
        0x00: "PUMP_NOT_PROGRAMMED",
        0x01: "RESET",
        0x02: "AUTHORIZED",
        0x04: "FILLING",
        0x05: "FILLING_COMPLETED",
        0x06: "LIMIT_REACHED",
        0x07: "SWITCHED_OFF",
        0x08: "SUSPENDED",
    }
    assert expected == DC1_STATUS_LABELS
    for code, label in expected.items():
        txs = parse_data_payload(bytes((0x01, 0x01, code)))
        assert len(txs) == 1
        assert txs[0]["decoded"]["statusLabel"] == label
        assert txs[0]["decoded"]["kind"] == "DC1"


def test_dc2_preserves_raw_bcd() -> None:
    payload = bytes.fromhex("02 08 00 00 00 12 00 00 34 56")
    txs = parse_data_payload(payload)
    assert len(txs) == 1
    dec = txs[0]["decoded"]
    assert dec["kind"] == "DC2"
    assert dec["volumeBcdHex"] == "00 00 00 12"
    assert dec["amountBcdHex"] == "00 00 34 56"
    assert dec["volumeRawScaled"] == 12
    assert dec["amountRawScaled"] == 3456  # packed BCD 00 00 34 56


def test_dc3_price_and_nozio_in_out() -> None:
    # NOZIO 0x01 = logical 1 IN; 0x11 = logical 1 OUT
    in_payload = bytes.fromhex("03 04 00 11 75 01")
    out_payload = bytes.fromhex("03 04 00 11 75 11")
    in_tx = parse_data_payload(in_payload)[0]
    out_tx = parse_data_payload(out_payload)[0]
    assert in_tx["decoded"]["priceBcdHex"] == "00 11 75"
    assert in_tx["decoded"]["logicalNozzle"] == 1
    assert in_tx["decoded"]["nozzlePosition"] == "IN"
    assert out_tx["decoded"]["logicalNozzle"] == 1
    assert out_tx["decoded"]["nozzlePosition"] == "OUT"
    assert out_tx["decoded"]["nozioRaw"] == 0x11


def test_dc3_reserved_bits_warn() -> None:
    # reserved 0xA0 + nozzle 1 IN
    payload = bytes.fromhex("03 04 00 11 75 A1")
    tx = parse_data_payload(payload)[0]
    assert tx["decoded"]["reservedBits"] == 0xA0
    assert tx["decoded"]["nozzlePosition"] == "IN"
    assert any("reserved" in w.lower() for w in tx["warnings"])


def test_unknown_transaction_preservation() -> None:
    payload = bytes.fromhex("FF 02 AA BB")
    txs = parse_data_payload(payload)
    assert len(txs) == 1
    assert txs[0]["unknown"] is True
    assert txs[0]["transactionId"] == 0xFF
    assert txs[0]["length"] == 2
    assert txs[0]["dataHex"] == "AA BB"
    assert txs[0]["decoded"]["kind"] == "UNKNOWN"
