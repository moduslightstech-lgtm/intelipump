from __future__ import annotations

from intelipump_fdc.protocol.dart.application.splitter import split_transactions


def test_empty_payload() -> None:
    result = split_transactions(b"")
    assert result.transactions == ()
    assert result.trailing_bytes == b""
    assert result.warnings == ()


def test_one_complete_transaction() -> None:
    result = split_transactions(bytes.fromhex("65 01 01"))
    assert len(result.transactions) == 1
    tx = result.transactions[0]
    assert tx.transaction_id == 0x65
    assert tx.length == 1
    assert tx.data == b"\x01"
    assert tx.raw == bytes.fromhex("65 01 01")
    assert result.trailing_bytes == b""


def test_multiple_transactions_in_one_payload() -> None:
    # From capture: DC2 + DC3 + CD1/DC1 ambiguous
    payload = bytes.fromhex(
        "02 08 00 00 00 00 00 00 00 00 03 04 00 11 75 01 01 01 05"
    )
    result = split_transactions(payload)
    assert [tx.transaction_id for tx in result.transactions] == [0x02, 0x03, 0x01]
    assert [tx.length for tx in result.transactions] == [8, 4, 1]
    joined = b"".join(tx.raw for tx in result.transactions) + result.trailing_bytes
    assert joined == payload
    assert result.trailing_bytes == b""


def test_truncated_transaction_preserves_bytes() -> None:
    payload = bytes.fromhex("02 08 00 00")  # claims 8 data bytes, has 2
    result = split_transactions(payload)
    assert result.transactions == ()
    assert result.trailing_bytes == payload
    assert any("truncated" in w for w in result.warnings)


def test_truncated_header_preserved() -> None:
    result = split_transactions(b"\x01")
    assert result.transactions == ()
    assert result.trailing_bytes == b"\x01"
    assert result.warnings


def test_no_bytes_silently_dropped() -> None:
    payload = bytes.fromhex("05 03 00 11 75 FF")
    result = split_transactions(payload)
    rebuilt = b"".join(tx.raw for tx in result.transactions) + result.trailing_bytes
    assert rebuilt == payload
    assert result.trailing_bytes == b"\xff"
