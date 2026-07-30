from __future__ import annotations

import json
from pathlib import Path

from intelipump_fdc.protocol.dart.application.constants import (
    DecodeStatus,
    TransactionType,
)
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame

FIXTURE_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "dart"


def _hex(value: object) -> bytes:
    assert isinstance(value, str)
    return bytes(int(part, 16) for part in value.split())


def _load_app_fixtures() -> list[dict[str, object]]:
    path = FIXTURE_DIR / "application_transactions.json"
    assert path.is_file(), "run scripts/analyze_dart_application.py first"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    return data


def test_representative_status_frame() -> None:
    data_frames = json.loads((FIXTURE_DIR / "data_frames.json").read_text(encoding="utf-8"))
    found = False
    for item in data_frames:
        bundle = decode_data_payload(_hex(item["payloadHex"]))
        for tx in bundle.transactions:
            if tx.transaction_type is TransactionType.AMBIGUOUS_CD1_OR_DC1:
                assert tx.decode_status is DecodeStatus.PARTIAL
                assert tx.decoded_body is not None
                assert "dc1_pump_status" in tx.decoded_body
                found = True
                break
        if found:
            break
    assert found


def test_representative_filling_and_completed_from_multi_payload() -> None:
    data_frames = json.loads((FIXTURE_DIR / "data_frames.json").read_text(encoding="utf-8"))
    multi = next(
        item
        for item in data_frames
        if "02 08" in item["payloadHex"] and "03 04" in item["payloadHex"]
    )
    bundle = decode_data_payload(
        _hex(multi["payloadHex"]),
        pump_address=int(multi["expectedAddress"]),  # type: ignore[arg-type]
        line_sequence=int(multi["expectedSequence"]),  # type: ignore[arg-type]
        source_frame_raw_hex=str(multi["rawHex"]),
    )
    types = {tx.transaction_type for tx in bundle.transactions}
    assert TransactionType.DC2_FILLED_VOLUME_AMOUNT in types
    assert TransactionType.AMBIGUOUS_CD3_OR_DC3 in types
    dc2 = next(
        tx
        for tx in bundle.transactions
        if tx.transaction_type is TransactionType.DC2_FILLED_VOLUME_AMOUNT
    )
    assert dc2.decode_status is DecodeStatus.DECODED
    assert dc2.decoded_body is not None
    assert "volume" in dc2.decoded_body and "amount" in dc2.decoded_body


def test_application_fixture_rows_decode() -> None:
    rows = _load_app_fixtures()
    assert rows
    for row in rows:
        if row.get("expectedTransactionType") == "MULTI":
            bundle = decode_data_payload(_hex(row["payloadHex"]))
            assert len(bundle.transactions) == row["expectedTransactionCount"]
            assert [t.transaction_type.value for t in bundle.transactions] == row[
                "transactionTypes"
            ]
            continue
        # Single raw transaction may be embedded in a larger frame; prefer rawTransactionHex
        raw_tx = _hex(row["rawTransactionHex"])
        bundle = decode_data_payload(raw_tx)
        assert len(bundle.transactions) == 1
        tx = bundle.transactions[0]
        assert tx.transaction_type.value == row["expectedTransactionType"]
        assert tx.decode_status.value == row["expectedDecodeStatus"]


def test_line_frame_to_application_pipeline() -> None:
    data_frames = json.loads((FIXTURE_DIR / "data_frames.json").read_text(encoding="utf-8"))
    item = next(
        (i for i in data_frames if "65 " in i["payloadHex"] or i["payloadHex"].startswith("65")),
        data_frames[0],
    )
    parsed = parse_frame(_hex(item["rawHex"]))
    assert isinstance(parsed, DartLineFrame)
    bundle = decode_data_payload(
        parsed.payload,
        pump_address=parsed.address,
        line_sequence=parsed.sequence,
        source_frame_raw_hex=parsed.raw_frame.hex(" ").upper(),
    )
    assert bundle.transactions
    rebuilt = b"".join(tx.raw_transaction for tx in bundle.transactions) + bundle.trailing_bytes
    assert rebuilt == parsed.payload
