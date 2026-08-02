"""Direction-aware offline analyzer regression tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.passive_dart_capture.analyzer import (
    analyze_path,
    load_session,
    parse_window_spec,
)
from tools.passive_dart_capture.dart_parser import parse_data_payload
from tools.passive_dart_capture.direction_inference import (
    CONFIDENCE_HIGH,
    DIRECTION_CONTROLLER_TO_PUMP,
    DIRECTION_PUMP_TO_CONTROLLER,
    annotate_frames,
    build_dc1_transitions,
    build_nozio_transitions,
    build_rejected_address_diagnostics,
)

from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame

LAB002 = (
    Path(__file__).resolve().parents[1] / "evidence" / "lab-002-epump-out.jsonl"
)


def _frame(
    *,
    seq: int,
    addr: str,
    frame_class: str,
    control: str = "20",
    raw: str = "",
    crc_valid: bool | None = None,
    payload: bytes | None = None,
    mono: int | None = None,
) -> dict:
    txs: list = []
    if payload is not None:
        txs = parse_data_payload(payload, pump_address=int(addr, 16))
        raw_bytes = build_data_frame(int(addr, 16), int(control, 16) & 0x0F, payload)
        raw = raw_bytes.hex(" ").upper()
        control = f"{raw_bytes[1]:02X}"
        crc_valid = True
        frame_class = "DATA"
    return {
        "recordType": "frame",
        "sessionId": "syn",
        "frameSequence": seq,
        "firstByteTimestampUtc": f"2026-08-02T00:00:00.{seq:06d}+00:00",
        "firstByteMonotonicNs": mono if mono is not None else seq * 1000,
        "rawHex": raw or f"{addr} {control} FA",
        "addressHex": addr,
        "controlHex": control,
        "frameClass": frame_class,
        "complete": True,
        "crcValid": crc_valid,
        "transactions": txs,
    }


def test_cd1_return_status_not_counted_as_dc1_pump_not_programmed() -> None:
    """Controller RETURN_STATUS (01 01 00) after EOT must not become DC1."""
    frames = [
        _frame(seq=1, addr="50", frame_class="POLL"),
        _frame(seq=2, addr="50", frame_class="SHORT_CONTROL_EOT", control="70"),
        _frame(
            seq=3,
            addr="50",
            frame_class="DATA",
            control="30",
            payload=bytes.fromhex("01 01 00"),
        ),
        _frame(seq=4, addr="50", frame_class="SHORT_ACK", control="C0"),
        # Establish a real pump status after POLL so a transition would fire
        # if RETURN_STATUS were mis-counted as PUMP_NOT_PROGRAMMED.
        _frame(seq=5, addr="50", frame_class="POLL"),
        _frame(
            seq=6,
            addr="50",
            frame_class="DATA",
            control="31",
            payload=bytes.fromhex("01 01 05"),
        ),
        _frame(seq=7, addr="50", frame_class="SHORT_ACK", control="C1"),
    ]
    directed = annotate_frames(frames)
    assert directed[2].direction == DIRECTION_CONTROLLER_TO_PUMP
    assert directed[2].confidence == CONFIDENCE_HIGH
    assert directed[2].transactions[0]["decoded"]["kind"] == "CD1"
    assert directed[2].transactions[0]["decoded"]["commandName"] == "RETURN_STATUS"

    assert directed[5].direction == DIRECTION_PUMP_TO_CONTROLLER
    assert directed[5].transactions[0]["decoded"]["kind"] == "DC1"
    assert directed[5].transactions[0]["decoded"]["statusLabel"] == "FILLING_COMPLETED"

    transitions = build_dc1_transitions(directed)
    assert not any(
        t["fromStatus"] == "PUMP_NOT_PROGRAMMED" or t["toStatus"] == "PUMP_NOT_PROGRAMMED"
        for t in transitions
    )


def test_data_after_same_address_poll_is_pump_response() -> None:
    frames = [
        _frame(seq=10, addr="50", frame_class="POLL"),
        _frame(
            seq=11,
            addr="50",
            frame_class="DATA",
            control="34",
            payload=bytes.fromhex("03 04 00 01 60 11"),
        ),
        _frame(seq=12, addr="50", frame_class="SHORT_ACK", control="C4"),
    ]
    directed = annotate_frames(frames)
    data = directed[1]
    assert data.direction == DIRECTION_PUMP_TO_CONTROLLER
    assert data.confidence == CONFIDENCE_HIGH
    assert data.correlated_poll_sequence == 10
    assert directed[2].direction == DIRECTION_CONTROLLER_TO_PUMP
    assert directed[2].correlated_data_sequence == 11
    assert data.correlated_ack_sequence == 12


def test_ordered_duplicate_dc3_transactions_within_one_frame() -> None:
    """Frame like 5999: NOZIO 11 then 01 — both preserved; ordered transitions."""
    payload = bytes.fromhex("01 01 02 03 04 00 01 60 11 03 04 00 01 60 01")
    frames = [
        _frame(seq=1, addr="50", frame_class="POLL"),
        _frame(
            seq=2,
            addr="50",
            frame_class="DATA",
            control="3F",
            payload=bytes.fromhex("03 04 00 01 60 11"),
        ),
        _frame(seq=3, addr="50", frame_class="SHORT_ACK", control="C0"),
        _frame(seq=4, addr="50", frame_class="POLL"),
        _frame(seq=5, addr="50", frame_class="DATA", control="3F", payload=payload),
        _frame(seq=6, addr="50", frame_class="SHORT_ACK", control="CF"),
    ]
    directed = annotate_frames(frames)
    txs = directed[4].transactions
    dc3 = [t for t in txs if (t.get("decoded") or {}).get("kind") == "DC3"]
    assert len(dc3) == 2
    assert dc3[0]["decoded"]["nozioRawHex"] == "11"
    assert dc3[0]["decoded"]["nozzlePosition"] == "OUT"
    assert dc3[1]["decoded"]["nozioRawHex"] == "01"
    assert dc3[1]["decoded"]["nozzlePosition"] == "IN"

    nozio = build_nozio_transitions(directed)
    assert any(
        t["frameSequence"] == 5 and t["fromPosition"] == "OUT" and t["toPosition"] == "IN"
        for t in nozio
    )


def test_corrupt_address_recovery_excluded_from_state_reports() -> None:
    frames = [
        _frame(seq=1, addr="50", frame_class="POLL"),
        _frame(
            seq=2,
            addr="50",
            frame_class="DATA",
            control="30",
            payload=bytes.fromhex("01 01 05"),
        ),
        {
            "recordType": "frame",
            "sessionId": "syn",
            "frameSequence": 3,
            "firstByteTimestampUtc": "2026-08-02T00:00:00.000003+00:00",
            "firstByteMonotonicNs": 3000,
            "rawHex": "A8 30 01 01 00 AA BB 03 FA",
            "addressHex": "A8",
            "controlHex": "30",
            "frameClass": "DATA",
            "complete": True,
            "crcValid": True,
            "transactions": parse_data_payload(
                bytes.fromhex("01 01 00"), pump_address=0x50
            ),
        },
        _frame(seq=4, addr="94", frame_class="SHORT_ACK", control="C0"),
    ]
    directed = annotate_frames(frames)
    dc1 = build_dc1_transitions(directed)
    assert all(t["addressHex"] in {"50", "51"} for t in dc1)
    rejected = build_rejected_address_diagnostics(directed)
    assert {r["addressHex"] for r in rejected} >= {"A8", "94"}


def test_cd5_not_accepted_without_controller_direction() -> None:
    # LNG=3 looks like CD5 structure but on pump response after POLL → not CD5.
    frames = [
        _frame(seq=1, addr="50", frame_class="POLL"),
        _frame(
            seq=2,
            addr="50",
            frame_class="DATA",
            control="30",
            payload=bytes.fromhex("05 03 00 11 75"),
        ),
    ]
    directed = annotate_frames(frames)
    assert directed[1].direction == DIRECTION_PUMP_TO_CONTROLLER
    kind = (directed[1].transactions[0].get("decoded") or {}).get("kind")
    assert kind != "CD5"

    # After EOT → controller CD5 accepted.
    frames2 = [
        _frame(seq=1, addr="50", frame_class="POLL"),
        _frame(seq=2, addr="50", frame_class="SHORT_CONTROL_EOT", control="70"),
        _frame(
            seq=3,
            addr="50",
            frame_class="DATA",
            control="30",
            payload=bytes.fromhex("05 03 00 11 75"),
        ),
    ]
    directed2 = annotate_frames(frames2)
    assert directed2[2].direction == DIRECTION_CONTROLLER_TO_PUMP
    assert directed2[2].transactions[0]["decoded"]["kind"] == "CD5"


def test_insufficient_context_preserves_unknown() -> None:
    frames = [
        _frame(seq=1, addr="50", frame_class="SHORT_ACK", control="C0"),
        _frame(
            seq=2,
            addr="50",
            frame_class="DATA",
            control="30",
            payload=bytes.fromhex("01 01 00"),
        ),
    ]
    directed = annotate_frames(frames)
    assert directed[1].direction == "UNKNOWN"
    assert directed[1].transactions[0]["decoded"]["kind"] == "AMBIGUOUS_CD1_OR_DC1"


@pytest.mark.skipif(not LAB002.is_file(), reason="lab-002 evidence not present")
def test_lab002_dc1_count_much_lower_than_naive(tmp_path: Path) -> None:
    result = analyze_path(
        LAB002,
        reports_dir=tmp_path,
        session_id="lab-002-epump-out",
        direction_aware=True,
    )
    assert result.summary["dc1TransitionCountLegacyNaive"] > 100
    assert result.summary["dc1TransitionCount"] < 100
    # Corrected count observed from direction rules on this capture.
    assert result.summary["dc1TransitionCount"] == 8
    nozio_seqs = {t["frameSequence"] for t in result.nozio_transitions}
    assert {4104, 4944, 5527, 5999} <= nozio_seqs
    assert (tmp_path / "lab-002-epump-out-direction-aware-summary.json").is_file()


@pytest.mark.skipif(not LAB002.is_file(), reason="lab-002 evidence not present")
def test_lab002_ordered_dc3_on_frame_5999() -> None:
    session = load_session(LAB002, session_id="lab-002-epump-out")
    fr = next(f for f in session.complete_frames if f.get("frameSequence") == 5999)
    directed = annotate_frames([fr])  # single frame → unknown direction
    # Re-run with local poll context:
    poll = next(f for f in session.complete_frames if f.get("frameSequence") == 5998)
    directed = annotate_frames([poll, fr])
    assert directed[1].direction == DIRECTION_PUMP_TO_CONTROLLER
    dc3 = [
        t
        for t in directed[1].transactions
        if (t.get("decoded") or {}).get("kind") == "DC3"
    ]
    assert [t["decoded"]["nozioRawHex"] for t in dc3] == ["11", "01"]


def test_parse_window_spec() -> None:
    assert parse_window_spec("0-250") == (0, 250)
    assert parse_window_spec("2560-2660") == (2560, 2660)
    with pytest.raises(ValueError):
        parse_window_spec("250")
    with pytest.raises(ValueError):
        parse_window_spec("10-5")
