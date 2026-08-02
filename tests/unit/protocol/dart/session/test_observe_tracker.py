"""Observe-only per-address DART session tracker tests."""

from __future__ import annotations

from intelipump_fdc.protocol.dart.application.constants import MessageDirection
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.session import (
    MIN_COMPLETE_PUMP_EXCHANGES_FOR_ACTIVE,
    DirectionConfidence,
    LinkPhase,
    NozioEdge,
    ObservedLineEvent,
    ObserveSessionHub,
    PerAddressObserveSession,
    Trans01Kind,
    resolve_trans01,
)


def _ev(
    *,
    addr: int,
    frame_id: int,
    control_type: ControlType,
    sequence: int = 0,
    payload: bytes = b"",
    crc_valid: bool | None = None,
    complete: bool = True,
) -> ObservedLineEvent:
    if control_type is ControlType.DATA and crc_valid is None:
        crc_valid = True
    return ObservedLineEvent(
        wire_address=addr,
        control_type=control_type,
        sequence=sequence,
        frame_id=frame_id,
        complete=complete,
        crc_valid=crc_valid,
        payload=payload,
        monotonic_ns=frame_id * 1000,
    )


def _poll_dc1_ack(
    session: PerAddressObserveSession | ObserveSessionHub,
    *,
    addr: int,
    start_id: int,
    status: int,
    seq: int = 0,
) -> list:
    events = [
        _ev(addr=addr, frame_id=start_id, control_type=ControlType.POLL),
        _ev(
            addr=addr,
            frame_id=start_id + 1,
            control_type=ControlType.DATA,
            sequence=seq,
            payload=bytes((0x01, 0x01, status)),
        ),
        _ev(
            addr=addr,
            frame_id=start_id + 2,
            control_type=ControlType.ACK,
            sequence=seq,
        ),
    ]
    return [session.ingest(e) for e in events]


def test_cd1_return_status_after_eot_not_dc1_pump_not_programmed() -> None:
    """Controller RETURN_STATUS (01 01 00) after EOT must not become DC1."""
    hub = ObserveSessionHub()
    results = [
        hub.ingest(_ev(addr=0x50, frame_id=1, control_type=ControlType.POLL)),
        hub.ingest(_ev(addr=0x50, frame_id=2, control_type=ControlType.EOT)),
        hub.ingest(
            _ev(
                addr=0x50,
                frame_id=3,
                control_type=ControlType.DATA,
                sequence=0,
                payload=bytes((0x01, 0x01, 0x00)),  # CD1 RETURN_STATUS
            )
        ),
        hub.ingest(
            _ev(addr=0x50, frame_id=4, control_type=ControlType.ACK, sequence=0)
        ),
    ]
    # DATA buffered; SM not eligible yet
    assert results[2].sm_gate is not None
    assert results[2].sm_gate.may_emit_dc1 is False
    # After ACK: CD1, not DC1
    done = results[3]
    assert done.exchange_completed is not None
    assert done.exchange_completed.direction is MessageDirection.MASTER_TO_SLAVE
    assert done.trans01 is not None
    assert done.trans01.kind is Trans01Kind.CD1_COMMAND
    assert done.trans01.raw_code == 0x00
    assert done.sm_gate is not None
    assert done.sm_gate.may_emit_dc1 is False
    assert done.sm_gate.may_advance_application_sm is False
    assert done.sm_gate.last_seen_only is True

    # Real pump DC1 FILLING_COMPLETED after POLL still works
    pump = _poll_dc1_ack(hub, addr=0x50, start_id=5, status=0x05, seq=1)
    assert pump[2].trans01 is not None
    assert pump[2].trans01.kind is Trans01Kind.DC1_STATUS
    assert pump[2].trans01.raw_code == 0x05
    assert pump[2].sm_gate is not None
    assert pump[2].sm_gate.may_emit_dc1 is True


def test_addresses_50_and_51_are_independent() -> None:
    hub = ObserveSessionHub()
    _poll_dc1_ack(hub, addr=0x50, start_id=1, status=0x01)  # RESET on 50
    _poll_dc1_ack(hub, addr=0x51, start_id=10, status=0x04)  # FILLING on 51

    s50 = hub.session(0x50)
    s51 = hub.session(0x51)
    assert s50 is not None and s51 is not None
    assert s50.snapshot.last_dc1_status == 0x01
    assert s51.snapshot.last_dc1_status == 0x04
    # Response on 51 must not change 50
    _poll_dc1_ack(hub, addr=0x51, start_id=20, status=0x05)
    assert s50.snapshot.last_dc1_status == 0x01
    assert s51.snapshot.last_dc1_status == 0x05


def test_sm_gate_requires_complete_poll_data_ack_exchange() -> None:
    session = PerAddressObserveSession(0x50)
    r_poll = session.ingest(_ev(addr=0x50, frame_id=1, control_type=ControlType.POLL))
    assert r_poll.sm_gate is None

    r_data = session.ingest(
        _ev(
            addr=0x50,
            frame_id=2,
            control_type=ControlType.DATA,
            payload=bytes((0x01, 0x01, 0x01)),
        )
    )
    assert r_data.direction is MessageDirection.SLAVE_TO_MASTER
    assert r_data.confidence is DirectionConfidence.HIGH
    assert r_data.sm_gate is not None
    assert r_data.sm_gate.may_advance_application_sm is False
    assert "awaiting ACK" in r_data.inference_reason or (
        r_data.sm_gate.reason == "exchange incomplete until ACK"
    )

    r_ack = session.ingest(
        _ev(addr=0x50, frame_id=3, control_type=ControlType.ACK)
    )
    assert r_ack.exchange_completed is not None
    assert r_ack.sm_gate is not None
    assert r_ack.sm_gate.may_emit_dc1 is True
    assert r_ack.sm_gate.may_advance_application_sm is True


def test_corrupt_address_diagnostics_only() -> None:
    hub = ObserveSessionHub()
    for bad in (0x94, 0xA8, 0xFC, 0xD4):
        r = hub.ingest(
            _ev(addr=bad, frame_id=bad, control_type=ControlType.DATA, payload=b"\x01\x01\x00")
        )
        assert r.rejected is not None
        assert r.link_phase is None
        assert r.sm_gate is not None
        assert r.sm_gate.may_advance_application_sm is False
        assert hub.session(bad) is None
    assert len(hub.rejected) == 4
    # Legacy session still creatable independently
    hub.ingest(_ev(addr=0x50, frame_id=1, control_type=ControlType.POLL))
    assert hub.session(0x50) is not None


def test_startup_not_active_after_one_exchange() -> None:
    session = PerAddressObserveSession(0x50)
    assert session.link_phase is LinkPhase.DISCONNECTED
    results = _poll_dc1_ack(session, addr=0x50, start_id=1, status=0x01)
    assert results[0].link_phase is LinkPhase.INITIALIZING
    assert results[2].link_phase is LinkPhase.INITIALIZING
    assert session.snapshot.complete_valid_pump_exchanges == 1
    assert session.snapshot.complete_valid_pump_exchanges < (
        MIN_COMPLETE_PUMP_EXCHANGES_FOR_ACTIVE
    )

    results2 = _poll_dc1_ack(session, addr=0x50, start_id=10, status=0x02, seq=1)
    assert session.snapshot.complete_valid_pump_exchanges == 2
    assert results2[2].link_phase is LinkPhase.SESSION_ACTIVE

    # Next complete exchange settles to IDLE (session proven, quiet)
    results3 = _poll_dc1_ack(session, addr=0x50, start_id=20, status=0x02, seq=2)
    assert results3[2].link_phase is LinkPhase.IDLE


def test_same_dc1_status_is_last_seen_only() -> None:
    session = PerAddressObserveSession(0x50)
    first = _poll_dc1_ack(session, addr=0x50, start_id=1, status=0x01)
    assert first[2].sm_gate is not None
    assert first[2].sm_gate.may_emit_dc1 is True

    second = _poll_dc1_ack(session, addr=0x50, start_id=10, status=0x01, seq=1)
    assert second[2].sm_gate is not None
    assert second[2].sm_gate.may_emit_dc1 is False
    assert second[2].sm_gate.last_seen_only is True


def test_nozio_edges_only_and_address_scoped() -> None:
    hub = ObserveSessionHub()
    # Baseline IN on 0x50 (nozio 0x01 = logical 1, in)
    for i, nozio in enumerate((0x01, 0x11, 0x11, 0x01)):
        start = 100 * (i + 1)
        payload = bytes((0x03, 0x04, 0x00, 0x00, 0x00, nozio))
        results = [
            hub.ingest(_ev(addr=0x50, frame_id=start, control_type=ControlType.POLL)),
            hub.ingest(
                _ev(
                    addr=0x50,
                    frame_id=start + 1,
                    control_type=ControlType.DATA,
                    sequence=i,
                    payload=payload,
                )
            ),
            hub.ingest(
                _ev(
                    addr=0x50,
                    frame_id=start + 2,
                    control_type=ControlType.ACK,
                    sequence=i,
                )
            ),
        ]
        edge = results[2].nozio_edge
        if i == 0:
            assert edge is None  # baseline, no edge
        elif i == 1:
            assert edge is not None
            assert edge.edge is NozioEdge.NOZZLE_OUT
        elif i == 2:
            assert edge is None  # duplicate OUT suppressed
        else:
            assert edge is not None
            assert edge.edge is NozioEdge.NOZZLE_IN

    # 0x51 baseline OUT must not create edge on 0x50
    s50 = hub.session(0x50)
    assert s50 is not None
    last50 = s50.snapshot.last_nozio_out
    hub.ingest(_ev(addr=0x51, frame_id=900, control_type=ControlType.POLL))
    hub.ingest(
        _ev(
            addr=0x51,
            frame_id=901,
            control_type=ControlType.DATA,
            payload=bytes((0x03, 0x04, 0x00, 0x00, 0x00, 0x11)),
        )
    )
    r = hub.ingest(_ev(addr=0x51, frame_id=902, control_type=ControlType.ACK))
    assert r.nozio_edge is None  # baseline on 51
    assert s50.snapshot.last_nozio_out is last50


def test_crc_invalid_data_not_sm_eligible() -> None:
    session = PerAddressObserveSession(0x50)
    session.ingest(_ev(addr=0x50, frame_id=1, control_type=ControlType.POLL))
    r = session.ingest(
        _ev(
            addr=0x50,
            frame_id=2,
            control_type=ControlType.DATA,
            payload=bytes((0x01, 0x01, 0x01)),
            crc_valid=False,
        )
    )
    assert r.sm_gate is not None
    assert r.sm_gate.may_advance_application_sm is False
    assert session.snapshot.pending is None


def test_ambiguous_trans01_without_direction() -> None:
    res = resolve_trans01(
        direction=MessageDirection.UNKNOWN,
        confidence=DirectionConfidence.LOW,
        payload=bytes((0x01, 0x01, 0x00)),
    )
    assert res is not None
    assert res.kind is Trans01Kind.AMBIGUOUS
