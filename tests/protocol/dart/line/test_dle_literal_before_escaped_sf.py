"""Regression: literal DLE then escaped SF (wire ``10 10 FA``) must not truncate.

Pump-5 field capture (2026-10-12) rejected a valid addr=1 DATA frame:

    50 3c 02 08 00 00 02 51 00 03 40 10 10 fa b8 03 fa

Assembler treated the embedded ``10 10 FA`` as terminating at the first SF
("final SF is DLE-escaped"), then resynced onto payload ``0x51``, while the
real terminator is the trailing unescaped ``03 FA``. parse_frame already
accepts the full frame (CRC valid); only stream assembly was wrong.
"""

from __future__ import annotations

import pytest

from intelipump_fdc.controller.rx_demux import AddressFrameDemux
from intelipump_fdc.protocol.dart.line.escaping import unescape_dle
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.protocol.dart.line.stream import FrameStreamAssembler

# Exact field RX chunk (addr=1 DATA with literal 0x10 then DLE-escaped 0xFA).
ADDR1_DATA = bytes.fromhex("50 3c 02 08 00 00 02 51 00 03 40 10 10 fa b8 03 fa")
ADDR2_EOT = bytes.fromhex("51 70 fa")


def test_field_frame_parse_frame_crc_valid() -> None:
    parsed = parse_frame(ADDR1_DATA)
    assert isinstance(parsed, DartLineFrame)
    assert parsed.address == 0x50
    assert parsed.crc_valid is True
    # Unescape keeps literal 0x10 and one 0xFA in the body before CRC/ETX.
    assert unescape_dle(ADDR1_DATA[:-1]) == bytes.fromhex(
        "50 3c 02 08 00 00 02 51 00 03 40 10 fa b8 03"
    )


def test_legacy_assembler_accepts_field_frame_as_one_chunk() -> None:
    events = LegacyIgemStreamAssembler().feed(ADDR1_DATA)
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    rejected = [e for e in events if e.kind is AssemblerEventKind.REJECTED]
    assert not rejected
    assert len(frames) == 1
    assert frames[0].raw == ADDR1_DATA
    assert frames[0].frame is not None
    assert frames[0].frame.crc_valid is True


@pytest.mark.parametrize("split_at", range(1, len(ADDR1_DATA)))
def test_legacy_assembler_field_frame_every_byte_boundary(split_at: int) -> None:
    asm = LegacyIgemStreamAssembler()
    events = asm.feed(ADDR1_DATA[:split_at]) + asm.feed(ADDR1_DATA[split_at:])
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    rejected = [e for e in events if e.kind is AssemblerEventKind.REJECTED]
    assert not rejected, f"split@{split_at}: {[e.message for e in rejected]}"
    assert len(frames) == 1
    assert frames[0].raw == ADDR1_DATA
    assert asm.pending_size == 0


def test_legacy_assembler_does_not_resync_onto_payload_0x51() -> None:
    """Prefix ending at embedded FA must not invent a short frame at payload 51."""
    # Truncated the way the buggy assembler used to cut.
    bad_prefix = bytes.fromhex("50 3c 02 08 00 00 02 51 00 03 40 10 10 fa")
    events = LegacyIgemStreamAssembler().feed(bad_prefix)
    # May REJECT the incomplete/bad candidate, but must not emit a FRAME for
    # a resynced ``51 …`` short frame from inside the body.
    for e in events:
        if e.kind is AssemblerEventKind.FRAME:
            assert e.raw[0] != 0x51 or e.raw == bad_prefix


def test_generic_stream_assembler_literal_dle_then_escaped_sf() -> None:
    events = FrameStreamAssembler().feed(ADDR1_DATA)
    frames = [e for e in events if e.kind.value == "FRAME"]
    assert len(frames) == 1
    assert frames[0].raw == ADDR1_DATA


def test_demux_addr1_frame_then_addr2_eot_fresh_timestamps() -> None:
    """After addr=1 DATA, addr=2 ``51 70 FA`` must not inherit pre-TX first-byte."""
    demux = AddressFrameDemux(wire_addresses=(0x50, 0x51))
    t0 = 1000.0
    demux.feed(ADDR1_DATA, capture_mono=t0)
    f1 = demux.get_nowait(0x50)
    assert f1 is not None
    assert f1.raw == ADDR1_DATA
    assert f1.first_byte_time == t0
    assert f1.last_byte_time == t0

    t1 = 1000.25  # after a later addr=2 TX
    demux.feed(ADDR2_EOT, capture_mono=t1)
    f2 = demux.get_nowait(0x51)
    assert f2 is not None
    assert f2.raw == ADDR2_EOT
    assert f2.first_byte_time == t1
    assert f2.last_byte_time == t1
    # Not stale relative to a TX completed just before t1.
    assert f2.first_byte_time >= t1 - 1e-9


def test_demux_pre_tx_frame_still_detectable_as_stale() -> None:
    """Genuine leftover replies before TX keep earlier capture times."""
    demux = AddressFrameDemux(wire_addresses=(0x50, 0x51))
    demux.feed(ADDR2_EOT, capture_mono=50.0)
    stamped = demux.get_nowait(0x51)
    assert stamped is not None
    tx_complete = 50.2
    assert stamped.first_byte_time < tx_complete
    assert stamped.last_byte_time < tx_complete
