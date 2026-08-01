"""Frame assembly: fragmentation, multi-frame reads, polls, ACKs, CRC."""

from __future__ import annotations

from datetime import UTC, datetime

from tools.passive_dart_capture.frame_assembler import PassiveFrameAssembler
from tools.passive_dart_capture.serial_reader import SerialChunk

from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame


def _chunk(data: bytes, *, seq_ns: int = 1) -> SerialChunk:
    return SerialChunk(
        data=data,
        capture_timestamp_utc=datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC),
        monotonic_timestamp_ns=seq_ns,
        serial_device="/dev/ttyUSB0",
        baud=9600,
        parity="ODD",
        stop_bits=1,
    )


def test_poll_recognition() -> None:
    asm = PassiveFrameAssembler()
    frames = asm.feed_chunk(_chunk(bytes.fromhex("50 20 FA")))
    assert len(frames) == 1
    assert frames[0].complete is True
    assert frames[0].frame_class == "POLL"
    assert frames[0].address_hex == "50"
    assert frames[0].control_hex == "20"


def test_short_ack_recognition() -> None:
    asm = PassiveFrameAssembler()
    out = []
    for raw in (
        bytes.fromhex("50 C0 FA"),
        bytes.fromhex("50 C1 FA"),
        bytes.fromhex("51 C0 FA"),
        bytes.fromhex("51 C1 FA"),
    ):
        out.extend(asm.feed_chunk(_chunk(raw)))
    assert all(f.frame_class == "SHORT_ACK" for f in out)
    assert [f.control_hex for f in out] == ["C0", "C1", "C0", "C1"]


def test_multiple_frames_one_read() -> None:
    asm = PassiveFrameAssembler()
    blob = bytes.fromhex("50 20 FA 51 20 FA 50 C0 FA")
    frames = asm.feed_chunk(_chunk(blob))
    assert len(frames) == 3
    assert [f.frame_class for f in frames] == ["POLL", "POLL", "SHORT_ACK"]


def test_fragmented_frames() -> None:
    asm = PassiveFrameAssembler()
    assert asm.feed_chunk(_chunk(bytes.fromhex("50"))) == []
    assert asm.feed_chunk(_chunk(bytes.fromhex("20"))) == []
    frames = asm.feed_chunk(_chunk(bytes.fromhex("FA")))
    assert len(frames) == 1
    assert frames[0].frame_class == "POLL"
    assert frames[0].raw == bytes.fromhex("50 20 FA")


def test_partial_final_frame_flush() -> None:
    asm = PassiveFrameAssembler()
    stamp_ns = 42
    assert asm.feed_chunk(_chunk(bytes.fromhex("50 30 01"), seq_ns=stamp_ns)) == []
    partial = asm.flush_partial(
        session_now=datetime(2026, 8, 1, tzinfo=UTC),
        mono_ns=99,
    )
    assert partial is not None
    assert partial.complete is False
    assert partial.frame_class == "PARTIAL_FRAME"
    assert partial.raw == bytes.fromhex("50 30 01")
    # Preserve chunk timestamps for the interrupted remnant (not only shutdown now).
    assert partial.first_byte_monotonic_ns == stamp_ns
    assert partial.last_byte_monotonic_ns == stamp_ns
    assert partial.direction == "UNKNOWN"
    assert partial.direction_inference_reason


def test_direction_never_claimed_without_reason() -> None:
    asm = PassiveFrameAssembler()
    data = build_data_frame(0x50, 0x0, bytes.fromhex("01 01 04"))
    blob = bytes.fromhex("50 20 FA 50 C0 FA") + data
    frames = asm.feed_chunk(_chunk(blob))
    assert len(frames) == 3
    for fr in frames:
        assert fr.direction in {"UNKNOWN", "INFERRED"}
        assert fr.direction_inference_reason
        if fr.direction == "INFERRED":
            assert fr.direction_confidence == "SHAPE_ONLY"


def test_valid_and_invalid_crc() -> None:
    asm = PassiveFrameAssembler()
    payload = bytes.fromhex("01 01 04")
    good = build_data_frame(0x50, 0x0, payload)
    bad = bytearray(build_data_frame(0x50, 0x1, payload))
    # Flip CRC-1 (third byte from end before SF: ... CRC1 CRC2 ETX SF)
    bad[-4] ^= 0xFF
    frames = asm.feed_chunk(_chunk(bytes(good) + bytes(bad)))
    assert len(frames) == 2
    assert frames[0].complete is True
    assert frames[0].crc_valid is True
    assert frames[0].frame_class == "DATA"
    assert frames[1].complete is True
    assert frames[1].crc_valid is False
    assert frames[1].frame_class == "DATA_CRC_INVALID"
