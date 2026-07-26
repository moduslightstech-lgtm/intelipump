"""Deterministic tests for captured Wayne iGEM wire-address / poll / reassembly."""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest

from intelipump_fdc.bench_poll.cli import build_parser as poll_bench_parser
from intelipump_fdc.bench_poll.session import PollBenchSession, PollBenchSessionConfig
from intelipump_fdc.continuous_poll_bench.cli import build_parser as continuous_parser
from intelipump_fdc.continuous_poll_bench.session import (
    ContinuousPollSession,
    ContinuousPollSessionConfig,
)
from intelipump_fdc.protocol.dart.line.addressing import (
    INVALID_SYNTHETIC_POLL_LOGICAL_AS_WIRE,
    AddressMappingError,
    decode_wire_address,
    encode_wire_address,
)
from intelipump_fdc.protocol.dart.line.captured_classify import (
    CapturedFrameClass,
    PayloadSemantics,
    is_nozzle_lift_classification,
    sequence_correlation_note,
)
from intelipump_fdc.protocol.dart.line.frame_builder import FrameBuildError, build_poll
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.simulator.config import PumpConfig, SimulatorConfig
from intelipump_fdc.simulator.session import SimulatorSession

FIXTURE_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "dart" / "legacy_igem_epump"


def _hex_file(name: str) -> bytes:
    return bytes.fromhex((FIXTURE_DIR / name).read_text(encoding="utf-8").strip())


def test_encode_wire_address_1_and_2() -> None:
    assert encode_wire_address(1) == 0x50
    assert encode_wire_address(2) == 0x51


def test_unsupported_logical_addresses_rejected() -> None:
    for bad in (0, 3, 0x10, 255):
        with pytest.raises(AddressMappingError):
            encode_wire_address(bad)


def test_build_poll_matches_capture() -> None:
    assert build_poll(1) == bytes.fromhex("50 20 FA")
    assert build_poll(2) == bytes.fromhex("51 20 FA")
    assert build_poll(1) == _hex_file("poll_side1.hex")
    assert build_poll(2) == _hex_file("poll_side2.hex")


def test_build_poll_never_returns_01_20_fa() -> None:
    assert build_poll(1) != INVALID_SYNTHETIC_POLL_LOGICAL_AS_WIRE
    assert build_poll(2) != INVALID_SYNTHETIC_POLL_LOGICAL_AS_WIRE
    assert build_poll(1) != _hex_file("INVALID_SYNTHETIC_POLL_01_20_FA.hex")


def test_raw_wire_address_as_logical_rejected() -> None:
    with pytest.raises((AddressMappingError, FrameBuildError)):
        encode_wire_address(0x50)
    with pytest.raises(FrameBuildError):
        build_poll(0x50)
    with pytest.raises(FrameBuildError):
        build_poll(0x51)


def test_single_complete_short_frame() -> None:
    asm = LegacyIgemStreamAssembler()
    events = asm.feed(_hex_file("poll_side1.hex"))
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert len(frames) == 1
    assert frames[0].captured is not None
    assert frames[0].captured.classification is CapturedFrameClass.POLL
    assert frames[0].captured.wire_address == 0x50
    assert frames[0].captured.logical_address == 1


def test_multiple_frames_in_one_read() -> None:
    blob = (
        _hex_file("poll_side1.hex")
        + _hex_file("data_side1_seq0.hex")
        + _hex_file("seq_ctrl_side1_c0.hex")
    )
    events = LegacyIgemStreamAssembler().feed(blob)
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert len(frames) == 3
    assert frames[0].captured is not None
    assert frames[1].captured is not None
    assert frames[2].captured is not None
    assert frames[0].captured.classification is CapturedFrameClass.POLL
    assert frames[1].captured.classification is CapturedFrameClass.DATA_FRAME
    assert frames[2].captured.classification is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK


def test_frame_split_across_two_reads() -> None:
    raw = _hex_file("poll_side2.hex")
    asm = LegacyIgemStreamAssembler()
    assert asm.feed(raw[:2]) == []
    events = asm.feed(raw[2:])
    assert any(e.kind is AssemblerEventKind.FRAME for e in events)


def test_large_frame_split_across_many_reads() -> None:
    raw = _hex_file("data_side1_fragmented_embedded_fa.hex")
    asm = LegacyIgemStreamAssembler()
    events: list = []
    for i in range(0, len(raw), 3):
        events.extend(asm.feed(raw[i : i + 3]))
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert len(frames) == 1
    assert frames[0].raw == raw


def test_payload_byte_0x13_not_treated_as_address() -> None:
    raw = _hex_file("data_side1_fragmented_embedded_fa.hex")
    assert 0x13 in raw
    asm = LegacyIgemStreamAssembler()
    # Feed up through first 0x13 while incomplete — must stay in DATA body.
    idx = raw.index(0x13)
    assert asm.feed(raw[: idx + 1]) == []
    assert asm.pending_size == idx + 1
    events = asm.feed(raw[idx + 1 :])
    assert len([e for e in events if e.kind is AssemblerEventKind.FRAME]) == 1


def test_embedded_fa_does_not_end_data_early() -> None:
    raw = _hex_file("data_side1_fragmented_embedded_fa.hex")
    assert bytes.fromhex("10 FA") in raw
    events = LegacyIgemStreamAssembler().feed(raw)
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert len(frames) == 1
    assert frames[0].raw.endswith(bytes.fromhex("03 FA"))


def test_03_fa_ends_longer_frame() -> None:
    raw = _hex_file("data_side2_seq0.hex")
    assert raw.endswith(bytes.fromhex("03 FA"))
    events = LegacyIgemStreamAssembler().feed(raw)
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert len(frames) == 1
    assert frames[0].captured is not None
    assert frames[0].captured.classification is CapturedFrameClass.DATA_FRAME


@pytest.mark.parametrize(
    "name",
    ["seq_ctrl_side1_c1.hex", "seq_ctrl_side2_c1.hex"],
)
def test_c1_not_classified_as_nozzle_lift(name: str) -> None:
    events = LegacyIgemStreamAssembler().feed(_hex_file(name))
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert len(frames) == 1
    captured = frames[0].captured
    assert captured is not None
    assert captured.classification is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
    assert captured.possible_acknowledgement is True
    assert not is_nozzle_lift_classification(captured.classification)
    assert "nozzle" not in captured.classification.value.lower()


def test_3n_to_cn_sequence_nibble_correlation() -> None:
    note = sequence_correlation_note(0x31, 0xC1)
    assert note is not None
    assert "sequenceNibble=1" in note
    assert "possibleAcknowledgement=true" in note
    assert "nozzle" not in note.lower()


def test_unknown_payload_semantics() -> None:
    events = LegacyIgemStreamAssembler().feed(_hex_file("data_side1_seq0.hex"))
    frame = next(e for e in events if e.kind is AssemblerEventKind.FRAME)
    assert frame.captured is not None
    assert frame.captured.payload_semantics is PayloadSemantics.UNKNOWN_PAYLOAD


def test_no_ascii_decode_path_in_line_modules() -> None:
    root = (
        Path(__file__).resolve().parents[3]
        / "src"
        / "intelipump_fdc"
        / "protocol"
        / "dart"
        / "line"
    )
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "decode"
            ):
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and arg.value == "ascii":
                        pytest.fail(f"ASCII decode found in {path}")


def test_no_unproven_automatic_bcd_in_line_modules() -> None:
    root = (
        Path(__file__).resolve().parents[3]
        / "src"
        / "intelipump_fdc"
        / "protocol"
        / "dart"
        / "line"
    )
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "decode_packed_bcd" not in text
        assert "decode_scaled_bcd" not in text


def test_raw_bytes_preserved_before_parsing() -> None:
    raw = _hex_file("data_side1_seq1.hex")
    events = LegacyIgemStreamAssembler().feed(raw)
    frame = next(e for e in events if e.kind is AssemblerEventKind.FRAME)
    assert frame.raw == raw
    assert frame.frame is not None
    assert frame.frame.raw_frame == raw


def test_buffer_overflow_bounded_and_evidenced() -> None:
    asm = LegacyIgemStreamAssembler(max_buffer=8)
    # Start a DATA frame then dump noise without SF.
    events = asm.feed(bytes.fromhex("50 30") + bytes(range(20)))
    assert any(e.kind is AssemblerEventKind.OVERFLOW for e in events)
    overflow = next(e for e in events if e.kind is AssemblerEventKind.OVERFLOW)
    assert overflow.raw


def test_partial_frame_timeout_preserves_diagnostics() -> None:
    asm = LegacyIgemStreamAssembler()
    assert asm.feed(bytes.fromhex("50 30 01")) == []
    events = asm.expire_partial()
    assert len(events) == 1
    assert events[0].kind is AssemblerEventKind.PARTIAL
    assert events[0].captured is not None
    assert events[0].captured.classification is CapturedFrameClass.PARTIAL_FRAME
    assert events[0].raw == bytes.fromhex("50 30 01")


def test_simulator_responds_to_50_20_fa() -> None:
    session = SimulatorSession(
        SimulatorConfig(pumps=(PumpConfig(pump_id="fp-1", dart_address=1),))
    )
    session.get_pump(1).enable_communication()
    result = session.receive(bytes.fromhex("50 20 FA"))
    assert result.responses
    assert result.responses[0] == bytes.fromhex("50 70 FA")
    assert not result.faults


def test_simulator_responds_to_51_20_fa() -> None:
    session = SimulatorSession(
        SimulatorConfig(pumps=(PumpConfig(pump_id="fp-2", dart_address=2),))
    )
    session.get_pump(2).enable_communication()
    result = session.receive(bytes.fromhex("51 20 FA"))
    assert result.responses
    assert result.responses[0] == bytes.fromhex("51 70 FA")


def test_simulator_rejects_01_20_fa() -> None:
    session = SimulatorSession()
    result = session.receive(bytes.fromhex("01 20 FA"))
    assert result.responses == ()
    assert result.faults
    assert "01 20 FA" in result.faults[0].message or "0x01" in result.faults[0].message


@pytest.mark.asyncio
async def test_one_poll_simulator_bench_logical_1(tmp_path: Path) -> None:
    from dataclasses import dataclass, field

    @dataclass
    class Fake:
        written: list[bytes] = field(default_factory=list)
        write_count: int = 0
        chunks: list[bytes] = field(default_factory=list)
        _open: bool = False

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        async def read(self, n: int) -> bytes:
            if not self.chunks:
                await asyncio.sleep(0.01)
                return b""
            return self.chunks.pop(0)[:n]

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            self.written.append(data)
            self.chunks.append(bytes.fromhex("50 70 FA"))
            return len(data)

    transport = Fake()
    session = PollBenchSession(
        transport,  # type: ignore[arg-type]
        PollBenchSessionConfig(
            port="/tmp/x",
            address=1,
            baud=9600,
            max_polls=1,
            response_timeout_ms=200,
            evidence_jsonl=tmp_path / "a.jsonl",
            evidence_md=tmp_path / "a.md",
            target_type="SIMULATOR",
            simulator_validation=True,
        ),
    )
    summary = await session.run()
    assert transport.written[0] == bytes.fromhex("50 20 FA")
    assert summary["result"] == "POLL_BENCH_PASS"
    assert summary["logicalAddress"] == 1
    assert summary["wireAddress"] == 0x50
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0


@pytest.mark.asyncio
async def test_one_poll_simulator_bench_logical_2(tmp_path: Path) -> None:
    from dataclasses import dataclass, field

    @dataclass
    class Fake:
        written: list[bytes] = field(default_factory=list)
        write_count: int = 0
        chunks: list[bytes] = field(default_factory=list)
        _open: bool = False

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        async def read(self, n: int) -> bytes:
            if not self.chunks:
                await asyncio.sleep(0.01)
                return b""
            return self.chunks.pop(0)[:n]

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            self.written.append(data)
            self.chunks.append(bytes.fromhex("51 70 FA"))
            return len(data)

    transport = Fake()
    session = PollBenchSession(
        transport,  # type: ignore[arg-type]
        PollBenchSessionConfig(
            port="/tmp/x",
            address=2,
            baud=9600,
            max_polls=1,
            response_timeout_ms=200,
            evidence_jsonl=tmp_path / "b.jsonl",
            evidence_md=tmp_path / "b.md",
            target_type="SIMULATOR",
            simulator_validation=True,
        ),
    )
    summary = await session.run()
    assert transport.written[0] == bytes.fromhex("51 20 FA")
    assert summary["result"] == "POLL_BENCH_PASS"
    assert summary["logicalAddress"] == 2
    assert summary["wireAddress"] == 0x51


@pytest.mark.asyncio
@pytest.mark.parametrize("logical,wire_eot", [(1, "50 70 FA"), (2, "51 70 FA")])
async def test_continuous_simulator_polling_both_sides(
    tmp_path: Path, logical: int, wire_eot: str
) -> None:
    from dataclasses import dataclass, field

    @dataclass
    class Fake:
        written: list[bytes] = field(default_factory=list)
        write_count: int = 0
        chunks: list[bytes] = field(default_factory=list)
        _open: bool = False
        eot: bytes = bytes.fromhex(wire_eot)

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        async def read(self, n: int) -> bytes:
            if not self.chunks:
                await asyncio.sleep(0.005)
                return b""
            return self.chunks.pop(0)[:n]

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            self.written.append(data)
            self.chunks.append(self.eot)
            return len(data)

    transport = Fake()
    session = ContinuousPollSession(
        transport,  # type: ignore[arg-type]
        ContinuousPollSessionConfig(
            port="/tmp/x",
            address=logical,
            baud=9600,
            duration_seconds=1.0,
            poll_interval_ms=100,
            response_timeout_ms=40,
            evidence_jsonl=tmp_path / f"c{logical}.jsonl",
            evidence_md=tmp_path / f"c{logical}.md",
            max_writes=20,
            target_type="SIMULATOR",
            simulator_validation=True,
        ),
    )
    summary = await session.run()
    expected = build_poll(logical)
    assert all(w == expected for w in transport.written)
    assert summary["validResponses"] >= 1
    assert summary["logicalAddress"] == logical
    assert summary["wireAddress"] == encode_wire_address(logical)
    assert summary["commandQueueCreated"] is False
    assert summary["authorizationObjectsCreated"] == 0


def test_evidence_records_logical_and_wire(tmp_path: Path) -> None:
    import json

    async def _run() -> None:
        from dataclasses import dataclass, field

        @dataclass
        class Fake:
            written: list[bytes] = field(default_factory=list)
            write_count: int = 0
            chunks: list[bytes] = field(default_factory=list)
            _open: bool = False

            @property
            def is_open(self) -> bool:
                return self._open

            async def open(self) -> None:
                self._open = True

            async def close(self) -> None:
                self._open = False

            async def read(self, n: int) -> bytes:
                if not self.chunks:
                    await asyncio.sleep(0.01)
                    return b""
                return self.chunks.pop(0)[:n]

            async def write(self, data: bytes) -> int:
                self.write_count += 1
                self.written.append(data)
                self.chunks.append(bytes.fromhex("50 70 FA"))
                return len(data)

        session = PollBenchSession(
            Fake(),  # type: ignore[arg-type]
            PollBenchSessionConfig(
                port="/tmp/x",
                address=1,
                baud=9600,
                max_polls=1,
                response_timeout_ms=200,
                evidence_jsonl=tmp_path / "e.jsonl",
                evidence_md=tmp_path / "e.md",
                target_type="SIMULATOR",
                simulator_validation=True,
            ),
        )
        await session.run()

    asyncio.run(_run())
    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    assert any(r.get("logicalAddress") == 1 and r.get("wireAddress") == 0x50 for r in records)


def test_no_raw_byte_cli_options() -> None:
    for parser in (poll_bench_parser(), continuous_parser()):
        opts = {
            opt
            for action in parser._actions
            for opt in (action.option_strings or [])
        }
        assert "--raw-hex" not in opts
        assert "--payload" not in opts
        assert "--wire-address" not in opts


def test_decode_wire_address_roundtrip() -> None:
    assert decode_wire_address(0x50) == 1
    assert decode_wire_address(0x51) == 2
