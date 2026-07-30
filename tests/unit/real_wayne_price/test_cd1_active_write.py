"""Tests for single-shot CD1 RESET and AUTHORIZE writes."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from intelipump_fdc.bench_poll.guards import PollBenchRefusedError
from intelipump_fdc.bench_poll.serial_reader import SerialChunk
from intelipump_fdc.controller.price_safety import (
    ActiveFrameKind,
    RealWayneActiveCommandRefusedError,
    assert_real_wayne_write_allowed,
    classify_active_data_frame,
    is_verified_status_poll,
)
from intelipump_fdc.protocol.cd1 import (
    CD1Error,
    build_cd1_candidate_frame,
    build_cd1_command,
)
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.control import extract_sequence
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_poll
from intelipump_fdc.protocol.sequence import WayneSequenceManager
from intelipump_fdc.real_wayne_price.authorize_session import AuthorizeWriteSession
from intelipump_fdc.real_wayne_price.guards import (
    AuthorizeWriteConfirmations,
    AuthorizeWriteParams,
    ResetWriteConfirmations,
    ResetWriteParams,
    validate_authorize_write_params,
    validate_reset_write_params,
)
from intelipump_fdc.real_wayne_price.reset_session import ResetWriteSession
from intelipump_fdc.real_wayne_price.session_helpers import wait_for_ack_frame
from intelipump_fdc.real_wayne_price.states import (
    AuthorizeWriteState,
    ResetDiagnosticResult,
    ResetWriteState,
)

# Documented nozzle OUT (NOZIO bit 0x10 set): logical nozzle 1 + OUT = 0x11.
_STATUS_FILLING_COMPLETE = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 11"
    "01 01 05"
)
_STATUS_RESET = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 11"
    "01 01 01"
)
_STATUS_AUTHORIZED = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 11"
    "01 01 02"
)

_RESET_FRAME_SEQ0 = "50 30 01 01 05 5f 5f 03 fa"
_RESET_FRAME_SEQ1 = "50 31 01 01 05 5e a3 03 fa"
_RESET_FRAME_SEQ2 = "50 32 01 01 05 5e e7 03 fa"
_RESET_FRAME = _RESET_FRAME_SEQ0
_AUTHORIZE_FRAME = "50 30 01 01 06 1f 5e 03 fa"
_AUTHORIZE_PAYLOAD = "01 01 06"


def _frame(payload: bytes, wire: int = 0x50, seq: int = 1) -> bytes:
    return build_data_frame(wire, seq, payload)


@dataclass
class _QueuedChunk:
    raw: bytes
    monotonic_s: float | None = None


@dataclass
class FakeActiveTransport:
    device: str = "/tmp/fake-active"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    active_write_count: int = 0
    cd1_reset_write_count: int = 0
    cd1_authorize_write_count: int = 0
    _open: bool = False
    _chunks: list[_QueuedChunk] = field(default_factory=list)
    _seq: int = 0
    _approved_active_frame: bytes | None = None
    _active_writes_remaining: int = 0
    _approved_kind: ActiveFrameKind | None = None
    _poll_n: int = 0
    expected_ack: bytes = bytes.fromhex("50 c0 fa")
    initial_status: bytes = _STATUS_FILLING_COMPLETE
    after_status: bytes = _STATUS_RESET
    # Injected after first status response; drained before RESET TX.
    stale_before_active: list[bytes] = field(default_factory=list)
    # When True, do not auto-queue ACK on active write (manual control).
    suppress_auto_ack: bool = False
    last_active_write_monotonic_s: float | None = None

    @property
    def is_open(self) -> bool:
        return self._open

    async def open(self) -> None:
        self._open = True

    async def close(self) -> None:
        self._open = False

    async def flush(self) -> None:
        return None

    def serial_config_snapshot(self) -> dict[str, object]:
        return {"baudrate": 9600, "timeout": 0.015, "open": True}

    def authorize_single_active_write(
        self, frame: bytes, *, kind: ActiveFrameKind
    ) -> None:
        classified = classify_active_data_frame(frame)
        if classified is None or classified is not kind:
            raise RealWayneActiveCommandRefusedError(f"not {kind.value}")
        self._approved_active_frame = bytes(frame)
        self._active_writes_remaining = 1
        self._approved_kind = kind

    def clear_active_write_authorization(self) -> None:
        self._approved_active_frame = None
        self._active_writes_remaining = 0
        self._approved_kind = None

    def enqueue(self, data: bytes, *, monotonic_s: float | None = None) -> None:
        self._chunks.append(_QueuedChunk(raw=data, monotonic_s=monotonic_s))

    async def get_chunk(self, timeout_s: float) -> SerialChunk | None:
        if timeout_s <= 0:
            if not self._chunks:
                return None
            return self._pop_chunk()
        deadline = time.monotonic() + timeout_s
        while True:
            if self._chunks:
                return self._pop_chunk()
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(0.002)

    def _pop_chunk(self) -> SerialChunk:
        item = self._chunks.pop(0)
        self._seq += 1
        mono = item.monotonic_s if item.monotonic_s is not None else time.monotonic()
        return SerialChunk(
            raw=item.raw,
            monotonic_ns=int(mono * 1_000_000_000),
            monotonic_s=mono,
            timestamp_utc=datetime.now(UTC).isoformat(),
            read_sequence=self._seq,
        )

    async def write(self, data: bytes) -> int:
        assert_real_wayne_write_allowed(
            data,
            approved_active_frame=self._approved_active_frame,
            active_writes_remaining=self._active_writes_remaining,
        )
        is_active = (
            self._approved_active_frame is not None
            and data == self._approved_active_frame
            and self._active_writes_remaining > 0
        )
        if is_active:
            kind = self._approved_kind
            self._active_writes_remaining -= 1
            self._approved_active_frame = None
            self._approved_kind = None
            self.active_write_count += 1
            if kind is ActiveFrameKind.CD1_RESET:
                self.cd1_reset_write_count += 1
            elif kind is ActiveFrameKind.CD1_AUTHORIZE:
                self.cd1_authorize_write_count += 1
            self.written.append(data)
            self.write_count += 1
            self.last_active_write_monotonic_s = time.monotonic()
            if not self.suppress_auto_ack:
                # ACK arrives after the write timestamp.
                self.enqueue(
                    self.expected_ack,
                    monotonic_s=self.last_active_write_monotonic_s + 0.001,
                )
            return len(data)

        self.write_count += 1
        self.written.append(data)
        self._poll_n += 1
        if self._poll_n == 1:
            self.enqueue(_frame(self.initial_status))
            for stale in self.stale_before_active:
                # Capture-time before the active write (stale in buffer).
                self.enqueue(stale, monotonic_s=time.monotonic() - 1.0)
        else:
            self.enqueue(_frame(self.after_status, seq=2))
        return len(data)


def _reset_confirms(**overrides: bool) -> ResetWriteConfirmations:
    base = dict(
        owned_lab_pump=True,
        technician_present=True,
        no_product_connected=True,
        motor_isolated=True,
        valves_isolated=True,
        emergency_isolation_ready=True,
        authorization_disabled=True,
        single_write_plan_reviewed=True,
        execute_cd1_reset=True,
        post_write_status_verification_required=True,
        understand_transmits_to_owned_lab_pump=True,
    )
    base.update(overrides)
    return ResetWriteConfirmations(**base)


def _auth_confirms(**overrides: bool) -> AuthorizeWriteConfirmations:
    base = dict(
        owned_lab_pump=True,
        technician_present=True,
        no_product_connected=True,
        motor_isolated=True,
        valves_isolated=True,
        emergency_isolation_ready=True,
        single_write_plan_reviewed=True,
        execute_cd1_authorize=True,
        post_write_status_verification_required=True,
        understand_transmits_to_owned_lab_pump=True,
        understand_authorize_enables_live_delivery_ui=True,
    )
    base.update(overrides)
    return AuthorizeWriteConfirmations(**base)


def test_cd1_reset_and_authorize_payloads() -> None:
    reset = build_cd1_command(PumpControlCommand.RESET)
    assert reset.payload_hex == "01 01 05"
    frame, crc, ack = build_cd1_candidate_frame(
        logical_address=1, sequence=0, cd1=reset
    )
    assert frame.hex(" ") == _RESET_FRAME
    assert crc == 0x5F5F
    assert ack.hex(" ") == "50 c0 fa"

    auth = build_cd1_command(PumpControlCommand.AUTHORIZE)
    assert auth.payload_hex == "01 01 06"
    frame_a, _, _ = build_cd1_candidate_frame(
        logical_address=1, sequence=0, cd1=auth
    )
    assert frame_a.hex(" ") == _AUTHORIZE_FRAME


def test_cd1_rejects_other_commands() -> None:
    with pytest.raises(CD1Error):
        build_cd1_command(PumpControlCommand.STOP)


def test_sequence_stale_hint_after_ack() -> None:
    from intelipump_fdc.real_wayne_price.session_helpers import (
        sequence_stale_status_hint,
    )

    hint = sequence_stale_status_hint(
        sequence=0,
        ack_outcome="ACK_MATCH",
        refusal_reasons=["post_reset_RESET_not_reached", "last_dc1=FILLING_COMPLETED/5"],
    )
    assert hint is not None
    assert "--sequence 1" in hint
    assert "0x30→0x31" in hint
    assert "nozzle OUT" in hint
    assert (
        sequence_stale_status_hint(
            sequence=0,
            ack_outcome="ACK_TIMEOUT",
            refusal_reasons=["post_reset_RESET_not_reached"],
        )
        is None
    )


def test_cd1_reset_sequence_0_1_2_wire_difference() -> None:
    """Offline candidate frames: sequence is DATA CTRL low nibble + CRC."""
    reset = build_cd1_command(PumpControlCommand.RESET)
    expected = {
        0: (_RESET_FRAME_SEQ0, "50 c0 fa"),
        1: (_RESET_FRAME_SEQ1, "50 c1 fa"),
        2: (_RESET_FRAME_SEQ2, "50 c2 fa"),
    }
    frames: dict[int, bytes] = {}
    for seq, (frame_hex, ack_hex) in expected.items():
        frame, _crc, ack = build_cd1_candidate_frame(
            logical_address=1, sequence=seq, cd1=reset
        )
        frames[seq] = frame
        assert frame.hex(" ") == frame_hex
        assert ack.hex(" ") == ack_hex
        assert extract_sequence(frame[1]) == seq
        assert frame[1] == WayneSequenceManager.message_byte(seq)
        assert ack[1] == WayneSequenceManager.expected_ack_byte(seq)

    # Fail if sequence 0 and 1 produce identical candidate frames.
    assert frames[0] != frames[1]
    assert frames[0][1] == 0x30
    assert frames[1][1] == 0x31
    # Application payload is identical; only CTRL (+CRC) differ.
    assert frames[0][2:5] == frames[1][2:5] == bytes.fromhex("01 01 05")
    # Byte-by-byte diff for seq0 vs seq1.
    diffs = [
        (i, frames[0][i], frames[1][i])
        for i in range(len(frames[0]))
        if frames[0][i] != frames[1][i]
    ]
    assert diffs == [(1, 0x30, 0x31), (5, 0x5F, 0x5E), (6, 0x5F, 0xA3)]


@pytest.mark.asyncio
async def test_wait_for_ack_rejects_stale_before_write() -> None:
    transport = FakeActiveTransport(suppress_auto_ack=True)
    await transport.open()
    expected = bytes.fromhex("50 c0 fa")
    # Stale ACK already in the buffer with capture time before the write.
    transport.enqueue(expected, monotonic_s=time.monotonic() - 5.0)
    write_mono = time.monotonic()
    result = await wait_for_ack_frame(
        transport,
        expected_ack=expected,
        timeout_ms=50,
        not_before_monotonic_s=write_mono,
    )
    assert result.matched is False
    assert result.outcome == "ACK_TIMEOUT"
    assert result.stale_rejected_hex == [expected.hex(" ")]
    assert result.observed_hex == []


@pytest.mark.asyncio
async def test_wait_for_ack_matches_after_write_with_latency() -> None:
    transport = FakeActiveTransport(suppress_auto_ack=True)
    await transport.open()
    expected = bytes.fromhex("50 c0 fa")
    write_mono = time.monotonic()
    transport.enqueue(expected, monotonic_s=write_mono + 0.012)
    result = await wait_for_ack_frame(
        transport,
        expected_ack=expected,
        timeout_ms=200,
        not_before_monotonic_s=write_mono,
    )
    assert result.matched is True
    assert result.outcome == "ACK_MATCH"
    assert result.ack_monotonic_s is not None
    assert result.ack_monotonic_s >= write_mono
    assert result.ack_latency_ms is not None
    assert 10.0 <= result.ack_latency_ms <= 20.0


@pytest.mark.asyncio
async def test_reset_session_transmits_once(tmp_path: Path) -> None:
    transport = FakeActiveTransport()
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(),
        post_write_settle_ms=0,
        post_reset_observation_seconds=0,
    )
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.state is ResetWriteState.RESET_VERIFIED
    assert transport.cd1_reset_write_count == 1
    active = [w for w in transport.written if not is_verified_status_poll(w)]
    assert len(active) == 1
    assert active[0].hex(" ") == _RESET_FRAME
    assert sum(1 for w in transport.written if w == build_poll(1)) == 2
    review = json.loads((tmp_path / "ev" / "cd1-reset-write-result.json").read_text())
    assert review["command"] == "RESET"
    assert review["transmitted"] is True
    assert result.summary["diagnosticResult"] == (
        "RESET_DIAGNOSTIC_ACK_STATE_CHANGED"
    )


@pytest.mark.asyncio
async def test_reset_refuses_without_filling_complete(tmp_path: Path) -> None:
    transport = FakeActiveTransport(initial_status=_STATUS_RESET)
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(),
        post_write_settle_ms=0,
        post_reset_observation_seconds=0,
    )
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is False
    assert result.state is ResetWriteState.REFUSED


@pytest.mark.asyncio
async def test_authorize_session_transmits_once(tmp_path: Path) -> None:
    transport = FakeActiveTransport(
        initial_status=_STATUS_RESET,
        after_status=_STATUS_AUTHORIZED,
    )
    params = AuthorizeWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_auth_confirms(),
        post_write_settle_ms=0,
    )
    result = await AuthorizeWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.state is AuthorizeWriteState.AUTHORIZED_VERIFIED
    assert transport.cd1_authorize_write_count == 1
    active = [w for w in transport.written if not is_verified_status_poll(w)]
    assert active[0].hex(" ") == _AUTHORIZE_FRAME
    review = json.loads(
        (tmp_path / "ev" / "cd1-authorize-write-result.json").read_text()
    )
    assert review["command"] == "AUTHORIZE"


def test_authorize_missing_confirm_refused(tmp_path: Path) -> None:
    params = AuthorizeWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_auth_confirms(execute_cd1_authorize=False),
    )
    with pytest.raises(PollBenchRefusedError):
        validate_authorize_write_params(params)


def test_reset_missing_confirm_refused(tmp_path: Path) -> None:
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(execute_cd1_reset=False),
    )
    with pytest.raises(PollBenchRefusedError):
        validate_reset_write_params(params)


@pytest.mark.asyncio
async def test_reset_evidence_records_encoded_sequence_and_latency(
    tmp_path: Path,
) -> None:
    transport = FakeActiveTransport()
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(),
        sequence=0,
        post_write_settle_ms=0,
        post_reset_observation_seconds=0,
        ack_timeout_ms=200,
    )
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.summary["encodedSequence"] == 0
    assert result.summary["controlByteHex"] == "30"
    assert result.summary["candidateFrameHex"] == _RESET_FRAME_SEQ0
    assert result.summary["ackOutcome"] == "ACK_MATCH"
    assert result.summary["writeMonotonicS"] is not None
    assert result.summary["ackMonotonicS"] is not None
    assert result.summary["ackLatencyMs"] is not None
    assert result.summary["ackLatencyMs"] >= 0.0
    record = json.loads((tmp_path / "ev" / "cd1-reset-write.jsonl").read_text())
    assert record["encodedSequence"] == 0
    assert record["controlByteHex"] == "30"
    assert record["ackLatencyMs"] is not None


@pytest.mark.asyncio
async def test_reset_drains_stale_ack_then_matches_post_write(
    tmp_path: Path,
) -> None:
    stale = bytes.fromhex("50 c0 fa")
    transport = FakeActiveTransport(stale_before_active=[stale])
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(),
        post_write_settle_ms=0,
        post_reset_observation_seconds=0,
    )
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.summary["ackOutcome"] == "ACK_MATCH"
    assert any("50 c0 fa" in x for x in result.summary["preWriteDrainedHex"])
    assert result.summary["ackLatencyMs"] is not None
    assert transport.cd1_reset_write_count == 1


@pytest.mark.asyncio
async def test_reset_ack_before_write_cannot_match(tmp_path: Path) -> None:
    """Stale pre-write ACK is drained; with no post-write ACK → ACK_TIMEOUT."""
    transport = FakeActiveTransport(suppress_auto_ack=True)

    async def write_controlled(data: bytes) -> int:
        assert_real_wayne_write_allowed(
            data,
            approved_active_frame=transport._approved_active_frame,
            active_writes_remaining=transport._active_writes_remaining,
        )
        is_active = (
            transport._approved_active_frame is not None
            and data == transport._approved_active_frame
            and transport._active_writes_remaining > 0
        )
        transport.written.append(data)
        transport.write_count += 1
        if is_active:
            transport._active_writes_remaining -= 1
            transport._approved_active_frame = None
            transport._approved_kind = None
            transport.active_write_count += 1
            transport.cd1_reset_write_count += 1
            transport.last_active_write_monotonic_s = time.monotonic()
            # Intentionally do not enqueue a fresh ACK.
            return len(data)
        transport._poll_n += 1
        if transport._poll_n == 1:
            transport.enqueue(_frame(transport.initial_status))
            # Stale ACK waiting in RX — drain should remove it.
            transport.enqueue(
                bytes.fromhex("50 c0 fa"),
                monotonic_s=time.monotonic() - 1.0,
            )
        else:
            transport.enqueue(_frame(transport.after_status, seq=2))
        return len(data)

    transport.write = write_controlled  # type: ignore[method-assign]
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(
            post_write_status_verification_required=False,
        ),
        post_write_settle_ms=0,
        post_reset_observation_seconds=0,
        ack_timeout_ms=80,
        post_write_max_status_polls=1,
    )
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert transport.cd1_reset_write_count == 1
    assert result.summary["ackOutcome"] == "ACK_TIMEOUT"
    assert any("50 c0 fa" in x for x in result.summary["preWriteDrainedHex"])


@pytest.mark.asyncio
async def test_reset_unchanged_dc1_is_ack_state_unchanged(tmp_path: Path) -> None:
    transport = FakeActiveTransport(
        after_status=_STATUS_FILLING_COMPLETE,
    )
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(
            post_write_status_verification_required=False,
        ),
        post_write_settle_ms=0,
        post_reset_observation_seconds=0,
        post_write_max_status_polls=2,
    )
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.summary["ackOutcome"] == "ACK_MATCH"
    assert result.summary["diagnosticResult"] == (
        ResetDiagnosticResult.RESET_DIAGNOSTIC_ACK_STATE_UNCHANGED.value
    )
    assert transport.cd1_reset_write_count == 1


@pytest.mark.asyncio
async def test_reset_workflow_cannot_transmit_authorize(tmp_path: Path) -> None:
    transport = FakeActiveTransport()
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(),
        post_write_settle_ms=0,
        post_reset_observation_seconds=0,
    )
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert transport.cd1_authorize_write_count == 0
    for frame in transport.written:
        if is_verified_status_poll(frame):
            continue
        assert _AUTHORIZE_PAYLOAD not in frame.hex(" ")
        assert classify_active_data_frame(frame) is ActiveFrameKind.CD1_RESET
    assert transport.cd1_reset_write_count == 1
    assert result.summary["activeWriteCount"] == 1
