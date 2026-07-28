"""Tests for CD1 RETURN_STATUS then RESET (capture-matched path)."""

from __future__ import annotations

import asyncio
import json
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
from intelipump_fdc.protocol.cd1 import build_cd1_candidate_frame, build_cd1_command
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_poll
from intelipump_fdc.real_wayne_price.guards import (
    ReturnStatusResetWriteConfirmations,
    ReturnStatusResetWriteParams,
    validate_return_status_reset_write_params,
)
from intelipump_fdc.real_wayne_price.return_status_reset_session import (
    ReturnStatusResetWriteSession,
)
from intelipump_fdc.real_wayne_price.session_helpers import next_sequence_nibble
from intelipump_fdc.real_wayne_price.states import ReturnStatusResetWriteState

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

_RS_FRAME_ADDR1_SEQ0 = "50 30 01 01 00 9f 5c 03 fa"
_RESET_FRAME_ADDR1_SEQ1 = "50 31 01 01 05 5e a3 03 fa"


def _frame(payload: bytes, wire: int = 0x50, seq: int = 1) -> bytes:
    return build_data_frame(wire, seq, payload)


@dataclass
class FakeReturnStatusResetTransport:
    device: str = "/tmp/fake-rs-reset"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    active_write_count: int = 0
    cd1_return_status_write_count: int = 0
    cd1_reset_write_count: int = 0
    _open: bool = False
    _chunks: list[bytes] = field(default_factory=list)
    _seq: int = 0
    _approved_active_frame: bytes | None = None
    _active_writes_remaining: int = 0
    _approved_kind: ActiveFrameKind | None = None
    _poll_n: int = 0
    initial_status: bytes = _STATUS_FILLING_COMPLETE
    mid_status: bytes = _STATUS_FILLING_COMPLETE
    after_status: bytes = _STATUS_RESET

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

    async def get_chunk(self, timeout_s: float) -> SerialChunk | None:
        import time

        deadline = time.monotonic() + max(0.0, timeout_s)
        while time.monotonic() < deadline:
            if self._chunks:
                data = self._chunks.pop(0)
                self._seq += 1
                now = time.monotonic()
                return SerialChunk(
                    raw=data,
                    monotonic_ns=time.monotonic_ns(),
                    monotonic_s=now,
                    timestamp_utc=datetime.now(UTC).isoformat(),
                    read_sequence=self._seq,
                )
            await asyncio.sleep(0.002)
        return None

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
            wire = data[0]
            seq = data[1] & 0x0F
            expected_ack = bytes((wire, 0xC0 | seq, 0xFA))
            if kind is ActiveFrameKind.CD1_RETURN_STATUS:
                self.cd1_return_status_write_count += 1
            elif kind is ActiveFrameKind.CD1_RESET:
                self.cd1_reset_write_count += 1
            self.written.append(data)
            self.write_count += 1
            self._chunks.append(expected_ack)
            return len(data)

        self.write_count += 1
        self.written.append(data)
        self._poll_n += 1
        if self._poll_n == 1:
            self._chunks.append(_frame(self.initial_status))
        elif self._poll_n == 2:
            self._chunks.append(_frame(self.mid_status, seq=2))
        else:
            self._chunks.append(_frame(self.after_status, seq=3))
        return len(data)


def _confirms(**overrides: bool) -> ReturnStatusResetWriteConfirmations:
    base = dict(
        owned_lab_pump=True,
        technician_present=True,
        no_product_connected=True,
        motor_isolated=True,
        valves_isolated=True,
        emergency_isolation_ready=True,
        authorization_disabled=True,
        single_write_plan_reviewed=True,
        nozzle_out_observed=True,
        execute_cd1_return_status_and_reset=True,
        post_write_status_verification_required=True,
        understand_transmits_to_owned_lab_pump=True,
    )
    base.update(overrides)
    return ReturnStatusResetWriteConfirmations(**base)


def test_return_status_payload_and_classification() -> None:
    rs = build_cd1_command(PumpControlCommand.RETURN_STATUS)
    assert rs.payload_hex == "01 01 00"
    frame, _, _ = build_cd1_candidate_frame(
        logical_address=1, sequence=0, cd1=rs
    )
    assert frame.hex(" ") == _RS_FRAME_ADDR1_SEQ0
    assert classify_active_data_frame(frame) is ActiveFrameKind.CD1_RETURN_STATUS

    reset = build_cd1_command(PumpControlCommand.RESET)
    frame_r, _, _ = build_cd1_candidate_frame(
        logical_address=1, sequence=1, cd1=reset
    )
    assert frame_r.hex(" ") == _RESET_FRAME_ADDR1_SEQ1
    assert next_sequence_nibble(0) == 1


@pytest.mark.asyncio
async def test_return_status_reset_session_two_active_writes(tmp_path: Path) -> None:
    transport = FakeReturnStatusResetTransport()
    params = ReturnStatusResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        sequence=0,
        post_write_settle_ms=0,
    )
    result = await ReturnStatusResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.state is ReturnStatusResetWriteState.RESET_VERIFIED
    assert transport.cd1_return_status_write_count == 1
    assert transport.cd1_reset_write_count == 1
    assert transport.active_write_count == 2
    active = [w for w in transport.written if not is_verified_status_poll(w)]
    assert len(active) == 2
    assert active[0].hex(" ") == _RS_FRAME_ADDR1_SEQ0
    assert active[1].hex(" ") == _RESET_FRAME_ADDR1_SEQ1
    assert sum(1 for w in transport.written if w == build_poll(1)) >= 3
    review = json.loads(
        (tmp_path / "ev" / "cd1-return-status-reset-write-result.json").read_text()
    )
    assert review["command"] == "RETURN_STATUS_AND_RESET"
    assert review["priorStep"]["name"] == "RETURN_STATUS"
    assert review["resetSequence"] == 1


@pytest.mark.asyncio
async def test_return_status_reset_refuses_without_filling_complete(
    tmp_path: Path,
) -> None:
    transport = FakeReturnStatusResetTransport(initial_status=_STATUS_RESET)
    params = ReturnStatusResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        sequence=0,
        post_write_settle_ms=0,
    )
    result = await ReturnStatusResetWriteSession(transport, params).run()
    assert result.transmitted is False
    assert result.state is ReturnStatusResetWriteState.REFUSED


def test_missing_confirm_refused(tmp_path: Path) -> None:
    params = ReturnStatusResetWriteParams(
        port="/tmp/fake",
        address=2,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(execute_cd1_return_status_and_reset=False),
        sequence=5,
    )
    with pytest.raises(PollBenchRefusedError):
        validate_return_status_reset_write_params(params)
