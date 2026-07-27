"""Tests for CD2 + CD1 RESET combined single-shot write."""

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
from intelipump_fdc.protocol.cd2_reset import (
    build_cd2_reset_block,
    build_cd2_reset_candidate_frame,
)
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_poll
from intelipump_fdc.real_wayne_price.cd2_reset_session import Cd2ResetWriteSession
from intelipump_fdc.real_wayne_price.guards import (
    Cd2ResetWriteConfirmations,
    Cd2ResetWriteParams,
    validate_cd2_reset_write_params,
)
from intelipump_fdc.real_wayne_price.states import Cd2ResetWriteState
from intelipump_fdc.real_wayne_price.status_decode import (
    DecodedStatusSnapshot,
    StatusPreconditionError,
    validate_cd2_reset_preconditions,
)

_STATUS_FILLING_IN = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 00 00 00"
    "01 01 05"
)
_STATUS_FILLING_OUT = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 00 00 10"
    "01 01 05"
)
_STATUS_RESET = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 00 00 10"
    "01 01 01"
)


def _frame(payload: bytes, wire: int = 0x50, seq: int = 1) -> bytes:
    return build_data_frame(wire, seq, payload)


@dataclass
class FakeCd2Transport:
    device: str = "/tmp/fake-cd2-reset"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    active_write_count: int = 0
    cd2_reset_write_count: int = 0
    _open: bool = False
    _chunks: list[bytes] = field(default_factory=list)
    _seq: int = 0
    _approved_active_frame: bytes | None = None
    _active_writes_remaining: int = 0
    _approved_kind: ActiveFrameKind | None = None
    _poll_n: int = 0
    expected_ack: bytes = bytes.fromhex("50 c4 fa")
    initial_status: bytes = _STATUS_FILLING_OUT
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
            if kind is ActiveFrameKind.CD2_AND_CD1_RESET:
                self.cd2_reset_write_count += 1
            self.written.append(data)
            self.write_count += 1
            self._chunks.append(self.expected_ack)
            return len(data)

        self.write_count += 1
        self.written.append(data)
        self._poll_n += 1
        if self._poll_n == 1:
            self._chunks.append(_frame(self.initial_status))
        else:
            self._chunks.append(_frame(self.after_status, seq=2))
        return len(data)


def _confirms(**overrides: bool) -> Cd2ResetWriteConfirmations:
    base = dict(
        owned_lab_pump=True,
        technician_present=True,
        no_product_connected=True,
        motor_isolated=True,
        valves_isolated=True,
        emergency_isolation_ready=True,
        authorization_disabled=True,
        single_write_plan_reviewed=True,
        logical_nozzle_mapping_confirmed=True,
        nozzle_out_observed=True,
        execute_cd2_and_cd1_reset=True,
        post_write_status_verification_required=True,
        understand_transmits_to_owned_lab_pump=True,
    )
    base.update(overrides)
    return Cd2ResetWriteConfirmations(**base)


def test_cd2_reset_block_payload_and_classify() -> None:
    block = build_cd2_reset_block([1, 2])
    assert block.payload_hex == "02 02 01 02 01 01 05"
    frame, crc, ack = build_cd2_reset_candidate_frame(
        logical_address=1, sequence=4, block=block
    )
    assert classify_active_data_frame(frame) is ActiveFrameKind.CD2_AND_CD1_RESET
    assert frame[1] == 0x34
    assert ack.hex(" ") == "50 c4 fa"
    assert crc == 0xED70
    assert frame.hex(" ") == "50 34 02 02 01 02 01 01 05 70 ed 03 fa"


def test_cd2_reset_requires_nozzle_out() -> None:
    ok = DecodedStatusSnapshot(
        wire_address=0x50,
        crc_valid=True,
        dc1_code=5,
        dc1_name="FILLING_COMPLETED",
        nozzle_out=True,
    )
    validate_cd2_reset_preconditions(ok, expected_wire_address=0x50)
    bad = DecodedStatusSnapshot(
        wire_address=0x50,
        crc_valid=True,
        dc1_code=5,
        dc1_name="FILLING_COMPLETED",
        nozzle_out=False,
    )
    with pytest.raises(StatusPreconditionError) as ei:
        validate_cd2_reset_preconditions(bad, expected_wire_address=0x50)
    assert "nozzle_not_OUT" in ei.value.reasons[0]


@pytest.mark.asyncio
async def test_cd2_reset_session_success(tmp_path: Path) -> None:
    transport = FakeCd2Transport()
    params = Cd2ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        allowed_nozzles=(1, 2),
        sequence=4,
        post_write_settle_ms=0,
    )
    result = await Cd2ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.state is Cd2ResetWriteState.RESET_VERIFIED
    assert transport.cd2_reset_write_count == 1
    active = [w for w in transport.written if not is_verified_status_poll(w)]
    assert len(active) == 1
    assert classify_active_data_frame(active[0]) is ActiveFrameKind.CD2_AND_CD1_RESET
    assert sum(1 for w in transport.written if w == build_poll(1)) == 2
    review = json.loads((tmp_path / "ev" / "cd2-reset-write-result.json").read_text())
    assert review["command"] == "CD2_AND_CD1_RESET"
    assert review["transmitted"] is True


@pytest.mark.asyncio
async def test_cd2_reset_refuses_nozzle_in(tmp_path: Path) -> None:
    transport = FakeCd2Transport(initial_status=_STATUS_FILLING_IN)
    params = Cd2ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
        allowed_nozzles=(1,),
        sequence=4,
        post_write_settle_ms=0,
    )
    result = await Cd2ResetWriteSession(transport, params).run()
    assert result.transmitted is False
    assert result.state is Cd2ResetWriteState.REFUSED
    assert any("nozzle_not_OUT" in r for r in result.summary["refusalReasons"])


def test_cd2_reset_missing_confirm_refused(tmp_path: Path) -> None:
    params = Cd2ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(execute_cd2_and_cd1_reset=False),
        allowed_nozzles=(1,),
        sequence=4,
    )
    with pytest.raises(PollBenchRefusedError):
        validate_cd2_reset_write_params(params)
