"""Tests for gated CD101 request-totals single-shot path."""

from __future__ import annotations

import asyncio
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
)
from intelipump_fdc.protocol.cd101 import (
    build_cd101_candidate_frame,
    build_cd101_request,
)
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_poll
from intelipump_fdc.real_wayne_price.cd101_session import Cd101WriteSession
from intelipump_fdc.real_wayne_price.guards import (
    Cd101WriteConfirmations,
    Cd101WriteParams,
    validate_cd101_write_params,
)
from intelipump_fdc.real_wayne_price.states import Cd101WriteState

_STATUS = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 05"
)
_DC101 = bytes.fromhex(
    "65 10 01"
    "00 00 00 00 12"
    "00 00 00 00 12"
    "00 00 00 00 00"
)


def _frame(payload: bytes, wire: int = 0x50, seq: int = 1) -> bytes:
    return build_data_frame(wire, seq, payload)


@dataclass
class FakeCd101Transport:
    device: str = "/tmp/fake-cd101"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    active_write_count: int = 0
    cd101_write_count: int = 0
    _open: bool = False
    _chunks: list[bytes] = field(default_factory=list)
    _seq: int = 0
    _approved_active_frame: bytes | None = None
    _active_writes_remaining: int = 0
    _approved_kind: ActiveFrameKind | None = None
    _poll_n: int = 0
    expected_ack: bytes = bytes.fromhex("50 c0 fa")
    initial_status: bytes = _STATUS
    after_status: bytes = _DC101

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
            if kind is ActiveFrameKind.CD101_REQUEST_TOTALS:
                self.cd101_write_count += 1
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


def _confirms(**overrides: bool) -> Cd101WriteConfirmations:
    base = dict(
        owned_lab_pump=True,
        technician_present=True,
        no_product_connected=True,
        motor_isolated=True,
        valves_isolated=True,
        emergency_isolation_ready=True,
        authorization_disabled=True,
        single_write_plan_reviewed=True,
        execute_cd101_request=True,
        understand_transmits_to_owned_lab_pump=True,
    )
    base.update(overrides)
    return Cd101WriteConfirmations(**base)


def test_build_cd101_matches_epump_payload() -> None:
    req = build_cd101_request(counter_select=1)
    assert req.application_payload == bytes.fromhex("65 01 01")
    frame, _crc, ack = build_cd101_candidate_frame(
        logical_address=2, sequence=1, cd101=req
    )
    assert frame[:2] == bytes.fromhex("51 31")
    assert bytes.fromhex("65 01 01") in frame
    assert classify_active_data_frame(frame) is ActiveFrameKind.CD101_REQUEST_TOTALS
    assert ack == bytes.fromhex("51 c1 fa")


def test_validate_cd101_requires_execute_flag(tmp_path: Path) -> None:
    params = Cd101WriteParams(
        port="/dev/null",
        address=1,
        evidence_dir=tmp_path,
        confirmations=_confirms(execute_cd101_request=False),
    )
    with pytest.raises(PollBenchRefusedError):
        validate_cd101_write_params(params)


def test_cd101_session_ack_and_dc101(tmp_path: Path) -> None:
    req = build_cd101_request()
    frame, _crc, ack = build_cd101_candidate_frame(
        logical_address=1, sequence=0, cd101=req
    )
    transport = FakeCd101Transport(expected_ack=ack)
    params = Cd101WriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path,
        confirmations=_confirms(),
        sequence=0,
        post_write_settle_ms=0,
        post_write_max_status_polls=2,
        ack_timeout_ms=200,
    )
    result = asyncio.run(Cd101WriteSession(transport, params).run())
    assert result.transmitted is True
    assert transport.cd101_write_count == 1
    assert result.state in {
        Cd101WriteState.ACK_RECEIVED,
        Cd101WriteState.DC101_OBSERVED,
    }
    assert result.summary["ackOutcome"] == "ACK_MATCH"
