"""Tests for single-shot real-Wayne CD5 price write."""

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
    assert_real_wayne_poll_only,
    assert_real_wayne_write_allowed,
    classify_active_data_frame,
    is_verified_status_poll,
)
from intelipump_fdc.protocol.cd1 import (
    build_cd1_candidate_frame,
    build_cd1_command,
)
from intelipump_fdc.protocol.cd5 import (
    build_cd5_candidate_frame,
    build_cd5_price_update,
)
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_poll
from intelipump_fdc.real_wayne_price.guards import (
    PriceWriteConfirmations,
    PriceWriteParams,
    validate_price_write_params,
)
from intelipump_fdc.real_wayne_price.states import PriceWriteState
from intelipump_fdc.real_wayne_price.status_decode import (
    DecodedStatusSnapshot,
    StatusPreconditionError,
    validate_post_write_status,
)
from intelipump_fdc.real_wayne_price.write_session import PriceWriteSession

_STATUS_NOT_PROGRAMMED = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 99 07 07"
    "01 01 00"
)
_STATUS_FILLING_COMPLETE = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 05"
)


def _frame(payload: bytes, wire: int = 0x50, seq: int = 1) -> bytes:
    return build_data_frame(wire, seq, payload)


@dataclass
class FakeWriteTransport:
    device: str = "/tmp/fake-price-write"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    cd5_write_count: int = 0
    active_write_count: int = 0
    _open: bool = False
    _chunks: list[bytes] = field(default_factory=list)
    _seq: int = 0
    _approved_active_frame: bytes | None = None
    _active_writes_remaining: int = 0
    _approved_kind: ActiveFrameKind | None = None
    _poll_n: int = 0
    expected_ack: bytes = bytes.fromhex("50 c0 fa")
    # Extra post-CD5 status payloads before FILLING_COMPLETE (settle retry).
    post_status_before_complete: list[bytes] = field(default_factory=list)

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

    def authorize_single_cd5_write(self, frame: bytes) -> None:
        self.authorize_single_active_write(frame, kind=ActiveFrameKind.CD5_PRICE)

    def clear_cd5_write_authorization(self) -> None:
        self.clear_active_write_authorization()

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
            if kind is ActiveFrameKind.CD5_PRICE:
                self.cd5_write_count += 1
            self.written.append(data)
            self.write_count += 1
            self._chunks.append(self.expected_ack)
            return len(data)

        # Status poll.
        self.write_count += 1
        self.written.append(data)
        self._poll_n += 1
        if self._poll_n == 1:
            self._chunks.append(_frame(_STATUS_NOT_PROGRAMMED))
        elif self.post_status_before_complete:
            payload = self.post_status_before_complete.pop(0)
            self._chunks.append(_frame(payload, seq=2))
        else:
            self._chunks.append(_frame(_STATUS_FILLING_COMPLETE, seq=2))
        return len(data)


def _write_confirms(**overrides: bool) -> PriceWriteConfirmations:
    base = dict(
        owned_lab_pump=True,
        technician_present=True,
        no_product_connected=True,
        motor_isolated=True,
        valves_isolated=True,
        emergency_isolation_ready=True,
        authorization_disabled=True,
        single_write_plan_reviewed=True,
        price_scale_confirmed=True,
        logical_nozzle_mapping_confirmed=True,
        execute_cd5_write=True,
        post_write_status_verification_required=True,
        understand_transmits_to_owned_lab_pump=True,
    )
    base.update(overrides)
    return PriceWriteConfirmations(**base)


def test_poll_only_still_refuses_cd5() -> None:
    cd5 = build_cd5_price_update(
        {1: 1175, 2: 1175},
        logical_nozzle_count=2,
        logical_nozzle_mapping_confirmed=True,
        price_scale_confirmed=True,
    )
    frame, _, _ = build_cd5_candidate_frame(logical_address=1, sequence=0, cd5=cd5)
    with pytest.raises(RealWayneActiveCommandRefusedError):
        assert_real_wayne_poll_only(frame)
    with pytest.raises(RealWayneActiveCommandRefusedError):
        assert_real_wayne_write_allowed(
            frame, approved_active_frame=None, active_writes_remaining=0
        )


def test_write_allowed_only_for_exact_approved_cd5() -> None:
    cd5 = build_cd5_price_update(
        {1: 1175, 2: 1175},
        logical_nozzle_count=2,
        logical_nozzle_mapping_confirmed=True,
        price_scale_confirmed=True,
    )
    frame, _, _ = build_cd5_candidate_frame(logical_address=1, sequence=0, cd5=cd5)
    assert_real_wayne_write_allowed(
        frame, approved_active_frame=frame, active_writes_remaining=1
    )
    other, _, _ = build_cd5_candidate_frame(logical_address=1, sequence=1, cd5=cd5)
    with pytest.raises(RealWayneActiveCommandRefusedError):
        assert_real_wayne_write_allowed(
            other, approved_active_frame=frame, active_writes_remaining=1
        )


def test_classify_cd1_reset_and_authorize() -> None:
    reset = build_cd1_command(PumpControlCommand.RESET)
    frame, _, _ = build_cd1_candidate_frame(
        logical_address=1, sequence=0, cd1=reset
    )
    assert classify_active_data_frame(frame) is ActiveFrameKind.CD1_RESET
    auth = build_cd1_command(PumpControlCommand.AUTHORIZE)
    frame_a, _, _ = build_cd1_candidate_frame(
        logical_address=1, sequence=0, cd1=auth
    )
    assert classify_active_data_frame(frame_a) is ActiveFrameKind.CD1_AUTHORIZE


def test_post_write_requires_filling_complete() -> None:
    ok = DecodedStatusSnapshot(
        wire_address=0x50,
        crc_valid=True,
        dc1_code=5,
        dc1_name="FILLING_COMPLETED",
    )
    validate_post_write_status(ok, expected_wire_address=0x50)
    bad = DecodedStatusSnapshot(
        wire_address=0x50,
        crc_valid=True,
        dc1_code=0,
        dc1_name="PUMP_NOT_PROGRAMMED",
    )
    with pytest.raises(StatusPreconditionError):
        validate_post_write_status(bad, expected_wire_address=0x50)


@pytest.mark.asyncio
async def test_write_session_transmits_cd5_once(tmp_path: Path) -> None:
    transport = FakeWriteTransport()
    params = PriceWriteParams(
        port="/tmp/fake",
        address=1,
        logical_nozzle_count=2,
        price_nozzle_1=1175,
        price_nozzle_2=1175,
        evidence_dir=tmp_path / "ev",
        confirmations=_write_confirms(),
        post_write_settle_ms=0,
        post_write_max_status_polls=3,
    )
    result = await PriceWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.serial_write_called_for_candidate is True
    assert result.state is PriceWriteState.FILLING_COMPLETE_VERIFIED
    assert transport.cd5_write_count == 1
    assert sum(1 for w in transport.written if w == build_poll(1)) == 2
    cd5_frames = [w for w in transport.written if not is_verified_status_poll(w)]
    assert len(cd5_frames) == 1
    assert cd5_frames[0].hex(" ") == (
        "50 30 05 06 00 11 75 00 11 75 da c7 03 fa"
    )
    review = json.loads((tmp_path / "ev" / "price-write-result.json").read_text())
    assert review["transmitted"] is True
    assert review["serialWriteCalledForCandidate"] is True
    assert review["cd5WriteCount"] == 1
    assert review["authorizationIncluded"] is False
    assert review["resetIncluded"] is False


@pytest.mark.asyncio
async def test_write_session_retries_until_filling_complete(tmp_path: Path) -> None:
    transport = FakeWriteTransport(
        post_status_before_complete=[_STATUS_NOT_PROGRAMMED]
    )
    params = PriceWriteParams(
        port="/tmp/fake",
        address=1,
        logical_nozzle_count=2,
        price_nozzle_1=1175,
        price_nozzle_2=1175,
        evidence_dir=tmp_path / "ev",
        confirmations=_write_confirms(),
        post_write_settle_ms=0,
        post_write_max_status_polls=4,
    )
    result = await PriceWriteSession(transport, params).run()
    assert result.state is PriceWriteState.FILLING_COMPLETE_VERIFIED
    assert sum(1 for w in transport.written if w == build_poll(1)) == 3


@pytest.mark.asyncio
async def test_write_refuses_without_execute_confirm(tmp_path: Path) -> None:
    params = PriceWriteParams(
        port="/tmp/fake",
        address=1,
        logical_nozzle_count=2,
        price_nozzle_1=1175,
        price_nozzle_2=1175,
        evidence_dir=tmp_path / "ev",
        confirmations=_write_confirms(execute_cd5_write=False),
    )
    with pytest.raises(PollBenchRefusedError):
        validate_price_write_params(params)


@pytest.mark.asyncio
async def test_second_cd5_write_refused_without_reapproval(tmp_path: Path) -> None:
    transport = FakeWriteTransport()
    cd5 = build_cd5_price_update(
        {1: 1175, 2: 1175},
        logical_nozzle_count=2,
        logical_nozzle_mapping_confirmed=True,
        price_scale_confirmed=True,
    )
    frame, _, _ = build_cd5_candidate_frame(logical_address=1, sequence=0, cd5=cd5)
    await transport.open()
    transport.authorize_single_cd5_write(frame)
    await transport.write(frame)
    with pytest.raises(RealWayneActiveCommandRefusedError):
        await transport.write(frame)
