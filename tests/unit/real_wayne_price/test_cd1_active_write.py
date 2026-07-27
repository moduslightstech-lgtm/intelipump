"""Tests for single-shot CD1 RESET and AUTHORIZE writes."""

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
from intelipump_fdc.protocol.cd1 import (
    CD1Error,
    build_cd1_candidate_frame,
    build_cd1_command,
)
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_poll
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
from intelipump_fdc.real_wayne_price.states import (
    AuthorizeWriteState,
    ResetWriteState,
)

_STATUS_FILLING_COMPLETE = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 05"
)
_STATUS_RESET = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 01"
)
_STATUS_AUTHORIZED = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 02"
)

_RESET_FRAME = "50 30 01 01 05 5f 5f 03 fa"
_AUTHORIZE_FRAME = "50 30 01 01 06 1f 5e 03 fa"


def _frame(payload: bytes, wire: int = 0x50, seq: int = 1) -> bytes:
    return build_data_frame(wire, seq, payload)


@dataclass
class FakeActiveTransport:
    device: str = "/tmp/fake-active"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    active_write_count: int = 0
    cd1_reset_write_count: int = 0
    cd1_authorize_write_count: int = 0
    _open: bool = False
    _chunks: list[bytes] = field(default_factory=list)
    _seq: int = 0
    _approved_active_frame: bytes | None = None
    _active_writes_remaining: int = 0
    _approved_kind: ActiveFrameKind | None = None
    _poll_n: int = 0
    expected_ack: bytes = bytes.fromhex("50 c0 fa")
    initial_status: bytes = _STATUS_FILLING_COMPLETE
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
            if kind is ActiveFrameKind.CD1_RESET:
                self.cd1_reset_write_count += 1
            elif kind is ActiveFrameKind.CD1_AUTHORIZE:
                self.cd1_authorize_write_count += 1
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
    assert (
        sequence_stale_status_hint(
            sequence=0,
            ack_outcome="ACK_TIMEOUT",
            refusal_reasons=["post_reset_RESET_not_reached"],
        )
        is None
    )


@pytest.mark.asyncio
async def test_reset_session_transmits_once(tmp_path: Path) -> None:
    transport = FakeActiveTransport()
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(),
        post_write_settle_ms=0,
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


@pytest.mark.asyncio
async def test_reset_refuses_without_filling_complete(tmp_path: Path) -> None:
    transport = FakeActiveTransport(initial_status=_STATUS_RESET)
    params = ResetWriteParams(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=_reset_confirms(),
        post_write_settle_ms=0,
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
