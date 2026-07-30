"""Owned-lab NOZIO-unknown override for CD1 RESET diagnostics."""

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
from intelipump_fdc.protocol.cd1 import build_cd1_command
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_poll
from intelipump_fdc.real_wayne_price.guards import (
    ResetWriteConfirmations,
    ResetWriteParams,
    validate_reset_write_params,
)
from intelipump_fdc.real_wayne_price.nozzle_physical import (
    ControllerProfile,
    NozzlePhysicalState,
    decode_nozzle_state,
)
from intelipump_fdc.real_wayne_price.reset_decision import (
    OWNED_LAB_NOZIO_UNKNOWN_WARNING,
    ResetNozzleDecision,
    controller_profile_for_reset,
    evaluate_reset_nozzle_gate,
    owned_lab_nozio_unknown_override_confirmed,
)
from intelipump_fdc.real_wayne_price.reset_session import ResetWriteSession
from intelipump_fdc.real_wayne_price.states import (
    ResetDiagnosticResult,
    ResetWriteState,
)

# FILLING_COMPLETED + NOZIO 0x01 (no out bit) — lab head that never asserts 0x10.
_STATUS_FILLING_UNKNOWN = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 05"
)
# Documented OUT: NOZIO 0x11.
_STATUS_FILLING_OUT = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 11"
    "01 01 05"
)
_STATUS_RESET = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 01"
)
_STATUS_FILLING_AFTER = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 05"
)
_STATUS_AUTHORIZED_DC1 = bytes.fromhex(
    "02 08 00 00 00 00 00 00 00 00"
    "03 04 00 11 75 01"
    "01 01 02"
)

_RESET_FRAME = "50 30 01 01 05 5f 5f 03 fa"
_AUTHORIZE_PAYLOAD = build_cd1_command(PumpControlCommand.AUTHORIZE).payload_hex


def _frame(payload: bytes, wire: int = 0x50, seq: int = 1) -> bytes:
    return build_data_frame(wire, seq, payload)


@dataclass
class FakeResetTransport:
    device: str = "/tmp/fake-reset-override"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    active_write_count: int = 0
    cd1_reset_write_count: int = 0
    cd1_authorize_write_count: int = 0
    status_poll_count: int = 0
    _open: bool = False
    _chunks: list[bytes] = field(default_factory=list)
    _seq: int = 0
    _approved_active_frame: bytes | None = None
    _active_writes_remaining: int = 0
    _approved_kind: ActiveFrameKind | None = None
    expected_ack: bytes = bytes.fromhex("50 c0 fa")
    initial_status: bytes = _STATUS_FILLING_UNKNOWN
    after_status: bytes = _STATUS_RESET
    # Distinct after payloads per poll to prove fresh observation.
    after_status_sequence: list[bytes] | None = None
    _after_i: int = 0

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
        self.status_poll_count += 1
        if self.status_poll_count == 1:
            self._chunks.append(_frame(self.initial_status))
        else:
            if self.after_status_sequence is not None:
                idx = min(self._after_i, len(self.after_status_sequence) - 1)
                payload = self.after_status_sequence[idx]
                self._after_i += 1
            else:
                payload = self.after_status
            self._chunks.append(_frame(payload, seq=2))
        return len(data)


def _base_confirms(**overrides: bool) -> ResetWriteConfirmations:
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


def _override_confirms(**overrides: bool) -> ResetWriteConfirmations:
    base = dict(
        confirm_physical_nozzle_out=True,
        confirm_price_visible=True,
        allow_nozio_unknown_for_reset=True,
        confirm_reset_only=True,
        confirm_no_authorize=True,
    )
    base.update(overrides)
    return _base_confirms(**base)


def _params(
    tmp_path: Path,
    confirms: ResetWriteConfirmations,
    **kwargs: object,
) -> ResetWriteParams:
    defaults: dict[str, object] = dict(
        port="/tmp/fake",
        address=1,
        evidence_dir=tmp_path / "ev",
        confirmations=confirms,
        post_write_settle_ms=0,
        post_reset_observation_seconds=0.0,
    )
    defaults.update(kwargs)
    return ResetWriteParams(**defaults)  # type: ignore[arg-type]


def test_decode_nozzle_state_profile() -> None:
    assert (
        decode_nozzle_state(0x11, ControllerProfile(supports_nozio_out_bit=True))
        is NozzlePhysicalState.OUT
    )
    assert (
        decode_nozzle_state(0x01, ControllerProfile(supports_nozio_out_bit=True))
        is NozzlePhysicalState.IN
    )
    assert (
        decode_nozzle_state(0x01, ControllerProfile(supports_nozio_out_bit=False))
        is NozzlePhysicalState.UNKNOWN
    )


def test_gate_filling_completed_documented_out_allows_normal() -> None:
    result = evaluate_reset_nozzle_gate(
        dc1_code=5,
        nozio=0x11,
        controller_profile=ControllerProfile(supports_nozio_out_bit=True),
        owned_lab_override_confirmed=False,
    )
    assert result.allow is True
    assert result.decision is ResetNozzleDecision.ALLOW_NORMAL
    assert result.nozzle_state is NozzlePhysicalState.OUT


def test_gate_unknown_without_override_refuses() -> None:
    result = evaluate_reset_nozzle_gate(
        dc1_code=5,
        nozio=0x01,
        controller_profile=ControllerProfile(supports_nozio_out_bit=False),
        owned_lab_override_confirmed=False,
    )
    assert result.allow is False
    assert result.refusal_reason == "physical_nozzle_state_unknown"


def test_gate_unknown_with_override_allows() -> None:
    result = evaluate_reset_nozzle_gate(
        dc1_code=5,
        nozio=0x01,
        controller_profile=ControllerProfile(supports_nozio_out_bit=False),
        owned_lab_override_confirmed=True,
    )
    assert result.allow is True
    assert result.decision is ResetNozzleDecision.ALLOW_OWNED_LAB_UNKNOWN_OVERRIDE
    assert result.warning == OWNED_LAB_NOZIO_UNKNOWN_WARNING


def test_gate_non_filling_completed_refuses() -> None:
    result = evaluate_reset_nozzle_gate(
        dc1_code=1,
        nozio=0x11,
        controller_profile=ControllerProfile(supports_nozio_out_bit=True),
        owned_lab_override_confirmed=True,
    )
    assert result.allow is False
    assert result.refusal_reason == "status_not_filling_completed"


def test_missing_physical_nozzle_confirm_refuses_params(tmp_path: Path) -> None:
    params = _params(
        tmp_path,
        _override_confirms(confirm_physical_nozzle_out=False),
    )
    with pytest.raises(PollBenchRefusedError, match="physical-nozzle"):
        validate_reset_write_params(params)


def test_missing_price_visible_confirm_refuses_params(tmp_path: Path) -> None:
    params = _params(
        tmp_path,
        _override_confirms(confirm_price_visible=False),
    )
    with pytest.raises(PollBenchRefusedError, match="price-visible"):
        validate_reset_write_params(params)


def test_missing_no_fuel_confirm_refuses_params(tmp_path: Path) -> None:
    params = _params(
        tmp_path,
        _override_confirms(no_product_connected=False),
    )
    with pytest.raises(PollBenchRefusedError):
        validate_reset_write_params(params)


def test_authorization_enabled_refuses_override_params(tmp_path: Path) -> None:
    # authorization_disabled=False fails base missing_flags first; keep other
    # base confirms and assert override validation refuses when auth enabled.
    confirms = _override_confirms(authorization_disabled=False)
    params = _params(tmp_path, confirms)
    with pytest.raises(PollBenchRefusedError):
        validate_reset_write_params(params)


def test_production_target_refuses_override(tmp_path: Path) -> None:
    params = _params(
        tmp_path,
        _override_confirms(),
        target_type="PRODUCTION",
    )
    with pytest.raises(PollBenchRefusedError, match="OWNED_LAB_WAYNE"):
        validate_reset_write_params(params)


def test_profile_selection() -> None:
    normal = _params(
        Path("/tmp"),
        _base_confirms(),
    )
    assert controller_profile_for_reset(normal).supports_nozio_out_bit is True
    override = _params(Path("/tmp"), _override_confirms())
    assert controller_profile_for_reset(override).supports_nozio_out_bit is False


@pytest.mark.asyncio
async def test_documented_out_allows_normal_reset(tmp_path: Path) -> None:
    transport = FakeResetTransport(initial_status=_STATUS_FILLING_OUT)
    params = _params(tmp_path, _base_confirms())
    validate_reset_write_params(params)
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.state is ResetWriteState.RESET_VERIFIED
    assert result.summary["resetNozzleDecision"] == "ALLOW_NORMAL"
    assert result.summary["diagnosticResult"] == (
        ResetDiagnosticResult.RESET_DIAGNOSTIC_ACK_STATE_CHANGED.value
    )


@pytest.mark.asyncio
async def test_unknown_without_override_refuses_session(tmp_path: Path) -> None:
    transport = FakeResetTransport(initial_status=_STATUS_FILLING_UNKNOWN)
    # Normal profile (supports bit) → NOZIO 0x01 is IN → nozzle_not_out
    params = _params(tmp_path, _base_confirms())
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is False
    assert result.state is ResetWriteState.REFUSED
    assert "nozzle_not_out" in result.summary["refusalReasons"]
    assert result.summary["diagnosticResult"] == (
        ResetDiagnosticResult.RESET_DIAGNOSTIC_REFUSED.value
    )


@pytest.mark.asyncio
async def test_unknown_profile_without_override_confirms_refuses(
    tmp_path: Path,
) -> None:
    """allow flag → UNKNOWN profile, but incomplete confirms refuse at gate."""
    transport = FakeResetTransport(initial_status=_STATUS_FILLING_UNKNOWN)
    params = _params(
        tmp_path,
        _override_confirms(confirm_physical_nozzle_out=False),
    )
    # Intentionally skip validate_reset_write_params (CLI would refuse earlier).
    assert owned_lab_nozio_unknown_override_confirmed(params) is False
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is False
    assert result.state is ResetWriteState.REFUSED
    assert "physical_nozzle_state_unknown" in result.summary["refusalReasons"]


@pytest.mark.asyncio
async def test_unknown_with_full_override_allows_one_reset(tmp_path: Path) -> None:
    transport = FakeResetTransport(initial_status=_STATUS_FILLING_UNKNOWN)
    params = _params(tmp_path, _override_confirms())
    validate_reset_write_params(params)
    assert owned_lab_nozio_unknown_override_confirmed(params) is True
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert transport.cd1_reset_write_count == 1
    assert result.summary["resetNozzleDecision"] == (
        "ALLOW_OWNED_LAB_UNKNOWN_OVERRIDE"
    )
    assert OWNED_LAB_NOZIO_UNKNOWN_WARNING in result.summary["warnings"]
    assert result.state is ResetWriteState.RESET_VERIFIED


@pytest.mark.asyncio
async def test_non_filling_completed_refuses_session(tmp_path: Path) -> None:
    transport = FakeResetTransport(initial_status=_STATUS_AUTHORIZED_DC1)
    params = _params(tmp_path, _override_confirms())
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is False
    assert result.state is ResetWriteState.REFUSED
    assert any("dc1_not_FILLING_COMPLETE" in r for r in result.summary["refusalReasons"])


@pytest.mark.asyncio
async def test_override_sends_exactly_one_reset(tmp_path: Path) -> None:
    transport = FakeResetTransport(initial_status=_STATUS_FILLING_UNKNOWN)
    params = _params(tmp_path, _override_confirms())
    result = await ResetWriteSession(transport, params).run()
    active = [w for w in transport.written if not is_verified_status_poll(w)]
    assert len(active) == 1
    assert active[0].hex(" ") == _RESET_FRAME
    assert transport.cd1_reset_write_count == 1
    assert result.summary["activeWriteCount"] == 1


@pytest.mark.asyncio
async def test_override_never_sends_authorize(tmp_path: Path) -> None:
    transport = FakeResetTransport(initial_status=_STATUS_FILLING_UNKNOWN)
    params = _params(tmp_path, _override_confirms())
    result = await ResetWriteSession(transport, params).run()
    assert transport.cd1_authorize_write_count == 0
    for w in transport.written:
        assert _AUTHORIZE_PAYLOAD not in w.hex(" ")
    assert "AUTHORIZE" not in result.summary["command"]
    assert result.summary["command"] == "RESET"


@pytest.mark.asyncio
async def test_post_reset_polling_is_fresh(tmp_path: Path) -> None:
    """After-state must come from a new poll, not the cached before snapshot."""
    distinct_after = bytes.fromhex(
        "02 08 00 00 00 00 00 00 00 00"
        "03 04 00 22 00 01"
        "01 01 01"
    )
    transport = FakeResetTransport(
        initial_status=_STATUS_FILLING_UNKNOWN,
        after_status=distinct_after,
    )
    params = _params(tmp_path, _override_confirms())
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    before = result.summary["decodedStatusBefore"]
    after = result.summary["decodedStatusAfter"]
    assert before["rawFrameHex"] != after["rawFrameHex"]
    assert after["dc1"]["code"] == 1
    assert after["dc3"]["fillingPriceRawScaled"] == 2200
    # At least one pre-TX poll and one post-TX poll.
    assert transport.status_poll_count >= 2
    assert sum(1 for w in transport.written if w == build_poll(1)) >= 2


@pytest.mark.asyncio
async def test_evidence_fields_written(tmp_path: Path) -> None:
    transport = FakeResetTransport(initial_status=_STATUS_FILLING_UNKNOWN)
    params = _params(tmp_path, _override_confirms())
    result = await ResetWriteSession(transport, params).run()
    review = json.loads((tmp_path / "ev" / "cd1-reset-write-result.json").read_text())
    jsonl = (tmp_path / "ev" / "cd1-reset-write.jsonl").read_text()
    record = json.loads(jsonl.strip().splitlines()[0])
    assert review["command"] == "RESET"
    assert review["transmitted"] is True
    assert review["diagnosticResult"] == (
        ResetDiagnosticResult.RESET_DIAGNOSTIC_ACK_STATE_CHANGED.value
    )
    assert review["nozzlePhysicalState"] == "UNKNOWN"
    assert review["ackOutcome"] == "ACK_MATCH"
    assert review["softwareCommit"]
    assert record["statusResponseBeforeHex"]
    assert record["statusResponseAfterHex"]
    assert record["candidateFrameHex"]
    assert record["decodedStatusBefore"]["dc1"]["code"] == 5
    assert record["decodedStatusAfter"]["dc1"]["code"] == 1
    assert "timestampUtc" in record
    assert result.summary["softwareCommit"]
    assert result.summary["timestampUtc"]


@pytest.mark.asyncio
async def test_ack_unchanged_maps_diagnostic(tmp_path: Path) -> None:
    transport = FakeResetTransport(
        initial_status=_STATUS_FILLING_UNKNOWN,
        after_status=_STATUS_FILLING_AFTER,
    )
    params = _params(
        tmp_path,
        _override_confirms(post_write_status_verification_required=False),
    )
    result = await ResetWriteSession(transport, params).run()
    assert result.transmitted is True
    assert result.summary["diagnosticResult"] == (
        ResetDiagnosticResult.RESET_DIAGNOSTIC_ACK_STATE_UNCHANGED.value
    )
