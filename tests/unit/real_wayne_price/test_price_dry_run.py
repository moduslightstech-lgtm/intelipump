"""Tests for documented CD5 price dry-run (no real write path)."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from intelipump_fdc.bench_poll.serial_reader import SerialChunk
from intelipump_fdc.controller.price_safety import (
    RealWayneActiveCommandRefusedError,
    assert_real_wayne_poll_only,
    is_verified_status_poll,
)
from intelipump_fdc.protocol.cd5 import (
    CD5Error,
    build_cd5_candidate_frame,
    build_cd5_price_update,
)
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame, build_poll
from intelipump_fdc.protocol.price_codec import (
    PriceCodecError,
    decode_price_bcd_3,
    encode_price_bcd_3,
)
from intelipump_fdc.protocol.sequence import WayneSequenceManager
from intelipump_fdc.real_wayne_price.dry_run import PriceDryRunSession
from intelipump_fdc.real_wayne_price.guards import (
    PriceDryRunConfirmations,
    PriceDryRunParams,
)
from intelipump_fdc.real_wayne_price.states import PriceDryRunState
from intelipump_fdc.real_wayne_price.status_decode import (
    DecodedStatusSnapshot,
    StatusPreconditionError,
    validate_preconditions,
)

# Real Wayne status-shaped payload: DC2 zero + DC3 + DC1 PUMP_NOT_PROGRAMMED
# Built via application layout: TRANS LNG DATA...
_STATUS_PAYLOAD = bytes.fromhex(
    # DC2 VOL=0 AMO=0 (8 BCD bytes)
    "02 08 00 00 00 00 00 00 00 00"
    # DC3 price 009907 + nozzle IN logical 7 → NOZIO=0x07
    "03 04 00 99 07 07"
    # DC1 status 0
    "01 01 00"
)


def _status_data_frame(wire: int = 0x50, seq: int = 1) -> bytes:
    return build_data_frame(wire, seq, _STATUS_PAYLOAD)


@pytest.mark.parametrize(
    ("price", "expected"),
    [
        (1, bytes.fromhex("00 00 01")),
        (850, bytes.fromhex("00 08 50")),
        (1175, bytes.fromhex("00 11 75")),
        (1195, bytes.fromhex("00 11 95")),
        (999999, bytes.fromhex("99 99 99")),
    ],
)
def test_encode_price_bcd_3(price: int, expected: bytes) -> None:
    assert encode_price_bcd_3(price) == expected
    assert decode_price_bcd_3(expected) == price


def test_price_bcd_3_rejects_invalid() -> None:
    with pytest.raises(PriceCodecError):
        encode_price_bcd_3(0)
    with pytest.raises(PriceCodecError):
        encode_price_bcd_3(1_000_000)
    with pytest.raises(PriceCodecError):
        encode_price_bcd_3(1.5)  # type: ignore[arg-type]
    with pytest.raises(PriceCodecError):
        decode_price_bcd_3(bytes.fromhex("00 1A 75"))
    with pytest.raises(PriceCodecError):
        decode_price_bcd_3(bytes.fromhex("00 11"))


def test_two_nozzle_cd5_payload() -> None:
    cd5 = build_cd5_price_update(
        {1: 1175, 2: 1175},
        logical_nozzle_count=2,
        logical_nozzle_mapping_confirmed=True,
        price_scale_confirmed=True,
    )
    assert cd5.application_payload == bytes.fromhex("05 06 00 11 75 00 11 75")
    assert cd5.payload_length == 0x06
    assert cd5.prices[0].packed_bcd == bytes.fromhex("00 11 75")
    assert cd5.prices[1].packed_bcd == bytes.fromhex("00 11 75")


def test_cd5_rejects_wrong_price_counts() -> None:
    with pytest.raises(CD5Error):
        build_cd5_price_update(
            {1: 1175},
            logical_nozzle_count=2,
            logical_nozzle_mapping_confirmed=True,
            price_scale_confirmed=True,
        )
    with pytest.raises(CD5Error):
        build_cd5_price_update(
            {1: 1175, 2: 1175, 3: 1175},
            logical_nozzle_count=2,
            logical_nozzle_mapping_confirmed=True,
            price_scale_confirmed=True,
        )
    with pytest.raises(CD5Error):
        build_cd5_price_update(
            [],
            logical_nozzle_count=2,
            logical_nozzle_mapping_confirmed=True,
            price_scale_confirmed=True,
        )


def test_cd5_preserves_order_and_different_prices() -> None:
    cd5 = build_cd5_price_update(
        {1: 1175, 2: 1195},
        logical_nozzle_count=2,
        logical_nozzle_mapping_confirmed=True,
        price_scale_confirmed=True,
    )
    assert cd5.application_payload == bytes.fromhex("05 06 00 11 75 00 11 95")


def test_cd5_requires_technician_confirmations() -> None:
    with pytest.raises(CD5Error):
        build_cd5_price_update(
            {1: 1175, 2: 1175},
            logical_nozzle_count=2,
            logical_nozzle_mapping_confirmed=False,
            price_scale_confirmed=True,
        )
    with pytest.raises(CD5Error):
        build_cd5_price_update(
            {1: 1175, 2: 1175},
            logical_nozzle_count=2,
            logical_nozzle_mapping_confirmed=True,
            price_scale_confirmed=False,
        )
    with pytest.raises(CD5Error):
        build_cd5_price_update(
            {1: 1175, 2: 1175},
            logical_nozzle_count=3,
            logical_nozzle_mapping_confirmed=True,
            price_scale_confirmed=True,
        )


def test_full_frame_crc_changes_with_price_or_sequence() -> None:
    cd5_a = build_cd5_price_update(
        {1: 1175, 2: 1175},
        logical_nozzle_count=2,
        logical_nozzle_mapping_confirmed=True,
        price_scale_confirmed=True,
    )
    cd5_b = build_cd5_price_update(
        {1: 1175, 2: 1195},
        logical_nozzle_count=2,
        logical_nozzle_mapping_confirmed=True,
        price_scale_confirmed=True,
    )
    fa, ca, _ = build_cd5_candidate_frame(
        logical_address=1, sequence=0, cd5=cd5_a
    )
    fb, cb, _ = build_cd5_candidate_frame(
        logical_address=1, sequence=0, cd5=cd5_b
    )
    assert ca != cb
    assert fa != fb
    fc, cc, ack = build_cd5_candidate_frame(
        logical_address=1, sequence=1, cd5=cd5_a
    )
    assert cc != ca
    assert fc[1] == WayneSequenceManager.message_byte(1)
    assert ack == bytes.fromhex("50 C1 FA")
    assert fa[:2] == bytes.fromhex("50 30")


def test_status_preconditions() -> None:
    ok = DecodedStatusSnapshot(
        wire_address=0x50,
        crc_valid=True,
        dc1_code=0,
        dc1_name="PUMP_NOT_PROGRAMMED",
        volume_raw_scaled=0,
        amount_raw_scaled=0,
        selected_logical_nozzle=7,
        nozzle_out=False,
    )
    validate_preconditions(
        ok, expected_wire_address=0x50, authorization_disabled=True
    )

    bad = DecodedStatusSnapshot(
        wire_address=0x50,
        crc_valid=True,
        dc1_code=4,
        dc1_name="FILLING",
        volume_raw_scaled=0,
        amount_raw_scaled=0,
        selected_logical_nozzle=1,
        nozzle_out=False,
    )
    with pytest.raises(StatusPreconditionError):
        validate_preconditions(
            bad, expected_wire_address=0x50, authorization_disabled=True
        )

    nonzero = DecodedStatusSnapshot(
        wire_address=0x50,
        crc_valid=True,
        dc1_code=0,
        dc1_name="PUMP_NOT_PROGRAMMED",
        volume_raw_scaled=1,
        amount_raw_scaled=0,
        selected_logical_nozzle=1,
        nozzle_out=False,
    )
    with pytest.raises(StatusPreconditionError):
        validate_preconditions(
            nonzero, expected_wire_address=0x50, authorization_disabled=True
        )

    alarm = DecodedStatusSnapshot(
        wire_address=0x50,
        crc_valid=True,
        dc1_code=0,
        dc1_name="PUMP_NOT_PROGRAMMED",
        volume_raw_scaled=0,
        amount_raw_scaled=0,
        selected_logical_nozzle=1,
        nozzle_out=False,
        has_dc5_alarm=True,
        alarm_code=1,
    )
    with pytest.raises(StatusPreconditionError):
        validate_preconditions(
            alarm, expected_wire_address=0x50, authorization_disabled=True
        )


def test_real_wayne_transport_refuses_non_poll() -> None:
    assert is_verified_status_poll(build_poll(1))
    assert_real_wayne_poll_only(build_poll(1))
    cd5 = build_cd5_price_update(
        {1: 1175, 2: 1175},
        logical_nozzle_count=2,
        logical_nozzle_mapping_confirmed=True,
        price_scale_confirmed=True,
    )
    frame, _, _ = build_cd5_candidate_frame(
        logical_address=1, sequence=0, cd5=cd5
    )
    with pytest.raises(RealWayneActiveCommandRefusedError):
        assert_real_wayne_poll_only(frame)
    with pytest.raises(RealWayneActiveCommandRefusedError):
        assert_real_wayne_poll_only(bytes.fromhex("50 01 02 FA"))


@dataclass
class FakeDryRunTransport:
    device: str = "/tmp/fake-price-dry-run"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    _open: bool = False
    _chunks: list[bytes] = field(default_factory=list)
    _seq: int = 0

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
        # Mirror production guard so fakes cannot sneak CD5 through.
        assert_real_wayne_poll_only(data)
        self.write_count += 1
        self.written.append(data)
        self._chunks.append(_status_data_frame())
        return len(data)


def _confirms(**overrides: bool) -> PriceDryRunConfirmations:
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
    )
    base.update(overrides)
    return PriceDryRunConfirmations(**base)


@pytest.mark.asyncio
async def test_dry_run_builds_candidate_without_transmitting(tmp_path: Path) -> None:
    transport = FakeDryRunTransport()
    params = PriceDryRunParams(
        port="/tmp/fake",
        address=1,
        logical_nozzle_count=2,
        price_nozzle_1=1175,
        price_nozzle_2=1175,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(),
    )
    session = PriceDryRunSession(transport, params)
    result = await session.run()
    assert result.transmitted is False
    assert result.serial_write_called_for_candidate is False
    assert result.state is PriceDryRunState.FILLING_COMPLETE_EXPECTED
    assert result.summary["cd5PayloadHex"] == "05 06 00 11 75 00 11 75"
    assert all(w == build_poll(1) for w in transport.written)
    assert transport.write_count >= 1
    review = json.loads((tmp_path / "ev" / "price-programming-review.json").read_text())
    assert review["transmitted"] is False
    assert review["serialWriteCalledForCandidate"] is False
    assert review["writeRefusedBySoftware"] is True
    assert review["candidateFrameHex"].startswith("50 30 05 06")


@pytest.mark.asyncio
async def test_dry_run_refuses_without_mapping_confirm(tmp_path: Path) -> None:
    transport = FakeDryRunTransport()
    params = PriceDryRunParams(
        port="/tmp/fake",
        address=1,
        logical_nozzle_count=2,
        price_nozzle_1=1175,
        price_nozzle_2=1175,
        evidence_dir=tmp_path / "ev",
        confirmations=_confirms(logical_nozzle_mapping_confirmed=False),
    )
    result = await PriceDryRunSession(transport, params).run()
    assert result.state is PriceDryRunState.REFUSED
    assert result.transmitted is False
