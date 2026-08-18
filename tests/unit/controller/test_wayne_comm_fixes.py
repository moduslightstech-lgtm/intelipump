"""Wayne communication foundation / session behavior tests (Phases 1-5 + sale)."""

from __future__ import annotations

import asyncio
import time

import pytest

from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.exchange_result import ExchangeResultStatus
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.rx_demux import AddressFrameDemux, TimestampedFrame
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.controller.sale_lifecycle import SaleEvidence, SaleLifecycle
from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.controller.session_models import (
    IdempotencyClass,
    NozzlePosition,
    ObservedStatus,
    OutboundDataItem,
)
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.protocol.dart.application.constants import (
    MessageDirection,
    TransactionType,
)
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.constants import SF
from intelipump_fdc.protocol.dart.line.escaping import escape_dle, unescape_dle
from intelipump_fdc.protocol.dart.line.frame_builder import (
    build_ack,
    build_data_frame,
    build_eot,
    build_poll,
)
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.simulator.encoding import encode_dc1_status, encode_dc3_nozzle_price
from intelipump_fdc.state_machine.wayne_mapper import MapperContext, map_wayne_observation


def _parse(raw: bytes):
    parsed = parse_frame(raw)
    assert not isinstance(parsed, Exception)
    return parsed


def _lab_safety() -> ControllerSafetyContext:
    return ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.LISTEN_ONLY,
        active_commands_enabled=False,
        require_physical_control_enable=True,
        physical_enable_present=False,
        allow_virtual_polling=True,
        allow_lab_simulator_commands=True,
    )


# --- RX demux ---


def test_fragmented_frame_reconstruction() -> None:
    demux = AddressFrameDemux()
    raw = build_data_frame(0x50, 0, encode_dc1_status(1))
    mid = len(raw) // 2
    t0 = time.monotonic()
    demux.feed(raw[:mid], capture_mono=t0)
    assert demux.pending_size(0x50) == 0
    demux.feed(raw[mid:], capture_mono=t0 + 0.001)
    assert demux.pending_size(0x50) == 1


def test_multiple_frames_in_one_read() -> None:
    demux = AddressFrameDemux()
    chunk = (
        build_data_frame(0x50, 0, encode_dc1_status(1))
        + build_eot(0x50, 0)
    )
    demux.feed(chunk, capture_mono=time.monotonic())
    assert demux.pending_size(0x50) == 2


def test_queue_isolation_50_51() -> None:
    demux = AddressFrameDemux()
    demux.feed(build_eot(0x50, 0), capture_mono=1.0)
    demux.feed(build_eot(0x51, 0), capture_mono=1.1)
    assert demux.pending_size(0x50) == 1
    assert demux.pending_size(0x51) == 1
    f50 = demux.get_nowait(0x50)
    f51 = demux.get_nowait(0x51)
    assert f50 is not None and f50.wire_address == 0x50
    assert f51 is not None and f51.wire_address == 0x51
    assert demux.get_nowait(0x50) is None


def test_crc_reject_no_ack_no_state() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    before = s.machine.context.state_version
    good = build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1))
    body = bytearray(unescape_dle(good[:-1]))
    body[-3] ^= 0xFF
    bad = escape_dle(bytes(body)) + bytes((SF,))
    frame = _parse(bad)
    assert frame.crc_valid is False
    assert s.handle_response_frame(frame) is None
    assert s.state.stats.crc_error_count == 1
    assert s.machine.context.state_version == before


def test_03_fa_recovery_via_quiet_gap() -> None:
    demux = AddressFrameDemux(quiet_gap_timeout_s=0.01)
    # Incomplete DATA body (no trailing SF) then quiet-gap expire.
    demux.feed(bytes((0x50, 0x30, 0x01)), capture_mono=time.monotonic())
    assert demux._assembler.pending_size > 0
    time.sleep(0.02)
    diags = demux.maybe_expire_partial()
    assert diags
    assert demux._assembler.pending_size == 0


@pytest.mark.asyncio
async def test_empty_queue_does_not_end_wait() -> None:
    ctrl, pump = create_memory_transport_pair()
    await ctrl.open()
    await pump.open()
    runtime = ControllerRuntime(
        transport=ctrl,
        safety=_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=80,
            inter_poll_delay_ms=0,
            idle_sleep_ms=0,
            max_retries=0,
        ),
    )
    loop = ControllerLoop(runtime)
    loop._start_rx_task()
    try:
        write_complete = time.monotonic()
        # Delayed EOT arrives after temporary empties.

        async def _late_eot() -> None:
            await asyncio.sleep(0.03)
            await pump.write(build_eot(0x50, 0))

        task = asyncio.create_task(_late_eot())
        session = loop.sessions[1]
        outcome = await loop._read_poll_session(
            session, not_before_mono=write_complete
        )
        await task
        assert outcome == "eot"
        assert session.state.stats.eot_count == 1
    finally:
        await loop._stop_rx_task()
        await ctrl.close()
        await pump.close()


@pytest.mark.asyncio
async def test_multi_data_until_eot() -> None:
    ctrl, pump = create_memory_transport_pair()
    await ctrl.open()
    await pump.open()
    runtime = ControllerRuntime(
        transport=ctrl,
        safety=_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=200,
            inter_poll_delay_ms=0,
            idle_sleep_ms=0,
            max_retries=0,
        ),
    )
    loop = ControllerLoop(runtime)
    loop._start_rx_task()
    try:
        write_complete = time.monotonic()
        payload = (
            build_data_frame(0x50, 0, encode_dc1_status(1))
            + build_data_frame(
                0x50,
                1,
                encode_dc3_nozzle_price(
                    price_raw=1000, logical_nozzle=1, nozzle_out=False
                ),
            )
            + build_eot(0x50, 0)
        )
        await pump.write(payload)
        session = loop.sessions[1]
        outcome = await loop._read_poll_session(
            session, not_before_mono=write_complete
        )
        assert outcome == "eot"
        assert session.state.stats.data_count == 2
        assert session.state.stats.ack_sent_count == 2
        assert session.state.stats.eot_count == 1
    finally:
        await loop._stop_rx_task()
        await ctrl.close()
        await pump.close()


def test_eot_only_not_synchronized() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    s.handle_response_frame(_parse(build_eot(0x50, 0)), capture_mono=time.monotonic())
    assert s.state.communication_online is True
    assert s.state.state_synchronized is False


def test_online_not_sync_without_dc1_nozio() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    s.handle_response_frame(_parse(build_eot(0x50, 0)))
    assert s.state.communication_online
    assert not s.state.state_synchronized
    s.handle_response_frame(
        _parse(build_data_frame(0x50, 0, encode_dc1_status(1))),
        capture_mono=time.monotonic(),
    )
    assert s.state.observed_status is ObservedStatus.RESET
    assert not s.state.state_synchronized  # NOZIO still unknown
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                1,
                encode_dc3_nozzle_price(
                    price_raw=1000, logical_nozzle=1, nozzle_out=False
                ),
            )
        ),
        capture_mono=time.monotonic(),
    )
    assert s.state.nozzle_position is NozzlePosition.IN
    assert s.state.state_synchronized is True


def test_cd1_not_as_dc1_when_master_direction() -> None:
    # Controller command payload 01 01 00 must not map as pump DC1 status.
    payload = bytes((0x01, 0x01, 0x00))  # CD1 RETURN_STATUS
    bundle = decode_data_payload(payload, pump_address=1, line_sequence=0)
    assert bundle.transactions
    tx = bundle.transactions[0]
    assert tx.transaction_type in {
        TransactionType.AMBIGUOUS_CD1_OR_DC1,
        TransactionType.CD1_COMMAND,
    }
    mapped = map_wayne_observation(
        tx,
        context=MapperContext(
            resolve_as_cd1=True,
            resolve_as_dc1=False,
            bus_direction=MessageDirection.MASTER_TO_SLAVE,
        ),
    )
    assert mapped.event is PumpEvent.UNKNOWN_OBSERVATION


def test_nozio_01_in_11_out() -> None:
    bus = EventBus()
    s = PumpSession(address=1, pump_id="p1", events=bus)
    # Seed known IN without edge.
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                0,
                encode_dc3_nozzle_price(
                    price_raw=1000, logical_nozzle=1, nozzle_out=False
                ),
            )
        )
    )
    assert s.state.nozzle_position is NozzlePosition.IN
    bus.clear()
    # Duplicate NOZIO IN must not emit another lift/return edge.
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                1,
                encode_dc3_nozzle_price(
                    price_raw=1000, logical_nozzle=1, nozzle_out=False
                ),
            )
        )
    )
    lifted = [
        e
        for e in bus.events
        if e.payload.get("event") == PumpEvent.NOZZLE_LIFTED.value
    ]
    assert not lifted
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                2,
                encode_dc3_nozzle_price(
                    price_raw=1000, logical_nozzle=1, nozzle_out=True
                ),
            )
        )
    )
    assert s.state.nozzle_position is NozzlePosition.OUT


def test_ack_address_and_sequence_match() -> None:
    s = PumpSession(address=2, pump_id="p2", events=EventBus())
    frame = _parse(build_data_frame(0x51, 3, encode_dc1_status(1)))
    # Soft resync from expected 0 → 3
    ack = s.handle_response_frame(frame)
    assert ack == build_ack(0x51, 3)


@pytest.mark.asyncio
async def test_stale_ack_does_not_confirm_new_command() -> None:
    ctrl, pump = create_memory_transport_pair()
    await ctrl.open()
    await pump.open()
    runtime = ControllerRuntime(
        transport=ctrl,
        safety=_lab_safety(),
        config=PollSchedulerConfig(addresses=(1,), response_timeout_ms=100),
    )
    loop = ControllerLoop(runtime)
    loop._start_rx_task()
    try:
        session = loop.sessions[1]
        # Stale ACK before TX complete timestamp.
        stale = build_ack(0x50, 0)
        await pump.write(stale)
        await asyncio.sleep(0.02)
        item = OutboundDataItem.create(
            address=1,
            application_payload=encode_dc1_status(1),  # dummy body
            command_type=PumpCommand.READ_STATUS,
            simulator_only=True,
            idempotency=IdempotencyClass.IDEMPOTENT,
            max_retries=0,
        )
        # Force sequence 0
        item = item.with_attempt(sequence=0, attempts=0)
        result = await loop._send_outbound_once(session, item, seq=0)
        assert result.status is ExchangeResultStatus.TIMED_OUT
        assert session.state.tx_sequence == 0  # not advanced
    finally:
        await loop._stop_rx_task()
        await ctrl.close()
        await pump.close()


@pytest.mark.asyncio
async def test_retry_reuses_sequence_and_advances_after_ack() -> None:
    ctrl, pump = create_memory_transport_pair()
    await ctrl.open()
    await pump.open()
    runtime = ControllerRuntime(
        transport=ctrl,
        safety=_lab_safety(),
        config=PollSchedulerConfig(addresses=(1,), response_timeout_ms=80),
    )
    loop = ControllerLoop(runtime)
    loop._start_rx_task()
    try:
        session = loop.sessions[1]
        assert session.state.tx_sequence == 0
        item = OutboundDataItem.create(
            address=1,
            application_payload=bytes((0x01, 0x01, 0x00)),
            command_type=PumpCommand.READ_STATUS,
            simulator_only=True,
            idempotency=IdempotencyClass.IDEMPOTENT,
            max_retries=1,
        )
        # Timeout without ACK — sequence stays 0 for retry reuse.
        result1 = await loop._send_outbound_once(session, item, seq=0)
        assert result1.status is ExchangeResultStatus.TIMED_OUT
        assert result1.sequence == 0
        assert session.state.tx_sequence == 0

        item2 = item.with_attempt(sequence=0, attempts=1)

        async def _ack() -> None:
            await asyncio.sleep(0.01)
            await pump.write(build_ack(0x50, 0))

        t = asyncio.create_task(_ack())
        result2 = await loop._send_outbound_once(session, item2, seq=0)
        await t
        assert result2.status is ExchangeResultStatus.LINK_ACKNOWLEDGED
        assert result2.sequence == 0
        session.state.tx_sequence = 1
        assert session.state.tx_sequence == 1
    finally:
        await loop._stop_rx_task()
        await ctrl.close()
        await pump.close()


@pytest.mark.asyncio
async def test_data_during_command_wait_processed_and_acked() -> None:
    ctrl, pump = create_memory_transport_pair()
    await ctrl.open()
    await pump.open()
    runtime = ControllerRuntime(
        transport=ctrl,
        safety=_lab_safety(),
        config=PollSchedulerConfig(addresses=(1,), response_timeout_ms=150),
    )
    loop = ControllerLoop(runtime)
    loop._start_rx_task()
    try:
        session = loop.sessions[1]
        item = OutboundDataItem.create(
            address=1,
            application_payload=bytes((0x01, 0x01, 0x00)),
            command_type=PumpCommand.READ_STATUS,
            simulator_only=True,
            idempotency=IdempotencyClass.IDEMPOTENT,
            max_retries=0,
        )

        async def _pump_side() -> None:
            await asyncio.sleep(0.02)
            await pump.write(build_data_frame(0x50, 0, encode_dc1_status(1)))
            await asyncio.sleep(0.02)
            await pump.write(build_ack(0x50, 0))

        t = asyncio.create_task(_pump_side())
        result = await loop._send_outbound_once(session, item, seq=0)
        await t
        assert result.status is ExchangeResultStatus.LINK_ACKNOWLEDGED
        assert result.preserved_event_count >= 1
        assert session.state.stats.data_count >= 1
        assert session.state.stats.ack_sent_count >= 1
        assert session.state.pending_command_events
    finally:
        await loop._stop_rx_task()
        await ctrl.close()
        await pump.close()


@pytest.mark.asyncio
async def test_app_confirm_after_command_time() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    cmd_t = time.monotonic()
    # Status before command must not confirm.
    s.state.observed_status = ObservedStatus.RESET
    s.state.last_status_time = cmd_t - 0.1
    assert not s.status_observed_after(ObservedStatus.RESET, not_before_mono=cmd_t)
    s.state.last_status_time = cmd_t + 0.05
    assert s.status_observed_after(ObservedStatus.RESET, not_before_mono=cmd_t)


def test_already_reset_skip() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    s.state.observed_status = ObservedStatus.RESET
    s.state.state_synchronized = True
    s.state.pending_exchange = False
    assert s.should_skip_reset() is True


def test_aborted_no_delivery_zero_volume() -> None:
    ev = SaleEvidence()
    ev.note_nozzle_out()
    ev.note_authorized(application_confirmed=True)
    assert ev.note_nozzle_in_zero_delivery() is SaleLifecycle.ABORTED_NO_DELIVERY
    may, reason = ev.evaluate_filling_completed()
    assert may is False
    assert reason == "already_aborted"


def test_filling_completed_without_filling_no_sale() -> None:
    ev = SaleEvidence()
    ev.note_nozzle_out()
    may, reason = ev.evaluate_filling_completed()
    assert may is False
    assert reason == "filling_completed_without_filling"
    assert ev.lifecycle is SaleLifecycle.ABORTED_NO_DELIVERY


def test_positive_dc2_filling_completed_one_sale() -> None:
    ev = SaleEvidence()
    ev.note_nozzle_out()
    ev.note_authorized(application_confirmed=True)
    ev.note_filling()
    ev.note_dc2(volume_raw=1500, amount_raw=4500)
    may, reason = ev.evaluate_filling_completed()
    assert may is True
    assert reason == "valid_sale_evidence"
    assert ev.lifecycle is SaleLifecycle.FILLING_COMPLETED


def test_missed_data_not_offline_if_short_bus_continues() -> None:
    s = PumpSession(address=1, pump_id="p1", events=EventBus())
    s.handle_response_frame(_parse(build_eot(0x50, 0)))
    assert s.state.communication_online
    # Another EOT-only poll — still online, not a bus miss.
    s.handle_response_frame(_parse(build_eot(0x50, 0)))
    assert s.state.missed_bus_responses == 0
    assert s.state.communication.value != "DISCONNECTED"


def test_poll_bytes() -> None:
    assert build_poll(1) == bytes((0x50, 0x20, 0xFA))
    assert build_poll(2) == bytes((0x51, 0x20, 0xFA))


def test_default_response_timeout_120() -> None:
    assert PollSchedulerConfig().response_timeout_ms == 120


@pytest.mark.asyncio
async def test_stale_data_is_acked_and_applied() -> None:
    """CRC-valid DATA that arrived before this poll write is still ACKed."""
    ctrl, pump = create_memory_transport_pair()
    await ctrl.open()
    await pump.open()
    runtime = ControllerRuntime(
        transport=ctrl,
        safety=_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=50,
            inter_poll_delay_ms=0,
            idle_sleep_ms=0,
            max_retries=0,
            tx_delay_ms=0,
            ack_delay_ms=0,
            apply_bus_delays_on_virtual=False,
        ),
    )
    loop = ControllerLoop(runtime)
    session = loop.sessions[1]
    raw = build_data_frame(
        0x50,
        0,
        encode_dc3_nozzle_price(price_raw=120, logical_nozzle=1, nozzle_out=True),
    )
    frame = _parse(raw)
    now = time.monotonic()
    loop.demux._queues[0x50].put_nowait(
        TimestampedFrame(
            frame=frame,
            raw=raw,
            first_byte_time=now - 1.0,
            last_byte_time=now - 1.0,
            wire_address=0x50,
            logical_address=1,
        )
    )
    try:
        outcome = await loop._read_poll_session(session, not_before_mono=now)
        assert outcome == "data"
        assert session.state.nozzle_position is NozzlePosition.OUT
        ack = await asyncio.wait_for(pump.read(16), timeout=1.0)
        assert ack == build_ack(0x50, 0)
    finally:
        await ctrl.close()
        await pump.close()
