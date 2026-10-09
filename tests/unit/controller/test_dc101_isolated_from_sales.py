"""DC101 totalizer replies must never enter the sale / SM / persistence path."""

from __future__ import annotations

from unittest.mock import MagicMock

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import ControllerEventType, EventBus
from intelipump_fdc.controller.session_models import NozzlePosition
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.simulator.encoding import encode_dc101_totals, encode_dc2_volume_amount


def _parse(raw: bytes):
    return parse_frame(raw)


def test_dc101_stores_evidence_without_touching_sale_face() -> None:
    bus = EventBus()
    s = PumpSession(address=1, pump_id="pump-2", events=bus)
    s.state.filled_volume_raw = 0
    s.state.filled_amount_raw = 0
    s.state.nozzle_position = NozzlePosition.IN

    frame = _parse(
        build_data_frame(
            0x50,
            0,
            encode_dc101_totals(
                counter_select=1,
                total_value_raw=1_959_090_277,
                total_meter1_raw=1_959_090_277,
                total_meter2_raw=0,
            ),
        )
    )
    s.handle_response_frame(frame)

    assert s.state.last_dc101 is not None
    raw = (s.state.last_dc101.get("raw_scaled") or {}).get("total_value")
    assert raw == 1_959_090_277
    assert s.state.filled_volume_raw == 0
    assert s.state.filled_amount_raw == 0
    assert s.machine.context.current_state not in {
        PumpState.AUTHORIZED,
        PumpState.FILLING,
        PumpState.NOZZLE_UP,
        PumpState.FILLING_COMPLETE,
        PumpState.LIMIT_REACHED,
    }
    assert s.state.nozzle_position is NozzlePosition.IN
    assert s.state.sale_evidence.has_positive_delivery is False


def test_dc2_still_updates_sale_face_after_dc101_isolation() -> None:
    bus = EventBus()
    s = PumpSession(address=2, pump_id="pump-2", events=bus)
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x51,
                0,
                encode_dc101_totals(
                    counter_select=1,
                    total_value_raw=100_000,
                    total_meter1_raw=100_000,
                    total_meter2_raw=0,
                ),
            )
        )
    )
    assert s.state.filled_volume_raw == 0

    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x51,
                1,
                encode_dc2_volume_amount(volume_raw=221, amount_raw=300_000),
            )
        )
    )
    assert s.state.filled_volume_raw == 221
    assert s.state.filled_amount_raw == 300_000


def test_mixed_dc101_then_dc2_does_not_bleed_totalizer_into_face() -> None:
    """Sequential frames in one session: totalizer then sale face."""
    bus = EventBus()
    s = PumpSession(address=1, pump_id="pump-2", events=bus)
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                0,
                encode_dc101_totals(
                    counter_select=1,
                    total_value_raw=9_999_999,
                    total_meter1_raw=9_999_999,
                    total_meter2_raw=1,
                ),
            )
        )
    )
    assert s.state.filled_volume_raw == 0
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                1,
                encode_dc2_volume_amount(volume_raw=112, amount_raw=150_000),
            )
        )
    )
    assert s.state.filled_volume_raw == 112
    assert (s.state.last_dc101.get("raw_scaled") or {}).get("total_value") == 9_999_999


def test_dc101_decoded_event_not_submitted_to_sale_worker() -> None:
    bridge = PersistenceBridge.__new__(PersistenceBridge)
    bridge._live = None
    bridge._worker = MagicMock()
    bridge._channel_identity = lambda _a: (None, None, None)  # type: ignore[method-assign]
    bridge._tx_by_address = {}
    bridge._station_id = "SAO"
    bridge._environment = "PRODUCTION"
    bridge._simulated = False

    bus = EventBus()
    s = PumpSession(address=1, pump_id="pump-2", events=bus)
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                0,
                encode_dc101_totals(
                    counter_select=1,
                    total_value_raw=12345,
                    total_meter1_raw=12345,
                    total_meter2_raw=0,
                ),
            )
        )
    )
    decoded = [
        e
        for e in bus.events
        if e.type is ControllerEventType.APPLICATION_TRANSACTION_DECODED
        and e.detail == "DC101_TOTAL_COUNTERS"
    ]
    assert decoded, "diagnostics decode event should still be published"
    for event in decoded:
        bridge.on_event(event)
    bridge._worker.submit.assert_not_called()


def test_sale_before_and_after_meter_read_face() -> None:
    bus = EventBus()
    s = PumpSession(address=1, pump_id="pump-2", events=bus)
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                0,
                encode_dc2_volume_amount(volume_raw=50, amount_raw=50_000),
            )
        )
    )
    assert s.state.filled_volume_raw == 50
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                1,
                encode_dc101_totals(
                    counter_select=1,
                    total_value_raw=888_888,
                    total_meter1_raw=888_888,
                    total_meter2_raw=0,
                ),
            )
        )
    )
    assert s.state.filled_volume_raw == 50  # meter must not clobber prior face
    s.handle_response_frame(
        _parse(
            build_data_frame(
                0x50,
                2,
                encode_dc2_volume_amount(volume_raw=75, amount_raw=75_000),
            )
        )
    )
    assert s.state.filled_volume_raw == 75
