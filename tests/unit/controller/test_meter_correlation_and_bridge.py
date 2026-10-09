"""Meter request/response correlation and exclusive file-bridge ownership."""

from __future__ import annotations

import json

import pytest

from intelipump_fdc.controller import meter_read_request as bridge
from intelipump_fdc.controller.meter_read_request import MeterReadRequest
from intelipump_fdc.controller.session_models import NozzlePosition
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.services import meter_reading as meter_svc


def test_dc101_matches_pending_requires_coun_address_and_window() -> None:
    decoded = {"counter_select": 1, "raw_scaled": {"total_value": 100}}
    ok, reason = meter_svc.dc101_matches_pending(
        decoded=decoded,
        observed_at_mono=20.0,
        expected_address=1,
        observed_address=1,
        expected_coun=1,
        queued_at_mono=10.0,
        tx_started_at_mono=15.0,
    )
    assert ok and reason == "matched"

    ok2, reason2 = meter_svc.dc101_matches_pending(
        decoded={"counter_select": 2},
        observed_at_mono=20.0,
        expected_address=1,
        observed_address=1,
        expected_coun=1,
        queued_at_mono=10.0,
        tx_started_at_mono=15.0,
    )
    assert not ok2 and reason2 == "wrong_coun"

    ok3, reason3 = meter_svc.dc101_matches_pending(
        decoded=decoded,
        observed_at_mono=12.0,  # before TX
        expected_address=1,
        observed_address=1,
        expected_coun=1,
        queued_at_mono=10.0,
        tx_started_at_mono=15.0,
    )
    assert not ok3 and reason3 == "before_request_window"

    ok4, reason4 = meter_svc.dc101_matches_pending(
        decoded=decoded,
        observed_at_mono=20.0,
        expected_address=1,
        observed_address=2,
        expected_coun=1,
        queued_at_mono=10.0,
        tx_started_at_mono=15.0,
    )
    assert not ok4 and reason4 == "wrong_address"


def test_late_reply_cannot_satisfy_newer_request_window() -> None:
    """Observation stamped before newer TX must not match."""
    ok, reason = meter_svc.dc101_matches_pending(
        decoded={"counter_select": 1},
        observed_at_mono=50.0,
        expected_address=1,
        observed_address=1,
        expected_coun=1,
        queued_at_mono=60.0,
        tx_started_at_mono=70.0,
    )
    assert not ok and reason == "before_request_window"


def test_timed_out_request_late_dc101_blocked_by_quarantine_floor() -> None:
    """Late DC101 after timeout must not match the next same-address/COUN TX.

    Protocol limitation: no request UUID on the wire. Quarantine raises the
    acceptance floor so a reply belonging to the timed-out CD101 cannot bind.
    """
    # Prior request timed out at t=100; quarantine until t=108.
    # Next TX starts at t=110. Late reply stamped t=105 (during quarantine)
    # must not match even though it is after the new TX... wait, 105 < 110.
    # Critical case: late reply arrives at t=112 after new TX at 110 but still
    # below quarantine floor if we extended quarantine past TX — floor is
    # max(tx_started, quarantine_until). If quarantine_until=108 and TX=110,
    # floor=110; reply at 112 matches. So quarantine alone isn't enough after
    # TX — we also refuse new TX until quarantine ends (controller gate).
    ok, reason = meter_svc.dc101_matches_pending(
        decoded={"counter_select": 1},
        observed_at_mono=105.0,
        expected_address=1,
        observed_address=1,
        expected_coun=1,
        queued_at_mono=100.0,
        tx_started_at_mono=102.0,
        quarantine_until_mono=108.0,
    )
    assert not ok and reason == "before_request_window"

    # Reply after both TX and quarantine floor is allowed by matcher; controller
    # still marks CAPTURED_AMBIGUOUS when a prior timeout was recent.
    ok2, reason2 = meter_svc.dc101_matches_pending(
        decoded={"counter_select": 1},
        observed_at_mono=112.0,
        expected_address=1,
        observed_address=1,
        expected_coun=1,
        queued_at_mono=109.0,
        tx_started_at_mono=110.0,
        quarantine_until_mono=108.0,
    )
    assert ok2 and reason2 == "matched"


def test_recommended_nozzle_freshness_from_poll_timing() -> None:
    age = meter_svc.recommended_nozzle_in_max_age_s(
        address_count=2,
        response_timeout_ms=120,
        inter_poll_delay_ms=5,
        idle_sleep_ms=20,
        tx_delay_ms=35,
        rounds=40,
    )
    # per_addr=160ms → round=340ms → 40 rounds = 13.6s; function uses that.
    assert 10.0 <= age <= 30.0


def test_eligibility_refuses_unknown_stale_out_and_sale() -> None:
    now = 1_000.0
    ok, code, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.READY,
        nozzle_position=NozzlePosition.UNKNOWN,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
    )
    assert not ok and code == "METER_READ_REFUSED_NOZZLE_UNKNOWN"

    ok2, code2, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.READY,
        nozzle_position=NozzlePosition.OUT,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
    )
    assert not ok2 and code2 == "METER_READ_DEFERRED_NOZZLE_OUT"

    ok3, code3, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.READY,
        nozzle_position=NozzlePosition.IN,
        last_nozio_mono=now - 400,
        now_mono=now,
        nozzle_in_max_age_s=300,
    )
    assert not ok3 and code3 == "METER_READ_REFUSED_NOZZLE_STALE"

    ok4, code4, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.READY,
        nozzle_position=NozzlePosition.IN,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
        held_completion=True,
    )
    assert not ok4 and code4 == "METER_READ_DEFERRED_COMPLETION_HOLD"

    ok5, code5, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.NOZZLE_UP,
        nozzle_position=NozzlePosition.OUT,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
    )
    assert not ok5 and code5 == "METER_READ_DEFERRED_DISPENSING"

    ok6, _, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.READY,
        nozzle_position=NozzlePosition.IN,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
        sale_lifecycle="IDLE",
    )
    assert ok6

    # Post-hangup idle: SM stays FILLING_COMPLETE (CANCELLED_NO_SALE or face
    # hold). Must allow CD101 when nozzle IN and no live sale/hold.
    ok7, _, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.FILLING_COMPLETE,
        nozzle_position=NozzlePosition.IN,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
        sale_lifecycle="CANCELLED_NO_SALE",
    )
    assert ok7

    ok8, _, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.FILLING_COMPLETE,
        nozzle_position=NozzlePosition.IN,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
        sale_lifecycle="FILLING_COMPLETED",
    )
    assert ok8

    ok9, code9, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.AUTHORIZED,
        nozzle_position=NozzlePosition.IN,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
        sale_lifecycle="AUTHORIZED",
    )
    assert not ok9 and code9 == "METER_READ_DEFERRED_DISPENSING"

    ok10, code10, _ = meter_svc.evaluate_meter_tx_eligibility(
        current_state=PumpState.READY,
        nozzle_position=NozzlePosition.IN,
        last_nozio_mono=now,
        now_mono=now,
        nozzle_in_max_age_s=300,
        sale_lifecycle="FILLING",
    )
    assert not ok10 and code10 == "METER_READ_DEFERRED_SALE_LIFECYCLE"


def test_liters_only_when_volume_coun_and_decimals(tmp_path, monkeypatch) -> None:
    assert meter_svc.liters_from_raw_scaled(1959090277, 3, counter_select=1) == 1959090.277
    assert meter_svc.liters_from_raw_scaled(1959090277, 3, counter_select=0x11) is None
    assert meter_svc.liters_from_raw_scaled(1959090277, None, counter_select=1) is None
    flags = meter_svc.build_capture_flags(
        counter_select=1, volume_decimals=3, liters=1.0
    )
    assert flags["scaleVerified"] is True
    assert flags["nozzleMappingVerified"] is False


def test_exclusive_request_and_correlation_scoped_result(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("INTELIPUMP_METER_READ_REQUEST_DIR", str(tmp_path))
    r1 = MeterReadRequest(correlation_id="aaa-111", dart_address=1, counter_select=1)
    path = bridge.write_meter_read_request(r1)
    assert path.is_file()
    with pytest.raises(FileExistsError):
        bridge.write_meter_read_request(
            MeterReadRequest(correlation_id="bbb-222", dart_address=2, counter_select=1)
        )

    claimed = bridge.claim_meter_read_request()
    assert claimed is not None and claimed.correlation_id == "aaa-111"
    assert not bridge.request_path().is_file()
    assert bridge.inflight_path().is_file()

    # Concurrent write still refused while inflight.
    with pytest.raises(FileExistsError):
        bridge.write_meter_read_request(
            MeterReadRequest(correlation_id="ccc-333", dart_address=1, counter_select=1)
        )

    bridge.write_meter_read_result(
        {
            "correlationId": "aaa-111",
            "status": "CAPTURED",
            "cumulativeVolumeRaw": 42,
            "volumeLiters": None,
        }
    )
    # Stale latest from another id must not satisfy waiters for aaa-111 after rewrite.
    bridge.write_meter_read_result(
        {
            "correlationId": "other-999",
            "status": "CAPTURED",
            "cumulativeVolumeRaw": 99,
        }
    )
    mine = bridge.read_meter_read_result(correlation_id="aaa-111")
    assert mine is not None
    assert mine["correlationId"] == "aaa-111"
    assert mine["cumulativeVolumeRaw"] == 42
    other = bridge.read_meter_read_result(correlation_id="other-999")
    assert other is not None and other["cumulativeVolumeRaw"] == 99

    bridge.clear_meter_read_request()
    assert not bridge.inflight_path().is_file()
    # Fresh request after clear
    bridge.write_meter_read_request(
        MeterReadRequest(correlation_id="ddd-444", dart_address=2, counter_select=1)
    )
    assert json.loads(bridge.request_path().read_text())["dartAddress"] == 2
