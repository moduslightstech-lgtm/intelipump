"""Verified dispensing gate — four-condition rule and nozzle isolation."""

from __future__ import annotations

from intelipump_fdc.controller.verified_dispensing import (
    VerifiedDispensingBook,
    VerifiedPhase,
    volume_increased,
)

STATION = "InteliPump-US-Lab"


def _book() -> VerifiedDispensingBook:
    return VerifiedDispensingBook(station_id=STATION)


def test_lift_only_not_dispensing() -> None:
    book = _book()
    state = book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1", dart_address=1)
    assert state.presentation_status() == "READY"
    assert state.verified_dispensing is False
    assert state.phase is VerifiedPhase.NOZZLE_LIFTED
    assert state.nozzle_lifted_at is not None
    diag = state.diagnostic()
    assert diag["nozzleLifted"] is True
    assert diag["pumpAuthorized"] is False
    assert diag["verifiedDispensing"] is False


def test_lift_plus_authorization_not_dispensing() -> None:
    book = _book()
    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1", dart_address=1)
    state = book.note_authorized(
        pump_id="pump-1",
        nozzle_id="nozzle-1",
        baseline_volume_raw=0,
        dart_address=1,
    )
    assert state.presentation_status() == "READY"
    assert state.verified_dispensing is False
    assert state.phase is VerifiedPhase.AUTHORIZED
    assert state.authorized_at is not None
    assert state.baseline_volume_raw == 0


def test_lift_auth_filling_without_volume_not_dispensing() -> None:
    book = _book()
    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1")
    book.note_authorized(pump_id="pump-1", nozzle_id="nozzle-1", baseline_volume_raw=0)
    state = book.note_dc1_state(pump_id="pump-1", nozzle_id="nozzle-1", dc1_state="FILLING")
    book.note_volume(pump_id="pump-1", nozzle_id="nozzle-1", volume_raw=0)
    state = book.get("pump-1", "nozzle-1")
    assert state is not None
    assert state.verified_dispensing is False
    assert state.phase is VerifiedPhase.FILLING_NO_FLOW
    assert state.presentation_status() == "READY"
    assert state.filling_started_at is not None


def test_all_four_conditions_verified_dispensing() -> None:
    book = _book()
    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1")
    book.note_authorized(pump_id="pump-1", nozzle_id="nozzle-1", baseline_volume_raw=0)
    book.note_dc1_state(pump_id="pump-1", nozzle_id="nozzle-1", dc1_state="FILLING")
    state = book.note_volume(pump_id="pump-1", nozzle_id="nozzle-1", volume_raw=6)
    assert state.verified_dispensing is True
    assert state.phase is VerifiedPhase.VERIFIED_DISPENSING
    assert state.presentation_status() == "DISPENSING"
    assert state.first_volume_increase_at is not None
    tx = book.begin_verified_sale_if_needed(state)
    assert tx is not None
    # Same id on subsequent calls.
    assert book.begin_verified_sale_if_needed(state) == tx
    diag = state.diagnostic()
    assert diag == {
        **diag,
        "nozzleLifted": True,
        "pumpAuthorized": True,
        "dc1State": "FILLING",
        "baselineVolumeLitres": 0.0,
        "currentVolumeLitres": 0.06,
        "volumeIncreased": True,
        "verifiedDispensing": True,
    }


def test_volume_increase_before_filling_is_unintended_flow() -> None:
    book = _book()
    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1")
    book.note_authorized(pump_id="pump-1", nozzle_id="nozzle-1", baseline_volume_raw=0)
    state = book.note_volume(
        pump_id="pump-1",
        nozzle_id="nozzle-1",
        volume_raw=12,
        amount_raw=1400,
        raw_frame="AA BB",
    )
    assert state.verified_dispensing is False
    assert state.possible_unintended_flow is True
    assert state.phase is VerifiedPhase.POSSIBLE_UNINTENDED_FLOW
    assert state.presentation_status() == "POSSIBLE_UNINTENDED_FLOW"
    assert state.preserved_raw_frames == ["AA BB"]
    assert state.transaction_id is None


def test_retained_previous_values_do_not_count_as_new_flow() -> None:
    book = _book()
    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1")
    # Face still shows previous sale 1.74 L after authorize — that is the baseline.
    book.note_authorized(pump_id="pump-1", nozzle_id="nozzle-1", baseline_volume_raw=174)
    book.note_dc1_state(pump_id="pump-1", nozzle_id="nozzle-1", dc1_state="FILLING")
    state = book.note_volume(pump_id="pump-1", nozzle_id="nozzle-1", volume_raw=174)
    assert state.volume_increased is False
    assert state.verified_dispensing is False
    assert book.begin_verified_sale_if_needed(state) is None


def test_lift_then_return_zero_volume_cancelled_no_transaction() -> None:
    book = _book()
    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1")
    book.note_authorized(pump_id="pump-1", nozzle_id="nozzle-1", baseline_volume_raw=0)
    state = book.note_nozzle_returned(pump_id="pump-1", nozzle_id="nozzle-1")
    assert state.phase is VerifiedPhase.CANCELLED_NO_SALE
    assert state.presentation_status() == "CANCELLED_NO_SALE"
    assert state.transaction_id is None
    assert state.completed_at is not None
    assert state.nozzle_returned_at is not None


def test_verified_sale_exactly_one_completed_transaction() -> None:
    book = _book()
    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1")
    book.note_authorized(pump_id="pump-1", nozzle_id="nozzle-1", baseline_volume_raw=0)
    book.note_dc1_state(pump_id="pump-1", nozzle_id="nozzle-1", dc1_state="FILLING")
    book.note_volume(pump_id="pump-1", nozzle_id="nozzle-1", volume_raw=10)
    book.note_volume(pump_id="pump-1", nozzle_id="nozzle-1", volume_raw=25)
    state = book.get("pump-1", "nozzle-1")
    assert state is not None
    tx = book.begin_verified_sale_if_needed(state)
    book.note_volume(pump_id="pump-1", nozzle_id="nozzle-1", volume_raw=40)
    assert state.transaction_id == tx
    done = book.note_nozzle_returned(pump_id="pump-1", nozzle_id="nozzle-1")
    assert done.phase is VerifiedPhase.COMPLETED
    assert done.transaction_id == tx
    assert done.presentation_status() == "COMPLETED"


def test_nozzle_1_and_nozzle_2_remain_independent() -> None:
    book = _book()
    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-1", dart_address=1)
    book.note_authorized(pump_id="pump-1", nozzle_id="nozzle-1", baseline_volume_raw=0)
    book.note_dc1_state(pump_id="pump-1", nozzle_id="nozzle-1", dc1_state="FILLING")
    book.note_volume(pump_id="pump-1", nozzle_id="nozzle-1", volume_raw=20)

    book.note_nozzle_lifted(pump_id="pump-1", nozzle_id="nozzle-2", dart_address=2)
    book.note_authorized(pump_id="pump-1", nozzle_id="nozzle-2", baseline_volume_raw=0)

    n1 = book.get("pump-1", "nozzle-1")
    n2 = book.get("pump-1", "nozzle-2")
    assert n1 is not None and n2 is not None
    assert n1.verified_dispensing is True
    assert n2.verified_dispensing is False
    assert n2.presentation_status() == "READY"
    assert n1.key != n2.key


def test_volume_tolerance_is_protocol_precision_not_min_sale() -> None:
    assert volume_increased(baseline_raw=100, current_raw=100) is False
    assert volume_increased(baseline_raw=100, current_raw=101) is False  # within 1 raw
    assert volume_increased(baseline_raw=100, current_raw=102) is True
