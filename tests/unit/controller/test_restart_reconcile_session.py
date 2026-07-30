"""Restart / reconnect reconciliation wired through PumpSession."""

from __future__ import annotations

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.reconciliation import (
    LiveObservationSummary,
    reconcile_after_restart,
)
from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation


def _session() -> PumpSession:
    return PumpSession(address=1, pump_id="pump-1", events=EventBus())


def _persisted(**kwargs: object) -> PumpContext:
    base: dict[str, object] = dict(
        pump_id="pump-1",
        dart_address=1,
        current_state=PumpState.FILLING,
        active_transaction_id="open-live",
        has_unresolved_transaction=True,
        communication_healthy=False,
        selected_nozzle=1,
        nozzle_out=True,
        dispensed_volume_raw=900,
        last_raw_wayne_status=4,
        state_version=4,
    )
    base.update(kwargs)
    return PumpContext(**base)  # type: ignore[arg-type]


def test_seed_recovery_arms_reconcile_and_preserves_tx() -> None:
    s = _session()
    s.seed_recovered_context(_persisted())
    assert s._needs_restart_reconcile is True
    assert s.machine.context.active_transaction_id == "open-live"
    assert s.machine.context.communication_healthy is False
    assert s.machine.context.current_state is PumpState.FILLING


def test_restart_while_filling_restores_same_transaction() -> None:
    s = _session()
    s.seed_recovered_context(_persisted())
    s._reconcile_then_apply(
        MappedWayneObservation(
            event=PumpEvent.FILLING_UPDATED,
            observation=ObservationRef(source_frame_raw_hex="ff01"),
            raw_wayne_status=int(WaynePumpStatus.FILLING),
            nozzle_out=True,
        ),
        raw_volume=950,
    )
    assert s._needs_restart_reconcile is False
    assert s.machine.context.current_state is PumpState.FILLING
    assert s.machine.context.active_transaction_id == "open-live"
    assert s.machine.context.communication_healthy is True


def test_reconnect_while_filling_does_not_create_duplicate_tx_id() -> None:
    s = _session()
    s.seed_recovered_context(_persisted())
    first_id = s.machine.context.active_transaction_id
    s._reconcile_then_apply(
        MappedWayneObservation(
            event=PumpEvent.FILLING_STARTED,
            observation=ObservationRef(source_frame_raw_hex="ff02"),
            raw_wayne_status=int(WaynePumpStatus.FILLING),
        ),
        raw_volume=900,
    )
    assert s.machine.context.active_transaction_id == first_id


def test_restart_after_pump_completed_finalizes_once() -> None:
    bus = EventBus()
    keys: list[str] = []

    def _cap(event: ControllerEvent) -> None:
        if event.type is ControllerEventType.STATE_CHANGED:
            key = (event.payload or {}).get("completion_evidence_key")
            if key:
                keys.append(str(key))

    bus.add_subscriber(_cap)
    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    s.seed_recovered_context(_persisted())
    s._reconcile_then_apply(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="ff03"),
            raw_wayne_status=int(WaynePumpStatus.FILLING_COMPLETED),
            completion_evidence_key="complete:ff03:5",
        ),
        raw_volume=1200,
    )
    assert s.machine.context.current_state is PumpState.FILLING_COMPLETE
    assert keys == ["restart-complete:open-live"]


def test_reset_plus_nozzle_in_does_not_derive_ready() -> None:
    result = reconcile_after_restart(
        _persisted(),
        LiveObservationSummary(
            communication_healthy=True,
            wayne_status=int(WaynePumpStatus.RESET),
            normalized_hint=PumpState.RESET,
            nozzle_out=False,
        ),
        had_active_transaction_persisted=True,
    )
    assert result.recovered_state is PumpState.RESET
    assert result.context.current_state is not PumpState.READY
    assert result.context.active_transaction_id == "open-live"
    assert result.context.has_unresolved_transaction is True
    assert result.auto_authorize_attempted is False
    assert any("do not derive READY" in w for w in result.warnings)


def test_reset_plus_nozzle_out_does_not_derive_ready() -> None:
    result = reconcile_after_restart(
        _persisted(),
        LiveObservationSummary(
            communication_healthy=True,
            wayne_status=int(WaynePumpStatus.RESET),
            normalized_hint=PumpState.RESET,
            nozzle_out=True,
        ),
        had_active_transaction_persisted=True,
    )
    assert result.recovered_state is PumpState.RESET
    assert result.context.current_state is not PumpState.READY
    assert any("nozzle OUT" in w for w in result.warnings)


def test_missing_communication_leaves_unresolved() -> None:
    result = reconcile_after_restart(
        _persisted(),
        LiveObservationSummary(communication_healthy=False),
    )
    assert result.recovered_state is PumpState.DISCONNECTED
    assert result.preserved_unresolved_transaction_id == "open-live"
    assert result.auto_authorize_attempted is False
    assert result.pending_commands_replayed is False


def test_communication_lost_preserves_active_transaction() -> None:
    from intelipump_fdc.state_machine.machine import PumpStateMachine

    machine = PumpStateMachine(_persisted(communication_healthy=True))
    result = machine.apply(PumpEvent.COMMUNICATION_LOST)
    assert result.current_state is PumpState.DISCONNECTED
    assert result.context.active_transaction_id == "open-live"
    assert result.context.has_unresolved_transaction is True


def test_no_auto_authorize_or_reset_on_restart() -> None:
    from intelipump_fdc.domain.pump_command import PumpCommand

    result = reconcile_after_restart(
        _persisted(current_state=PumpState.NOZZLE_UP),
        LiveObservationSummary(
            communication_healthy=True,
            wayne_status=int(WaynePumpStatus.RESET),
            nozzle_out=False,
        ),
        pending_commands=(PumpCommand.AUTHORIZE, PumpCommand.RESET),
    )
    assert result.auto_authorize_attempted is False
    assert result.pending_commands_replayed is False
    assert "AUTHORIZE" in result.discarded_pending_commands
    assert "RESET" in result.discarded_pending_commands
