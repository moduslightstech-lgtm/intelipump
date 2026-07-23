"""Unit tests for restart reconciliation."""

from __future__ import annotations

from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.models import PumpContext
from intelipump_fdc.state_machine.reconciliation import (
    LiveObservationSummary,
    reconcile_after_restart,
)


def _persisted(**kwargs: object) -> PumpContext:
    base = dict(
        pump_id="p1",
        dart_address=1,
        current_state=PumpState.READY,
        communication_healthy=True,
        state_version=9,
    )
    base.update(kwargs)
    return PumpContext(**base)  # type: ignore[arg-type]


def test_restart_while_persisted_filling_prefers_live() -> None:
    result = reconcile_after_restart(
        _persisted(
            current_state=PumpState.FILLING,
            active_transaction_id="tx-99",
        ),
        LiveObservationSummary(
            communication_healthy=True,
            wayne_status=WaynePumpStatus.FILLING_COMPLETED,
        ),
        had_active_transaction_persisted=True,
    )
    assert result.recovered_state is PumpState.FILLING_COMPLETE
    assert result.preserved_unresolved_transaction_id == "tx-99"
    assert result.auto_authorize_attempted is False
    assert any("FILLING_COMPLETE" in w for w in result.warnings)


def test_restart_while_persisted_authorize_pending_never_replays() -> None:
    result = reconcile_after_restart(
        _persisted(current_state=PumpState.NOZZLE_UP),
        LiveObservationSummary(
            communication_healthy=True,
            wayne_status=WaynePumpStatus.RESET,
        ),
        pending_commands=(PumpCommand.AUTHORIZE,),
    )
    assert result.auto_authorize_attempted is False
    assert result.pending_commands_replayed is False
    assert "AUTHORIZE" in result.discarded_pending_commands
    assert result.recovered_state is PumpState.RESET
    assert result.context.current_state is not PumpState.AUTHORIZED
    assert any("AUTHORIZE" in w for w in result.warnings)


def test_restart_without_communication() -> None:
    result = reconcile_after_restart(
        _persisted(current_state=PumpState.FILLING, active_transaction_id="tx-1"),
        LiveObservationSummary(communication_healthy=False),
    )
    assert result.recovered_state is PumpState.DISCONNECTED
    assert result.context.communication_healthy is False
    assert result.preserved_unresolved_transaction_id == "tx-1"


def test_restart_unknown_live_remains_discovering() -> None:
    result = reconcile_after_restart(
        _persisted(current_state=PumpState.AUTHORIZED),
        LiveObservationSummary(
            communication_healthy=True,
            wayne_status=None,
        ),
    )
    assert result.recovered_state is PumpState.DISCOVERING
    assert result.context.price_verified is False


def test_restart_live_filling_recovers_with_warning() -> None:
    result = reconcile_after_restart(
        _persisted(current_state=PumpState.READY),
        LiveObservationSummary(
            communication_healthy=True,
            wayne_status=WaynePumpStatus.FILLING,
        ),
    )
    assert result.recovered_state is PumpState.FILLING
    assert any("FILLING" in w for w in result.warnings)
