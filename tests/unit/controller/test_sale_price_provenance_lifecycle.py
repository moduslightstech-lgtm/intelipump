"""Sale-price provenance: CD5 link-ack seeds face; amount÷volume never does."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceRequest,
    write_set_price_request,
)
from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.exchange_result import ExchangeResultStatus
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.controller.session_models import NozzlePosition, ObservedStatus
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.state_machine.models import ObservationRef
from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation


def _lab_safety() -> ControllerSafetyContext:
    return ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.LISTEN_ONLY,
        active_commands_enabled=False,
        require_physical_control_enable=True,
        physical_enable_present=False,
        allow_virtual_polling=True,
        allow_lab_simulator_commands=True,
        owned_lab_active_session=True,
    )


def _dual_addr_loop() -> ControllerLoop:
    ctrl, _pump = create_memory_transport_pair()
    return ControllerLoop(
        ControllerRuntime(
            transport=ctrl,
            safety=_lab_safety(),
            config=PollSchedulerConfig(addresses=(1, 2)),
            logical_nozzle_count=1,
        )
    )


def _fresh_in(session, *, status: ObservedStatus = ObservedStatus.RESET) -> None:
    import time

    session.state.observed_status = status
    session.state.nozzle_position = NozzlePosition.IN
    now = time.monotonic()
    session.state.last_nozio_time = now
    session.state.last_status_time = now


def _write_price(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corr: str,
    price: int,
    pump: str = "pump-5",
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_request(
        SetPriceRequest(
            correlation_id=corr,
            command_id=f"cmd-{corr}",
            unit_price_raw=price,
            prices_raw=(price,),
            requested_by="admin@example.com",
            pump_id=pump,
        )
    )


@pytest.mark.asyncio
async def test_link_ack_seeds_provisional_face_when_dc3_idle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LINK_ACK seeds unit_price_raw so hang-up sales are not price-Unknown."""
    _write_price(tmp_path, monkeypatch, "corr-link-only", 1355)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = None
        loop.sessions[addr].state.unit_price_obs_gen = 0

    link = AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.LINK_ACKNOWLEDGED},
        )()
    )
    loop._run_owned_command = link  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()

    for addr in (1, 2):
        st = loop.sessions[addr].state
        assert st.link_acked_unit_price_raw == 1355
        assert st.requested_unit_price_raw == 1355
        assert st.application_confirmed_unit_price_raw is None
        assert st.unit_price_raw == 1355  # provisional face for idle DC3

    # Sale hang-up without positive DC3 still has programmed face.
    session = loop.sessions[1]
    session.state.filled_volume_raw = 74
    session.state.filled_amount_raw = 100000
    assert session.state.unit_price_raw == 1355


@pytest.mark.asyncio
async def test_link_ack_replaces_stale_observed_across_price_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Price-change lifecycle: old face is replaced by the new CD5 command."""
    _write_price(tmp_path, monkeypatch, "corr-chg", 1400)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 1355
        loop.sessions[addr].state.unit_price_obs_gen = 3

    link = AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.LINK_ACKNOWLEDGED},
        )()
    )
    loop._run_owned_command = link  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()

    for addr in (1, 2):
        st = loop.sessions[addr].state
        assert st.link_acked_unit_price_raw == 1400
        assert st.unit_price_raw == 1400


@pytest.mark.asyncio
async def test_delayed_dc3_and_idle_zeros_scoped_per_address(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Idle DC3=0 must not wipe positive observation; addresses stay independent."""
    _write_price(tmp_path, monkeypatch, "corr-scoped", 1355)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = None
        loop.sessions[addr].state.unit_price_obs_gen = 0

    link = AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.LINK_ACKNOWLEDGED},
        )()
    )
    loop._run_owned_command = link  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    # LINK_ACK seeds provisional 1355 on both addresses.
    assert loop.sessions[1].state.unit_price_raw == 1355
    assert loop.sessions[2].state.unit_price_raw == 1355

    # Addr1 keeps face; clear addr2 to simulate missing seed then idle zeros.
    loop.sessions[1].state.unit_price_obs_gen = 2
    loop.sessions[2].state.unit_price_raw = None
    loop.sessions[2].state.unit_price_obs_gen = 1

    # Idle zero frames on both — preserve addr1 evidence only.
    for addr in (1, 2):
        loop.sessions[addr]._update_observed_from_mapped(
            MappedWayneObservation(
                event=PumpEvent.NOZZLE_STATUS_OBSERVED,
                observation=ObservationRef(source_frame_raw_hex=f"dc3-z-{addr}"),
                nozzle_out=False,
                filling_price_raw=0,
            ),
            capture_mono=0.0,
        )
    assert loop.sessions[1].state.unit_price_raw == 1355
    assert loop.sessions[2].state.unit_price_raw is None

    # Delayed matching DC3 on addr2.
    loop.sessions[2]._update_observed_from_mapped(
        MappedWayneObservation(
            event=PumpEvent.NOZZLE_STATUS_OBSERVED,
            observation=ObservationRef(source_frame_raw_hex="dc3-late-2"),
            nozzle_out=False,
            filling_price_raw=1355,
        ),
        capture_mono=1.0,
    )
    assert loop.sessions[2].state.unit_price_raw == 1355
    assert loop.sessions[1].state.unit_price_raw == 1355


@pytest.mark.asyncio
async def test_two_addresses_different_confirmed_prices_independent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each dart address keeps its own pump-observed sale price."""
    loop = _dual_addr_loop()
    loop.sessions[1].state.unit_price_raw = 1355
    loop.sessions[1].state.application_confirmed_unit_price_raw = 1355
    loop.sessions[2].state.unit_price_raw = 1400
    loop.sessions[2].state.application_confirmed_unit_price_raw = 1400

    assert loop.sessions[1].state.unit_price_raw == 1355
    assert loop.sessions[2].state.unit_price_raw == 1400

    # Idle zero on addr1 must not affect addr2.
    loop.sessions[1]._update_observed_from_mapped(
        MappedWayneObservation(
            event=PumpEvent.NOZZLE_STATUS_OBSERVED,
            observation=ObservationRef(source_frame_raw_hex="dc3-z-1"),
            nozzle_out=False,
            filling_price_raw=0,
        ),
        capture_mono=0.0,
    )
    assert loop.sessions[1].state.unit_price_raw == 1355
    assert loop.sessions[2].state.unit_price_raw == 1400
