"""AGO whole-naira amounts must rescale when pump-observed price arrives late."""

from __future__ import annotations

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.state_machine.models import ObservationRef
from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation


def test_late_dc3_price_rescales_ago_wire_amount_before_hangup_publish() -> None:
    """8.00 L wire ₦15000 + later face 1875 → ledger 1500000 (₦15,000.00)."""
    s = PumpSession(address=1, pump_id="pump-8", events=EventBus())
    # DC2 ticks while sale price not yet observed (LINK_ACK must not seed it).
    s.state.filled_volume_raw = 800
    s.state.filled_amount_raw = 15000
    s.state.sale_evidence.peak_volume_raw = 800
    s.state.sale_evidence.peak_amount_raw = 15000
    assert s.state.unit_price_raw is None

    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.NOZZLE_STATUS_OBSERVED,
            observation=ObservationRef(source_frame_raw_hex="dc3-ago"),
            nozzle_out=False,
            filling_price_raw=1875,
        )
    )
    assert s.state.unit_price_raw == 1875
    assert s.state.filled_amount_raw == 1500000
    assert s.state.sale_evidence.peak_amount_raw == 1500000
