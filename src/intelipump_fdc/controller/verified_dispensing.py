"""Verified dispensing gate: lift ∧ authorize ∧ DC1 FILLING ∧ volume increase.

Financial sales and DISPENSING publishes require all four conditions. Volume
movement without DC1 FILLING is preserved as POSSIBLE_UNINTENDED_FLOW — never
as a normal sale. State is keyed by stationId + pumpId + nozzleId.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

# Wayne DC2 uses 2 decimal places (1 raw = 0.01 L). Allow 1 raw unit of
# protocol noise — not a minimum-sale threshold.
VOLUME_TOLERANCE_RAW = 1


class VerifiedPhase(StrEnum):
    IDLE = "IDLE"
    NOZZLE_LIFTED = "NOZZLE_LIFTED"
    AUTHORIZED = "AUTHORIZED"  # Ready — not dispensing
    FILLING_NO_FLOW = "FILLING_NO_FLOW"  # DC1 FILLING but volume flat
    VERIFIED_DISPENSING = "VERIFIED_DISPENSING"
    COMPLETED = "COMPLETED"
    CANCELLED_NO_SALE = "CANCELLED_NO_SALE"
    POSSIBLE_UNINTENDED_FLOW = "POSSIBLE_UNINTENDED_FLOW"


def session_key(station_id: str, pump_id: str, nozzle_id: str) -> str:
    return f"{station_id.strip()}|{pump_id.strip()}|{nozzle_id.strip()}"


def _now() -> datetime:
    return datetime.now(UTC)


def volume_increased(
    *,
    baseline_raw: int | None,
    current_raw: int | None,
    tolerance: int = VOLUME_TOLERANCE_RAW,
) -> bool:
    if baseline_raw is None or current_raw is None:
        return False
    return int(current_raw) > int(baseline_raw) + int(tolerance)


@dataclass(slots=True)
class NozzleVerifiedState:
    station_id: str
    pump_id: str
    nozzle_id: str
    dart_address: int = 0

    nozzle_lifted: bool = False
    pump_authorized: bool = False
    dc1_state: str = "UNKNOWN"
    baseline_volume_raw: int | None = None
    current_volume_raw: int | None = None

    nozzle_lifted_at: datetime | None = None
    authorized_at: datetime | None = None
    filling_started_at: datetime | None = None
    first_volume_increase_at: datetime | None = None
    last_volume_increase_at: datetime | None = None
    nozzle_returned_at: datetime | None = None
    completed_at: datetime | None = None

    transaction_id: str | None = None
    started_published: bool = False
    possible_unintended_flow: bool = False
    preserved_raw_frames: list[str] = field(default_factory=list)
    preserved_volume_raw: int | None = None
    preserved_amount_raw: int | None = None
    phase: VerifiedPhase = VerifiedPhase.IDLE

    @property
    def key(self) -> str:
        return session_key(self.station_id, self.pump_id, self.nozzle_id)

    @property
    def volume_increased(self) -> bool:
        return volume_increased(
            baseline_raw=self.baseline_volume_raw,
            current_raw=self.current_volume_raw,
        )

    @property
    def verified_dispensing(self) -> bool:
        return (
            self.nozzle_lifted
            and self.pump_authorized
            and self.dc1_state.upper() == "FILLING"
            and self.volume_increased
        )

    def baseline_volume_litres(self, decimals: int = 2) -> float | None:
        if self.baseline_volume_raw is None:
            return None
        return round(self.baseline_volume_raw / (10**decimals), decimals)

    def current_volume_litres(self, decimals: int = 2) -> float | None:
        if self.current_volume_raw is None:
            return None
        return round(self.current_volume_raw / (10**decimals), decimals)

    def diagnostic(self, *, decimals: int = 2) -> dict[str, Any]:
        return {
            "stationId": self.station_id,
            "pumpId": self.pump_id,
            "nozzleId": self.nozzle_id,
            "dartAddress": self.dart_address,
            "nozzleLifted": self.nozzle_lifted,
            "pumpAuthorized": self.pump_authorized,
            "dc1State": self.dc1_state,
            "baselineVolumeLitres": self.baseline_volume_litres(decimals),
            "currentVolumeLitres": self.current_volume_litres(decimals),
            "volumeIncreased": self.volume_increased,
            "verifiedDispensing": self.verified_dispensing,
            "phase": self.phase.value,
            "transactionId": self.transaction_id,
            "timestamps": {
                "nozzleLiftedAt": _iso(self.nozzle_lifted_at),
                "authorizedAt": _iso(self.authorized_at),
                "fillingStartedAt": _iso(self.filling_started_at),
                "firstVolumeIncreaseAt": _iso(self.first_volume_increase_at),
                "lastVolumeIncreaseAt": _iso(self.last_volume_increase_at),
                "nozzleReturnedAt": _iso(self.nozzle_returned_at),
                "completedAt": _iso(self.completed_at),
            },
            "possibleUnintendedFlow": self.possible_unintended_flow,
        }

    def presentation_status(self) -> str:
        """Cloud/dashboard presentation status for this nozzle."""
        if self.phase is VerifiedPhase.VERIFIED_DISPENSING:
            return "DISPENSING"
        if self.phase is VerifiedPhase.COMPLETED:
            return "COMPLETED"
        if self.phase is VerifiedPhase.CANCELLED_NO_SALE:
            return "CANCELLED_NO_SALE"
        if self.phase is VerifiedPhase.POSSIBLE_UNINTENDED_FLOW:
            return "POSSIBLE_UNINTENDED_FLOW"
        if self.phase in {
            VerifiedPhase.NOZZLE_LIFTED,
            VerifiedPhase.AUTHORIZED,
            VerifiedPhase.FILLING_NO_FLOW,
        }:
            return "READY"
        return "IDLE"


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


@dataclass
class VerifiedDispensingBook:
    """Per-hose verified dispensing tracker."""

    station_id: str
    _by_key: dict[str, NozzleVerifiedState] = field(default_factory=dict)

    def get(self, pump_id: str, nozzle_id: str) -> NozzleVerifiedState | None:
        return self._by_key.get(session_key(self.station_id, pump_id, nozzle_id))

    def get_or_create(
        self,
        *,
        pump_id: str,
        nozzle_id: str,
        dart_address: int = 0,
    ) -> NozzleVerifiedState:
        key = session_key(self.station_id, pump_id, nozzle_id)
        state = self._by_key.get(key)
        if state is None:
            state = NozzleVerifiedState(
                station_id=self.station_id,
                pump_id=pump_id,
                nozzle_id=nozzle_id,
                dart_address=dart_address,
            )
            self._by_key[key] = state
        elif dart_address:
            state.dart_address = dart_address
        return state

    def note_nozzle_lifted(
        self,
        *,
        pump_id: str,
        nozzle_id: str,
        dart_address: int = 0,
        at: datetime | None = None,
    ) -> NozzleVerifiedState:
        state = self.get_or_create(
            pump_id=pump_id, nozzle_id=nozzle_id, dart_address=dart_address
        )
        if state.phase in {
            VerifiedPhase.COMPLETED,
            VerifiedPhase.CANCELLED_NO_SALE,
            VerifiedPhase.IDLE,
            VerifiedPhase.POSSIBLE_UNINTENDED_FLOW,
        }:
            self._reset_attempt(state)
        state.nozzle_lifted = True
        state.nozzle_lifted_at = at or _now()
        state.nozzle_returned_at = None
        if state.phase is VerifiedPhase.IDLE:
            state.phase = VerifiedPhase.NOZZLE_LIFTED
        return state

    def note_authorized(
        self,
        *,
        pump_id: str,
        nozzle_id: str,
        dart_address: int = 0,
        baseline_volume_raw: int | None = None,
        at: datetime | None = None,
    ) -> NozzleVerifiedState:
        state = self.get_or_create(
            pump_id=pump_id, nozzle_id=nozzle_id, dart_address=dart_address
        )
        state.pump_authorized = True
        state.authorized_at = at or _now()
        # Establish session baseline from the face after reset/authorize so a
        # retained previous-sale DC2 value is not treated as new flow.
        if baseline_volume_raw is not None:
            state.baseline_volume_raw = int(baseline_volume_raw)
            if state.current_volume_raw is None:
                state.current_volume_raw = int(baseline_volume_raw)
        elif state.baseline_volume_raw is None and state.current_volume_raw is not None:
            state.baseline_volume_raw = state.current_volume_raw
        if state.phase in {VerifiedPhase.IDLE, VerifiedPhase.NOZZLE_LIFTED}:
            state.phase = VerifiedPhase.AUTHORIZED
        self._refresh_phase(state)
        return state

    def note_dc1_state(
        self,
        *,
        pump_id: str,
        nozzle_id: str,
        dc1_state: str,
        dart_address: int = 0,
        at: datetime | None = None,
    ) -> NozzleVerifiedState:
        state = self.get_or_create(
            pump_id=pump_id, nozzle_id=nozzle_id, dart_address=dart_address
        )
        normalized = str(dc1_state or "UNKNOWN").strip().upper()
        state.dc1_state = normalized
        if normalized == "FILLING" and state.filling_started_at is None:
            state.filling_started_at = at or _now()
        if normalized == "AUTHORIZED":
            state.pump_authorized = True
            if state.authorized_at is None:
                state.authorized_at = at or _now()
        self._refresh_phase(state)
        return state

    def note_volume(
        self,
        *,
        pump_id: str,
        nozzle_id: str,
        volume_raw: int,
        amount_raw: int | None = None,
        dart_address: int = 0,
        raw_frame: str | None = None,
        at: datetime | None = None,
    ) -> NozzleVerifiedState:
        state = self.get_or_create(
            pump_id=pump_id, nozzle_id=nozzle_id, dart_address=dart_address
        )
        stamp = at or _now()
        prev = state.current_volume_raw
        state.current_volume_raw = int(volume_raw)
        if state.baseline_volume_raw is None and state.pump_authorized:
            # First reading after auth with no explicit baseline — lock it so
            # retained face values do not count as an increase.
            state.baseline_volume_raw = int(volume_raw)
        increased = volume_increased(
            baseline_raw=state.baseline_volume_raw,
            current_raw=state.current_volume_raw,
        )
        if increased:
            if state.first_volume_increase_at is None:
                state.first_volume_increase_at = stamp
            state.last_volume_increase_at = stamp
            # Volume before DC1 FILLING → incident, not a normal sale.
            if state.dc1_state.upper() != "FILLING":
                state.possible_unintended_flow = True
                state.preserved_volume_raw = int(volume_raw)
                if amount_raw is not None:
                    state.preserved_amount_raw = int(amount_raw)
                if raw_frame:
                    state.preserved_raw_frames.append(raw_frame)
                if state.phase is not VerifiedPhase.VERIFIED_DISPENSING:
                    state.phase = VerifiedPhase.POSSIBLE_UNINTENDED_FLOW
        elif prev is not None and state.current_volume_raw != prev:
            # Flat or within tolerance — still track current face.
            pass
        self._refresh_phase(state)
        return state

    def begin_verified_sale_if_needed(
        self,
        state: NozzleVerifiedState,
        *,
        transaction_id: str | None = None,
    ) -> str | None:
        """Assign a stable transaction id when verified dispensing first begins."""
        if not state.verified_dispensing:
            return None
        if state.transaction_id:
            return state.transaction_id
        state.transaction_id = transaction_id or str(uuid4())
        state.phase = VerifiedPhase.VERIFIED_DISPENSING
        return state.transaction_id

    def note_nozzle_returned(
        self,
        *,
        pump_id: str,
        nozzle_id: str,
        dart_address: int = 0,
        at: datetime | None = None,
    ) -> NozzleVerifiedState:
        state = self.get_or_create(
            pump_id=pump_id, nozzle_id=nozzle_id, dart_address=dart_address
        )
        state.nozzle_returned_at = at or _now()
        was_verified = (
            state.phase is VerifiedPhase.VERIFIED_DISPENSING
            or state.verified_dispensing
            or bool(state.transaction_id and state.volume_increased and state.filling_started_at)
        )
        state.nozzle_lifted = False
        if was_verified:
            state.phase = VerifiedPhase.COMPLETED
            state.completed_at = at or _now()
        elif state.possible_unintended_flow:
            state.phase = VerifiedPhase.POSSIBLE_UNINTENDED_FLOW
            state.completed_at = at or _now()
        else:
            # Lift/auth/FILLING but no verified volume movement → cancelled.
            state.phase = VerifiedPhase.CANCELLED_NO_SALE
            state.completed_at = at or _now()
            state.transaction_id = None
        return state

    def mark_started_published(self, state: NozzleVerifiedState) -> None:
        state.started_published = True

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {key: state.diagnostic() for key, state in self._by_key.items()}

    def _refresh_phase(self, state: NozzleVerifiedState) -> None:
        if state.phase in {
            VerifiedPhase.COMPLETED,
            VerifiedPhase.CANCELLED_NO_SALE,
        }:
            return
        if state.verified_dispensing:
            state.phase = VerifiedPhase.VERIFIED_DISPENSING
            if not state.transaction_id:
                state.transaction_id = str(uuid4())
            return
        if state.possible_unintended_flow and state.dc1_state.upper() != "FILLING":
            state.phase = VerifiedPhase.POSSIBLE_UNINTENDED_FLOW
            return
        if state.dc1_state.upper() == "FILLING" and state.pump_authorized and state.nozzle_lifted:
            state.phase = VerifiedPhase.FILLING_NO_FLOW
            return
        if state.pump_authorized:
            state.phase = VerifiedPhase.AUTHORIZED
            return
        if state.nozzle_lifted:
            state.phase = VerifiedPhase.NOZZLE_LIFTED
            return
        state.phase = VerifiedPhase.IDLE

    def _reset_attempt(self, state: NozzleVerifiedState) -> None:
        state.nozzle_lifted = False
        state.pump_authorized = False
        state.dc1_state = "UNKNOWN"
        state.baseline_volume_raw = None
        state.current_volume_raw = None
        state.nozzle_lifted_at = None
        state.authorized_at = None
        state.filling_started_at = None
        state.first_volume_increase_at = None
        state.last_volume_increase_at = None
        state.nozzle_returned_at = None
        state.completed_at = None
        state.transaction_id = None
        state.started_published = False
        state.possible_unintended_flow = False
        state.preserved_raw_frames = []
        state.preserved_volume_raw = None
        state.preserved_amount_raw = None
        state.phase = VerifiedPhase.IDLE
