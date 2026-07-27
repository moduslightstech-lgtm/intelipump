"""Hard refusal of non-poll frames on real-Wayne serial transports.

Status polls are always allowed. Exactly one pre-approved active DATA frame
(CD5 price, CD1 RESET, or CD1 AUTHORIZE) may be written when explicitly
authorized on the transport for a single shot.

No environment variable may bypass this gate.
"""

from __future__ import annotations

from enum import StrEnum

from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll


class RealWayneActiveCommandRefusedError(RuntimeError):
    """Raised before serial.write() when a non-allowed frame is presented."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.reason = "real_wayne_active_command_refused"

    @property
    def reasons(self) -> list[str]:
        return [self.reason, str(self)]


class ActiveFrameKind(StrEnum):
    CD5_PRICE = "CD5_PRICE"
    CD1_RESET = "CD1_RESET"
    CD1_AUTHORIZE = "CD1_AUTHORIZE"


def is_verified_status_poll(frame: bytes) -> bool:
    return frame in {build_poll(1), build_poll(2)}


def classify_active_data_frame(frame: bytes) -> ActiveFrameKind | None:
    """Return kind if frame looks like an allowed active DATA candidate."""
    if len(frame) < 8 or (frame[1] & 0xF0) != 0x30:
        return None
    # Application starts at index 2: TRANS LNG ...
    trans = frame[2]
    lng = frame[3] if len(frame) > 3 else -1
    if trans == 0x05 and lng >= 3:
        return ActiveFrameKind.CD5_PRICE
    if trans == 0x01 and lng == 1 and len(frame) >= 5:
        dcc = frame[4]
        if dcc == int(PumpControlCommand.RESET):
            return ActiveFrameKind.CD1_RESET
        if dcc == int(PumpControlCommand.AUTHORIZE):
            return ActiveFrameKind.CD1_AUTHORIZE
    return None


def assert_real_wayne_poll_only(frame: bytes) -> None:
    """Allow only verified status polls; refuse everything else."""
    if is_verified_status_poll(frame):
        return
    raise RealWayneActiveCommandRefusedError(
        "real-Wayne serial transport refuses non-poll frames "
        "(no CD5/price/authorize/reset/raw path)"
    )


def assert_real_wayne_write_allowed(
    frame: bytes,
    *,
    approved_active_frame: bytes | None,
    active_writes_remaining: int,
) -> None:
    """Allow status polls, or exactly one pre-approved active DATA frame."""
    if is_verified_status_poll(frame):
        return
    if (
        approved_active_frame is not None
        and active_writes_remaining > 0
        and frame == approved_active_frame
    ):
        return
    raise RealWayneActiveCommandRefusedError(
        "real-Wayne serial transport refuses frame "
        "(only status poll or one pre-approved active candidate allowed)"
    )


# Back-compat aliases used by older CD5 call sites / tests.
def assert_real_wayne_write_allowed_cd5(
    frame: bytes,
    *,
    approved_cd5_frame: bytes | None,
    cd5_writes_remaining: int,
) -> None:
    assert_real_wayne_write_allowed(
        frame,
        approved_active_frame=approved_cd5_frame,
        active_writes_remaining=cd5_writes_remaining,
    )
