"""Hard refusal of non-poll frames on real-Wayne serial transports.

Status polls are always allowed. A CD5 price DATA frame is allowed only when
the transport has an explicit single-shot approval for that exact frame.
No environment variable, CLI flag, or configuration override may widen this.
"""

from __future__ import annotations

from intelipump_fdc.protocol.dart.line.frame_builder import build_poll


class RealWayneActiveCommandRefusedError(RuntimeError):
    """Raised before serial.write() when a non-allowed frame is presented."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.reason = "real_wayne_active_command_refused"


def is_verified_status_poll(frame: bytes) -> bool:
    return frame in {build_poll(1), build_poll(2)}


def assert_real_wayne_poll_only(frame: bytes) -> None:
    """Allow only verified status polls; refuse everything else.

    Used by dry-run proof paths and any caller that must remain poll-only.
    """
    if is_verified_status_poll(frame):
        return
    raise RealWayneActiveCommandRefusedError(
        "real-Wayne serial transport refuses non-poll frames "
        "(no CD5/price/authorize/reset/raw path)"
    )


def assert_real_wayne_write_allowed(
    frame: bytes,
    *,
    approved_cd5_frame: bytes | None,
    cd5_writes_remaining: int,
) -> None:
    """Allow status polls, or exactly one pre-approved CD5 DATA frame."""
    if is_verified_status_poll(frame):
        return
    if (
        approved_cd5_frame is not None
        and cd5_writes_remaining > 0
        and frame == approved_cd5_frame
    ):
        return
    raise RealWayneActiveCommandRefusedError(
        "real-Wayne serial transport refuses frame "
        "(only status poll or one pre-approved CD5 candidate allowed)"
    )
