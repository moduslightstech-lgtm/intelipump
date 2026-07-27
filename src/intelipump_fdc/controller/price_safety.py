"""Hard refusal of non-poll frames on real-Wayne serial transports."""

from __future__ import annotations

from intelipump_fdc.protocol.dart.line.frame_builder import build_poll


class RealWayneActiveCommandRefusedError(RuntimeError):
    """Raised before serial.write() when a non-poll frame is presented."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.reason = "real_wayne_active_command_refused"


def is_verified_status_poll(frame: bytes) -> bool:
    return frame in {build_poll(1), build_poll(2)}


def assert_real_wayne_poll_only(frame: bytes) -> None:
    """Allow only verified status polls; refuse everything else unconditionally.

    No environment variable, CLI flag, or configuration override may bypass.
    """
    if is_verified_status_poll(frame):
        return
    raise RealWayneActiveCommandRefusedError(
        "real-Wayne serial transport refuses non-poll frames "
        "(no CD5/price/authorize/reset/raw path)"
    )
