"""Internal controller liveness tracking (Phase 11A).

Uses monotonic clocks for hang detection. Wall-clock timestamps are only for
reporting (process start). No database I/O.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class LivenessSnapshot:
    """Lightweight, non-blocking liveness view for STATUS= / diagnostics."""

    process_started_at: datetime
    process_uptime_s: float
    last_loop_progress_age_s: float | None
    last_successful_poll_age_s: float | None
    total_polls: int
    total_valid_responses: int
    total_timeouts: int
    controller_mode: str
    database_health: str
    serial_device_status: str
    watchdog_enabled: bool = False
    notify_socket_present: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "process_started_at": self.process_started_at.isoformat(),
            "process_uptime_s": self.process_uptime_s,
            "last_loop_progress_age_s": self.last_loop_progress_age_s,
            "last_successful_poll_age_s": self.last_successful_poll_age_s,
            "total_polls": self.total_polls,
            "total_valid_responses": self.total_valid_responses,
            "total_timeouts": self.total_timeouts,
            "controller_mode": self.controller_mode,
            "database_health": self.database_health,
            "serial_device_status": self.serial_device_status,
            "watchdog_enabled": self.watchdog_enabled,
            "notify_socket_present": self.notify_socket_present,
        }

    def status_line(self) -> str:
        """Concise systemd STATUS= payload (single line, no newlines)."""
        loop_age = (
            "n/a"
            if self.last_loop_progress_age_s is None
            else f"{self.last_loop_progress_age_s:.1f}s"
        )
        poll_age = (
            "n/a"
            if self.last_successful_poll_age_s is None
            else f"{self.last_successful_poll_age_s:.1f}s"
        )
        return (
            f"mode={self.controller_mode} "
            f"serial={self.serial_device_status} "
            f"db={self.database_health} "
            f"loop_age={loop_age} "
            f"poll_age={poll_age} "
            f"polls={self.total_polls} "
            f"timeouts={self.total_timeouts}"
        )


@dataclass
class LivenessTracker:
    """Tracks main-loop and successful-poll progress with monotonic time."""

    process_started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    _started_mono: float = field(default_factory=time.monotonic)
    last_loop_progress_mono: float | None = None
    last_successful_poll_mono: float | None = None
    database_health: str = "unknown"
    serial_device_status: str = "unknown"
    controller_mode: str = "LISTEN_ONLY"
    watchdog_enabled: bool = False
    notify_socket_present: bool = False

    def mark_loop_progress(self) -> None:
        """Call once per completed controller-loop iteration (non-blocking)."""
        self.last_loop_progress_mono = time.monotonic()

    def mark_successful_poll(self) -> None:
        """Call when a poll cycle receives a valid response (EOT/DATA/NAK handled)."""
        self.last_successful_poll_mono = time.monotonic()

    def snapshot(
        self,
        *,
        total_polls: int = 0,
        total_valid_responses: int = 0,
        total_timeouts: int = 0,
        now_mono: float | None = None,
    ) -> LivenessSnapshot:
        now = time.monotonic() if now_mono is None else now_mono

        def _age(mark: float | None) -> float | None:
            if mark is None:
                return None
            return max(0.0, now - mark)

        return LivenessSnapshot(
            process_started_at=self.process_started_at,
            process_uptime_s=max(0.0, now - self._started_mono),
            last_loop_progress_age_s=_age(self.last_loop_progress_mono),
            last_successful_poll_age_s=_age(self.last_successful_poll_mono),
            total_polls=total_polls,
            total_valid_responses=total_valid_responses,
            total_timeouts=total_timeouts,
            controller_mode=self.controller_mode,
            database_health=self.database_health,
            serial_device_status=self.serial_device_status,
            watchdog_enabled=self.watchdog_enabled,
            notify_socket_present=self.notify_socket_present,
        )
