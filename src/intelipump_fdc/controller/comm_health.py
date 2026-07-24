"""Phase 11C serial and communication health monitoring.

Classifies serial/port and per-pump link health without changing DART framing,
retries, ACK behavior, or safety mode. Persistent protocol faults are retained
after recovery; only transient link errors clear on valid EOT/DATA.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from intelipump_fdc.controller.session_models import CommunicationHealth

logger = logging.getLogger(__name__)


class SerialHealth(StrEnum):
    """Serial transport health classification (Phase 11C)."""

    OPEN = "OPEN"
    DEGRADED = "DEGRADED"
    DISCONNECTED = "DISCONNECTED"
    FAULTED = "FAULTED"


@dataclass(frozen=True, slots=True)
class HealthThresholds:
    """Configurable communication health thresholds."""

    degraded_after_timeouts: int = 3
    disconnected_after_timeouts: int = 10
    # Consecutive persistent protocol faults before FAULTED (CRC/seq/address).
    faulted_after_protocol_errors: int = 3
    # Consecutive reconnect failures before serial FAULTED (still recoverable).
    serial_faulted_after_failures: int = 20
    reconnect_min_delay_s: float = 0.5
    reconnect_max_delay_s: float = 15.0
    # 0.0 keeps reconnect delays deterministic for unit tests.
    reconnect_jitter: float = 0.0


@dataclass
class SerialHealthState:
    """Observable serial-port health (Phase 11C)."""

    configured_path: str = ""
    device_present: bool | None = None
    is_open: bool = False
    health: SerialHealth = SerialHealth.DISCONNECTED
    last_open_ok_at: datetime | None = None
    last_open_ok_mono: float | None = None
    last_open_fail_at: datetime | None = None
    last_open_error: str | None = None
    current_reconnect_delay_s: float = 0.0
    reconnect_attempt_count: int = 0
    successful_reconnect_count: int = 0
    consecutive_reconnect_failures: int = 0

    @property
    def status(self) -> str:
        """Compact STATUS token: open|missing|disconnected|degraded|faulted."""
        if self.health is SerialHealth.OPEN:
            return "open"
        if self.health is SerialHealth.DEGRADED:
            return "degraded"
        if self.health is SerialHealth.FAULTED:
            return "faulted"
        if self.device_present is False:
            return "missing"
        return "disconnected"

    def to_dict(self) -> dict[str, Any]:
        return {
            "configured_path": self.configured_path,
            "device_present": self.device_present,
            "is_open": self.is_open,
            "health": self.health.value,
            "status": self.status,
            "last_open_ok_at": (
                self.last_open_ok_at.isoformat() if self.last_open_ok_at else None
            ),
            "last_open_fail_at": (
                self.last_open_fail_at.isoformat() if self.last_open_fail_at else None
            ),
            "last_open_error": self.last_open_error,
            "current_reconnect_delay_s": self.current_reconnect_delay_s,
            "reconnect_attempt_count": self.reconnect_attempt_count,
            "successful_reconnect_count": self.successful_reconnect_count,
            "consecutive_reconnect_failures": self.consecutive_reconnect_failures,
        }


@dataclass
class SerialReconnectBackoff:
    """Bounded exponential reconnect delay: 0.5, 1, 2, 4, 8 … capped at max."""

    min_delay_s: float = 0.5
    max_delay_s: float = 15.0
    jitter: float = 0.0
    rng: random.Random = field(default_factory=random.Random)
    failures: int = 0

    def next_delay_s(self) -> float:
        exp = min(self.max_delay_s, self.min_delay_s * (2**self.failures))
        if self.jitter > 0:
            exp = exp * (1.0 + self.rng.uniform(-self.jitter, self.jitter))
        delay = float(max(self.min_delay_s, min(self.max_delay_s, exp)))
        self.failures += 1
        return delay

    def reset(self) -> None:
        self.failures = 0


def device_path_present(path: str) -> bool | None:
    """Return whether a filesystem serial path exists.

    Virtual/memory paths (empty, ``memory``, ``pty:``, ``/tmp/`` PTYs used in
    LAB) are treated as present so unit tests do not depend on real devices.
    """
    if not path:
        return None
    if path in {"memory", "in-memory"} or path.startswith("pty:"):
        return True
    # LAB virtual serial paths — presence is determined by open() success.
    if path.startswith("/tmp/"):
        return True
    try:
        return Path(path).exists()
    except OSError:
        return False


@dataclass
class HealthTransitionLog:
    """Captures transition events once for tests / diagnostics."""

    events: list[dict[str, Any]] = field(default_factory=list)

    def emit(self, event: str, **fields: Any) -> None:
        payload = {"event": event, **fields}
        self.events.append(payload)
        extras = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
        logger.info("%s %s", event, extras)


class SerialHealthMonitor:
    """Tracks serial presence/open state and reconnect backoff."""

    def __init__(
        self,
        *,
        configured_path: str,
        thresholds: HealthThresholds | None = None,
        transitions: HealthTransitionLog | None = None,
        virtual: bool = False,
    ) -> None:
        self.thresholds = thresholds or HealthThresholds()
        self.transitions = transitions or HealthTransitionLog()
        self.virtual = virtual
        self.state = SerialHealthState(configured_path=configured_path)
        self.backoff = SerialReconnectBackoff(
            min_delay_s=self.thresholds.reconnect_min_delay_s,
            max_delay_s=self.thresholds.reconnect_max_delay_s,
            jitter=self.thresholds.reconnect_jitter,
        )
        self._was_open = False
        self._missing_logged = False
        self._refresh_presence()
        if self.state.device_present is False:
            self.state.health = SerialHealth.DISCONNECTED
            self._emit_missing_once()

    def _refresh_presence(self) -> None:
        if self.virtual:
            self.state.device_present = True
            return
        self.state.device_present = device_path_present(self.state.configured_path)

    def _emit_missing_once(self) -> None:
        if self._missing_logged:
            return
        self._missing_logged = True
        self.transitions.emit(
            "serial_missing",
            port=self.state.configured_path,
            previous_state=None,
            new_state=SerialHealth.DISCONNECTED.value,
            error="device_path_absent",
        )

    def _classify_closed(self) -> SerialHealth:
        if (
            self.state.consecutive_reconnect_failures
            >= self.thresholds.serial_faulted_after_failures
        ):
            return SerialHealth.FAULTED
        if self.state.consecutive_reconnect_failures >= 3:
            return SerialHealth.DEGRADED
        return SerialHealth.DISCONNECTED

    def observe_open_success(self, *, is_reconnect: bool = False) -> None:
        self._refresh_presence()
        previous = self.state.health
        was_disconnected = not self._was_open and self.state.reconnect_attempt_count > 0
        self.state.is_open = True
        self.state.health = SerialHealth.OPEN
        self.state.last_open_ok_at = datetime.now(UTC)
        self.state.last_open_ok_mono = time.monotonic()
        self.state.last_open_error = None
        self.state.current_reconnect_delay_s = 0.0
        self._missing_logged = False
        if is_reconnect or was_disconnected:
            self.state.successful_reconnect_count += 1
            self.transitions.emit(
                "serial_reconnected",
                port=self.state.configured_path,
                previous_state=previous.value,
                new_state=SerialHealth.OPEN.value,
                reconnect_attempt=self.state.reconnect_attempt_count,
                reconnect_delay_seconds=None,
                error=None,
            )
        self.state.consecutive_reconnect_failures = 0
        self.backoff.reset()
        self._was_open = True

    def observe_open_failure(self, error: str) -> float:
        """Record failure, advance backoff, return delay before next attempt."""
        self._refresh_presence()
        previous = self.state.health
        if self._was_open:
            self.transitions.emit(
                "serial_disconnected",
                port=self.state.configured_path,
                previous_state=previous.value,
                new_state=SerialHealth.DISCONNECTED.value,
                error=error,
            )
        if self.state.device_present is False:
            self._emit_missing_once()
        self._was_open = False
        self.state.is_open = False
        self.state.last_open_fail_at = datetime.now(UTC)
        self.state.last_open_error = error
        self.state.reconnect_attempt_count += 1
        self.state.consecutive_reconnect_failures += 1
        delay = self.backoff.next_delay_s()
        self.state.current_reconnect_delay_s = delay
        new_health = self._classify_closed()
        self.state.health = new_health
        if new_health is SerialHealth.FAULTED and previous is not SerialHealth.FAULTED:
            self.transitions.emit(
                "serial_faulted",
                port=self.state.configured_path,
                previous_state=previous.value,
                new_state=new_health.value,
                reconnect_attempt=self.state.reconnect_attempt_count,
                reconnect_delay_seconds=delay,
                error=error,
            )
        self.transitions.emit(
            "serial_reconnect_attempt",
            port=self.state.configured_path,
            previous_state=previous.value,
            new_state=new_health.value,
            reconnect_attempt=self.state.reconnect_attempt_count,
            reconnect_delay_seconds=delay,
            error=error,
            consecutive_reconnect_failures=self.state.consecutive_reconnect_failures,
        )
        return delay

    def observe_closed(self, *, reason: str = "closed") -> None:
        self._refresh_presence()
        previous = self.state.health
        if self._was_open:
            self.transitions.emit(
                "serial_disconnected",
                port=self.state.configured_path,
                previous_state=previous.value,
                new_state=SerialHealth.DISCONNECTED.value,
                error=reason,
            )
        if self.state.device_present is False:
            self._emit_missing_once()
        self._was_open = False
        self.state.is_open = False
        if reason == "shutdown":
            self.state.health = SerialHealth.DISCONNECTED
        elif self.state.health is not SerialHealth.FAULTED:
            self.state.health = self._classify_closed()

    def sync_from_transport(self, *, is_open: bool) -> None:
        self._refresh_presence()
        if is_open:
            self.state.is_open = True
            self.state.health = SerialHealth.OPEN
            self._was_open = True
            self._missing_logged = False
            return
        self.state.is_open = False
        if self.state.device_present is False:
            self._emit_missing_once()
        if self.state.health is SerialHealth.OPEN:
            self.state.health = SerialHealth.DISCONNECTED


def pump_health_counts(
    states: Mapping[int, CommunicationHealth],
) -> dict[str, int]:
    counts = {"healthy": 0, "degraded": 0, "disconnected": 0, "faulted": 0, "unknown": 0}
    for health in states.values():
        if health is CommunicationHealth.HEALTHY:
            counts["healthy"] += 1
        elif health is CommunicationHealth.DEGRADED:
            counts["degraded"] += 1
        elif health is CommunicationHealth.DISCONNECTED:
            counts["disconnected"] += 1
        elif health is CommunicationHealth.FAULTED:
            counts["faulted"] += 1
        else:
            counts["unknown"] += 1
    return counts


def pump_health_summary(states: Mapping[int, CommunicationHealth]) -> str:
    """STATUS fragment ``N/M healthy`` (healthy count / configured)."""
    counts = pump_health_counts(states)
    total = sum(counts.values())
    return f"{counts['healthy']}/{total} healthy"


def pump_health_compact(states: Mapping[int, CommunicationHealth]) -> str:
    """Legacy compact ``H/D/X/F`` form for diagnostics."""
    counts = pump_health_counts(states)
    return (
        f"H{counts['healthy']}/D{counts['degraded']}/"
        f"X{counts['disconnected']}/F{counts['faulted']}"
    )
