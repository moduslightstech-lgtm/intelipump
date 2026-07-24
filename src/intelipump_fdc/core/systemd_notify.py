"""systemd sd_notify abstraction (Phase 11B).

Sends READY / WATCHDOG / STATUS / STOPPING over the Unix datagram socket in
``NOTIFY_SOCKET`` when enabled. Missing socket or disabled mode is a no-op —
never crashes the controller.
"""

from __future__ import annotations

import logging
import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

logger = logging.getLogger(__name__)


class Notifier(Protocol):
    """Minimal notify interface used by the controller loop / CLI."""

    @property
    def enabled(self) -> bool: ...

    @property
    def notify_socket_present(self) -> bool: ...

    def ready(self, status: str | None = None) -> bool: ...

    def watchdog(self) -> bool: ...

    def status(self, message: str) -> bool: ...

    def stopping(self) -> bool: ...


@dataclass
class NullNotifier:
    """No-op notifier for tests and development."""

    enabled: bool = False
    notify_socket_present: bool = False
    sent: list[str] = field(default_factory=list)

    def ready(self, status: str | None = None) -> bool:
        payload = "READY=1"
        if status:
            payload = f"{payload}\nSTATUS={_sanitize_status(status)}"
        self.sent.append(payload)
        return False

    def watchdog(self) -> bool:
        self.sent.append("WATCHDOG=1")
        return False

    def status(self, message: str) -> bool:
        self.sent.append(f"STATUS={_sanitize_status(message)}")
        return False

    def stopping(self) -> bool:
        self.sent.append("STOPPING=1")
        return False


def _sanitize_status(message: str) -> str:
    # systemd STATUS= must be a single line.
    return message.replace("\n", " ").replace("\r", " ").strip()[:512]


def watchdog_usec_from_env(
    environ: Mapping[str, str] | None = None,
) -> int | None:
    """Return WATCHDOG_USEC from the environment, if present and valid."""
    env: Mapping[str, str] = os.environ if environ is None else environ
    raw = env.get("WATCHDOG_USEC")
    if raw is None or raw == "":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


@dataclass
class SystemdNotifier:
    """Unix-datagram sd_notify client.

    Watchdog pings must be driven by real controller-loop progress — never from
    an independent free-running timer.
    """

    enabled: bool = False
    notify_socket: str | None = None
    watchdog_usec: int | None = None
    _ready_sent: bool = False
    _stopping_sent: bool = False
    _watchdog_pings: int = 0
    _last_error: str | None = None

    @classmethod
    def from_env(
        cls,
        *,
        enabled: bool,
        environ: Mapping[str, str] | None = None,
    ) -> SystemdNotifier:
        env: Mapping[str, str] = os.environ if environ is None else environ
        socket_path = env.get("NOTIFY_SOCKET") or None
        return cls(
            enabled=enabled,
            notify_socket=socket_path,
            watchdog_usec=watchdog_usec_from_env(env),
        )

    @property
    def notify_socket_present(self) -> bool:
        return bool(self.notify_socket)

    @property
    def active(self) -> bool:
        return self.enabled and bool(self.notify_socket)

    @property
    def watchdog_pings(self) -> int:
        return self._watchdog_pings

    @property
    def last_error(self) -> str | None:
        return self._last_error

    def ready(self, status: str | None = None) -> bool:
        if self._ready_sent:
            return self.active
        parts = ["READY=1"]
        if status:
            parts.append(f"STATUS={_sanitize_status(status)}")
        ok = self._send("\n".join(parts))
        if ok or not self.active:
            self._ready_sent = True
            if ok:
                logger.info("watchdog_ready", extra={"event": "watchdog_ready"})
        return ok

    def watchdog(self) -> bool:
        """Feed the systemd watchdog. Call only from loop progress."""
        if not self.active:
            return False
        if not self._ready_sent:
            return False
        ok = self._send("WATCHDOG=1")
        if ok:
            self._watchdog_pings += 1
            # Debug-level only — avoid per-ping INFO spam.
            logger.debug("watchdog_ping", extra={"event": "watchdog_ping"})
        return ok

    def status(self, message: str) -> bool:
        return self._send(f"STATUS={_sanitize_status(message)}")

    def stopping(self) -> bool:
        if self._stopping_sent:
            return self.active
        ok = self._send("STOPPING=1")
        if ok or not self.active:
            self._stopping_sent = True
            if ok:
                logger.info("graceful_shutdown", extra={"event": "graceful_shutdown"})
        return ok

    def _send(self, payload: str) -> bool:
        if not self.enabled:
            return False
        path = self.notify_socket
        if not path:
            return False
        try:
            data = payload.encode("utf-8")
            # Abstract namespace sockets start with '@' in the env var.
            addr = ("\0" + path[1:]) if path.startswith("@") else path
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            try:
                sock.connect(addr)
                sock.sendall(data)
            finally:
                sock.close()
            self._last_error = None
            return True
        except OSError as exc:
            self._last_error = f"{type(exc).__name__}:{exc}"
            logger.warning(
                "watchdog_notify_failed error=%s",
                self._last_error,
                extra={"event": "watchdog_notify_failed"},
            )
            return False
