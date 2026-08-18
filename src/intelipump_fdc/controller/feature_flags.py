"""Wayne controller feature flags — safe defaults (poll-and-observe).

Active automation stays OFF unless explicitly enabled. Never treat these as
authorization to bypass physical-enable / LISTEN_ONLY gates.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WayneFeatureFlags:
    """Production-safe defaults: observe only; no auto TX side effects."""

    # Default operating mode: poll for status, observe, do not auto-command.
    poll_and_observe: bool = True
    automatic_startup_price_programming: bool = False
    automatic_reset: bool = False
    automatic_authorization: bool = False
    # MQTT / persistence interfaces remain; auto cloud publish stays off.
    automatic_transaction_publishing: bool = False

    def any_automatic_command_enabled(self) -> bool:
        return (
            self.automatic_startup_price_programming
            or self.automatic_reset
            or self.automatic_authorization
        )
