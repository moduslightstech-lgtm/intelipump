"""Outbound exchange result states (link ACK ≠ application confirmation)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class ExchangeResultStatus(StrEnum):
    """Wire / application outcome for a controller→pump DATA exchange."""

    TRANSMITTED = "TRANSMITTED"
    LINK_ACKNOWLEDGED = "LINK_ACKNOWLEDGED"
    APPLICATION_CONFIRMED = "APPLICATION_CONFIRMED"
    TIMED_OUT = "TIMED_OUT"
    REJECTED = "REJECTED"


@dataclass(slots=True)
class ExchangeResult:
    """Result of one outbound attempt (possibly after retries)."""

    status: ExchangeResultStatus
    address: int
    sequence: int | None = None
    correlation_id: str | None = None
    command_tx_mono: float | None = None
    write_start_mono: float | None = None
    link_ack_mono: float | None = None
    application_confirm_mono: float | None = None
    attempts: int = 0
    detail: str | None = None
    preserved_event_count: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def link_acknowledged(self) -> bool:
        return self.status in {
            ExchangeResultStatus.LINK_ACKNOWLEDGED,
            ExchangeResultStatus.APPLICATION_CONFIRMED,
        }

    @property
    def application_confirmed(self) -> bool:
        return self.status is ExchangeResultStatus.APPLICATION_CONFIRMED
