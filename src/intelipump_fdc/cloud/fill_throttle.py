"""Pure filling-update publish throttle policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True, slots=True)
class FillThrottleConfig:
    min_interval_seconds: float = 2.0
    min_volume_delta: int = 100
    min_amount_delta: int = 100


@dataclass(frozen=True, slots=True)
class FillThrottleState:
    last_published_at: datetime | None = None
    last_raw_volume: int | None = None
    last_raw_amount: int | None = None
    published_first: bool = False


def should_publish_fill(
    *,
    now: datetime,
    raw_volume: int,
    raw_amount: int,
    state: FillThrottleState,
    config: FillThrottleConfig,
    is_final: bool = False,
) -> bool:
    """Return True if this fill update should be published to the cloud."""
    if is_final:
        return True
    if not state.published_first:
        return True
    # Hang-up holds the same totals on the pump. Do not keep republishing
    # that snapshot or the dashboard stays stuck on DISPENSING.
    if (
        state.last_raw_volume is not None
        and state.last_raw_amount is not None
        and raw_volume == state.last_raw_volume
        and raw_amount == state.last_raw_amount
    ):
        return False
    vol_ok = (
        state.last_raw_volume is not None
        and abs(raw_volume - state.last_raw_volume) >= config.min_volume_delta
    )
    amt_ok = (
        state.last_raw_amount is not None
        and abs(raw_amount - state.last_raw_amount) >= config.min_amount_delta
    )
    if vol_ok or amt_ok:
        return True
    if state.last_published_at is None:
        return True
    return (now - state.last_published_at) >= timedelta(
        seconds=config.min_interval_seconds
    )


def advance_fill_state(
    state: FillThrottleState,
    *,
    now: datetime,
    raw_volume: int,
    raw_amount: int,
) -> FillThrottleState:
    del state  # replaced wholesale
    return FillThrottleState(
        last_published_at=now,
        last_raw_volume=raw_volume,
        last_raw_amount=raw_amount,
        published_first=True,
    )


@dataclass
class FillPublishBook:
    """Per-transaction throttle bookkeeping for cloud fill publishes."""

    config: FillThrottleConfig = field(default_factory=FillThrottleConfig)
    _states: dict[str, FillThrottleState] = field(default_factory=dict)

    def decide(
        self,
        transaction_uuid: str,
        *,
        raw_volume: int,
        raw_amount: int,
        is_final: bool = False,
        now: datetime | None = None,
    ) -> bool:
        now = now or datetime.now(UTC)
        state = self._states.get(transaction_uuid, FillThrottleState())
        if not should_publish_fill(
            now=now,
            raw_volume=raw_volume,
            raw_amount=raw_amount,
            state=state,
            config=self.config,
            is_final=is_final,
        ):
            return False
        self._states[transaction_uuid] = advance_fill_state(
            state, now=now, raw_volume=raw_volume, raw_amount=raw_amount
        )
        if is_final:
            self._states.pop(transaction_uuid, None)
        return True
