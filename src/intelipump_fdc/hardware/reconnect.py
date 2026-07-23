"""Bounded reconnect helpers for LAB RS-485 bench."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field


@dataclass
class ReconnectPolicy:
    """Bounded exponential backoff with jitter (no busy loop)."""

    min_delay_s: float = 0.5
    max_delay_s: float = 8.0
    max_attempts: int = 10
    jitter: float = 0.2
    attempts: int = 0
    events: list[str] = field(default_factory=list)

    def next_delay_s(self) -> float | None:
        if self.attempts >= self.max_attempts:
            return None
        exp = min(self.max_delay_s, self.min_delay_s * (2**self.attempts))
        delay = exp * (1.0 + random.uniform(-self.jitter, self.jitter))
        delay = float(max(self.min_delay_s, delay))
        self.attempts += 1
        self.events.append(f"attempt={self.attempts};delay_s={delay:.3f}")
        return delay

    def reset(self) -> None:
        self.attempts = 0

    async def sleep_backoff(self) -> bool:
        delay = self.next_delay_s()
        if delay is None:
            return False
        await asyncio.sleep(delay)
        return True
