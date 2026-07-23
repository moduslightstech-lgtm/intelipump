"""Deterministic simulated clock (no real sleeps)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from heapq import heappop, heappush


@dataclass(order=True, slots=True)
class _Scheduled:
    due_ms: int
    seq: int
    callback: Callable[[], None] = field(compare=False)


class SimulatedClock:
    """Monotonic millisecond clock with scheduled callbacks."""

    def __init__(self, start_ms: int = 0) -> None:
        self._now_ms = start_ms
        self._seq = 0
        self._heap: list[_Scheduled] = []

    def now(self) -> int:
        """Return current simulated time in milliseconds."""
        return self._now_ms

    def advance(self, milliseconds: int) -> None:
        """Advance time and fire due callbacks in order."""
        if milliseconds < 0:
            raise ValueError("milliseconds must be non-negative")
        target = self._now_ms + milliseconds
        while self._heap and self._heap[0].due_ms <= target:
            item = heappop(self._heap)
            self._now_ms = item.due_ms
            item.callback()
        self._now_ms = target

    def schedule(self, delay_ms: int, callback: Callable[[], None]) -> None:
        """Schedule ``callback`` after ``delay_ms`` from now."""
        if delay_ms < 0:
            raise ValueError("delay_ms must be non-negative")
        self._seq += 1
        heappush(
            self._heap,
            _Scheduled(due_ms=self._now_ms + delay_ms, seq=self._seq, callback=callback),
        )

    def run_until_idle(self, *, max_ms: int = 1_000_000) -> int:
        """Advance until no scheduled events remain (or ``max_ms`` elapsed).

        Returns milliseconds advanced.
        """
        if not self._heap:
            return 0
        start = self._now_ms
        deadline = start + max_ms
        while self._heap and self._heap[0].due_ms <= deadline:
            item = heappop(self._heap)
            self._now_ms = item.due_ms
            item.callback()
        return self._now_ms - start

    @property
    def pending_count(self) -> int:
        return len(self._heap)
