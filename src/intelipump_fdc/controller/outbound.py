"""Outbound DATA queue with expiry and bound size."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime

from intelipump_fdc.controller.safety import (
    ControllerSafetyContext,
    evaluate_outbound_safety,
)
from intelipump_fdc.controller.session_models import OutboundDataItem


class OutboundQueueFullError(Exception):
    pass


class OutboundRejectedError(Exception):
    def __init__(self, reasons: tuple[str, ...]) -> None:
        self.reasons = reasons
        super().__init__(",".join(reasons))


class OutboundQueue:
    def __init__(self, *, max_size: int = 32) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._max_size = max_size
        self._items: deque[OutboundDataItem] = deque()

    def __len__(self) -> int:
        return len(self._items)

    def enqueue(
        self,
        item: OutboundDataItem,
        safety: ControllerSafetyContext,
    ) -> None:
        decision = evaluate_outbound_safety(item, safety)
        if not decision.allowed:
            raise OutboundRejectedError(decision.reasons)
        if item.is_expired():
            raise OutboundRejectedError(("item_expired",))
        if len(self._items) >= self._max_size:
            raise OutboundQueueFullError(f"outbound queue full (max={self._max_size})")
        self._items.append(item)

    def pop_for_address(self, address: int) -> OutboundDataItem | None:
        now = datetime.now(UTC)
        kept: deque[OutboundDataItem] = deque()
        selected: OutboundDataItem | None = None
        while self._items:
            item = self._items.popleft()
            if item.is_expired(now):
                continue
            if selected is None and item.address == address:
                selected = item
                continue
            kept.append(item)
        self._items = kept
        return selected

    def drop_for_address(self, address: int) -> int:
        """Drop pending items for one address. Returns how many were removed."""
        kept: deque[OutboundDataItem] = deque()
        dropped = 0
        for item in self._items:
            if item.address == address:
                dropped += 1
                continue
            kept.append(item)
        self._items = kept
        return dropped

    def peek_addresses(self) -> set[int]:
        now = datetime.now(UTC)
        return {i.address for i in self._items if not i.is_expired(now)}
