"""Latency / timing instrumentation for RS-485 bench (software timestamps).

Four intervals are tracked separately (never merged into one generic latency):

1. POLL write completion → first response byte
2. POLL write completion → complete response frame
3. DATA receive completion → ACK write start
4. ACK write completion → next POLL write start

``protocol_target_ms`` (25) and ``configured_bench_timeout_ms`` (typically 100)
are metadata only — they must not redefine measured intervals.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class TimingMark:
    """One named timing point (monotonic seconds)."""

    name: str
    monotonic_s: float
    address: int | None = None


@dataclass
class ExchangeTiming:
    """Timing for one poll↔response attempt (correlated by address + attempt)."""

    address: int
    attempt: int
    write_start_s: float | None = None
    write_complete_s: float | None = None
    first_response_byte_s: float | None = None
    complete_response_s: float | None = None
    data_receive_complete_s: float | None = None
    ack_write_start_s: float | None = None
    ack_write_complete_s: float | None = None
    next_poll_write_start_s: float | None = None
    response_kind: str | None = None
    timed_out: bool = False

    def _delta_ms(self, start: float | None, end: float | None) -> float | None:
        if start is None or end is None:
            return None
        return (end - start) * 1000.0

    def _poll_anchor_s(self) -> float | None:
        """Prefer write-complete; fall back to write-start for legacy callers."""
        if self.write_complete_s is not None:
            return self.write_complete_s
        return self.write_start_s

    @property
    def write_complete_to_first_byte_ms(self) -> float | None:
        return self._delta_ms(self._poll_anchor_s(), self.first_response_byte_s)

    @property
    def write_complete_to_complete_response_ms(self) -> float | None:
        return self._delta_ms(self._poll_anchor_s(), self.complete_response_s)

    @property
    def data_to_ack_write_start_ms(self) -> float | None:
        return self._delta_ms(self.data_receive_complete_s, self.ack_write_start_s)

    @property
    def ack_complete_to_next_poll_ms(self) -> float | None:
        return self._delta_ms(self.ack_write_complete_s, self.next_poll_write_start_s)

    @property
    def poll_to_response_ms(self) -> float | None:
        """Alias for interval 2 (write complete → complete response)."""
        return self.write_complete_to_complete_response_ms

    @property
    def write_duration_ms(self) -> float | None:
        return self._delta_ms(self.write_start_s, self.write_complete_s)


@dataclass
class TimingStats:
    """Aggregate latency statistics for one interval series."""

    values_ms: list[float] = field(default_factory=list)
    timeout_count: int = 0
    protocol_target_ms: float = 25.0
    configured_bench_timeout_ms: float = 100.0

    def add(self, value_ms: float) -> None:
        self.values_ms.append(value_ms)

    def record_timeout(self) -> None:
        self.timeout_count += 1

    @property
    def count(self) -> int:
        return len(self.values_ms)

    def _percentile(self, p: float) -> float | None:
        if not self.values_ms:
            return None
        ordered = sorted(self.values_ms)
        if len(ordered) == 1:
            return ordered[0]
        idx = min(len(ordered) - 1, round(p * (len(ordered) - 1)))
        return ordered[idx]

    @property
    def minimum_ms(self) -> float | None:
        return min(self.values_ms) if self.values_ms else None

    @property
    def maximum_ms(self) -> float | None:
        return max(self.values_ms) if self.values_ms else None

    @property
    def mean_ms(self) -> float | None:
        if not self.values_ms:
            return None
        return statistics.fmean(self.values_ms)

    @property
    def p50_ms(self) -> float | None:
        return self._percentile(0.50)

    @property
    def p95_ms(self) -> float | None:
        return self._percentile(0.95)

    @property
    def p99_ms(self) -> float | None:
        return self._percentile(0.99)

    @property
    def jitter_ms(self) -> float | None:
        """Population stdev as simple jitter estimate."""
        if len(self.values_ms) < 2:
            return 0.0 if self.values_ms else None
        return statistics.pstdev(self.values_ms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "minimum_ms": self.minimum_ms,
            "maximum_ms": self.maximum_ms,
            "mean_ms": self.mean_ms,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
            "jitter_ms": self.jitter_ms,
            "timeout_count": self.timeout_count,
            "protocol_target_ms": self.protocol_target_ms,
            "configured_bench_timeout_ms": self.configured_bench_timeout_ms,
            "within_protocol_target_ratio": (
                (
                    sum(1 for v in self.values_ms if v <= self.protocol_target_ms)
                    / len(self.values_ms)
                )
                if self.values_ms
                else None
            ),
        }


class LatencyTracker:
    """Tracks the four Phase-10 intervals without baking in bench timeout."""

    def __init__(
        self,
        *,
        protocol_target_ms: float = 25.0,
        configured_bench_timeout_ms: float = 100.0,
    ) -> None:
        self.protocol_target_ms = protocol_target_ms
        self.configured_bench_timeout_ms = configured_bench_timeout_ms
        self.poll_to_first_byte = TimingStats(
            protocol_target_ms=protocol_target_ms,
            configured_bench_timeout_ms=configured_bench_timeout_ms,
        )
        self.poll_to_complete_response = TimingStats(
            protocol_target_ms=protocol_target_ms,
            configured_bench_timeout_ms=configured_bench_timeout_ms,
        )
        self.data_to_ack = TimingStats(
            protocol_target_ms=protocol_target_ms,
            configured_bench_timeout_ms=configured_bench_timeout_ms,
        )
        self.ack_to_next_poll = TimingStats(
            protocol_target_ms=protocol_target_ms,
            configured_bench_timeout_ms=configured_bench_timeout_ms,
        )
        # Primary series for reports/CLI (interval 2).
        self.stats = self.poll_to_complete_response
        self.summary = self.stats
        self._open: dict[int, ExchangeTiming] = {}
        self._awaiting_next_poll: ExchangeTiming | None = None
        self._attempts: dict[int, int] = {}
        self.exchanges: list[ExchangeTiming] = []

    def _new_attempt(self, address: int) -> int:
        n = self._attempts.get(address, 0) + 1
        self._attempts[address] = n
        return n

    def mark_write_start(self, address: int, *, monotonic_s: float) -> None:
        self._close_awaiting_next_poll(next_poll_s=monotonic_s)
        stale = self._open.pop(address, None)
        if stale is not None and stale not in self.exchanges:
            self.exchanges.append(stale)
        attempt = self._new_attempt(address)
        self._open[address] = ExchangeTiming(
            address=address, attempt=attempt, write_start_s=monotonic_s
        )

    def mark_poll_start(self, address: int, *, monotonic_s: float) -> None:
        """Alias for write_start (POLL TX begin)."""
        self.mark_write_start(address, monotonic_s=monotonic_s)

    def mark_write_complete(self, address: int, *, monotonic_s: float) -> None:
        ex = self._open.get(address)
        if ex is not None:
            ex.write_complete_s = monotonic_s
            if ex.write_start_s is None:
                ex.write_start_s = monotonic_s

    def mark_first_response_byte(self, address: int, *, monotonic_s: float) -> None:
        ex = self._open.get(address)
        if ex is not None and ex.first_response_byte_s is None:
            ex.first_response_byte_s = monotonic_s
            ms = ex.write_complete_to_first_byte_ms
            if ms is not None:
                self.poll_to_first_byte.add(ms)

    def mark_complete_response(
        self,
        address: int,
        *,
        monotonic_s: float,
        response_kind: str,
    ) -> ExchangeTiming | None:
        ex = self._open.get(address)
        if ex is None:
            return None
        if ex.first_response_byte_s is None:
            self.mark_first_response_byte(address, monotonic_s=monotonic_s)
        ex.complete_response_s = monotonic_s
        ex.response_kind = response_kind
        ms = ex.write_complete_to_complete_response_ms
        if ms is not None:
            self.poll_to_complete_response.add(ms)
        if response_kind == "DATA_RECEIVED":
            ex.data_receive_complete_s = monotonic_s
        return ex

    def mark_ack_write_start(self, address: int, *, monotonic_s: float) -> None:
        ex = self._open.get(address)
        if ex is not None and ex.ack_write_start_s is None:
            ex.ack_write_start_s = monotonic_s
            ms = ex.data_to_ack_write_start_ms
            if ms is not None:
                self.data_to_ack.add(ms)

    def mark_ack_write(self, address: int, *, monotonic_s: float) -> None:
        """ACK write completion (also fills start if missing)."""
        ex = self._open.get(address)
        if ex is None:
            return
        if ex.ack_write_start_s is None:
            self.mark_ack_write_start(address, monotonic_s=monotonic_s)
        ex.ack_write_complete_s = monotonic_s
        # Pop address slot; keep exchange until next POLL write start (interval 4).
        self._open.pop(address, None)
        self._awaiting_next_poll = ex

    def mark_next_poll(self, address: int, *, monotonic_s: float) -> None:
        del address  # next POLL may be a different address
        self._close_awaiting_next_poll(next_poll_s=monotonic_s)

    def _close_awaiting_next_poll(self, *, next_poll_s: float) -> None:
        ex = self._awaiting_next_poll
        self._awaiting_next_poll = None
        if ex is None:
            return
        ex.next_poll_write_start_s = next_poll_s
        ms = ex.ack_complete_to_next_poll_ms
        if ms is not None:
            self.ack_to_next_poll.add(ms)
        if ex in self._open.values():
            # Still the open slot for that address — leave until response path closes.
            pass
        if ex not in self.exchanges:
            self.exchanges.append(ex)

    def mark_response(
        self,
        address: int,
        *,
        monotonic_s: float,
        response_kind: str,
    ) -> ExchangeTiming | None:
        """Complete response; close non-DATA exchanges (compat with prior API)."""
        if response_kind == "RESPONSE_TIMEOUT":
            ex = self._open.pop(address, None)
            if ex is not None:
                ex.timed_out = True
                ex.response_kind = response_kind
                self.poll_to_complete_response.record_timeout()
                self.poll_to_first_byte.record_timeout()
                self.exchanges.append(ex)
            else:
                self.poll_to_complete_response.record_timeout()
                self.poll_to_first_byte.record_timeout()
            return ex
        ex = self.mark_complete_response(
            address, monotonic_s=monotonic_s, response_kind=response_kind
        )
        if response_kind != "DATA_RECEIVED":
            closed = self._open.pop(address, None)
            if closed is not None and closed not in self.exchanges:
                self.exchanges.append(closed)
        return ex

    def to_dict(self) -> dict[str, Any]:
        primary = self.poll_to_complete_response.to_dict()
        return {
            **primary,
            "intervals": {
                "poll_write_complete_to_first_response_byte": (
                    self.poll_to_first_byte.to_dict()
                ),
                "poll_write_complete_to_complete_response": (
                    self.poll_to_complete_response.to_dict()
                ),
                "data_receive_complete_to_ack_write_start": self.data_to_ack.to_dict(),
                "ack_write_complete_to_next_poll_write_start": (
                    self.ack_to_next_poll.to_dict()
                ),
            },
            "exchange_count": len(self.exchanges),
            "last_samples": [
                {
                    "address": e.address,
                    "attempt": e.attempt,
                    "write_complete_to_first_byte_ms": e.write_complete_to_first_byte_ms,
                    "write_complete_to_complete_response_ms": (
                        e.write_complete_to_complete_response_ms
                    ),
                    "data_to_ack_write_start_ms": e.data_to_ack_write_start_ms,
                    "ack_complete_to_next_poll_ms": e.ack_complete_to_next_poll_ms,
                    "response_kind": e.response_kind,
                    "timed_out": e.timed_out,
                    "recorded_at": datetime.now(UTC).isoformat(),
                }
                for e in self.exchanges[-5:]
            ],
        }
