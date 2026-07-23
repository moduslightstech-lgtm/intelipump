"""Latency / timing instrumentation for RS-485 bench (software timestamps)."""

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
    """Timing for one poll↔response exchange."""

    address: int
    write_start_s: float | None = None
    write_complete_s: float | None = None
    first_response_byte_s: float | None = None
    complete_response_s: float | None = None
    ack_write_s: float | None = None
    next_poll_s: float | None = None
    response_kind: str | None = None
    timed_out: bool = False

    @property
    def poll_to_response_ms(self) -> float | None:
        if self.write_start_s is None or self.complete_response_s is None:
            return None
        return (self.complete_response_s - self.write_start_s) * 1000.0

    @property
    def write_duration_ms(self) -> float | None:
        if self.write_start_s is None or self.write_complete_s is None:
            return None
        return (self.write_complete_s - self.write_start_s) * 1000.0


@dataclass
class TimingStats:
    """Aggregate latency statistics; protocol target kept separate from bench timeout."""

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
    """Tracks poll timing without redefining protocol_target from USB timings."""

    def __init__(
        self,
        *,
        protocol_target_ms: float = 25.0,
        configured_bench_timeout_ms: float = 100.0,
    ) -> None:
        self.stats = TimingStats(
            protocol_target_ms=protocol_target_ms,
            configured_bench_timeout_ms=configured_bench_timeout_ms,
        )
        self._open: dict[int, ExchangeTiming] = {}
        self.exchanges: list[ExchangeTiming] = []
        # Backward-compatible alias used by older callers/tests.
        self.summary = self.stats

    def mark_write_start(self, address: int, *, monotonic_s: float) -> None:
        ex = ExchangeTiming(address=address, write_start_s=monotonic_s)
        self._open[address] = ex

    def mark_poll_start(self, address: int, *, monotonic_s: float) -> None:
        """Alias for write_start (POLL TX)."""
        self.mark_write_start(address, monotonic_s=monotonic_s)

    def mark_write_complete(self, address: int, *, monotonic_s: float) -> None:
        ex = self._open.get(address)
        if ex is not None:
            ex.write_complete_s = monotonic_s

    def mark_first_response_byte(self, address: int, *, monotonic_s: float) -> None:
        ex = self._open.get(address)
        if ex is not None and ex.first_response_byte_s is None:
            ex.first_response_byte_s = monotonic_s

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
        ex.complete_response_s = monotonic_s
        ex.response_kind = response_kind
        if ex.poll_to_response_ms is not None:
            self.stats.add(ex.poll_to_response_ms)
        return ex

    def mark_ack_write(self, address: int, *, monotonic_s: float) -> None:
        ex = self._open.get(address)
        if ex is not None:
            ex.ack_write_s = monotonic_s

    def mark_next_poll(self, address: int, *, monotonic_s: float) -> None:
        ex = self._open.pop(address, None)
        if ex is None:
            return
        ex.next_poll_s = monotonic_s
        self.exchanges.append(ex)

    def mark_response(
        self,
        address: int,
        *,
        monotonic_s: float,
        response_kind: str,
    ) -> ExchangeTiming | None:
        """Complete response then close exchange (compat with prior API)."""
        if response_kind == "RESPONSE_TIMEOUT":
            ex = self._open.pop(address, None)
            if ex is not None:
                ex.timed_out = True
                ex.response_kind = response_kind
                self.stats.record_timeout()
                self.exchanges.append(ex)
            else:
                self.stats.record_timeout()
            return ex
        ex = self.mark_complete_response(
            address, monotonic_s=monotonic_s, response_kind=response_kind
        )
        closed = self._open.pop(address, None)
        if closed is not None:
            self.exchanges.append(closed)
        return ex

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.stats.to_dict(),
            "exchange_count": len(self.exchanges),
            "last_samples": [
                {
                    "address": e.address,
                    "poll_to_response_ms": e.poll_to_response_ms,
                    "response_kind": e.response_kind,
                    "timed_out": e.timed_out,
                    "recorded_at": datetime.now(UTC).isoformat(),
                }
                for e in self.exchanges[-5:]
            ],
        }
