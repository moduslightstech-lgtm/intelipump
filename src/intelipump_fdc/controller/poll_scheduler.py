"""Polling scheduler configuration (shared by controller loop)."""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.simulator.config import SequencePolicy


@dataclass(frozen=True, slots=True)
class PollSchedulerConfig:
    addresses: tuple[int, ...] = (1, 2)
    response_timeout_ms: int = 25
    inter_poll_delay_ms: int = 5
    idle_sleep_ms: int = 20
    max_retries: int = 2
    # Phase 11C defaults: DEGRADED@3, DISCONNECTED@10 consecutive timeouts.
    degraded_after_timeouts: int = 3
    max_consecutive_timeouts: int = 10
    read_chunk_size: int = 256
    sequence_policy: SequencePolicy = SequencePolicy.SPEC_F_TO_1
    reconnect_delay_s: float = 1.0  # legacy floor; SerialHealthMonitor owns backoff
    reconnect_min_delay_s: float = 0.5
    reconnect_max_delay_s: float = 15.0
    reconnect_jitter: float = 0.0
    faulted_after_protocol_errors: int = 3
