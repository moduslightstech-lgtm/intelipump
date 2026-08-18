"""Polling scheduler configuration (shared by controller loop)."""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.simulator.config import SequencePolicy


@dataclass(frozen=True, slots=True)
class PollSchedulerConfig:
    addresses: tuple[int, ...] = (1, 2)
    # Software response deadline from actual serial write time (not OS read timeout).
    response_timeout_ms: int = 120
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
    # Hang-up: wait for DC1 FILLING_COMPLETE before inferred close.
    awaiting_filling_complete_timeout_s: float = 30.0
    dc2_stability_window_s: float = 2.0
    # RS-485 turnaround (applied on physical serial unless overridden).
    tx_delay_ms: int = 35
    ack_delay_ms: int = 5
    apply_bus_delays_on_virtual: bool = False
    # Quiet-gap incomplete-frame discard (seconds).
    quiet_gap_timeout_s: float = 0.015
    # Application confirmation poll budget after link ACK (gated commands).
    application_confirm_timeout_ms: int = 600
    application_confirm_max_polls: int = 8
    # Soft RX sequence: ACK CRC-valid DATA by frame seq and resync (default ON).
    soft_rx_sequence: bool = True
