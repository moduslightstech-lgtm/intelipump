"""Fault-injection helpers for LAB RS-485 bench / memory tests."""

from __future__ import annotations

import random


def corrupt_frame_byte(frame: bytes, *, index: int | None = None) -> bytes:
    """Flip one byte to simulate line corruption (LAB only)."""
    if not frame:
        return frame
    data = bytearray(frame)
    i = index if index is not None else len(data) // 2
    i = max(0, min(i, len(data) - 1))
    data[i] ^= 0xFF
    return bytes(data)


def inject_noise(frame: bytes, *, noise_bytes: int = 3) -> bytes:
    """Prefix random noise before a valid frame."""
    noise = bytes(random.randint(0, 255) for _ in range(max(0, noise_bytes)))
    return noise + frame


def truncate_frame(frame: bytes, *, keep: int = 3) -> bytes:
    """One-way / truncated transmission simulation."""
    if keep <= 0:
        return b""
    return frame[:keep]
