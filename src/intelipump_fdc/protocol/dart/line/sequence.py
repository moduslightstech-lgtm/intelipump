"""Block sequence number (TX#) helpers.

Source: DART Serial Communication / Line-Level Specification, page 3.

Documented rules:
- TX# is initiated to 0 after a protocol restart.
- Incremented by one for every successfully transmitted data block.
- After Fh it starts with 1 again (skips 0 on wrap).

Ambiguity: captured traffic sometimes shows DATA CTRL wrapping F->0.
That conflict is deferred to Phase 2 captured-frame validation.
This helper implements the authorized document rule (F -> 1).
"""

from __future__ import annotations

from intelipump_fdc.protocol.dart.line.control import (
    ControlType,
    classify_control,
    compose_control,
    extract_sequence,
)

__all__ = [
    "ControlType",
    "classify_control",
    "compose_control",
    "extract_sequence",
    "next_tx_sequence",
    "validate_sequence",
]


def validate_sequence(sequence: int) -> int:
    """Return sequence if it is in 0x0..0xF."""
    if not 0 <= sequence <= 0x0F:
        raise ValueError(f"sequence out of range 0x0-0xF: {sequence}")
    return sequence


def next_tx_sequence(sequence: int) -> int:
    """Return the next TX# after a successfully transmitted data block.

    Per DART Serial Communication / Line-Level Specification, page 3:
    after Fh the sequence starts at 1 again.
    From 0..E the next value is sequence+1.
    """
    validate_sequence(sequence)
    if sequence == 0x0F:
        return 0x01
    return sequence + 1
