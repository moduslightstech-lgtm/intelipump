from __future__ import annotations

import pytest

from intelipump_fdc.protocol.dart.line.sequence import next_tx_sequence, validate_sequence


def test_sequence_range() -> None:
    assert validate_sequence(0) == 0
    assert validate_sequence(0xF) == 0xF
    with pytest.raises(ValueError):
        validate_sequence(0x10)


def test_increment_and_wraparound_per_spec() -> None:
    # DART Serial Communication / Line-Level Specification, page 3: after F -> 1
    assert next_tx_sequence(0x0) == 0x1
    assert next_tx_sequence(0x1) == 0x2
    assert next_tx_sequence(0xE) == 0xF
    assert next_tx_sequence(0xF) == 0x1
