from __future__ import annotations

import pytest

from intelipump_fdc.protocol.dart.line.control import (
    ControlType,
    classify_control,
    compose_control,
    extract_sequence,
)


@pytest.mark.parametrize(
    ("control", "expected"),
    [
        (0x20, ControlType.POLL),
        (0x30, ControlType.DATA),
        (0x3F, ControlType.DATA),
        (0x40, ControlType.IAP),
        (0x50, ControlType.NAK),
        (0x5A, ControlType.NAK),
        (0x70, ControlType.EOT),
        (0x7F, ControlType.EOT),
        (0xC0, ControlType.ACK),
        (0xCF, ControlType.ACK),
        (0xE0, ControlType.ACKPOLL),
        (0xEF, ControlType.ACKPOLL),
        (0x00, ControlType.UNKNOWN),
        (0x21, ControlType.UNKNOWN),  # not the fixed POLL value
        (0x41, ControlType.UNKNOWN),  # not the fixed IAP value
        (0x60, ControlType.UNKNOWN),
        (0xFA, ControlType.UNKNOWN),
    ],
)
def test_classify_control_families(control: int, expected: ControlType) -> None:
    assert classify_control(control) is expected


@pytest.mark.parametrize(
    ("control", "sequence"),
    [
        (0x20, 0x0),
        (0x3A, 0xA),
        (0x3F, 0xF),
        (0xC5, 0x5),
        (0xE0, 0x0),
        (0x7F, 0xF),
    ],
)
def test_extract_sequence(control: int, sequence: int) -> None:
    assert extract_sequence(control) == sequence


def test_compose_control_roundtrip() -> None:
    assert compose_control(ControlType.DATA, 0xA) == 0x3A
    assert compose_control(ControlType.ACK, 0x5) == 0xC5
    assert compose_control(ControlType.POLL, 0) == 0x20
    assert compose_control(ControlType.IAP, 0) == 0x40


def test_compose_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        compose_control(ControlType.POLL, 1)
    with pytest.raises(ValueError):
        compose_control(ControlType.DATA, 0x10)
    with pytest.raises(ValueError):
        compose_control(ControlType.UNKNOWN, 0)
