"""Static safety: passive package must not contain transmit / active paths.

Banned patterns (package source only; tests/ excluded):

1. ``serial.write`` / ``Serial.write``
2. ``ser.write(`` / ``self._ser.write(`` / ``transport.write(`` (byte TX)
3. RTS/DE assert helpers: ``setRTS``, ``assert_de``, ``driver_enable``, ``de_assert``
4. Forbidden imports: ``real_wayne_price``, ``reset_session``, ``authorize_session``,
   ``cd2_reset``, ``frame_builder``, ``SerialTransport``, ``BenchPollSerialTransport``
5. ``def transmit`` / ``def replay``

Allowed: evidence file ``fh.write(`` / ``Path.write_text`` for JSONL.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]

BANNED_SUBSTRINGS = [
    "serial.write",
    "Serial.write",
    "setRTS",
    "assert_de",
    "driver_enable",
    "de_assert",
]

BANNED_IMPORT_NAMES = {
    "real_wayne_price",
    "reset_session",
    "authorize_session",
    "cd2_reset",
    "frame_builder",
    "SerialTransport",
    "BenchPollSerialTransport",
}

# Attribute.write calls that indicate serial/byte TX (not file writes).
TX_WRITE_ATTR_RE = re.compile(
    r"\b(ser|self\._ser|transport|_transport|self\._transport)\.write\s*\("
)

DEF_BANNED_RE = re.compile(r"^\s*def\s+(transmit|replay)\s*\(", re.MULTILINE)


def _iter_package_py() -> list[Path]:
    files: list[Path] = []
    for path in PACKAGE.rglob("*.py"):
        if "tests" in path.parts:
            continue
        if path.name == "__pycache__":
            continue
        files.append(path)
    return files


def test_no_banned_substrings() -> None:
    violations: list[str] = []
    for path in _iter_package_py():
        text = path.read_text(encoding="utf-8")
        for banned in BANNED_SUBSTRINGS:
            if banned in text:
                # Allow documenting banned patterns in REQUIREMENTS/README comments
                # only outside .py — this scan is .py only.
                violations.append(f"{path.relative_to(PACKAGE)}: contains {banned!r}")
        if TX_WRITE_ATTR_RE.search(text):
            violations.append(f"{path.relative_to(PACKAGE)}: serial-like .write(")
        if DEF_BANNED_RE.search(text):
            violations.append(f"{path.relative_to(PACKAGE)}: def transmit/replay")
    assert not violations, "Transmit/safety violations:\n" + "\n".join(violations)


def test_no_forbidden_imports() -> None:
    violations: list[str] = []
    for path in _iter_package_py():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _import_banned(alias.name):
                        violations.append(
                            f"{path.name}: import {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                if _import_banned(mod):
                    violations.append(f"{path.name}: from {mod} import ...")
                for alias in node.names:
                    if alias.name in BANNED_IMPORT_NAMES:
                        violations.append(
                            f"{path.name}: from {mod} import {alias.name}"
                        )
    assert not violations, "Forbidden imports:\n" + "\n".join(violations)


def _import_banned(module: str) -> bool:
    parts = module.split(".")
    return any(p in BANNED_IMPORT_NAMES for p in parts)


def test_passive_serial_reader_has_no_write_method() -> None:
    from tools.passive_dart_capture.serial_reader import PassiveSerialReader

    assert not hasattr(PassiveSerialReader, "write")
    assert not hasattr(PassiveSerialReader, "transmit")
    assert not hasattr(PassiveSerialReader, "drain")
