"""Static safety: passive package must not contain transmit / active paths.

Banned patterns (package source only; tests/ excluded):

1. ``serial.write`` / ``Serial.write``
2. ``ser.write(`` / ``self._ser.write(`` / ``transport.write(`` (byte TX)
3. RTS/DE assert helpers: ``setRTS``, ``assert_de``, ``driver_enable``, ``de_assert``,
   ``rts = True`` / ``rts=True``
4. Forbidden imports: ``real_wayne_price``, ``reset_session``, ``authorize_session``,
   ``cd2_reset``, ``frame_builder``, ``SerialTransport``, ``BenchPollSerialTransport``
5. ``def transmit`` / ``def replay``
6. Active-command call names: ``authorize(``, ``reset_session(``, price-write helpers

Allowed: evidence file ``fh.write(`` / ``Path.write_text`` for JSONL.
Ban-list string literals in *this test file* are excluded from the package scan
because ``tests/`` is skipped.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[1]

BANNED_SUBSTRINGS = [
    "serial.write",
    "Serial.write",
    "setRTS",
    "assert_de",
    "driver_enable",
    "de_assert",
    "rts = True",
    "rts=True",
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

# Active command-ish calls (not operator marker string names).
ACTIVE_CALL_RE = re.compile(
    r"\b(authorize_session|reset_session|build_authorize|write_price|replay_frame)\s*\("
)


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
                violations.append(f"{path.relative_to(PACKAGE)}: contains {banned!r}")
        if TX_WRITE_ATTR_RE.search(text):
            violations.append(f"{path.relative_to(PACKAGE)}: serial-like .write(")
        if DEF_BANNED_RE.search(text):
            violations.append(f"{path.relative_to(PACKAGE)}: def transmit/replay")
        if ACTIVE_CALL_RE.search(text):
            violations.append(f"{path.relative_to(PACKAGE)}: active-command call")
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


def test_dry_run_module_has_no_serial_import() -> None:
    dry_run = PACKAGE / "dry_run.py"
    tree = ast.parse(dry_run.read_text(encoding="utf-8"), filename=str(dry_run))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name != "serial"
                assert not alias.name.startswith("serial.")
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            assert mod != "serial" and not mod.startswith("serial.")
            assert "PassiveSerialReader" not in {a.name for a in node.names}


def test_package_does_not_auto_capture_on_import() -> None:
    # Importing the package / CLI builder must not open a serial device.
    import tools.passive_dart_capture as pkg
    from tools.passive_dart_capture import cli

    assert pkg.__version__
    parser = cli.build_parser()
    # Subcommand required — bare invocation cannot silently capture.
    with pytest.raises(SystemExit):
        parser.parse_args([])
