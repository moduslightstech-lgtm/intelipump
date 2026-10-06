"""Controller CLI must not force simulated=True on persistence."""

from __future__ import annotations

import inspect

from intelipump_fdc.controller import cli


def test_cli_persistence_honors_api_simulated_flag() -> None:
    src = inspect.getsource(cli)
    assert "simulated=bool(settings.api.simulated)" in src
    assert "simulated=True" not in src or src.count("simulated=True") == 0
