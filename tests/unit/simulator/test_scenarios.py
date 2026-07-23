"""Scenario runner integration tests."""

from __future__ import annotations

import pytest

from intelipump_fdc.simulator.scenarios import (
    ALL_SCENARIOS,
    ScenarioRunner,
    get_scenario,
    list_scenarios,
    normal_sale_scenario,
)


def test_list_scenarios_covers_ten() -> None:
    names = list_scenarios()
    assert len(names) == 10
    assert "normal_sale" in names


@pytest.mark.parametrize("factory", ALL_SCENARIOS)
def test_each_scenario_passes(factory: object) -> None:
    scenario = factory()  # type: ignore[operator]
    result = ScenarioRunner().run(scenario)
    assert result.ok, (scenario.name, result.errors)


def test_normal_sale_factory_kwargs() -> None:
    scenario = normal_sale_scenario(pump_id="fp-1", nozzle=1, price_raw=1175)
    result = ScenarioRunner().run(scenario)
    assert result.ok


def test_get_scenario() -> None:
    s = get_scenario("cold_start")
    assert s.name == "cold_start"
