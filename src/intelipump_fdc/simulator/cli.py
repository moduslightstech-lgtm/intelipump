"""CLI for the virtual Wayne DART pump simulator (in-memory only)."""

from __future__ import annotations

import argparse
import json
import sys

from intelipump_fdc.simulator.scenarios import (
    ALL_SCENARIOS,
    ScenarioRunner,
    get_scenario,
    list_scenarios,
)


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="intelipump-simulator",
        description=(
            "Virtual Wayne DART pump simulator (Phase 5). "
            "Runs in-memory scenarios only — no real serial ports."
        ),
    )
    parser.add_argument(
        "--list-scenarios",
        action="store_true",
        help="List available scenario names",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default=None,
        help="Run a named scenario (in-memory)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all scenarios",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable results",
    )
    args = parser.parse_args(argv)

    if args.list_scenarios:
        for name in list_scenarios():
            print(name)
        return

    runner = ScenarioRunner()
    results = []
    if args.all:
        scenarios = [factory() for factory in ALL_SCENARIOS]
    elif args.scenario:
        scenarios = [get_scenario(args.scenario)]
    else:
        parser.print_help()
        raise SystemExit(2)

    failed = 0
    for scenario in scenarios:
        result = runner.run(scenario)
        results.append(result)
        if not result.ok:
            failed += 1
        if args.json:
            continue
        status = "OK" if result.ok else "FAIL"
        print(f"[{status}] {result.name}")
        for obs in result.observations:
            print(f"  - {obs}")
        for err in result.errors:
            print(f"  ! {err}")

    if args.json:
        payload = [
            {
                "name": r.name,
                "ok": r.ok,
                "observations": list(r.observations),
                "errors": list(r.errors),
                "clock_ms": r.snapshot.clock_ms,
                "pumps": [
                    {
                        "pump_id": p.pump_id,
                        "state": p.normalized_state.value,
                        "wayne_status": int(p.wayne_status),
                        "volume_raw": p.volume_raw,
                        "amount_raw": p.amount_raw,
                    }
                    for p in r.snapshot.pumps
                ],
            }
            for r in results
        ]
        json.dump(payload, sys.stdout, indent=2)
        print()

    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    run()
