"""Queue gated CD101 meter read(s) for the sole controller (Pi attended test).

Does not open RS-485. Writes an exclusive meter-read-request.json and waits
for meter-read-result.<correlationId>.json produced by intelipump-controller.

SAO one-Pi-per-pump: each physical pump polls DART addresses 1 and 2
(nozzle-1 / nozzle-2). Test address 1 first, then address 2 separately.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from intelipump_fdc.controller.meter_read_request import (
    MeterReadRequest,
    new_correlation_id,
    read_meter_read_result,
    request_busy,
    request_path,
    result_path_for,
    write_meter_read_request,
)
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.protocol.cd101 import build_cd101_request


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Request read-only CD101 totalizer read(s) via the running controller "
            "(file bridge). Requires INTELIPUMP_METER_READING__HARDWARE_CD101=true "
            "and address/device allowlists. Never opens serial. Never authorizes."
        )
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument(
        "--address",
        type=int,
        help="Single DART logical address (must be allowlisted), e.g. 1 or 2",
    )
    g.add_argument(
        "--addresses",
        type=str,
        help="Comma-separated addresses to read sequentially, e.g. 1,2",
    )
    g.add_argument(
        "--all-allowed",
        action="store_true",
        help="Read every address in INTELIPUMP_METER_READING__ALLOWED_ADDRESSES",
    )
    p.add_argument(
        "--counter-select",
        type=int,
        default=None,
        help="CD101 COUN byte (default: settings / 1). Spec p.19.",
    )
    p.add_argument("--nozzle-hint", default=None, help="Operator note, eg nozzle-1")
    p.add_argument("--notes", default=None, help="Attended test note / photo id")
    p.add_argument(
        "--wait-seconds",
        type=float,
        default=20.0,
        help="Seconds to wait for each result (0 = write first request and exit)",
    )
    p.add_argument(
        "--gap-seconds",
        type=float,
        default=2.0,
        help="Pause between sequential address reads (respects min-interval)",
    )
    p.add_argument(
        "--print-protocol",
        action="store_true",
        help="Print documented CD101 payload hex and exit without requesting",
    )
    return p


def _parse_addresses(args: argparse.Namespace, allowed: frozenset[int]) -> list[int]:
    if args.all_allowed:
        addrs = sorted(allowed)
        if not addrs:
            print(
                "REFUSED: ALLOWED_ADDRESSES is empty "
                "(INTELIPUMP_METER_READING__ALLOWED_ADDRESSES).",
                file=sys.stderr,
            )
            raise SystemExit(2)
        return addrs
    if args.addresses:
        out: list[int] = []
        for part in str(args.addresses).split(","):
            text = part.strip()
            if not text:
                continue
            out.append(int(text))
        if not out:
            print("REFUSED: --addresses produced an empty list.", file=sys.stderr)
            raise SystemExit(2)
        return out
    return [int(args.address)]


def _request_one(
    *,
    address: int,
    coun: int,
    nozzle_hint: str | None,
    notes: str | None,
    wait_seconds: float,
) -> int:
    """Write one exclusive request; return 0 CAPTURED, 1 other result, 2 busy, 3 timeout."""
    if request_busy():
        print(
            "REFUSED: meter-read bridge busy (another request or inflight claim). "
            "Wait for the prior result or clear stale files under "
            f"{request_path().parent}.",
            file=sys.stderr,
        )
        return 2

    corr = new_correlation_id()
    try:
        path = write_meter_read_request(
            MeterReadRequest(
                correlation_id=corr,
                dart_address=int(address),
                counter_select=coun,
                requested_by="intelipump-meter-read-once",
                nozzle_hint=nozzle_hint,
                notes=notes,
            )
        )
    except FileExistsError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2

    cd101 = build_cd101_request(counter_select=coun)
    result_file = result_path_for(corr)
    print(f"REQUEST_WRITTEN path={path} address={address} correlationId={corr}")
    print(f"CD101_PAYLOAD {cd101.payload_hex}")
    print(f"RESULT_FILE path={result_file}")
    print(f"AWAITING_RESULT path={result_file} wait={wait_seconds}s")
    print(
        "NOTE: export THIS correlation file (not only meter-read-result.json latest)."
    )
    if wait_seconds <= 0:
        return 0

    deadline = time.monotonic() + float(wait_seconds)
    last: dict | None = None
    while time.monotonic() < deadline:
        last = read_meter_read_result(correlation_id=corr)
        if last and last.get("correlationId") == corr:
            print(json.dumps(last, indent=2, sort_keys=True))
            print(f"RESULT_FILE_EXPORT path={result_file}")
            status = str(last.get("status") or "")
            if status in {"CAPTURED", "CAPTURED_AMBIGUOUS"}:
                return 0 if status == "CAPTURED" else 1
            return 1
        time.sleep(0.25)
    print(
        f"TIMEOUT waiting for result matching {corr} address={address} "
        f"(expected {result_file})",
        file=sys.stderr,
    )
    if last:
        print(json.dumps(last, indent=2, sort_keys=True), file=sys.stderr)
    return 3


def run(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    meter = settings.meter_reading
    coun = (
        int(args.counter_select)
        if args.counter_select is not None
        else int(meter.counter_select)
    )
    cd101 = build_cd101_request(counter_select=coun)
    if args.print_protocol:
        print(
            json.dumps(
                {
                    "spec_ref": cd101.source_reference,
                    "application_payload_hex": cd101.payload_hex,
                    "counter_select": cd101.counter_select,
                    "read_only": True,
                    "dc101_spec_ref": "Pump Interface Rev 2.11, page 25, DC101",
                    "correlation_limitation": (
                        "DC101 does not echo a request UUID; the controller "
                        "correlates by DART address + COUN + post-TX window."
                    ),
                    "note": (
                        "CAPTURED retains raw counters; litres only when COUN is "
                        "volume-class and volume_decimals are configured. "
                        "Mapping/scale still require attended verification."
                    ),
                },
                indent=2,
            )
        )
        return

    if not meter.hardware_cd101:
        print(
            "REFUSED: INTELIPUMP_METER_READING__HARDWARE_CD101 is not true "
            "(gate off; no request written).",
            file=sys.stderr,
        )
        raise SystemExit(2)
    allowed = meter.allowed_address_set()
    expected = (meter.allowed_device_id or "").strip()
    actual = (settings.controller.device_id or "").strip()
    if not expected or expected != actual:
        print(
            "REFUSED: device allowlist mismatch "
            f"(allowed={expected!r} device={actual!r}).",
            file=sys.stderr,
        )
        raise SystemExit(2)

    addresses = _parse_addresses(args, allowed)
    for addr in addresses:
        if addr not in allowed:
            print(
                f"REFUSED: address {addr} not in allowlist "
                f"{sorted(allowed)} (INTELIPUMP_METER_READING__ALLOWED_ADDRESSES).",
                file=sys.stderr,
            )
            raise SystemExit(2)

    exit_code = 0
    for i, addr in enumerate(addresses):
        hint = args.nozzle_hint
        if hint is None and len(addresses) > 1:
            hint = f"nozzle-{addr}"
        rc = _request_one(
            address=addr,
            coun=coun,
            nozzle_hint=hint,
            notes=args.notes,
            wait_seconds=float(args.wait_seconds),
        )
        if rc != 0:
            exit_code = rc
        if i + 1 < len(addresses) and float(args.wait_seconds) > 0:
            gap = max(0.0, float(args.gap_seconds))
            if gap:
                print(f"GAP {gap}s before next address…")
                time.sleep(gap)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    run()
