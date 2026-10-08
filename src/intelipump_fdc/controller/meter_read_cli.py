"""Queue one gated CD101 meter read for the sole controller (attended canary).

Does not open RS-485. Writes /var/lib/intelipump/meter-read-request.json and
optionally waits for meter-read-result.json produced by intelipump-controller.
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
    request_path,
    result_path,
    write_meter_read_request,
)
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.protocol.cd101 import build_cd101_request


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Request one read-only CD101 totalizer read via the running controller "
            "(file bridge). Requires INTELIPUMP_METER_READING__HARDWARE_CD101=true "
            "and address/device allowlists. Never opens serial."
        )
    )
    p.add_argument(
        "--address",
        type=int,
        required=True,
        help="DART logical address to read (must be allowlisted)",
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
        help="Seconds to wait for result file (0 = write request and exit)",
    )
    p.add_argument(
        "--print-protocol",
        action="store_true",
        help="Print documented CD101 payload hex and exit without requesting",
    )
    return p


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
                    "note": (
                        "Decimals for litres require known DPVOL / configured "
                        "volume_decimals; otherwise use raw_scaled only."
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
    if args.address not in allowed:
        print(
            f"REFUSED: address {args.address} not in allowlist "
            f"{sorted(allowed)} (INTELIPUMP_METER_READING__ALLOWED_ADDRESSES).",
            file=sys.stderr,
        )
        raise SystemExit(2)
    expected = (meter.allowed_device_id or "").strip()
    actual = (settings.controller.device_id or "").strip()
    if not expected or expected != actual:
        print(
            "REFUSED: device allowlist mismatch "
            f"(allowed={expected!r} device={actual!r}).",
            file=sys.stderr,
        )
        raise SystemExit(2)

    corr = new_correlation_id()
    path = write_meter_read_request(
        MeterReadRequest(
            correlation_id=corr,
            dart_address=int(args.address),
            counter_select=coun,
            requested_by="intelipump-meter-read-once",
            nozzle_hint=args.nozzle_hint,
            notes=args.notes,
        )
    )
    print(f"REQUEST_WRITTEN path={path} correlationId={corr}")
    print(f"CD101_PAYLOAD {cd101.payload_hex}")
    print(f"AWAITING_RESULT path={result_path()} wait={args.wait_seconds}s")
    if args.wait_seconds <= 0:
        return

    deadline = time.monotonic() + float(args.wait_seconds)
    last: dict | None = None
    while time.monotonic() < deadline:
        last = read_meter_read_result()
        if last and last.get("correlationId") == corr:
            print(json.dumps(last, indent=2, sort_keys=True))
            status = str(last.get("status") or "")
            if status == "CAPTURED":
                raise SystemExit(0)
            raise SystemExit(1)
        time.sleep(0.25)
    print(
        f"TIMEOUT waiting for result matching {corr} "
        f"(request still at {request_path()} if controller did not consume it)",
        file=sys.stderr,
    )
    if last:
        print(json.dumps(last, indent=2, sort_keys=True), file=sys.stderr)
    raise SystemExit(3)


if __name__ == "__main__":
    run()
