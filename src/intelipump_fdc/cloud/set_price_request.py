"""File bridge: cloud-sync queues SET_PRICE; controller applies CD5 on the bus.

Cloud-sync and the RS-485 controller are separate processes. The sidecar never
opens the serial port. A single JSON request under /var/lib/intelipump lets the
controller (sole bus master) apply CD5 when safe.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import contextlib


DEFAULT_REQUEST_NAME = "set-price-request.json"
DEFAULT_STORED_PRICE_NAME = "unit-price.json"


def request_dir() -> Path:
    return Path(os.environ.get("INTELIPUMP_SET_PRICE_REQUEST_DIR", "/var/lib/intelipump"))


def request_path() -> Path:
    return request_dir() / DEFAULT_REQUEST_NAME


def stored_price_path() -> Path:
    return request_dir() / DEFAULT_STORED_PRICE_NAME


@dataclass(frozen=True, slots=True)
class SetPriceRequest:
    correlation_id: str
    command_id: str
    unit_price_raw: int
    prices_raw: tuple[int, ...]
    requested_by: str | None = None
    requested_at: str | None = None
    pump_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlationId": self.correlation_id,
            "commandId": self.command_id,
            "unitPriceRaw": self.unit_price_raw,
            "pricesRaw": list(self.prices_raw),
            "requestedBy": self.requested_by,
            "requestedAt": self.requested_at or datetime.now(UTC).isoformat(),
            "pumpId": self.pump_id,
        }


@dataclass(frozen=True, slots=True)
class PersistedUnitPrice:
    unit_price_raw: int
    prices_raw: tuple[int, ...]
    source: str = "cloud"
    updated_at: str | None = None


def parse_prices_from_payload(payload: dict[str, Any]) -> tuple[int, tuple[int, ...]]:
    """Return (unit_price_raw, prices_raw tuple) from a cloud command payload."""
    prices = payload.get("pricesRaw") or payload.get("prices_raw")
    unit = payload.get("unitPriceRaw")
    if unit is None:
        unit = payload.get("unit_price_raw")
    if prices is None and unit is None:
        raise ValueError("SET_PRICE payload requires pricesRaw or unitPriceRaw")
    if prices is not None:
        if not isinstance(prices, list) or not prices:
            raise ValueError("pricesRaw must be a non-empty list of integers")
        raw_list: list[int] = []
        for item in prices:
            if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
                raise ValueError("pricesRaw entries must be positive integers")
            raw_list.append(item)
        unit_price = int(unit) if isinstance(unit, int) and not isinstance(unit, bool) else raw_list[0]
        if unit_price <= 0:
            raise ValueError("unitPriceRaw must be a positive integer")
        return unit_price, tuple(raw_list)
    if isinstance(unit, bool) or not isinstance(unit, int) or unit <= 0:
        raise ValueError("unitPriceRaw must be a positive integer")
    return unit, (unit,)


def write_set_price_request(req: SetPriceRequest) -> Path:
    path = request_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(req.to_dict(), separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)
    return path


def write_persisted_unit_price(
    unit_price_raw: int,
    prices_raw: tuple[int, ...] | list[int] | None = None,
    *,
    source: str = "cloud",
) -> Path:
    """Persist last applied unit price so controller restart does not revert to --price."""
    if isinstance(unit_price_raw, bool) or not isinstance(unit_price_raw, int) or unit_price_raw <= 0:
        raise ValueError("unit_price_raw must be a positive integer")
    prices = tuple(prices_raw) if prices_raw else (unit_price_raw,)
    for p in prices:
        if isinstance(p, bool) or not isinstance(p, int) or p <= 0:
            raise ValueError("prices_raw entries must be positive integers")
    path = stored_price_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "unitPriceRaw": unit_price_raw,
        "pricesRaw": list(prices),
        "source": source,
        "updatedAt": datetime.now(UTC).isoformat(),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)
    return path


def read_persisted_unit_price() -> PersistedUnitPrice | None:
    """Load last applied unit price, if present."""
    path = stored_price_path()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        unit, prices = parse_prices_from_payload(
            {
                "unitPriceRaw": raw.get("unitPriceRaw"),
                "pricesRaw": raw.get("pricesRaw"),
            }
        )
    except ValueError:
        return None
    return PersistedUnitPrice(
        unit_price_raw=unit,
        prices_raw=prices,
        source=str(raw.get("source") or "cloud"),
        updated_at=str(raw["updatedAt"]) if raw.get("updatedAt") else None,
    )


def read_set_price_request() -> SetPriceRequest | None:
    """Read a pending request without removing it."""
    path = request_path()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        unit, prices = parse_prices_from_payload(
            {
                "unitPriceRaw": raw.get("unitPriceRaw"),
                "pricesRaw": raw.get("pricesRaw"),
            }
        )
    except ValueError:
        return None
    corr = str(raw.get("correlationId") or "").strip()
    cmd = str(raw.get("commandId") or corr or "set-price").strip()
    if not corr:
        corr = cmd
    return SetPriceRequest(
        correlation_id=corr,
        command_id=cmd,
        unit_price_raw=unit,
        prices_raw=prices,
        requested_by=str(raw["requestedBy"]) if raw.get("requestedBy") else None,
        requested_at=str(raw["requestedAt"]) if raw.get("requestedAt") else None,
        pump_id=str(raw["pumpId"]) if raw.get("pumpId") else None,
    )


def consume_set_price_request() -> SetPriceRequest | None:
    """Read and remove a pending request. Returns None if absent or invalid."""
    path = request_path()
    req = read_set_price_request()
    if req is None:
        if path.is_file():
            with contextlib.suppress(OSError):
                path.unlink()
        return None
    try:
        path.unlink()
    except OSError:
        return None
    return req

