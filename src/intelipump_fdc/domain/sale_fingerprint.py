"""Stable completed-sale fingerprints for restart / reconnect dedupe."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SaleFingerprintParts:
    station_id: str
    dart_address: int
    nozzle_id: int | None
    raw_volume: int
    raw_amount: int
    raw_price: int | None = None

    def canonical(self) -> str:
        nozzle = "n?" if self.nozzle_id is None else f"n{int(self.nozzle_id)}"
        price = "p?" if self.raw_price is None else f"p{int(self.raw_price)}"
        return (
            f"{self.station_id}|a{int(self.dart_address)}|{nozzle}|"
            f"v{int(self.raw_volume)}|a{int(self.raw_amount)}|{price}"
        )

    def digest(self) -> str:
        return hashlib.sha256(self.canonical().encode("utf-8")).hexdigest()[:32]


def sale_fingerprint(
    *,
    station_id: str,
    dart_address: int,
    nozzle_id: int | None,
    raw_volume: int,
    raw_amount: int,
    raw_price: int | None = None,
) -> str:
    """Return a compact fingerprint for a completed face reading."""
    return SaleFingerprintParts(
        station_id=station_id,
        dart_address=dart_address,
        nozzle_id=nozzle_id,
        raw_volume=raw_volume,
        raw_amount=raw_amount,
        raw_price=raw_price,
    ).digest()


def startup_baseline_completion_key(fingerprint: str) -> str:
    """Non-publishable completion key used only to mark baseline observation."""
    return f"startup-baseline:{fingerprint}"


def stable_completion_key(
    *,
    transaction_uuid: str | None,
    fingerprint: str,
) -> str:
    """Prefer sale UUID; fall back to fingerprint for retries of the same sale."""
    if transaction_uuid:
        return f"complete:{transaction_uuid}"
    return f"complete-fp:{fingerprint}"
