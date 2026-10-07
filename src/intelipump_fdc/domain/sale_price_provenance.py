"""Sale unit-price provenance — keep command path separate from observation.

Authoritative sale ``raw_price`` / ``pricePerLiter`` must be **pump-observed**
(positive DC3 / session face evidence). LINK_ACK and application confirmation
of SET_PRICE are command lifecycle signals only. Amount÷volume rounding is a
diagnostic estimate and must never be stored as observed price.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class SalePriceSource(StrEnum):
    PUMP_OBSERVED = "pump_observed"
    # Diagnostic only — never authoritative for Sales / cloud price_per_liter.
    ESTIMATED_FROM_TOTALS = "estimated_from_totals"


def positive_raw_price(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def estimate_unit_price_from_totals(
    raw_volume: int | None, raw_amount: int | None
) -> int | None:
    """Rounded amount/volume estimate (uncertain). Not pump-observed.

    Examples (genuine 1355 sales with rounded volumes):
      amount=100000, volume=74 → 1351
      amount=110000, volume=81 → 1358
    """
    if (
        not isinstance(raw_volume, int)
        or not isinstance(raw_amount, int)
        or isinstance(raw_volume, bool)
        or isinstance(raw_amount, bool)
        or raw_volume <= 0
        or raw_amount <= 0
    ):
        return None
    inferred = int(round(raw_amount / raw_volume))
    return inferred if inferred > 0 else None


def sale_price_mqtt_fields(
    *,
    observed_raw: int | None,
    price_decimals: int | None,
    raw_volume: int | None = None,
    raw_amount: int | None = None,
) -> dict[str, Any]:
    """Build MQTT price keys for TRANSACTION_COMPLETED.

    When observation is missing, ``pricePerLiter`` is omitted/null and
    ``priceUncertain`` is true. An optional estimated raw may be attached for
    diagnostics only.
    """
    observed = positive_raw_price(observed_raw)
    if observed is not None:
        dec = price_decimals if price_decimals is not None else 0
        price_per = round(observed / (10**dec), 2)
        price_s = f"{price_per:.2f}"
        return {
            "raw_unit_price": observed,
            "price_decimals": dec,
            "pricePerLitre": price_s,
            "pricePerLiter": price_s,
            "priceSource": SalePriceSource.PUMP_OBSERVED.value,
            "priceUncertain": False,
            "estimatedUnitPriceRaw": None,
            "estimatedPriceUncertain": None,
        }
    estimated = estimate_unit_price_from_totals(raw_volume, raw_amount)
    return {
        "raw_unit_price": None,
        "price_decimals": None,
        "pricePerLitre": None,
        "pricePerLiter": None,
        "priceSource": None,
        "priceUncertain": True,
        "estimatedUnitPriceRaw": estimated,
        "estimatedPriceUncertain": True if estimated is not None else None,
        "estimatedPriceSource": (
            SalePriceSource.ESTIMATED_FROM_TOTALS.value
            if estimated is not None
            else None
        ),
    }
