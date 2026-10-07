"""Sale unit-price provenance — command path vs pump observation."""

from __future__ import annotations

from intelipump_fdc.domain.sale_price_provenance import (
    SalePriceSource,
    estimate_unit_price_from_totals,
    sale_price_mqtt_fields,
)


def test_rounded_volume_estimates_are_uncertain_not_1355() -> None:
    """Genuine 1355 sales with rounded volumes must not become authoritative."""
    assert estimate_unit_price_from_totals(74, 100000) == 1351
    assert estimate_unit_price_from_totals(81, 110000) == 1358
    fields_a = sale_price_mqtt_fields(
        observed_raw=None,
        price_decimals=0,
        raw_volume=74,
        raw_amount=100000,
    )
    assert fields_a["priceUncertain"] is True
    assert fields_a["pricePerLiter"] is None
    assert fields_a["raw_unit_price"] is None
    assert fields_a["estimatedUnitPriceRaw"] == 1351
    assert fields_a["estimatedPriceUncertain"] is True
    assert fields_a["estimatedPriceSource"] == SalePriceSource.ESTIMATED_FROM_TOTALS.value

    fields_b = sale_price_mqtt_fields(
        observed_raw=None,
        price_decimals=0,
        raw_volume=81,
        raw_amount=110000,
    )
    assert fields_b["estimatedUnitPriceRaw"] == 1358
    assert fields_b["priceUncertain"] is True
    assert fields_b["pricePerLiter"] is None


def test_observed_price_is_authoritative() -> None:
    fields = sale_price_mqtt_fields(
        observed_raw=1355,
        price_decimals=0,
        raw_volume=74,
        raw_amount=100000,
    )
    assert fields["priceUncertain"] is False
    assert fields["pricePerLiter"] == "1355.00"
    assert fields["raw_unit_price"] == 1355
    assert fields["priceSource"] == SalePriceSource.PUMP_OBSERVED.value
    assert fields["estimatedUnitPriceRaw"] is None
