"""SET_PRICE request file bridge tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceRequest,
    consume_set_price_request,
    parse_prices_from_payload,
    read_set_price_request,
    write_set_price_request,
)


def test_parse_prices_unit_only() -> None:
    unit, prices = parse_prices_from_payload({"unitPriceRaw": 1400})
    assert unit == 1400
    assert prices == (1400,)


def test_parse_prices_list() -> None:
    unit, prices = parse_prices_from_payload({"pricesRaw": [1400, 1400]})
    assert unit == 1400
    assert prices == (1400, 1400)


def test_write_read_consume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_request(
        SetPriceRequest(
            correlation_id="c1",
            command_id="cmd1",
            unit_price_raw=1400,
            prices_raw=(1400,),
            requested_by="admin@example.com",
        )
    )
    peeked = read_set_price_request()
    assert peeked is not None
    assert peeked.unit_price_raw == 1400
    assert (tmp_path / "set-price-request.json").is_file()
    got = consume_set_price_request()
    assert got is not None
    assert got.correlation_id == "c1"
    assert not (tmp_path / "set-price-request.json").is_file()
    assert consume_set_price_request() is None


def test_persisted_unit_price_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        read_persisted_unit_price,
        write_persisted_unit_price,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    assert read_persisted_unit_price() is None
    write_persisted_unit_price(1450, (1450,), source="cloud")
    got = read_persisted_unit_price()
    assert got is not None
    assert got.unit_price_raw == 1450
    assert got.prices_raw == (1450,)
    assert got.source == "cloud"
    assert (tmp_path / "unit-price.json").is_file()
