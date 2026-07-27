"""Documented CD5 Price Update transaction model (Pump Interface Rev 2.11).

TRANS=0x05, LNG=3*N, each price is three-byte packed BCD (MSB first).
PRI1 -> logical nozzle 1, PRI2 -> logical nozzle 2, ...
"""

from __future__ import annotations

from dataclasses import dataclass, field

from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.crc import dart_crc16
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame
from intelipump_fdc.protocol.price_codec import (
    PriceCodecError,
    decode_price_bcd_3,
    encode_price_bcd_3,
)
from intelipump_fdc.protocol.sequence import WayneSequenceManager

CD5_TRANS = 0x05
SUPPORTED_LOGICAL_NOZZLE_COUNTS = frozenset({1, 2})


class CD5Error(ValueError):
    """Invalid CD5 construction arguments."""


@dataclass(frozen=True, slots=True)
class NozzlePrice:
    logical_nozzle: int
    input_price: int
    packed_bcd: bytes

    @property
    def packed_bcd_hex(self) -> str:
        return self.packed_bcd.hex(" ")


@dataclass(frozen=True, slots=True)
class CD5PriceUpdate:
    """Explicit documented CD5 Price Update object."""

    logical_nozzle_count: int
    prices: tuple[NozzlePrice, ...]
    transaction_type: int = CD5_TRANS
    payload_length: int = 0
    application_payload: bytes = b""
    source_reference: str = "Pump Interface Rev 2.11, page 15, CD5"
    confidence: str = "HIGH_DOCUMENTED_CD5_FORMAT"
    logical_nozzle_mapping_confirmed: bool = False
    price_scale_confirmed: bool = False
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def payload_hex(self) -> str:
        return self.application_payload.hex(" ")


def build_cd5_price_update(
    prices_by_nozzle: dict[int, int] | list[int],
    *,
    logical_nozzle_count: int,
    logical_nozzle_mapping_confirmed: bool,
    price_scale_confirmed: bool,
) -> CD5PriceUpdate:
    """Build a CD5 application payload for the documented nozzle count."""
    if logical_nozzle_count not in SUPPORTED_LOGICAL_NOZZLE_COUNTS:
        raise CD5Error(
            f"unsupported/unknown logical_nozzle_count={logical_nozzle_count}; "
            f"supported={sorted(SUPPORTED_LOGICAL_NOZZLE_COUNTS)}"
        )
    if not logical_nozzle_mapping_confirmed:
        raise CD5Error("logical_nozzle_mapping_confirmed required")
    if not price_scale_confirmed:
        raise CD5Error("price_scale_confirmed required (no inferred decimal scale)")

    if isinstance(prices_by_nozzle, dict):
        if set(prices_by_nozzle.keys()) != set(range(1, logical_nozzle_count + 1)):
            raise CD5Error(
                f"prices must supply exactly nozzles 1..{logical_nozzle_count}, "
                f"got {sorted(prices_by_nozzle.keys())}"
            )
        ordered = [prices_by_nozzle[i] for i in range(1, logical_nozzle_count + 1)]
    else:
        ordered = list(prices_by_nozzle)
        if len(ordered) != logical_nozzle_count:
            raise CD5Error(
                f"exactly {logical_nozzle_count} prices required, got {len(ordered)}"
            )

    if len(ordered) == 0:
        raise CD5Error("reject zero prices")
    if len(ordered) != logical_nozzle_count:
        raise CD5Error(
            f"exactly {logical_nozzle_count} prices required, got {len(ordered)}"
        )

    nozzle_prices: list[NozzlePrice] = []
    price_bytes = bytearray()
    for idx, price in enumerate(ordered, start=1):
        if type(price) is not int:
            raise CD5Error("floating-point / non-int prices rejected")
        try:
            bcd = encode_price_bcd_3(price)
        except PriceCodecError as exc:
            raise CD5Error(str(exc)) from exc
        # Round-trip validate.
        if decode_price_bcd_3(bcd) != price:
            raise CD5Error("BCD round-trip failed")
        nozzle_prices.append(
            NozzlePrice(logical_nozzle=idx, input_price=price, packed_bcd=bcd)
        )
        price_bytes.extend(bcd)

    lng = len(price_bytes)
    if lng != 3 * logical_nozzle_count:
        raise CD5Error(f"internal LNG mismatch: {lng}")
    payload = bytes((CD5_TRANS, lng)) + bytes(price_bytes)
    return CD5PriceUpdate(
        logical_nozzle_count=logical_nozzle_count,
        prices=tuple(nozzle_prices),
        transaction_type=CD5_TRANS,
        payload_length=lng,
        application_payload=payload,
        logical_nozzle_mapping_confirmed=logical_nozzle_mapping_confirmed,
        price_scale_confirmed=price_scale_confirmed,
        metadata={
            "pri_order": "PRI1=logical_nozzle_1, PRI2=logical_nozzle_2, ...",
        },
    )


def build_cd5_candidate_frame(
    *,
    logical_address: int,
    sequence: int,
    cd5: CD5PriceUpdate,
) -> tuple[bytes, int, bytes]:
    """Build complete outer DART DATA frame in memory (never transmit).

    Returns ``(raw_frame, crc_int, expected_ack_frame_hypothesis)``.
    """
    wire = encode_wire_address(logical_address)
    frame = build_data_frame(wire, sequence, cd5.application_payload)
    control = WayneSequenceManager.message_byte(sequence)
    crc = dart_crc16(bytes((wire, control)) + cd5.application_payload)
    expected_ack = bytes(
        (wire, WayneSequenceManager.expected_ack_byte(sequence), 0xFA)
    )
    return frame, crc, expected_ack
