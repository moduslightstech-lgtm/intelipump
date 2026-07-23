"""Split a DART DATA payload into application transactions.

Source: WAYNE EUROPE - Protocol Specification Dart Pump Interface Revision 2.11,
page 10:

  TRANS (1) + LNG (1) + DATA (LNG bytes)

A block may contain one or more transactions. LNG is present even for
fixed-length transactions so unknown types can be skipped. Parsing is
complete when the sum of handled transaction sizes equals the block length.
Trailing bytes that cannot form a complete header/body are preserved.
"""

from __future__ import annotations

from intelipump_fdc.protocol.dart.application.constants import HEADER_SIZE
from intelipump_fdc.protocol.dart.application.models import RawTransaction, SplitResult


def split_transactions(payload: bytes) -> SplitResult:
    """Split DATA-payload bytes into raw application transactions.

    Never discards bytes: incomplete tails are returned in ``trailing_bytes``.
    """
    transactions: list[RawTransaction] = []
    warnings: list[str] = []
    index = 0
    length = len(payload)

    while index < length:
        remaining = length - index
        if remaining < HEADER_SIZE:
            warnings.append(
                f"truncated transaction header at offset {index}: "
                f"{remaining} byte(s) remain"
            )
            break

        transaction_id = payload[index]
        data_len = payload[index + 1]
        total = HEADER_SIZE + data_len
        if index + total > length:
            warnings.append(
                f"truncated transaction data at offset {index}: "
                f"TRANS=0x{transaction_id:02X} LNG={data_len} but only "
                f"{length - index - HEADER_SIZE} data byte(s) available"
            )
            break

        raw = bytes(payload[index : index + total])
        data = bytes(payload[index + HEADER_SIZE : index + total])
        transactions.append(
            RawTransaction(
                transaction_id=transaction_id,
                length=data_len,
                data=data,
                raw=raw,
                offset=index,
            )
        )
        index += total

    trailing = bytes(payload[index:])
    return SplitResult(
        transactions=tuple(transactions),
        trailing_bytes=trailing,
        warnings=tuple(warnings),
    )
