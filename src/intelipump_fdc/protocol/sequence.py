"""Wayne sequence manager for DATA 0x3N / ACK-family 0xCN pairing."""

from __future__ import annotations

from dataclasses import dataclass, field

from intelipump_fdc.protocol.dart.line.constants import ACK_BASE, DATA_BASE, SEQUENCE_MASK


class SequenceError(ValueError):
    """Invalid sequence or address."""


@dataclass
class WayneSequenceManager:
    """Per-address sequence state (0x0..0xF). Observed wrap F→0."""

    _by_address: dict[int, int] = field(default_factory=dict)
    share_across_addresses: bool = False
    _shared: int = 0

    def current_sequence(self, address: int) -> int:
        self._validate_address(address)
        if self.share_across_addresses:
            return self._shared
        return self._by_address.get(address, 0)

    def next_sequence(self, address: int) -> int:
        """Return current sequence then advance (F→0)."""
        self._validate_address(address)
        current = self.current_sequence(address)
        nxt = 0 if current == 0x0F else current + 1
        if self.share_across_addresses:
            self._shared = nxt
        else:
            self._by_address[address] = nxt
        return current

    def set_sequence(self, address: int, sequence: int) -> None:
        self._validate_address(address)
        if not 0 <= sequence <= 0x0F:
            raise SequenceError(f"sequence out of range: {sequence}")
        if self.share_across_addresses:
            self._shared = sequence
        else:
            self._by_address[address] = sequence

    @staticmethod
    def message_byte(sequence: int) -> int:
        if not 0 <= sequence <= 0x0F:
            raise SequenceError(f"sequence out of range: {sequence}")
        return DATA_BASE | sequence

    @staticmethod
    def expected_ack_byte(sequence: int) -> int:
        if not 0 <= sequence <= 0x0F:
            raise SequenceError(f"sequence out of range: {sequence}")
        return ACK_BASE | sequence

    @staticmethod
    def extract_sequence_nibble(control: int) -> int:
        return control & SEQUENCE_MASK

    def _validate_address(self, address: int) -> None:
        if not 0 <= address <= 0xFF:
            raise SequenceError(f"address out of range: {address}")
