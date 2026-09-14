def int_to_bcd(value: int, length_bytes: int) -> bytes:
    """Converts integer to packed BCD. Raises ValueError on overflow."""
    s = f"{value:0{length_bytes * 2}d}"
    if len(s) > length_bytes * 2:
        raise ValueError(f"Value {value} overflows BCD length {length_bytes}")
    return bytes.fromhex(s)

def bcd_to_int(bcd_bytes: bytes) -> int:
    """Converts Packed BCD bytes to integer, validating decimal nibbles (0-9)."""
    val = 0
    for b in bcd_bytes:
        high = (b >> 4) & 0x0F
        low = b & 0x0F
        if high > 9 or low > 9:
            raise ValueError(f"Invalid BCD nibble detected: {hex(b)}")
        val = (val * 100) + (high * 10) + low
    return val