def calculate_crc16_dart(data: bytes) -> int:
    """Calculates CRC-16 for Wayne DART Level 2 frames (Init 0x0000, Poly 0xA001)."""
    crc = 0x0000
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF