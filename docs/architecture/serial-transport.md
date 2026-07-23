# Serial Transport (Phase 6 / Phase 10)

## Boundaries

- Protocol/session code depends on `ByteTransport`, never on pyserial types.
- `SerialTransport` is the only transport module that imports `serial`.
- Hardware discovery/validation lives under `intelipump_fdc.hardware`.
- `MemoryTransport` pairs support LAB tests without PTYs.

## Interface

`open`, `close`, `read`, `write`, `drain`, `is_open`, `metadata`, async context manager.

## Serial configuration

DART defaults (validated, no silent fallback):

- 9600 baud (19200 also allowed in SerialConfig; Phase 10 bench requires 9600)
- 8 data bits
- odd parity
- 1 stop bit
- flow control disabled
- optional exclusive open (Linux)

Odd parity failures raise explicitly — never downgrade to 8N1.

## Stream assembly

`FrameStreamAssembler` accepts arbitrary chunks and emits frames ending at an
**unescaped** trailing SF. Escaped `DLE SF` is never a terminator. Overflow and
malformed candidates preserve rejected bytes for diagnostics.

## Physical RS-485 office bench

See `docs/testing/rs485-office-bench.md` and `docs/hardware/adapter-discovery.md`.

## Virtual serial

See `docs/testing/virtual-serial-lab.md`.
