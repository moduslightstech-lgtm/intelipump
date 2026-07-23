# Serial Adapter Discovery

Phase 10 enumerates USB serial adapters via pyserial `list_ports`.

## Captured fields

- device path (prefer `/dev/serial/by-id/...` on Linux)
- stable identifier
- VID / PID
- serial number
- manufacturer / product
- interface
- USB location
- hardware ID

## Path policy

Do **not** hard-code `/dev/ttyUSB0` as the only identity. Device numbering can change across reboots.

| Platform | Preferred paths |
| --- | --- |
| Linux | `/dev/serial/by-id/...`, then `/dev/ttyUSB*`, `/dev/ttyACM*` |
| macOS | `/dev/cu.usbserial-*`, `/dev/cu.wchusbserial*`, `/dev/cu.usbmodem*` |

## Configuration

```bash
INTELIPUMP_BENCH__CONTROLLER_PORT=/dev/serial/by-id/...
INTELIPUMP_BENCH__SIMULATOR_PORT=/dev/serial/by-id/...
INTELIPUMP_BENCH__CONTROLLER_ADAPTER_STABLE_ID=...
INTELIPUMP_BENCH__SIMULATOR_ADAPTER_STABLE_ID=...
```
