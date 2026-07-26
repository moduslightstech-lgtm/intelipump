# Captured Wayne iGEM / ePump line fixtures

Source of truth: passive ePump capture (merged ONE_PORT bus).
These bytes are preserved exactly. Payload field meanings are **not** proven.

- Logical side 1 → wire ADR `0x50`
- Logical side 2 → wire ADR `0x51`
- Status polls: `50 20 FA`, `51 20 FA`
- Direction is inferred only (merged bus)
- `C0`–`CF` are sequence-control / possible acknowledgement — **not** nozzle lift
- Simulator response fixtures are labeled separately and are not field-semantic definitions

Invalid for this profile: synthetic `01 20 FA` (see `INVALID_SYNTHETIC_POLL_01_20_FA.hex`).
