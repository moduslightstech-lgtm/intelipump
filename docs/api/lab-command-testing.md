# LAB command testing

## Evaluate only

`POST /api/v1/pumps/{pump_id}/commands/evaluate`

Persists command + audit. **Never transmits.**

## LAB simulator enqueue

`POST /api/v1/lab/pumps/{pump_id}/commands`

Requires **all** of:

1. `environment == LAB`
2. `simulator_only == true`
3. `safety.allow_lab_simulator_commands == true`
4. Controller loop attached
5. Transport metadata `kind` in `{memory, serial_virtual}`
6. Supported CD1 command mapping

Rejects physical/unknown serial devices.

## Why production command APIs are absent

LISTEN_ONLY remains default. Field active commands require later phases,
physical enable, and explicit authorization — not Phase 8.
