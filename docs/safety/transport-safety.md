# Transport Safety (Phase 6)

## Defaults

- Environment: LAB
- Mode: LISTEN_ONLY
- `active_commands_enabled`: false
- Physical enable required for any future active path

## Allowed

- Virtual / LAB polling
- Simulator-only READ_STATUS / READ_TOTALS queued items in LAB

## Blocked

- AUTHORIZE / SET_PRICE / STOP / RESET / SUSPEND / RESUME / presets on the
  controller outbound path
- FIELD_CONTROL mode
- Hidden bypasses

## Queue rules

- Bounded size
- Expired items rejected
- Every item carries `simulator_only`, command type, idempotency, TTL
