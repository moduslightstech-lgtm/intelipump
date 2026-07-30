# Command Eligibility (Evaluation Only)

Phase 4 implements **pure guard evaluation**. No command is transmitted,
queued, encoded, or executed. Default controller mode remains `LISTEN_ONLY`
with active commands disabled.

## Command types

| Command | Role |
|---|---|
| READ_STATUS | Read eligibility |
| READ_TOTALS | Read eligibility |
| SET_PRICE | Active (future) |
| RESET | Active (future) |
| AUTHORIZE | Active (future) |
| STOP | Active (future) |
| SUSPEND | Active (future) |
| RESUME | Active (future) |
| PRESET_AMOUNT | Active (future) |
| PRESET_VOLUME | Active (future) |

## Result fields

Every evaluation returns:

- `eligible` — state/context rules passed
- `command`
- `current_state`
- `blocking_reasons`
- `warnings`
- `requires_physical_enable`
- `requires_active_commands_enabled`

For active commands, `requires_physical_enable` and
`requires_active_commands_enabled` are always `True` in the result. They do
not alone set `eligible=False`; they document mandatory future execution
gates (physical control-enable + software active-command enable).

## Rules summary

### READ_STATUS

Allowed in all normalized states. Does not require physical enable or active
commands. No poll is sent.

### READ_TOTALS

Allowed when `communication_healthy`. Evaluation only.

### SET_PRICE

- Communication healthy
- State in `{READY, RESET, NOT_PROGRAMMED}` only
- Not FILLING / AUTHORIZED / SUSPENDED / FILLING_COMPLETE
- No active unresolved transaction

### AUTHORIZE

**Protocol-complete default (Wayne):** AUTHORIZE may precede or follow
nozzle lift. Eligibility allows state in `{RESET, READY, NOZZLE_UP}`.

- Selected nozzle known (`selected_nozzle` set; nozzle 0 / unknown blocks)
- Price verified (`price_verified`; DC3 filling-price check when a nozzle
  is selected; CD5 programming may mark verified when all nozzles are
  programmed and decimals are known)
- Communication healthy
- No active unresolved transaction
- No fault
- Execution remains disabled outside this evaluation
- Never auto-authorize after restart or communication recovery

**Optional deployment policy:** when
`require_nozzle_lift_before_authorize=True`, AUTHORIZE is blocked unless
state is `NOZZLE_UP` (lift-first). This is InteliPump policy, not a Wayne
protocol requirement. Blocking reason:
`require_nozzle_lift_before_authorize`.

### STOP

Eligible only in `{AUTHORIZED, FILLING, SUSPENDED}` with healthy communication.

### SUSPEND

Eligible only in `FILLING`.

### RESUME

Eligible only in `SUSPENDED`.

### RESET

- Not blindly allowed during `FILLING`
- Explicit allow-list: FILLING_COMPLETE, LIMIT_REACHED, READY, RESET,
  NOT_PROGRAMMED, NOZZLE_UP, AUTHORIZED, FAULTED, DISCOVERING

### PRESET_AMOUNT / PRESET_VOLUME

- Only before filling (READY, RESET, NOZZLE_UP, AUTHORIZED)
- Positive preset value required
- Selected nozzle and verified price required
- Communication healthy

## Active-command safety requirements

1. Default mode is `LISTEN_ONLY`.
2. Never enable active TX unless an approved later phase requires it.
3. Never automatically authorize after restart.
4. Never blindly retry non-idempotent commands.
5. Require a physical control-enable signal before any future execution path.
6. Guard results must surface `requires_physical_enable` and
   `requires_active_commands_enabled` for active commands.

## Restart interaction

If a pending `AUTHORIZE` (or other non-idempotent command) was persisted,
restart reconciliation **discards** it and never replays it. See
`docs/architecture/pump-state-machine.md`.
