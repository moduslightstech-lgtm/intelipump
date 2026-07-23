# Restart recovery (Phase 7)

## Startup sequence

1. Configure SQLite pragmas and migrate/init schema.
2. Ensure configured pump rows exist.
3. Load latest `pump_state_snapshots` per pump.
4. Load unresolved transactions (`ACTIVE` / `SUSPENDED` / `OPEN`).
5. Load `PENDING` / `IN_PROGRESS` commands.
6. Expire time-expired commands → `EXPIRED`.
7. Convert interrupted non-idempotent commands → `NEEDS_RECONCILIATION`
   (**never replay** AUTHORIZE, SET_PRICE, RESET, STOP, SUSPEND, RESUME, presets).
8. Keep unresolved active transactions open.
9. Seed Phase 4 state machines with persisted context; force
   `communication_healthy=False` until live observations arrive.
10. Release stale sync-queue locks.
11. Produce a typed `RecoveryReport`.

## Recovery report fields

- `schema_version`
- `pumps_restored`
- `unresolved_transactions`
- `commands_expired`
- `commands_needing_reconciliation`
- `queue_locks_released`
- `warnings`
- `pump_contexts` (for seeding)

## Safety invariants

- Never automatically authorize after restart.
- Never blindly retry non-idempotent commands.
- LISTEN_ONLY remains default; no production active command endpoints.

## CLI

```bash
intelipump-controller --database-url sqlite+aiosqlite:///./data/lab.db \
  --show-recovery-report --duration 10
```

`--reset-lab-database --yes` works only when `environment=LAB`.
