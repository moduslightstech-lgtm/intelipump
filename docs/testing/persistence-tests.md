# Persistence tests (Phase 7)

## Layout

- `tests/unit/persistence/` — schema, repos, constraints, audit, sync queue
- `tests/unit/services/` — worker backpressure, recovery
- `tests/integration/test_persistent_controller_lifecycle.py`
- `tests/integration/test_restart_recovery.py`

## Rules

- Use **temporary SQLite files** per test (`tmp_path`).
- Do **not** share one DB across parallel tests.
- Prefer `unit_of_work` for explicit commits.

## Coverage map

| # | Concern | Location |
|---|---------|----------|
| 1–4 | init, WAL, FK, pump uniqueness | `test_database.py` |
| 5–7 | latest state, monotonic version, suppress dup snaps | `test_states.py` |
| 8–13 | tx create/update/complete/dedupe/raw ints | `test_transactions.py` |
| 14–18 | rejected cmd, attempts, audit chain | `test_commands_audit.py` |
| 19–22 | sync queue | `test_sync_queue.py` |
| 23–27, 29–30 | restart + worker | `test_worker_recovery.py` |
| 28, 31–33 | controller lifecycle / restart | integration tests |

## Manual restart check

1. Start simulator serial + controller with file-backed LAB DB.
2. Complete a simulated sale (or seed a completed transaction).
3. Stop controller.
4. Restart with same `--database-url --show-recovery-report`.
5. Verify completed transaction exists once; pending AUTHORIZE not replayed.
