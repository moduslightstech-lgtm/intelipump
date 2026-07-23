# Cursor Master Prompt

Implement the InteliPump Raspberry Pi Wayne DART forecourt controller.

Read:

1. `docs/architecture/architecture.md`
2. `docs/architecture/state-machine.md`
3. `docs/safety/safety-requirements.md`
4. `docs/safety/bench-test-policy.md`
5. `docs/protocol-notes/dart-line-summary.md`
6. `docs/protocol-notes/dart-application-summary.md`
7. `docs/development/implementation-plan.md`
8. Authorized references under `docs/reference/private/`

Constraints:

- Raspberry Pi 5 and Linux
- Python 3.12
- uv
- FastAPI
- asyncio
- pyserial/pyserial-asyncio
- SQLite WAL
- MQTT
- pytest, Ruff, mypy
- default mode LISTEN_ONLY
- active commands require configuration and physical safety enable
- never automatically authorize after restart
- never blindly retry non-idempotent commands
- preserve raw frames
- audit every external command
- never guess protocol fields

Work only on the current phase. Start with Phase 0. Do not implement serial transmission or active commands.
