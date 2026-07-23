# API tests

- `tests/unit/api/` — health, correlation, pumps, transactions, evaluate
- `tests/unit/events/` — broker limits / critical delivery
- `tests/integration/test_api_event_stream.py` — SSE/WS/LAB transport gates
- `tests/integration/test_api_controller_lifecycle.py` — shared engine + lifecycle
- `tests/integration/test_api_command_safety.py` — no production command route

Each test uses a temporary SQLite URL via `INTELIPUMP_DATABASE__URL` and
clears `get_settings` cache.
