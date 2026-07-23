# Event broker architecture

In-process fan-out (`events/broker.py`), independent of FastAPI and SQLite.

- Monotonic `sequence` per process lifetime
- UUID `event_id`
- Bounded subscriber queues
- Independent SSE/WS subscriber caps
- Filtering via `EventFilter`
- Persistence remains authoritative for history

Controller poll loop must never await subscriber drains; publish is `put_nowait`.
