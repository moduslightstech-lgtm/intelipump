# Persistence architecture (Phase 7)

## Overview

Local durability uses **SQLite** via **SQLAlchemy async** + **aiosqlite**.

Default URL (from settings):

```text
sqlite+aiosqlite:///./data/intelipump.db
```

Schema version: **1** (`controller_metadata.schema_version`).

## Layering

| Layer | Responsibility |
|-------|----------------|
| `persistence/models.py` | ORM tables (internal) |
| `persistence/repositories/*` | Typed DTO access; no ORM leakage |
| `persistence/unit_of_work.py` | Single DB transaction boundary |
| `services/*` | Orchestration (transactions, state, recovery) |
| `services/persistence_worker.py` | Non-blocking write queue |
| Pure state machine | **No** DB calls |

## Schema (tables)

1. `controller_metadata`
2. `pumps` — unique `(station_id, logical_pump_id)`, `(station_id, dart_address)`
3. `pump_state_snapshots`
4. `transactions` — unique `transaction_uuid`, unique `source_completion_key`
5. `transaction_events` — unique `(transaction_id, event_key)`
6. `commands` / `command_attempts`
7. `alarms` — unique `(station_id, source_key)`
8. `audit_log` — tamper-evident hash chain
9. `sync_queue` — unique `deduplication_key` (no MQTT delivery in Phase 7)
10. `configuration_versions`

## SQLite operational guidance (Raspberry Pi)

- **WAL mode** is enabled on every connection (`journal_mode=WAL`).
- **Foreign keys** enabled per connection (`foreign_keys=ON`).
- **Busy timeout** 30s to reduce `SQLITE_BUSY` under concurrent readers.
- Place the DB on durable storage (SD/SSD); avoid NFS for the live DB file.
- Keep `synchronous=NORMAL` with WAL (balance durability vs SD wear).

## Consistent backups

A consistent backup must include:

- the main database file (e.g. `intelipump.db`)
- the **WAL** file (`intelipump.db-wal`) when present
- the **shared-memory** file (`intelipump.db-shm`) when present

Prefer `sqlite3 .backup` / online backup API, or stop writers then copy all three.

## Transaction boundaries

Repositories mutate only through `unit_of_work()` / `session.begin()`.  
Transaction completion + `transaction_events` + `sync_queue` enqueue share one commit.

## Initialization

- No DB connection at import time.
- `create_engine` + `init_schema` + connect-hook pragmas.
- Simple versioned initializer in `migrations.py` (Alembic optional later).

## Retention (config only)

`DatabaseSettings` exposes retention day counts. Automated deletion is **not**
implemented. Never delete unresolved transactions or undelivered sync items.
