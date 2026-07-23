# DigitalOcean Cloud Integration (Phase 9)

This repository publishes LAB MQTT messages for the existing DigitalOcean stack:

- Mosquitto broker
- PostgreSQL cloud database
- Python MQTT consumer
- FastAPI cloud API

Cloud server code is **not** modified from this local Phase 9 tree when it lives in a separate repository. Use this contract plus `docs/cloud/cursor-prompt-cloud-consumer.md`.

## Device → cloud responsibilities

| Layer | Responsibility |
| --- | --- |
| Local SQLite | Source of truth while offline; durable `sync_queue` |
| Local MQTT client | Heartbeats, ONLINE/OFFLINE status, queue delivery, command intake |
| Cloud consumer | Upsert by `deduplicationKey`, separate LAB vs PROD, reject simulator rows from prod reporting |

## LAB topic prefix

All Phase 9 device traffic uses:

`intelipump/lab/...`

Production will later use `intelipump/prod/...`. Do not hardcode Nigerian production station IDs in the device repo.

## Required consumer updates

1. Accept `schemaVersion` (`1.0`).
2. Prefer `messageId` + `deduplicationKey` for idempotent upserts.
3. Preserve raw scaled integers (`raw_volume`, `raw_amount`, `raw_unit_price`) plus decimal metadata.
4. Distinguish `environment` and `simulated`.
5. Handle heartbeat and pump-event topics separately from transactions.
6. Never insert `simulated=true` rows into production reporting tables.

## Security

- Credentials from environment variables only.
- TLS when configured.
- Device publishes only its station/device topics.
- Device subscribes only to its command topic when enabled.
- No production active command execution in Phase 9.

See also: [mqtt-topics.md](mqtt-topics.md), [message-schemas.md](message-schemas.md), [command-intake.md](command-intake.md), [cloud-deduplication.md](cloud-deduplication.md).
