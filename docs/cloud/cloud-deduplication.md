# Cloud Deduplication

MQTT QoS does **not** provide business exactly-once delivery. The device and cloud must cooperate.

## Device rules

- Every outbound envelope includes a stable `deduplicationKey`.
- Transaction completion keys are `tx-completed:{source_completion_key}` and survive restart.
- Sync queue enqueue is unique on `deduplication_key` (SQLite).
- Delivered rows are marked `DELIVERED` but not deleted immediately.
- Heartbeats use an ephemeral replacement slot (`heartbeat:{deviceId}:latest`) — missed beats are not unbounded-queued.

## Cloud consumer rules

- Upsert or reject duplicates by `deduplicationKey` (preferred) and/or `messageId`.
- Treat reconnect republish as expected.
- Keep LAB and PROD separated.
- Exclude `simulated=true` from production reporting aggregates.
