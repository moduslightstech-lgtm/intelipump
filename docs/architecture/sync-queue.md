# Sync queue foundations (Phase 7)

Durable **outbound** queue storage only. **No MQTT publishing** and **no cloud
worker** in this phase.

## Operations

| Op | Behavior |
|----|----------|
| `enqueue` | Insert with `deduplication_key`; existing key → no-op |
| `claim_batch` | Claim bounded PENDING rows (`status=CLAIMED`, set `locked_at`) |
| `mark_delivered` | Terminal success |
| `mark_failed` | Return to PENDING, increment `attempt_count`, set `available_at` backoff |
| `release_stale_locks` | CLAIMED older than threshold → PENDING |
| `pending_count` | PENDING + CLAIMED |

Ordering: claim sorts by `created_at` ascending (practical per-entity FIFO).

## Transactional producers

Queue rows are created in the same DB transaction as:

- completed / started transactions
- meaningful state changes needing cloud visibility
- alarms
- (optionally) audit-linked events when configured by the bridge

## Payload requirements

Include `environment`, `station_id`, and `simulated` where applicable.

## Non-goals

- MQTT/topic mapping
- delivery retries against a broker
- cloud acknowledgment protocols
