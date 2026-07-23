# Transaction lifecycle (Phase 7)

## Responsibilities

`TransactionService` owns durable sale lifecycle:

1. **Begin** when dispensing becomes active (`FILLING`).
2. **Update** live raw price / volume / amount from decoded DC2 (scaled integers).
3. **Complete exactly once** using `source_completion_key`.
4. Emit `transaction_events` with unique `event_key`.
5. Enqueue `sync_queue` rows in the **same** DB transaction.

## Raw scaled integers

Decimal placement is **never invented**. Store:

- `raw_price`, `raw_volume`, `raw_amount`
- optional `*_decimals` only when known from pump parameters

`Decimal` appears only at service/API boundaries when decimals are known.

## Duplicate prevention

| Mechanism | Effect |
|-----------|--------|
| `transaction_uuid` unique | One row per logical sale id |
| `source_completion_key` unique | Retransmitted DATA cannot double-complete |
| `(transaction_id, event_key)` unique | Duplicate fill/complete events ignored |
| Completion returns `(record, newly_completed)` | Callers distinguish noop vs first complete |

## Sync queue coupling

On start / complete, enqueue with keys:

- `tx-started:{uuid}`
- `tx-completed:{source_completion_key}`

Payload always includes `environment`, `station_id`, and `simulated` when applicable.

## Failure isolation

State snapshot write failures must not silently mutate completed transaction
rows: use separate UoW commits per persistence job. Critical completion jobs
use higher worker priority and refuse silent drop on queue-full.
