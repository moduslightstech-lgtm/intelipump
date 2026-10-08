# Durable dispensing sessions — state transitions (audit + target)

**Repos:** `intelipump-fdc` (Pi) · `DigitalTwin` (cloud)  
**Branch:** `prod_feature`  
**Date:** 2026-10-08  

## Competing completion paths (audit)

| Path | Trigger | Writes financial COMPLETED? | Problem |
| --- | --- | --- | --- |
| **A. Hang-up / FILLING_COMPLETED** | `PersistenceBridge` on verified nozzle return + final DC2/DC3 | Yes (`complete:{uuid}` / fingerprint) | Authoritative — keep |
| **B. Sidecar settle** | `LiveFillStream._finalize_settled` flat meter / terminal state | Yes (`sidecar-settle:{uuid}`) | Premature settle; high-water glue across sessions |
| **C. Same-UUID reopen** | DC2 growth after provisional sidecar | Rolls COMPLETED→ACTIVE same UUID | OK *within* one physical session; fatal across meter RESET |
| **D. Same-totals abandon** | Hang-up finds recent COMPLETED with equal vol/amt | Abandons open UUID | Collapses two equal-value purchases |
| **E. Live telemetry** | `FILLING_UPDATED` / `TRANSACTION_STARTED` | Twin DISPENSING only (reports filter COMPLETED) | Must never promote/settle financial sales |
| **F. Controller SALE event** | Wayne SALE / FILLING_COMPLETED payload | Same as A when bridged | Must share one finalization path with A |

Observed glue (pump-3): `0e9c0a49` merged 1.11+5.17+0.74 L; `a16d5958` merged 14.76+0.74 L because `_ensure_open_sale` reused the open UUID after meter RESET and `update_filling` applied high-water `max()`.

## Target session states (Pi SQLite)

```
IDLE / no row
  → (verified dispensing) ACTIVE          # live twin + recovery evidence
  → (hang-up + sufficient final evidence) COMPLETED + sync_queue PENDING
  → (insufficient evidence / missed hang-up) UNCERTAIN (visible reason; not invented sale)
  → (lift/return no flow) CANCELLED_NO_SALE / abandon without publish

COMPLETED + delivery:
  CAPTURED_LOCAL → PENDING_CLOUD_ACK → CLOUD_COMMITTED
  (or INTEGRITY_CONFLICT — both evidence sets retained)

After COMPLETED (verified hang-up): unexpected growth → conflict/uncertain; never silent rewrite.
After ACTIVE + meter RESET (new physical session): prior row stays; NEW UUID for the new session.
```

## Protocol-backed boundaries

A **new physical session** begins when any of:

1. Verified nozzle lift after return / completed / idle (VerifiedDispensingBook reset).
2. **Meter RESET face**: open/provisional row volumes significantly exceed the new DC2 face (authorize cleared the display).
3. Hang-up already COMPLETED the mapped UUID and verified dispensing begins again.

Never:

- High-water across different physical sessions.
- Deduplicate solely by amount / litres / time / frame / MQTT packet id.
- Treat broker PUBACK or legacy `DELIVERED` as cloud commit proof.

## Authoritative finalization (single path)

1. Prefer **verified hang-up** + session-specific final volume/amount/price evidence.
2. Late DC2/DC3 and preset completion feed the **same** complete path (same UUID, immutable finals).
3. Sidecar may hold provisional snapshots for the twin and raise **UNCERTAIN** for ops — it must **not** call `TransactionService.complete` / enqueue financial `TRANSACTION_COMPLETED`.
4. Application ACK (`SALE_COMMITTED`) proves durable PostgreSQL commit; gated off by default on SAO (`require_application_sale_ack=false`).

## Delivery states (orthogonal to sale finality)

| Sale | Delivery |
| --- | --- |
| COMPLETED (local) | PENDING / AWAITING_APP_ACK |
| COMPLETED (local) | CLOUD_COMMITTED (after validated ACK) |
| COMPLETED (local) | INTEGRITY_CONFLICT |

An empty outbox proves neither complete pump capture nor a complete day.
