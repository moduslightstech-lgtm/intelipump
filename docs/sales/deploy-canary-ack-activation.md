# Cloud-first deploy, one-Pi canary, application-ACK activation

**Branches:** `intelipump-fdc` + `DigitalTwin` → `prod_feature`  
**Pins:** Pi `95e5d5e` · Cloud tip including `ab2b8c5` + live-telemetry separation (`030_live_dispensing_telemetry`) — see `DigitalTwin/docs/sales/release-pins-oct8-session-ack.md`.  
**Rule:** Do **not** enable `require_application_sale_ack` on SAO until cloud ACK is verified and one attended Pi canary passes. Broker PUBACK ≠ cloud commit.

Physical acceptance is **not** complete until: one observed dispense → one Pi identity → one cloud COMPLETED → one dashboard row, matching amount/litres and delivery status.

---

## 1. Safe migrations / preflight (non-destructive)

Cloud (DigitalTwin), before migrate:

```bash
cd DigitalTwin
# Inspect historical collisions (additive unique / identity) — no deletes
psql "$DATABASE_URL" -f scripts/migration_028_preflight.sql
psql "$DATABASE_URL" -f scripts/preflight_sale_dedupe_collisions.sql

# Apply additive migrations only (028 sale identity; do not auto-rewrite history)
# Keep 029 meter readings separate / already present — do not rewrite sales history.
docker compose exec api alembic upgrade head
# or LAB:
cd lab && ./verify-lab-isolation.sh && docker compose -f docker-compose.lab.yml exec api alembic upgrade head
```

Pi: SQLite schema evolves via app open; no destructive RESET of sale evidence. Keep meter/CD101 work on separate flags.

Rollback containment (preserve captured sales):

```bash
# Cloud: redeploy previous image tag; do NOT reverse-migrate dropping identity tables
# Pi: restore previous controller package; SQLite COMPLETED + sync_queue rows remain
# Never DELETE FROM pump_transactions / transactions to “fix” totals
```

---

## 2. Manual cloud-first deployment (you run these)

```bash
# --- DigitalTwin (cloud) first ---
cd DigitalTwin
git checkout prod_feature
git pull --ff-only
# build/push your usual image tag, e.g.:
# docker build … && docker push kacytunde/intelipump-{api,consumer,dashboard}:<tag>
# deploy consumer + api + dashboard to LAB or staging only first

# Confirm consumer publishes SALE_COMMITTED when MQTT_PUBLISH_SALE_ACKS=true
# (default in consumer config). Pi must still have require_application_sale_ack=false.

# --- Pi controller second (one canary device only) ---
cd intelipump-fdc
git checkout prod_feature
# package/install on ONE attended Pi only — do not fleet-roll SAO yet
# Keep: INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=false  (or omit)
```

---

## 3. One attended Pi canary procedure

1. Note Pi `station_id` / `device_id` and pump under test.  
2. Deploy new Pi build with **ACK requirement off**.  
3. Independently observe one dispense (face amount + litres + time).  
4. On Pi (read-only):

```bash
sqlite3 /path/to/intelipump.db "
SELECT transaction_uuid, status, raw_volume, raw_amount, source_completion_key, completed_at
FROM transactions ORDER BY updated_at DESC LIMIT 5;
SELECT id, event_type, status, entity_id, deduplication_key
FROM sync_queue ORDER BY created_at DESC LIMIT 10;
"
```

5. Confirm **one** new COMPLETED UUID with matching raw_volume/raw_amount.  
6. Confirm cloud `pump_transactions` has that id as COMPLETED (dashboard sales for Lagos window).  
7. Confirm digital twin showed DISPENSING during fill, then COMPLETED — no second financial row from telemetry.  
8. If a prior session was left ACTIVE (missed hang-up), it must remain visible unresolved — not glued into the new UUID.

**Pass criteria:** 1 observed dispense = 1 Pi identity = 1 cloud COMPLETED = 1 dashboard inclusion.

---

## 4. Application-ACK activation (separate from SAO fleet)

Only after canary §3 passes and cloud `SALE_COMMITTED` is observed on the device topic:

```bash
# On the SAME canary Pi only:
# INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=true
# Restart controller (you run restart — agents must not)

# Verify:
# - sync_queue moves PENDING → AWAITING_APP_ACK after MQTT publish
# - SALE_COMMITTED arrives on intelipump/<env>/devices/<deviceId>/sale-acks
# - row becomes DELIVERED only after validated ACK (station/device/tx match)
# - lost ACK → replay same identity; identical cloud commit → replay ACK
```

Wrong-device / wrong-sale ACKs must be rejected (see `tests/unit/cloud/test_sale_ack_validation.py`).

Do **not** silently set `require_application_sale_ack=true` on all SAO controllers.

---

## 5. Read-only reconciliation

Africa/Lagos start-inclusive / end-exclusive day window. Compare **gross dispensing** separately from manager net (expenses, credit, return-to-tank).

```bash
# Pi ledger vs face (example)
sqlite3 /path/to/intelipump.db "
SELECT transaction_uuid, status, raw_volume/100.0 AS litres, raw_amount/100.0 AS amount,
       source_completion_key, completed_at
FROM transactions
WHERE station_id = '<STATION>'
  AND status IN ('COMPLETED','COMPLETE')
  AND completed_at >= '<LAGOS_START_UTC>'
  AND completed_at <  '<LAGOS_END_UTC>'
ORDER BY completed_at;
"

# Unresolved / uncertain
sqlite3 /path/to/intelipump.db "
SELECT transaction_uuid, status, raw_volume, raw_amount, updated_at
FROM transactions WHERE status IN ('ACTIVE','SUSPENDED','OPEN');
"

# Pending uploads (empty outbox ≠ complete day)
sqlite3 /path/to/intelipump.db "
SELECT status, COUNT(*), MIN(created_at), MAX(created_at)
FROM sync_queue GROUP BY status;
"
```

Trace (read-only): session UUID → final evidence / completion key → SQLite sale + outbox → MQTT event → cloud ingest decision → `SALE_COMMITTED` → dashboard completed filter.

---

## 6. Remaining limitations / capture windows

| Window | Risk |
| --- | --- |
| Hard crash after CRITICAL handoff schedule, before durable spill | Possible loss; uncertainty on restart — not silent invent |
| Sidecar flat-meter without hang-up | ACTIVE + `CAPTURE_UNCERTAINTY_AWAITING_HANGUP` — not financial COMPLETED |
| Prior ACTIVE after meter RESET | Left unresolved; new UUID for new session — operator must reconcile hung face |
| App ACK off | PUBACK may mark DELIVERED without PostgreSQL proof |
| Empty outbox | Proves neither complete pump capture nor complete day |

These session-boundary fixes do **not** claim to explain the full manager↔dashboard discrepancy (e.g. 4.80 L / 2.95 L unproven gaps).
