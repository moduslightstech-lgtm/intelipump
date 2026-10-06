# Sales reliability findings (LAB stage-1)

**Branches:** `intelipump-fdc` / `DigitalTwin` → `prod_feature`  
**Repos:** intelipump-fdc (Pi), DigitalTwin (cloud/dashboard)  
**Scope:** sales capture → durable store → MQTT → cloud commit → dashboard totals  
**Out of scope here:** CCTV; SAO deploy / production data changes

## Pipeline (as implemented)

```
DC1/DC2/NOZIO frames
  → PumpSession / VerifiedDispensingBook
  → PersistenceBridge completion (source_completion_key + fingerprint)
  → SQLite transactions + sync_queue (same UoW on complete)
  → PersistenceWorker async write-ahead (CRITICAL)
  → cloud SyncWorker MQTT publish (QoS 1)
  → DigitalTwin consumer normalize + PostgreSQL upsert
  → mqtt_messages row + optional consumer sale_delivery_outbox
  → dashboard sales_summary / reconciliations (Africa/Lagos day)
```

## Existing guarantees

| Layer | Guarantee |
| --- | --- |
| Pi completion | `source_completion_key` unique; `complete:{transaction_uuid}` preferred over fingerprint |
| Pi outbox | `sync_queue.deduplication_key` unique; reopen after restart via claim/retry |
| Pi baselines | Per-nozzle fingerprint suppresses retained-display republish after restart |
| Cloud DB | `UNIQUE (station_id, deduplication_key) WHERE deduplication_key IS NOT NULL` (Alembic 021) |
| Cloud ingest | Idempotent insert by `id` / dedupe key; hangup merge is nozzle-scoped |
| Consumer outbox | Local JSONL spill when PostgreSQL is down; MQTT PUBACK only after PG or durable spill |
| Reporting | Completed-only filters; business day via Africa/Lagos bounds |
| LAB isolation | `DigitalTwin/lab` separate compose/DB/broker/credentials/ports |

## Prioritized gaps (stage-1 targets marked)

1. **[P0][stage-1] Broker PUBACK ≠ cloud commit on the Pi**  
   `SyncWorker` marks `sync_queue` `DELIVERED` after MQTT PUBACK. A sale can be dropped after the broker accepts it if the consumer never commits.  
   *Fix:* optional application ACK (`SALE_COMMITTED`) from consumer after PostgreSQL commit; Pi retains `AWAITING_APP_ACK` until then (`mqtt.require_application_sale_ack`).

2. **[P0][stage-1] UI hangup collapse of equal-value completed sales**  
   `isHangupDuplicateSale` treated two `COMPLETED` rows with the same amount/volume within 120s as twins. Legitimate consecutive equal purchases could disappear from live UI totals.  
   *Fix:* hangup twin only when one side is in-progress (or stale dispensing after complete).

3. **[P0][stage-1] Consumer hangup absorb without stable keys**  
   Two completed sales with missing `deduplication_key` could still be folded by amount/volume/time (legacy path).  
   *Fix:* never fold two completed rows unless keys match; surface integrity conflicts on conflicting finals.

4. **[P0][stage-1 follow-up] Controller persist forced `simulated=True`**  
   Confirmed by [Audit Pi sale capture pipeline](ac4fd0a2-1320-4f14-ace3-b08fe4447b9f): `controller/cli.py` always passed `simulated=True` into `start_persistence`, so production outbox envelopes could be marked simulated and dropped by cloud reporting filters.  
   *Fix:* pass `settings.api.simulated` (env/config) instead of a hard-coded `True`.

5. **[P0][stage-1 follow-up] Dashboard totals included non-completed rows**  
   Confirmed by [Audit cloud sale idempotency](8cc6378b-2950-4f3c-985e-a32a05176569): `dashboard.get_summary` / hourly / product / station_performance summed all statuses, inflating “today” vs Executive/Sales/Twin (completed-only). Likely contributor to SAO dashboard > manager report.  
   *Fix:* apply the same completed+amount clause as `sales.py`.

6. **[P1] Hard-crash window after queue accept / before durable write**  
   Documented in `docs/safety/audit-and-durability.md`. Async write-ahead avoids blocking the serial loop; a hard crash in that window can lose the sale.  
   *Remaining:* keep documented; do not claim zero missing sales from an empty outbox.

7. **[P1][stage-1] No Pi↔cloud sale reconciliation CLI**  
   Station day-close reconciles cash/stock vs cloud sales, not Pi durable ledger vs cloud.  
   *Fix:* `intelipump-sale-reconcile` read-only compare + report format.

8. **[P1] Conflicting payload same identity**  
   Duplicate key path logged and ignored; conflicting amount/volume was not a first-class integrity incident.  
   *Fix:* detect conflict, return `integrity_conflict`, do not overwrite completed finals.

9. **[P2] Dashboard money via float at API boundary**  
   SQL sums use Numeric; API `_as_float` for JSON. Acceptable for display; exports should prefer Decimal/string for audit.

10. **[P2] Fingerprint equality**  
   Fingerprint is volume+amount(+price) scoped by station/address/nozzle. Equal consecutive sales rely on distinct `transaction_uuid` / completion keys. Open-sale path prevents baseline suppression mid-lifecycle; inferred completion without an open UUID remains a residual risk — flag uncertainty, do not invent sales.

11. **[P2] Legacy SAO payloads**  
   Older rows may lack `deduplication_key`. Uniqueness is partial-index nullable. Do not invent amount/time dedupe keys. Preflight script reports collisions before tightening constraints.

## Crash windows (explicit)

| Window | Risk | Mitigation today |
| --- | --- | --- |
| CRITICAL job accepted → durable spill complete | Hard crash can lose sale | Documented; graceful flush refuses “success” if undurable |
| MQTT published → consumer PG commit | Pi used to treat PUBACK as done | Stage-1 app ACK when enabled |
| Consumer PG commit → Pi app ACK received | Sale is in cloud; Pi still awaiting | Idempotent replay; health shows awaiting age |
| Consumer PG down → local outbox | MQTT ACK after spill | Replay on recovery; not a Pi app ACK |

## Unrelated local work preserved

DigitalTwin working tree already had unrelated edits under `backend/app/routers/edge_devices.py`, `resources.py`, `rbac.py`, and `backend/tests/test_tenant_isolation.py`. Stage-1 sales work must not overwrite those.

## Software verification vs physical acceptance

| Layer | Status |
| --- | --- |
| Unit / UI / ephemeral LAB PG+MQTT IT | Passed this stage (see requirement-matrix.md) |
| `require_application_sale_ack` on live LAB stack | **Not enabled** — `.env.lab` not present; enable only after `verify-lab-isolation.sh` + `lab-up` + migrate |
| Physical pump face / totalizer acceptance | **BLOCKED** — needs independently recorded sales |

## Remaining after stage-1

- Create `DigitalTwin/lab/.env.lab`, run lab-up/migrate/seed, then enable `MQTT_PUBLISH_SALE_ACKS` + Pi `require_application_sale_ack` for LAB soak.
- Measure awaiting-ack ages under reconnect / Pi restart.
- Preflight SAO duplicate-key report before any uniqueness migration tighten.
- One-controller SAO rollout plan (config-only; no mass historical rewrite).
- Physical LAB checklist with face amounts and opening/closing totalizers.
