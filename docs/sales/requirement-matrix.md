# Sales reliability — requirement matrix (Stage 1+)

**Branches:** `intelipump-fdc` / `DigitalTwin` @ `prod_feature`  
**Rule:** documentation or a successful function return alone is **not** evidence.

Legend: **DONE** = code + automated test asserting identities/totals/behavior | **PARTIAL** = code exists, evidence incomplete | **OPEN** | **BLOCKED** = needs physical pump / SAO ops

| # | Requirement | Status | Evidence |
| --- | --- | --- | --- |
| 1 | One real completed session → one recorded sale | PARTIAL | Gates + LAB IT assert 3 sales / 3 ids; physical face match **BLOCKED** |
| 2 | Retries/frames/reconnect/restart do not duplicate | DONE | Startup baseline tests; LAB IT duplicate replay + concurrent race → 1 row/id |
| 3 | Durable capture before clearing evidence | PARTIAL | Write-ahead + CRITICAL; crash window A documented (`docs/safety/audit-and-durability.md`) |
| 4 | Gaps/uncertainty visible | PARTIAL | Health pending/awaiting; `integrity_conflict`; reconcile CLI |
| 5 | Dashboard totals traceable to transactions | DONE | `test_dashboard_completed_filter` + export arithmetic tests |
| 6 | Historical records intact | DONE | No auto-delete/merge; preflight SQL read-only |
| 7 | Stable sale identity (UUID session) | DONE | `complete:{uuid}` + equal-value fingerprint share / distinct keys test |
| 8 | Identity survives persist/MQTT/restart/ACK | DONE | Outbox + `test_pi_restart_preserves_awaiting_app_ack`; LAB consumer restart |
| 9 | Scope by station/device/address/nozzle | DONE | Station-scoped unique index + nozzle baselines |
| 10 | Never dedupe solely by amount/time/MQTT mid | DONE | Consumer hangup + UI `saleDuplicates` tests (equal completed kept) |
| 11 | Two equal-value consecutive sales distinct | DONE | Fingerprint test + UI test + LAB IT amounts `[5000,5000]` two ids |
| 12 | Retained display / repeated finals no new sale | DONE | `test_startup_baseline_*` |
| 13 | Lift-return without flow → no sale | DONE | `has_positive_delivery` gate tests |
| 14 | Keep previous-sale values separate from new session | PARTIAL | New UUID after COMPLETED; residual fingerprint risk documented |
| 15 | Final-data ordering / late frames / preset | PARTIAL | Awaiting FILLING_COMPLETED paths; physical late-frame **BLOCKED** |
| 16 | Immutable final amount/volume/price | DONE | Completed finals frozen on conflict |
| 17 | Flag uncertainty when recovery ambiguous | DONE | `integrity_conflict` unit tests |
| 18 | Persist before clear evidence | PARTIAL | Async write-ahead; session clear while outbox in flight still possible |
| 19 | Close queue-admit→durable write window or document | DONE | Documented unavoidable; flush-incomplete ≠ success tests |
| 20 | Separate recovery state from upload outbox | DONE | transactions + sync_queue + baselines |
| 21 | Retain until application ACK (not PUBACK) | DONE | `require_application_sale_ack` + AWAITING_APP_ACK recovery tests (default off) |
| 22 | Validate ACK identity/scope | DONE | Wrong-device topic/payload ignored; wrong-sale unmatched |
| 23 | Recover pending after restart/reconnect | DONE | Awaiting preserved across worker recreate; LAB outbox replay |
| 24 | Disk failure: retain evidence, surface fault | DONE | Retained sales + non-zero flush exit |
| 25 | Persistence I/O off serial loop | DONE | PersistenceWorker async |
| 26 | Bounded resources without silent CRITICAL drop | DONE | Queue-full refuse / spill |
| 27 | Metrics: pending, age, failures, awaiting ACK | PARTIAL | Health pending/delivered/failed/awaiting_app_ack_count |
| 28 | Durable reconciliation ledger after upload | DONE | DELIVERED retained; reconcile CLI |
| 29 | PG uniqueness on sale identity | DONE | Alembic 021 + LAB IT concurrent ingest |
| 30 | Atomic sale+ingest before ACK | PARTIAL | Sale commit before SALE_COMMITTED; mqtt_messages not same TX |
| 31 | Identical replay → success ACK, no second row | DONE | `test_sale_committed_replay` + LAB duplicate → 3 rows only |
| 32 | Same identity conflicting finals → visible conflict | DONE | `test_sale_integrity_conflict` + ACK amount conflict leaves awaiting |
| 33 | Equal-value different identities preserved | DONE | Consumer + UI + LAB IT |
| 34 | No process-memory-only dedupe | DONE | PG unique + SQLite unique |
| 35 | Preflight before uniqueness tighten | DONE | `preflight_sale_dedupe_collisions.sql` |
| 36 | Never auto-delete/merge historical | DONE | Audits report-only |
| 37 | Old SAO payload compatibility | DONE | Boluwaji / Phase9 tests |
| 38 | Dashboard: no progress-as-sale | DONE | Completed+amount clause on dashboard aggregations |
| 39 | Exact money/volume arithmetic | PARTIAL | Numeric in PG; API float at edge; Decimal export-sum tests |
| 40 | Africa/Lagos inclusive start / exclusive-style end | DONE | `test_lagos_day_window_*` + sales day-start tests |
| 41 | Occurrence vs receipt time policy | DONE | coalesce(completed, device, received); receipt for late flags |
| 42 | Shift boundaries | PARTIAL | business_day_cutoff in recon; dashboard midnight |
| 43 | Totals reconcile to export | PARTIAL | Reconcile CLI + arithmetic guards; full CSV↔summary IT open |
| 44 | Report time + completeness warnings | PARTIAL | Reconcile warnings; dashboard incomplete |
| 45 | Price change must not rewrite earlier totals | DONE | Completed finals frozen |
| 46 | Reconciliation role restrictions | DONE | `require_reconciliation_access` |
| 47 | Pi↔cloud reconcile tooling | DONE | `intelipump-sale-reconcile` |
| 48 | LAB isolation | DONE | `verify-lab-isolation.sh` static OK; compose intelipump-lab |
| 49 | LAB-only owned auto-auth | DONE | Env gated; not enabled in prod configs here |
| 50 | No SAO deploy / prod mutation | DONE | Policy followed this stage |
| 51 | Automated + integration fault tests | DONE | Unit + ephemeral LAB PG+MQTT IT (outage/restart) |
| 52 | LAB acceptance checklist | PARTIAL | Docs ready; physical totals **BLOCKED** |

## Crash windows (remaining)

| Window | Status |
| --- | --- |
| CRITICAL accept → durable spill | **Open / documented** — hard crash can lose sale |
| MQTT PUBACK → consumer PG commit | Closed when `require_application_sale_ack=true` |
| Consumer PG commit → Pi SALE_COMMITTED | Sale in cloud; Pi awaiting; identical replay recovers |
| Consumer PG down → local outbox | Covered by LAB IT deferred_local → replay |

## Stage priorities (this continuation)

1. Equal-value + retained-display across restart — **tested**  
2. Durability / crash-window docs + ledger retention — **documented + ACK path**  
3. Application-ACK recovery — **unit PASS** (lost ACK, wrong device/sale, conflict, replay, Pi restart)  
4. PG uniqueness / concurrent — **LAB IT PASS**  
5. Report arithmetic / Lagos / export — **backend tests PASS**  
6. Frontend lockfile + tests/build — **PASS** (`npm ci`, vitest 22, vite build)  
7. Isolated LAB MQTT+PG IT — **PASS** (3 ids, ₦11370, 8.30 L)

Physical pump acceptance remains **BLOCKED** on independently recorded face/totalizer evidence.
