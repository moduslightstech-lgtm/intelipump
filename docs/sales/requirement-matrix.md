# Sales reliability — requirement matrix (Stage 1+ continuation)

**Branches:** `intelipump-fdc` @ `21e6b708…` (+ uncommitted Stage-1+ handoff work) · `DigitalTwin` @ `0e2dd6fa…` (+ uncommitted float/conflict/LAB config)  
**Images pushed:** `kacytunde/intelipump-{api,consumer,dashboard}:lab-stage1-0e2dd6f`  
**Rule:** docs or a bare function return ≠ evidence.

| # | Requirement | Status | Evidence |
| --- | --- | --- | --- |
| 1 | One completed session → one recorded sale | PARTIAL | Software IT; **physical BLOCKED** |
| 2 | Retries/reconnect/restart no duplicate | DONE | Baseline + LAB IT + handoff recover |
| 3 | Durable capture before clearing evidence | DONE | Handoff-first write-ahead; RESET gated until durable |
| 4 | Gaps/uncertainty visible | DONE | `capture_uncertainty` + audit on ambiguous restart face |
| 5 | Dashboard totals ↔ transactions | DONE | Completed filter + Decimal summary strings |
| 6 | Historical intact | DONE | No auto-delete |
| 7–9 | Stable scoped identity | DONE | UUID keys + station unique index |
| 10–11 | Equal-value consecutive distinct | DONE | Fingerprint share / UUID differ; filling_seen gate |
| 12 | Retained display no new sale | DONE | Startup baseline |
| 13 | Lift-return no flow | DONE | Positive-delivery gate |
| 14 | Prev sale ≠ new session | DONE | Equal-value after fill accepted; baseline only for startup mark |
| 15 | Late frames / preset | PARTIAL | Code paths; **physical BLOCKED** |
| 16–17 | Immutable finals / conflict flag | DONE | Price + mapping + amount/volume conflicts |
| 18 | Persist before clear | DONE | RESET blocked while `handoff_pending` |
| 19 | Queue→durable window | PARTIAL | Narrowed to mid-write only; see gap note |
| 20 | Recovery ≠ outbox | DONE | |
| 21–23 | App ACK retain/validate/recover | DONE | Unit recovery tests; LAB enable pending secrets |
| 24–26 | Disk fault / off serial / CRITICAL bound | DONE | |
| 27 | Metrics | PARTIAL | +handoff_pending / capture_uncertainty |
| 28–37 | Ledger / PG unique / SAO compat / preflight | DONE | |
| 38 | No progress-as-sale | DONE | |
| 39 | Exact money arithmetic | DONE | `sales_summary` decimal strings; CSV Decimal |
| 40–41 | Lagos / occurrence time | DONE | |
| 42–44 | Shift / export reconcile / warnings | PARTIAL | CLI + arithmetic; full soak open |
| 45–50 | Price freeze / RBAC / reconcile / LAB isolation / no SAO | DONE | |
| 51 | Automated + IT fault tests | DONE | Process-termination + LAB IT |
| 52 | LAB physical acceptance | **BLOCKED** | Procedure ready; needs face/totalizer evidence |

## Remaining crash gap (exact)

**Condition:** process hard-killed **after** CRITICAL handoff task is scheduled and **before** `PersistRecoveryStore.upsert` fsync completes, **and** RESET somehow cleared the face (should be gated; if gate bypassed or power-loss zeroes display).  
**Then:** no JSONL pending, no queue job, restart may only see retained face → `CAPTURE_UNCERTAINTY_RETAINED_FACE` (not silent invent).  
**Not a gap:** after durable handoff → `recover_pending` restores identity/totals.

## Partial / blocked list (operator view)

**PARTIAL:** 1 (physical), 15 (physical late-frame), 19 (mid-write residual), 27 (metrics polish), 42–44 (shift/export soak), LAB live stack (secrets not set).  
**BLOCKED:** 52 physical acceptance; face/totalizer independent evidence required.

## LAB config status

- `.env.lab` present; `IMAGE_TAG=lab-stage1-0e2dd6f`; `MQTT_PUBLISH_SALE_ACKS=true`
- `lab-up` **blocked** until real LAB MQTT/Postgres/JWT secrets replace `CHANGE_ME_*`
- Production containers not touched
- Pi `require_application_sale_ack` not enabled (no Pi access this session)
