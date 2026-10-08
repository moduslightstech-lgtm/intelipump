# Requirement checklist — session identity + app ACK (2026-10-08)

**Overall objective:** NOT complete until attended physical transaction-level reconciliation passes.

| # | Requirement | Implemented | Tested | Physically validated | Unresolved |
| --- | --- | --- | --- | --- | --- |
| 1 | Live telemetry ≠ financial sales | Yes (sidecar no COMPLETED; reports completed-only) | Unit (fill_stream uncertainty) | No | Ghost DISPENSING UX polish |
| 2 | New physical session → new UUID | Yes (`session_boundary` + detach) | 1.11/5.17/0.74; 14.76/0.74 | No | — |
| 3 | No high-water across sessions | Yes | Meter-reset tests | No | — |
| 4 | No amount/time-only dedupe | Yes (removed twin same-totals hang-up abandon; DC2 baseline after verified) | Equal-value consecutive | No | — |
| 5 | Hang-up authoritative finalization | Yes | Hang-up paths + sidecar disabled | No | Missed hang-up stays ACTIVE/uncertain |
| 6 | Sidecar not financial settle | Yes | phase9 fill_stream tests | No | — |
| 7 | Atomic local capture + outbox | Prior stage-1 | Prior | No | Mid-write crash window |
| 8 | App ACK after PG commit | Yes (gated off SAO) | sale_ack_validation + consumer replay | No | Fleet enable pending canary |
| 9 | ACK identity/device/station scope | Yes | Unit + intake wrong-device/station | No | — |
| 10 | Legacy SAO compat / additive migrate | Yes (028 preflight) | Prior IT | No | Preflight on prod DB before migrate |
| 11 | Meter/CD101 separate | Preserved | — | — | Unrelated dirty tree on DigitalTwin |
| 12 | Trace / recon commands | Docs | — | No | Needs live Pi/cloud access |
| 13 | Physical 1:1:1 acceptance | Procedure documented | — | **BLOCKED** | Operator evidence required |

## Exact tests run (this change)

```
tests/unit/domain/test_session_boundary.py
tests/unit/persistence/test_durable_session_boundaries.py
tests/unit/persistence/test_reopen_provisional_sidecar.py
tests/unit/persistence/test_new_fill_after_completed.py (sidecar growth reopen)
tests/unit/cloud/test_phase9_cloud.py (sidecar uncertainty subset + filter)
tests/unit/cloud/test_sale_ack_validation.py
```

## What you must supply next (live access unavailable here)

1. Attended canary: face photo/notes for one dispense (amount, litres, pump, time Africa/Lagos).  
2. Pi SQLite rows for that UUID (`transactions` + `sync_queue`).  
3. Cloud `pump_transactions` row + dashboard inclusion for the Lagos window.  
4. Optional: `SALE_COMMITTED` MQTT capture when enabling ACK on that Pi only.  
5. Confirmation deploy commands were run (agents must not SSH/restart/deploy).
