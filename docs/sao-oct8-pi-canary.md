# SAO Oct-8 Pi canary (Stage 2)

**Pin:** build/install from `prod_feature` at `git rev-parse HEAD` (base feature commit `955aea1` + review follow-ups).  
Deploy only after cloud Stage 1 (`028_sale_identity_decisions` + pinned consumer image) is live.

## What changed

1. **UUID completion keys** — `stable_completion_key(complete:{uuid})` is the MQTT / DB business key. Wayne frame hex stays evidence (`completion_frame_evidence_not_identity` log).  
2. **Provisional sidecar** — while DC1 still reports a live fill, fill_stream **does not** authoritative-complete (`provisional_sidecar_snapshot_held`).  
3. **Same-session reopen** — if a provisional `sidecar-settle:` COMPLETED already exists and DC2 climbs, reopen **same UUID** (`action=reopen_same_identity`). No second countable sale. Non-sidecar COMPLETED growth → `post_completion_growth_uncertain` (preserve original).

## Canary procedure (one attended Pi)

1. Install this `prod_feature` build on **one** controller only.  
2. Leave application sale ACK **disabled**.  
3. Record each dispense: nozzle, Lagos start/end, face litres/amount/price, totalizers if available.  
4. Cases to cover:  
   - Normal hang-up  
   - Mid-fill pause then resume (43→54 style)  
   - Two equal-value consecutive sales  
   - Lift/return without dispense  
5. Trace: Pi UUID → sync_queue key → cloud `pump_transactions` + `sale_ingestion_decisions` → dashboard.  
6. Pass: one physical dispense → one Pi identity → one cloud COMPLETED → one dashboard row.  

## Rollback

Reinstall previous controller package/image. Do not wipe SQLite.

## Limitation

If a provisional sidecar COMPLETED was already delivered to cloud before reopen, cloud may still hold that COMPLETED row (cloud does not auto-retract). Prefer never publishing while live (fill_stream hold) — that is the primary guard.
