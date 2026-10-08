# SAO Oct-8 Pi canary (Stage 2)

**Pin (install this SHA):** `7ad5209063321b8dc30c4e341e3ccad67f495e62` on `prod_feature` (feature base `955aea1` + review tests).  
Deploy only after cloud Stage 1 at consumer image `prod_feature-3510426f6ee5` (`CLOUD_SHA=3510426f6ee5126531c76749d3c810c5c6e4e192`) is live.

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
