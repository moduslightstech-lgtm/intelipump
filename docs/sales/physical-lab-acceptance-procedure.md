# Physical LAB sales reliability acceptance procedure

**Status:** Software verification only until independently observed pump evidence is filled in.  
**Do not mark physical acceptance passed without face/totalizer notes.**

Branches / images:
- DigitalTwin image tag: `lab-stage1-0e2dd6f` (or current `IMAGE_TAG` in `lab/.env.lab`)
- intelipump-fdc: record `git rev-parse HEAD` on the LAB Pi after deploy

## Preconditions

```bash
# Droplet — LAB only
cd /opt/intelipump-cloud/lab
./scripts/verify-lab-isolation.sh
./scripts/validate-lab-compose.sh
./scripts/lab-up.sh
./scripts/lab-migrate.sh
./scripts/lab-seed-us-lab.sh

# Confirm ACK publish (LAB consumer)
docker exec intelipump-lab-consumer printenv MQTT_PUBLISH_SALE_ACKS MQTT_TOPIC_ENVIRONMENT
# expect: true / lab

# LAB Pi — application ACK required
# INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=true
# INTELIPUMP_MQTT__TOPIC_ENVIRONMENT=LAB
# broker host = droplet, port 1884
intelipump-cloud-sync ...   # or systemctl restart LAB sync unit only
```

Pending-until-cloud-commit check (software):
```bash
# On Pi health / sqlite: sync_queue status AWAITING_APP_ACK after MQTT PUBACK
# until SALE_COMMITTED arrives on intelipump/lab/devices/<deviceId>/sale-acks
```

## Evidence table (fill during physical run)

| # | Scenario | Sale identity (UUID / dedupe) | Nozzle | Face amount | Face litres | Face price | Opening totalizer | Closing totalizer | Cloud row id | Pending→ACK | Pass? |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Single genuine dispense | | | | | | | | | | |
| 2 | Equal-value consecutive #1 | | | | | | | | | | |
| 3 | Equal-value consecutive #2 (same ₦/L) | | | | | | | | | | |
| 4 | No-flow lift/return | _(none / CANCELLED)_ | | 0 | 0 | | | | _(none)_ | | |
| 5 | Retained display + Pi restart | | | | | | | | | | |
| 6 | Internet/MQTT outage then recover | | | | | | | | | | |
| 7 | Price change between sales | | | | | | | | | | |

## Numbered procedure

1. **Record opening totalizers** (if available) and wall-clock Africa/Lagos.
2. **Sale 1 — genuine dispense:** note face amount/litres/price/nozzle; wait for COMPLETED; confirm one Pi ledger row + one cloud row; note whether Pi showed `AWAITING_APP_ACK` until SALE_COMMITTED.
3. **Sales 2–3 — equal value:** two consecutive dispenses with the same face amount/litres; confirm **two** identities and **two** cloud rows; UI live totals include both.
4. **No-flow lift/return:** lift, return without flow; confirm no COMPLETED sale / no cloud money.
5. **Retained display restart:** complete a sale, leave face showing, restart Pi controller **before** next lift; confirm no duplicate cloud row; then run a new equal-value sale and confirm a **new** identity (not suppressed).
6. **Outage:** disconnect LAB uplink / block MQTT briefly mid-backlog; restore; confirm identities and totals unchanged (no duplicates).
7. **Price change:** change unit price via LAB command path only; prior sale amount unchanged; next sale uses new price.
8. **Reconcile:**
```bash
uv run intelipump-sale-reconcile \
  --pi-export /tmp/lab-pi-sales.json \
  --cloud-export /tmp/lab-cloud-sales.json \
  --station-id InteliPump-US-Lab \
  --output /tmp/lab-sale-reconcile.json
```
9. Sign the evidence table. Physical acceptance = all rows Pass with independent face/totalizer notes.

## Explicit non-claims

- Software IT pass ≠ physical acceptance.
- Empty outbox ≠ complete pump capture.
- Do not run this procedure against SAO or production broker :1883.
