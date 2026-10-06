# LAB acceptance — sales reliability stage-1

**Environment:** LAB only (`DigitalTwin/lab`, LAB MQTT/DB). Do **not** deploy to SAO.

## Branches

- `intelipump-fdc` @ `prod_feature`
- `DigitalTwin` @ `prod_feature` (preserve unrelated RBAC/edge-device local edits)

## Preconditions

```bash
cd /Users/babatundealaraje/Documents/moduslights/DigitalTwin/lab
# 1) Static isolation (no secrets required)
./scripts/verify-lab-isolation.sh

# 2) Create secrets once
cp .env.lab.example .env.lab
# Edit: MQTT_PASSWORD, POSTGRES_PASSWORD, JWT_SECRET (must differ from production .env)
./scripts/lab-create-mqtt-passwd.sh

# 3) Bring up LAB stack (never production compose)
./scripts/lab-up.sh
./scripts/lab-migrate.sh
./scripts/lab-seed-us-lab.sh

# 4) Only after isolation + migrate: enable application ACK on LAB consumer/Pi
# In lab/.env.lab (consumer compose picks this up):
#   MQTT_PUBLISH_SALE_ACKS=true
# On LAB Pi intelipump-fdc config only:
#   INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=true
#   INTELIPUMP_MQTT__TOPIC_ENVIRONMENT=LAB
```

Ephemeral software IT (no physical pump; asserts identities/money/litres):

```bash
# Start throwaway PG+MQTT bound to 127.0.0.1 only, then:
cd /Users/babatundealaraje/Documents/moduslights/DigitalTwin/consumer
INTELIPUMP_LAB_INTEGRATION=1 \
INTELIPUMP_TEST_POSTGRES_HOST=127.0.0.1 \
INTELIPUMP_TEST_POSTGRES_PORT=55432 \
INTELIPUMP_TEST_POSTGRES_DB=intelipump_lab_itest \
INTELIPUMP_TEST_POSTGRES_USER=intelipump_lab \
INTELIPUMP_TEST_POSTGRES_PASSWORD=lab_itest_only \
INTELIPUMP_TEST_MQTT_PORT=18884 \
INTELIPUMP_SALE_OUTBOX_ALLOW_TMP=1 \
PYTHONPATH=. .venv/bin/python -m pytest tests/test_lab_pg_mqtt_identity_integration.py -q --tb=short
```

## Controlled physical / virtual checklist

For each controlled sale, record independently: pump face amount, volume, unit price, nozzle, wall-clock (Africa/Lagos), and opening/closing totalizers when available.

| # | Scenario | Expected |
| --- | --- | --- |
| 1 | One genuine dispense | One Pi transaction + one cloud row; report total matches face |
| 2 | Repeated completion frames / retained display | No extra sale |
| 3 | Two equal-value consecutive sales | Two rows, two identities |
| 4 | Lift-and-return without flow | No sale (`CANCELLED_NO_SALE` / no COMPLETED) |
| 5 | Late final data after holster | One sale; no double count |
| 6 | MQTT duplicate delivery | Idempotent; still one cloud row |
| 7 | Broker disconnect + reconnect | Backlog drains; identities unchanged |
| 8 | Consumer PG pause (LAB) | Outbox spill; recovery inserts once |
| 9 | Pi restart mid-delivery with app ACK on | Row stays `AWAITING_APP_ACK` until `SALE_COMMITTED` |
| 10 | Conflicting payload same identity | `integrity_conflict` visible; finals not overwritten |
| 11 | Price change between sales | Earlier sale amount unchanged |
| 12 | Nigeria day boundary late upload | Assigned by documented occurrence-time policy |

## Reconciliation command (LAB)

Export Pi ledger and cloud sales for the window (JSON list or `{sales:[...]}`), then:

```bash
cd /Users/babatundealaraje/Documents/moduslights/intelipump-fdc
uv run intelipump-sale-reconcile \
  --pi-export /tmp/lab-pi-sales.json \
  --cloud-export /tmp/lab-cloud-sales.json \
  --station-id InteliPump-US-Lab \
  --manager-reported-amount 50000 \
  --output /tmp/lab-sale-reconcile.json
```

Example labelled fixtures: `docs/sales/examples/`.

## Preflight before any uniqueness tighten (read-only)

```bash
psql "$LAB_DATABASE_URL" -f \
  /Users/babatundealaraje/Documents/moduslights/DigitalTwin/scripts/preflight_sale_dedupe_collisions.sql
```

## Success criteria (LAB)

- Controlled sales appear exactly once end-to-end.
- Equal-value legitimate sales remain separate.
- Captured outage sales recover after reconnect.
- Totals match independent face/totalizer notes within documented uncertainty.
- Faults / conflicts / awaiting-ack ages are visible on health + reconcile report.
- Historical rows untouched.

## Explicit non-claims

- Empty outbox ≠ complete pump capture.
- Totalizer delta match ≠ every individual sale correct.
- Do not enable `require_application_sale_ack` on SAO until LAB soak passes.

## Later one-controller SAO rollout (plan only)

1. Run preflight SQL on a SAO read replica / backup — archive collision report.
2. Deploy consumer + dashboard builds that are backward compatible (ACK publish optional).
3. Enable app-ACK on **one** controller via config only.
4. Compare Pi ledger vs cloud for that controller for ≥1 business day.
5. Expand controller-by-controller; never auto-delete/merge historical transactions.
6. Rollback: set `require_application_sale_ack=false` / `MQTT_PUBLISH_SALE_ACKS=false` and redeploy prior compose tags.
