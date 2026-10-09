# Pi meter read — attended SAO test (addresses 1 then 2)

Read-only cumulative totalizer via CD101/DC101 on the **running** controller.
Does **not** open a second serial port, authorize, reset, or set price.

**CAPTURED** means a correlated DC101 reply was retained — **not** that
field-to-nozzle mapping or litre scale is physically verified.

**Protocol correlation limitation:** Wayne DC101 does not echo a request UUID.
The controller correlates by **DART address + requested COUN + observation after
our CD101 TX**. Unsolicited, late, wrong-address, or wrong-COUN replies are ignored
for completion and cannot satisfy a newer request.

## Safety (default OFF)

| Gate | Behavior |
| --- | --- |
| `HARDWARE_CD101` | Default false — no TX |
| Device allowlist | Must match controller `device_id` |
| Address allowlist | e.g. `1,2` |
| Idle | Refuse FILLING / AUTHORIZED / NOZZLE_UP / LIMIT / SUSPENDED |
| Nozzle | Require verified **IN**; refuse UNKNOWN / OUT / stale IN |
| Sale | Refuse active sale identity, completion hold, pending exchange |
| TX recheck | Eligibility checked again immediately before CD101 TX |
| Retries | `max_retries=0` — never delay critical polling |

## Install code on pump Pi (you run)

From your Mac (example pump-2):

```bash
cd ~/Documents/moduslights/intelipump-fdc
git checkout meter-reading
git pull   # if remote updated
./scripts/deploy_sao_rs1_pump_to_pi.sh --pump 2 intelipump@100.85.77.22
# or your usual rsync + on-Pi install path for meter-reading SHA
```

On the Pi, confirm SHA:

```bash
cd ~/intelipump-fdc   # adjust to install path
git rev-parse HEAD
sudo systemctl restart intelipump.service
# leave cloud-sync as-is for local CLI tests
```

## Enable (attended only)

Add to `/etc/intelipump/intelipump.env` (controller):

```bash
INTELIPUMP_METER_READING__HARDWARE_CD101=true
INTELIPUMP_METER_READING__ALLOWED_DEVICE_ID=InteliPump-SAO-RS1-pi-002
INTELIPUMP_METER_READING__ALLOWED_ADDRESSES=1,2
INTELIPUMP_METER_READING__COUNTER_SELECT=1
INTELIPUMP_METER_READING__VOLUME_DECIMALS=3
INTELIPUMP_METER_READING__MIN_INTERVAL_SECONDS=5
INTELIPUMP_METER_READING__NOZZLE_IN_MAX_AGE_SECONDS=300
INTELIPUMP_METER_READING__BLOCK_DURING_DISPENSING=true
```

```bash
sudo systemctl restart intelipump.service
```

## Disable / rollback

```bash
# Disable gate (preferred after test)
sudo sed -i 's/^INTELIPUMP_METER_READING__HARDWARE_CD101=.*/INTELIPUMP_METER_READING__HARDWARE_CD101=false/' \
  /etc/intelipump/intelipump.env
# or comment out the METER_READING lines
sudo systemctl restart intelipump.service

# Code rollback to prior prod_feature tip (example)
cd ~/intelipump-fdc
git checkout prod_feature   # or pinned SHA
# reinstall/restart per your usual SAO procedure
sudo systemctl restart intelipump.service
```

Clear a stuck bridge (only if CLI reports busy and no controller activity):

```bash
sudo rm -f /var/lib/intelipump/meter-read-request.json \
           /var/lib/intelipump/meter-read-inflight.json
```

## One-address-first attended procedure (addr 1, then addr 2)

**Preconditions:** nozzles hung up (IN), pump idle, no dispense in progress.

### Address 1 (nozzle-1)

1. Photograph the **manager / face cumulative counter** for hose 1.
2. Run:

```bash
intelipump-meter-read-once --address 1 --wait-seconds 20 --notes 'photo-hose1-before'
```

3. Save JSON (`status` should be `CAPTURED`). Record `rawCounters` / `cumulativeVolumeRaw` / `volumeLiters`.
4. Observe **one normal dispense** on that hose; confirm sale still completes on twin/Sales as usual.
5. After hang-up / idle again, photograph the face counter.
6. Second read:

```bash
intelipump-meter-read-once --address 1 --wait-seconds 20 --notes 'photo-hose1-after'
```

7. Compare: face delta ≈ second raw − first raw (scale check). Mapping verified only if they agree.

### Address 2 (nozzle-2) — separately

Repeat steps 1–7 with `--address 2`. Do **not** start scheduled production reads until both hoses pass.

Optional both (only after single-address passes):

```bash
intelipump-meter-read-once --addresses 1,2 --wait-seconds 20 --gap-seconds 5
```

## Refuse cases (expected)

| Condition | Result |
| --- | --- |
| Nozzle OUT / lift while queued | `DEFERRED` / `METER_READ_DEFERRED_NOZZLE_OUT` |
| UNKNOWN / stale IN | `METER_READ_REFUSED_NOZZLE_*` |
| Concurrent CLI | `REFUSED: meter-read bridge busy` |
| Gate off | CLI exit 2, no request file |

## Raw result + journal export

```bash
# Latest + correlation-scoped results
ls -la /var/lib/intelipump/meter-read-result*.json
sudo cat /var/lib/intelipump/meter-read-result.json

# Controller journal around the test
sudo journalctl -u intelipump.service --since '10 min ago' --no-pager \
  | tee ~/meter-read-journal-$(date -u +%Y%m%dT%H%M%SZ).log \
  | grep -E 'METER-READ|meter_read'

# Copy results off-Pi
scp intelipump@intelipump-2:/var/lib/intelipump/meter-read-result*.json .
scp intelipump@intelipump-2:~/meter-read-journal-*.log .
```

## No scheduled production reads

Keep `HARDWARE_CD101=false` when not attending. Do not enable cloud “Read now” /
schedules until physical counter mapping and scale pass for both addresses.
