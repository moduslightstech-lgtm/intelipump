# Pi meter read — attended SAO test (addresses 1 then 2)

Read-only cumulative totalizer via CD101/DC101 on the **running** controller.
Does **not** open a second serial port, authorize, reset, or set price.

**CAPTURED** = correlated DC101 reply retained — **not** verified nozzle mapping/scale.  
**CAPTURED_AMBIGUOUS** = matched, but a prior timeout on same address/COUN was recent; re-read after quarantine if unsure.

**Protocol correlation limitation:** Wayne DC101 does not echo a request UUID.
Correlation = DART address + COUN + post-TX window, plus a **post-timeout quarantine**
so a late reply cannot silently satisfy the next same-address/COUN request.

## Nozzle-IN freshness (why not 300s)

Stock dual-address poll timing (`PollSchedulerConfig`):

| Parameter | Default |
| --- | --- |
| `response_timeout_ms` | 120 |
| `inter_poll_delay_ms` | 5 |
| `tx_delay_ms` | 35 |
| `idle_sleep_ms` | 20 |
| Addresses | 1 and 2 |

Approx round = `2 × (120+5+35) + 20 ≈ 340 ms`.  
Default **`NOZZLE_IN_MAX_AGE_SECONDS=30`** ≈ **~88 poll rounds** — enough for CLI typing, tight enough to catch a stuck poller.  
The old **300 s** (~900 rounds) was too loose. Helper: `recommended_nozzle_in_max_age_s()`.

## Safety (default OFF)

| Gate | Behavior |
| --- | --- |
| `HARDWARE_CD101` | Default false |
| Device / address allowlists | Required |
| Idle + verified nozzle IN | Refuse OUT / UNKNOWN / stale IN |
| Sale / hold / exchange | Refuse |
| TX recheck | Again immediately before CD101 send |
| Post-timeout quarantine | Default 8s — no new TX; late DC101 ignored for completion |
| Retries | `max_retries=0` |

---

## Exact pinned install + running-code verification (pump-2)

**Pin:** `2dae401` or the tip of `meter-reading` after the late-reply quarantine commit (verify SHA below).

### A. Mac — sync exact commit to Pi

```bash
cd /Users/babatundealaraje/Documents/moduslights/intelipump-fdc
git fetch origin
git checkout meter-reading
git rev-parse HEAD   # note this SHA — must match Pi after install

# Sync tree (excludes .venv). Replace host if needed.
PIN_SHA="$(git rev-parse HEAD)"
HOST=intelipump@100.85.77.22
REMOTE_DIR=/home/intelipump/intelipump/intelipump-fdc

ssh "$HOST" "mkdir -p '$(dirname "$REMOTE_DIR")'"
rsync -az --delete \
  --exclude '.venv' --exclude '__pycache__' --exclude '.pytest_cache' \
  --exclude '.git' --exclude '*.pyc' --exclude '.mypy_cache' \
  ./ "${HOST}:${REMOTE_DIR}/"

# Record pin on Pi for audit
ssh "$HOST" "printf '%s\n' '$PIN_SHA' > '${REMOTE_DIR}/.meter-reading-pin'"
```

### B. Pi — recreate venv from locked project + install package

```bash
ssh intelipump@100.85.77.22
cd /home/intelipump/intelipump/intelipump-fdc

# Show pin and tree identity
cat .meter-reading-pin
# Optional: if .git was synced, also: git rev-parse HEAD

export PATH="$HOME/.local/bin:$PATH"
command -v uv || curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# Requires Python >=3.12 (project pin). Fresh sync from pyproject/uv.lock:
uv sync --dev

# Controllers must resolve from this venv:
test -x .venv/bin/intelipump-controller
test -x .venv/bin/intelipump-meter-read-once
.venv/bin/intelipump-meter-read-once --help | head -5
```

### C. Pi — confirm **running** service uses this tree/venv

```bash
# Unit ExecStart must point at this repo's venv binary
systemctl cat intelipump.service | grep -E 'ExecStart|WorkingDirectory'

# Expected ExecStart contains:
#   /home/intelipump/intelipump/intelipump-fdc/.venv/bin/intelipump-controller

# Package version / meter module present in THAT interpreter:
.venv/bin/python -c "
from importlib.metadata import version
import intelipump_fdc.services.meter_reading as m
import intelipump_fdc.controller.meter_read_request as br
print('pkg', version('intelipump-fdc'))
print('software', m.SOFTWARE_VERSION)
print('nozzle_default', __import__('intelipump_fdc.core.config', fromlist=['MeterReadingSettings']).MeterReadingSettings().nozzle_in_max_age_seconds)
print('quarantine_default', __import__('intelipump_fdc.core.config', fromlist=['MeterReadingSettings']).MeterReadingSettings().post_timeout_quarantine_seconds)
print('bridge', br.__file__)
"

# After enable+restart, confirm process:
sudo systemctl restart intelipump.service
systemctl is-active intelipump.service
pid=$(systemctl show -p MainPID --value intelipump.service)
# cwd / exe of running process
sudo ls -l /proc/$pid/exe
sudo tr '\\0' ' ' < /proc/$pid/cmdline; echo
# Must be .../intelipump-fdc/.venv/bin/python ... intelipump-controller
```

### D. Enable attended gate (then restart)

`/etc/intelipump/intelipump.env` — add/replace:

```bash
INTELIPUMP_METER_READING__HARDWARE_CD101=true
INTELIPUMP_METER_READING__ALLOWED_DEVICE_ID=InteliPump-SAO-RS1-pi-002
INTELIPUMP_METER_READING__ALLOWED_ADDRESSES=1,2
INTELIPUMP_METER_READING__COUNTER_SELECT=1
INTELIPUMP_METER_READING__VOLUME_DECIMALS=3
INTELIPUMP_METER_READING__MIN_INTERVAL_SECONDS=5
INTELIPUMP_METER_READING__NOZZLE_IN_MAX_AGE_SECONDS=30
INTELIPUMP_METER_READING__POST_TIMEOUT_QUARANTINE_SECONDS=8
INTELIPUMP_METER_READING__BLOCK_DURING_DISPENSING=true
```

```bash
sudo systemctl restart intelipump.service
```

### E. Disable / rollback

```bash
sudo sed -i 's/^INTELIPUMP_METER_READING__HARDWARE_CD101=.*/INTELIPUMP_METER_READING__HARDWARE_CD101=false/' \
  /etc/intelipump/intelipump.env
sudo systemctl restart intelipump.service

# Stuck bridge only:
sudo rm -f /var/lib/intelipump/meter-read-request.json \
           /var/lib/intelipump/meter-read-inflight.json
```

---

## One-address-first attended procedure

Idle, nozzles IN. **Address 1 first**, then address 2 separately.

```bash
cd /home/intelipump/intelipump/intelipump-fdc
# Photo face counter hose 1, then:
.venv/bin/intelipump-meter-read-once --address 1 --wait-seconds 20 --notes 'photo-hose1-before'
# Note RESULT_FILE path printed (correlation-specific).
# Dispense once; confirm sale OK; photo; second read:
.venv/bin/intelipump-meter-read-once --address 1 --wait-seconds 20 --notes 'photo-hose1-after'
# Then hose 2:
.venv/bin/intelipump-meter-read-once --address 2 --wait-seconds 20 --notes 'photo-hose2-before'
```

## Export **correlation-specific** result JSON (not only latest)

CLI prints `RESULT_FILE path=/var/lib/intelipump/meter-read-result.<correlationId>.json`.

```bash
# List correlation-scoped files (preferred for evidence pack)
ls -la /var/lib/intelipump/meter-read-result.*.json

# Copy a specific capture (replace CORR from CLI output)
CORR='........-....-....-....-............'
sudo cp -a "/var/lib/intelipump/meter-read-result.${CORR}.json" \
  "$HOME/meter-read-${CORR}.json"
# Or export the newest correlation file:
newest=$(ls -t /var/lib/intelipump/meter-read-result.*.json | head -1)
sudo cp -a "$newest" "$HOME/$(basename "$newest")"

# Do NOT rely solely on meter-read-result.json (latest pointer — can be overwritten).
# Optional: also save pointer for convenience, but keep correlation file as truth.
sudo cp -a /var/lib/intelipump/meter-read-result.json \
  "$HOME/meter-read-result-LATEST-POINTER-only.json"

# Journal
sudo journalctl -u intelipump.service --since '15 min ago' --no-pager \
  | tee "$HOME/meter-read-journal-$(date -u +%Y%m%dT%H%M%SZ).log" \
  | grep -E 'METER-READ|meter_read'
```

Off-Pi:

```bash
scp intelipump@100.85.77.22:~/meter-read-*.json .
scp intelipump@100.85.77.22:~/meter-read-journal-*.log .
```

## No scheduled production reads

Keep `HARDWARE_CD101=false` when not attending. No cloud schedules until both addresses pass physical mapping/scale.
