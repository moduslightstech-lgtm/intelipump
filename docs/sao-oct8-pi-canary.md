# SAO Oct-8 Pi canary — pump 5

**Pin (install this SHA):** `7c07c4b9159eea0fd87871e476bd836c4cd284e0` on `prod_feature` (feature base `955aea1` + review + zero-gate fix).  
Deploy only after cloud Stage 1 consumer `kacytunde/intelipump-consumer:prod_feature-3510426f6ee5` is live.

**Repo path on pump 5:** `/home/intelipump/intelipump-fdc/intelipump`  
**Controller binary:** `/home/intelipump/intelipump-fdc/intelipump/.venv/bin/intelipump-controller`

Do **not** enable `INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK`.  
Do **not** issue pump/RS-485 commands.  
Preserve `/etc/intelipump/intelipump.env`, channel maps, prices, SQLite.

---

## What changed

1. **UUID completion keys** — `complete:{uuid}` is the business key; Wayne frame hex is evidence.  
2. **Provisional sidecar** — hold while DC1 live (`provisional_sidecar_snapshot_held`).  
3. **Same-UUID reopen** — only `sidecar-settle:` COMPLETED; verified COMPLETED refused.  
4. **Pre-auth zero gate** — persistent non-zero DC2 after RESET fails closed (not treated as display-hold silence).

---

## A. Install pinned code on pump 5 (attended idle)

Run on the Pi as `intelipump` (or with sudo where shown). Nozzles idle; no active dispense.

```bash
export PI_SHA=7c07c4b9159eea0fd87871e476bd836c4cd284e0
export PREV_SHA="$(git -C /home/intelipump/intelipump-fdc/intelipump rev-parse HEAD)"
echo "PREV_SHA=$PREV_SHA" | tee /tmp/intelipump-pump5-prev-sha.txt

cd /home/intelipump/intelipump-fdc/intelipump

# 1) Fetch + clean-worktree checks (do not discard local SAO edits blindly)
git fetch --prune origin
git status --porcelain=v1
# Expect empty. If not empty: stop, review, stash only if you intend to.
# Do NOT git clean -fdx (would wipe .venv / local config).

# 2) Preserve live SAO configuration (installer must not rewrite these)
sudo cp -a /etc/intelipump/intelipump.env /tmp/intelipump.env.pre-${PI_SHA:0:12}
sudo cp -a /etc/intelipump/intelipump-cloud-sync.env /tmp/intelipump-cloud-sync.env.pre-${PI_SHA:0:12}
test -f /etc/intelipump/intelipump.env
grep -E 'STATION_ID|DEVICE_ID|CHANNEL_MAP|LOGICAL_PUMP|PRODUCT|SERIAL_PORT|REQUIRE_APPLICATION_SALE_ACK' \
  /etc/intelipump/intelipump.env /etc/intelipump/intelipump-cloud-sync.env || true
# ACK must stay off:
grep -E 'REQUIRE_APPLICATION_SALE_ACK' /etc/intelipump/intelipump-cloud-sync.env \
  && grep -E 'REQUIRE_APPLICATION_SALE_ACK=.*true' /etc/intelipump/intelipump-cloud-sync.env \
  && echo 'REFUSE: sale ACK is on' && exit 1 || echo 'ACK off or unset (OK)'

# 3) Pinned checkout (detached OK for canary)
git checkout "$PI_SHA"
git rev-parse HEAD   # must print 7c07c4b9159eea0fd87871e476bd836c4cd284e0
git status --porcelain=v1   # still empty

# 4) Package install into existing venv (preserves /etc configs)
export PATH="${HOME}/.local/bin:${PATH}"
command -v uv
uv sync --dev
test -x /home/intelipump/intelipump-fdc/intelipump/.venv/bin/intelipump-controller
test -x /home/intelipump/intelipump-fdc/intelipump/.venv/bin/intelipump-cloud-sync

# 5) Installed-code verification
git rev-parse HEAD
/home/intelipump/intelipump-fdc/intelipump/.venv/bin/intelipump-controller --help >/dev/null
# Confirm reopen helper and zero-gate fix are present in installed tree:
rg -n "reopen_provisional_sidecar|persistent_nonzero_dc2" \
  src/intelipump_fdc/persistence/repositories/transactions.py \
  src/intelipump_fdc/controller/controller_loop.py
# Confirm systemd still points at this venv binary:
systemctl cat intelipump.service | grep -E 'ExecStart='
# Expect ExecStart=.../intelipump-fdc/intelipump/.venv/bin/intelipump-controller

# 6) Restore env if anything touched it (should be unchanged)
sudo cp -a /tmp/intelipump.env.pre-${PI_SHA:0:12} /etc/intelipump/intelipump.env
sudo cp -a /tmp/intelipump-cloud-sync.env.pre-${PI_SHA:0:12} /etc/intelipump/intelipump-cloud-sync.env

# 7) Attended idle restart (no pump commands)
systemctl is-active intelipump intelipump-cloud-sync
sudo systemctl restart intelipump.service
sudo systemctl restart intelipump-cloud-sync.service

# 8) Post-start checks
systemctl is-active intelipump intelipump-cloud-sync
systemctl show intelipump -p ActiveState,SubState,NRestarts,ExecMainStatus
journalctl -u intelipump -u intelipump-cloud-sync -n 80 --no-pager
# Confirm ACK still off in running process environment if exported:
systemctl show intelipump-cloud-sync -p EnvironmentFiles
grep -E 'REQUIRE_APPLICATION_SALE_ACK' /etc/intelipump/intelipump-cloud-sync.env || echo 'ACK unset (defaults false)'
```

Do **not** run `./scripts/install_sao_rs1_pump_pi.sh` or `bootstrap_sao_rs1_pump_pi.sh` for this canary — those rewrite `/etc/intelipump/*.env` and channel-map paths.

---

## B. Attended canary cases

1. Leave application sale ACK **disabled**.  
2. Record each dispense: nozzle, Lagos start/end, face litres/amount/price, totalizers.  
3. Cover: normal hang-up; mid-fill pause then resume (43→54); two equal-value sales; lift/return without dispense.  
4. Trace: Pi UUID → sync_queue → cloud `sale_ingestion_decisions` + `pump_transactions` → dashboard.  
5. Pass: one physical dispense → one Pi identity → one cloud COMPLETED → one dashboard row.

---

## C. Rollback pump 5

```bash
# Use PREV_SHA captured at install time
export PREV_SHA="$(cat /tmp/intelipump-pump5-prev-sha.txt | cut -d= -f2)"
cd /home/intelipump/intelipump-fdc/intelipump

git fetch --prune origin
git status --porcelain=v1   # expect empty
git checkout "$PREV_SHA"
git rev-parse HEAD

export PATH="${HOME}/.local/bin:${PATH}"
uv sync --dev

# Restore pre-canary env backups if present
sudo cp -a /tmp/intelipump.env.pre-* /etc/intelipump/intelipump.env 2>/dev/null || true
# Prefer the exact backup file name from install step; do not wipe prices/maps.

sudo systemctl restart intelipump.service
sudo systemctl restart intelipump-cloud-sync.service
systemctl is-active intelipump intelipump-cloud-sync
journalctl -u intelipump -u intelipump-cloud-sync -n 40 --no-pager

# Do not wipe /var/lib/intelipump/intelipump.db
```

---

## Limitation

If a provisional sidecar COMPLETED was already delivered to cloud before reopen, cloud may still hold that COMPLETED row (no auto-retract). Prefer never publishing while live (fill_stream hold).
