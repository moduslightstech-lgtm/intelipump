# Real-pump poll bench (POLL_ONLY_BENCH)

**Tool:** `intelipump-poll-bench`  
**Scope:** One privately owned Wayne lab dispenser — bounded verified status poll only.  
**Not in scope:** Authorization, preset, price, reset, fuel dispense, continuous polling, Phase 11D–11H.

## Safety guards (exit code 2 if any fail)

- `INTELIPUMP_ENVIRONMENT=LAB`
- `INTELIPUMP_CONTROLLER__MODE=POLL_ONLY_BENCH`
- Exactly one `--address`
- `--max-polls` in 1..10 (default 1)
- Active / remote / automatic authorization disabled
- Command replay disabled
- MQTT disabled
- Lab simulator command path disabled
- `intelipump.service` inactive (`systemctl is-active`)
- Simulator process must not own controller or simulator adapters
- Serial exclusive open + app flock (canonical path)
- Evidence directory writable
- All confirmation flags required

Outbound bytes: only `build_poll(address)` from the existing DART line builder.

## Simulator validation vs real Wayne

Default (no flag) is **real Wayne** mode and refuses any running simulator.

For LAB simulator one-poll validation only:

```bash
intelipump-poll-bench \
  --port /dev/intelipump-controller \
  --address 1 \
  --baud 9600 \
  --max-polls 1 \
  --response-timeout-ms 100 \
  --evidence-dir data/bench/real-wayne \
  --simulator-validation \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-emergency-isolation-ready \
  --confirm-no-fuel-test \
  --confirm-authorization-disabled
```

With `--simulator-validation`:

- known simulator may own only `/dev/intelipump-simulator`
- `/dev/intelipump-controller` must be free
- refuse if simulator owns the controller adapter
- refuse unrelated holders on either adapter
- evidence records `targetType=SIMULATOR` and `simulatorValidation=true`

Without the flag (real Wayne):

- refuse if any simulator process is running
- evidence records `targetType=OWNED_LAB_WAYNE` and `simulatorValidation=false`

## Pi one-poll field procedure

```bash
cd /home/intelipump/intelipump/intelipump-fdc

# 1) Stop normal controller + ensure simulator is not on the adapters
sudo systemctl stop intelipump.service
systemctl is-active intelipump.service   # inactive
# stop any intelipump-simulator-serial processes if running
sudo lsof /dev/intelipump-controller /dev/intelipump-simulator || true

# 2) Export required mode for this session only
export INTELIPUMP_ENVIRONMENT=LAB
export INTELIPUMP_CONTROLLER__MODE=POLL_ONLY_BENCH
export INTELIPUMP_SAFETY__ACTIVE_COMMANDS_ENABLED=false
export INTELIPUMP_SAFETY__REMOTE_AUTHORIZATION_ENABLED=false
export INTELIPUMP_SAFETY__AUTOMATIC_AUTHORIZATION_ENABLED=false
export INTELIPUMP_SAFETY__COMMAND_REPLAY_ENABLED=false
export INTELIPUMP_MQTT__ENABLED=false

# 3) One verified status poll
intelipump-poll-bench \
  --port /dev/intelipump-controller \
  --address 1 \
  --baud 9600 \
  --max-polls 1 \
  --response-timeout-ms 100 \
  --evidence-dir data/bench/real-wayne \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-emergency-isolation-ready \
  --confirm-no-fuel-test \
  --confirm-authorization-disabled

# 4) Review evidence
ls -la data/bench/real-wayne/
# open poll-bench-*.jsonl and poll-bench-*.md

# 5) Restore simulator + LISTEN_ONLY controller
unset INTELIPUMP_CONTROLLER__MODE
# or: export INTELIPUMP_CONTROLLER__MODE=LISTEN_ONLY
sudo systemctl start intelipump.service
systemctl show intelipump -p ActiveState,StatusText
```

Record dispenser UI observations in the Markdown file (address, idle state, any change).

Stop immediately if dispenser behavior changes unexpectedly.

## Rollback

```bash
# Abort poll-bench if running (Ctrl+C / SIGTERM)
sudo systemctl stop intelipump.service
# Physically disconnect lab serial if needed
export INTELIPUMP_CONTROLLER__MODE=LISTEN_ONLY
sudo systemctl start intelipump.service
systemctl show intelipump -p ActiveState,StatusText,NRestarts
```
