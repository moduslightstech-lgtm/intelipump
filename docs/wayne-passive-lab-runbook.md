# Wayne passive lab runbook (receive-only)

**Scope:** Privately owned Wayne dispenser in a controlled development lab in Nigeria.  
**Not in scope:** Production stations, customer equipment, active authorization, Phase 11D–11H.

Related prep doc: `docs/phase-11c-wayne-passive-lab-test.md`.

---

## Safety boundaries (non-negotiable)

- Environment: `LAB`
- Controller mode when service runs: `LISTEN_ONLY`
- Active commands: disabled
- Remote / automatic authorization: disabled
- MQTT: disabled
- Passive capture: **receive-only** — no poll, ACK, EOT, authorize, or command replay
- Dispenser remains in **standalone/local** mode for this milestone
- Do **not** change the dispenser into serial/controller mode during this test
- Do **not** connect to pump power, mains, motor, solenoid, or authorization terminals
- Qualified Wayne technician present
- Emergency power isolation available
- Stop immediately if dispenser behavior changes unexpectedly

Software **cannot** prove that TX is inhibited. Capture refuses to start without
`--confirm-tx-physically-inhibited`.

---

## Service isolation sequence (required)

The normal controller service must **not** own the serial port during passive capture.

1. Stop the controller service:
   ```bash
   sudo systemctl stop intelipump.service
   systemctl is-active intelipump.service   # expect: inactive
   ```
2. Verify the controller port is free:
   ```bash
   ls -l /dev/intelipump-controller
   # Optional: sudo fuser /dev/intelipump-controller  # expect empty
   ```
3. Start passive capture (`intelipump-capture-passive` — see Stage A commands).
4. Perform the observation (Stages A–C as approved).
5. Stop passive capture (duration expiry, Ctrl+C, or SIGTERM).
6. Inspect the capture file and offline decode.
7. Reconnect the simulator (if used for controller validation after the session).
8. Restart the controller service:
   ```bash
   sudo systemctl start intelipump.service
   systemctl show intelipump -p ActiveState,StatusText,NRestarts
   ```
9. Confirm healthy simulator status, for example:
   `mode=LISTEN_ONLY serial=open pumps=2/2 healthy …`

Passive capture refuses to start when the port is already in use (exclusive open /
port-in-use guard), where the OS supports it.

---

## Pre-connection electrical checklist

Complete **before** any cable touches the dispenser serial interface.

### Interface confirmation (do not invent pinouts)

- [ ] Confirm Wayne interface model and connector **from the service manual or technician**
- [ ] Confirm RS-232 versus RS-485 (**do not assume**)
- [ ] Confirm voltage levels (**manual / technician**)
- [ ] If RS-485: confirm A/B polarity (**manual / technician**)
- [ ] Confirm signal ground / reference
- [ ] Confirm baud and serial format (DART lab default assumption: **9600 8O1** — still confirm on site)
- [ ] Confirm **no** connection to mains or pump power terminals
- [ ] Use an isolated USB–serial adapter where appropriate
- [ ] TX disconnected, inhibited, or physically prevented on the Pi side
- [ ] Continuity check of intended signal/ground paths before energizing
- [ ] Power off before initial wiring
- [ ] Qualified technician present
- [ ] Emergency isolation available and understood
- [ ] Pi power source stable
- [ ] `udev` alias confirmed (`/dev/intelipump-controller`)
- [ ] Current SQLite backup confirmed (`/var/lib/intelipump/intelipump.db`)
- [ ] Controller rollback path confirmed (previous unit/package or git tag)
- [ ] Simulator rollback environment preserved (PTY/udev aliases known)

**Pin assignments:** Not documented here. Any unavailable pinout detail requires the
exact Wayne service manual or on-site technician confirmation. Do not guess.

---

## First passive lab procedure

### Stage A — no pump operation (first electrical / idle session)

- Dispenser remains in standalone/local mode
- Power and communication wiring checked per checklist
- Start passive capture (5 minutes idle)
- Confirm the dispenser behaves normally
- Record displayed idle state (photo or handwritten notes with UTC time)
- Stop capture and inspect offline
- **Fuel flow is not required** for Stage A

### Stage B — local operator interaction without dispensing fuel

Only when the lab setup supports it safely:

- Local keypad / nozzle interaction as directed by the technician
- No Pi transmission
- Capture state-change traffic if any appears on the bus
- Record displayed state and exact timestamps
- Stop if any unexpected behavior occurs

### Stage C — one controlled local transaction

Only when the owned lab dispenser is safely configured and a qualified technician
approves it:

- Authorization remains **local/standalone** (not from the Pi)
- Pi stays receive-only
- Record: pump address, nozzle, product, unit price, volume, amount, start/end time
- Preserve photos or handwritten observations separately from the JSONL
- Capture serial bytes for the complete transaction window
- Do not infer correctness until offline review
- Do not advise bypassing safety systems

---

## Exact first-session commands (Stage A)

Project path on Pi: `/home/intelipump/intelipump/intelipump-fdc`

```bash
cd /home/intelipump/intelipump/intelipump-fdc

# 1) Stop controller so it does not own the port
sudo systemctl stop intelipump.service
systemctl is-active intelipump.service

# 2) Confirm port exists / free
ls -l /dev/intelipump-controller

# 3) Passive capture (TX must already be physically inhibited)
intelipump-capture-passive \
  --port /dev/intelipump-controller \
  --baud 9600 \
  --duration 300 \
  --format jsonl \
  --read-size 256 \
  --idle-gap-ms 50 \
  --output data/captures/wayne-passive-$(date -u +%Y%m%d-%H%M%S).jsonl \
  --confirm-tx-physically-inhibited \
  --confirm-controller-stopped

# 4) Offline decode (no serial open)
intelipump-decode-capture \
  --input data/captures/wayne-passive-YYYYMMDD-HHMMSS.jsonl \
  --json \
  --write-report \
  --duration 300

# 5) After observation: restore simulator + controller
# (reconnect simulator PTYs / adapters as used in your lab)
sudo systemctl start intelipump.service
systemctl show intelipump -p ActiveState,StatusText,NRestarts
```

Clean stop during capture: Ctrl+C or `kill -TERM <pid>` — both flush JSONL and emit `capture_stopped`.

---

## Rollback

```bash
# Abort capture if running (Ctrl+C / SIGTERM)
sudo systemctl stop intelipump.service   # if partially restarted
# Physically disconnect lab serial cable from the dispenser interface
# Restore previous known-good software/unit if needed, then:
sudo systemctl daemon-reload
sudo systemctl start intelipump.service
systemctl show intelipump -p ActiveState,StatusText,NRestarts
```

---

## Validation conclusion rules

Report template: `data/captures/<capture-id>/validation-report.md`

Conclusions:

- `PASSIVE_CAPTURE_PASS`
- `PASSIVE_CAPTURE_INCONCLUSIVE`
- `PASSIVE_CAPTURE_FAIL`

Do **not** mark PASS unless:

- no operational interference occurred
- bytes were captured reliably
- frame boundaries are reproducible
- CRC behavior is consistent
- observed display values can be matched **without guesswork**

The offline report generator never auto-selects PASS.
