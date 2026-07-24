# Phase 11C — Passive Wayne lab-pump preparation (documentation only)

**Status:** Preparation checklist for the first real-hardware *passive* observation.  
**Not started:** Electrical connection to the Wayne dispenser, active authorization, or Mode change.

This document does **not** authorize connecting to any production station or customer dispenser. It applies only to a privately owned Wayne dispenser in a controlled development lab.

---

## Scope and boundaries

| Item | Requirement |
|------|-------------|
| Hardware | Privately owned Wayne dispenser in a controlled development lab (Nigeria) |
| Personnel | Qualified technician present for the entire session |
| Dispenser mode | Initially standalone / local operating mode |
| Pi controller mode | `LISTEN_ONLY` (LAB); active commands disabled |
| Transmit | Pi TX physically inhibited where practical (TX open, TX–RX short removed, or adapter TX pin disconnected) |
| Fuel | No fuel dispensing required for the first electrical/serial observation |
| Commands | No command replay; no guessed authorization; no guessed control sequences |
| Dispenser serial mode | Do **not** change dispenser serial mode during the first passive session |
| Automated tests | Must not require the real Wayne dispenser |

Out of scope for this first session: Phase 12 active electronic-head work, authorization bytes, MQTT, and any non-LISTEN_ONLY controller mode.

---

## Goals of the first passive session

1. Confirm electrical/serial interface compatibility without driving the dispenser.
2. Capture raw frames and timestamps (if the dispenser emits traffic while standalone).
3. Record what the dispenser UI shows (address, price, amount, volume, state) for offline correlation.
4. Confirm the dispenser continues to function normally in standalone mode.
5. Preserve logs and raw captures for offline review.
6. Disconnect immediately if dispenser operation changes unexpectedly.

Success for this session is **observation and safety**, not protocol completeness.

---

## Pre-connection checklist

Complete **all** items before any cable touches the dispenser serial interface.

### Interface and isolation

- [ ] Verified voltage / interface type (do not assume RS-485 vs RS-232)
- [ ] RS-232 versus RS-485 confirmed against dispenser documentation / markings
- [ ] Signal ground / reference confirmed
- [ ] No connection to mains or pump power terminals
- [ ] Isolated adapter used where appropriate
- [ ] TX physically inhibited or disconnected on the Pi/controller side
- [ ] Emergency power isolation available and understood by the technician

### Controller / host readiness

- [ ] Database backup completed (`/var/lib/intelipump/intelipump.db` or configured path)
- [ ] Controller service rollback available (previous unit + package, or known-good git tag)
- [ ] Stable udev alias confirmed (`/dev/intelipump-controller`)
- [ ] Watchdog behavior confirmed (`Type=notify`, `WatchdogSec=30s`, healthy STATUS)
- [ ] Serial reconnect behavior confirmed (simulator disconnect/reconnect; process stays up)
- [ ] Environment remains `LAB`
- [ ] Controller remains `LISTEN_ONLY`
- [ ] Active commands disabled; remote authorization disabled; automatic authorization disabled
- [ ] MQTT disabled / not required

### Session readiness

- [ ] Qualified technician present
- [ ] Capture tooling ready (raw frame log path, journalctl, optional serial sniffer)
- [ ] Disconnect procedure rehearsed (who pulls which cable; power isolation if needed)
- [ ] No production or customer site involved

---

## Recommended session procedure (high level)

1. Start with the dispenser in standalone/local mode and **no** Pi serial cable attached.
2. Confirm Pi service healthy in LISTEN_ONLY against the simulator or with serial missing (reconnect backoff).
3. Inhibit TX on the controller adapter.
4. Connect **receive path and ground only** as approved by the technician for this interface type.
5. Observe only: journal, raw captures, dispenser UI.
6. Record: displayed pump address, price, amount, volume, and state (as shown).
7. If anything unexpected occurs (error tone, mode change, unauthorized dispense attempt, bus contention), disconnect immediately.
8. End session; leave dispenser in a verified normal standalone state.
9. Archive logs and captures offline; do not iterate guessed commands.

---

## What to capture

- UTC timestamps for connect / disconnect / any anomaly
- Raw frame bytes and direction (RX-only expected when TX inhibited)
- `journalctl -u intelipump` around the session (serial_* / pump_* health events)
- systemd `StatusText` samples (`mode=`, `serial=`, `pumps=`, `reconnects=`, …)
- Dispenser UI: address, price, amount, volume, state
- Notes on whether the dispenser continued normal standalone operation

---

## Explicit prohibitions

- Do not transmit guessed authorization or control command sequences.
- Do not replay captured commands onto the dispenser.
- Do not enable active controller mode for this first passive session.
- Do not change DART framing, CRC, ACK, timing, polling, retry, or state-machine behavior ad hoc on site.
- Do not treat a missing USB adapter as an application failure requiring unsafe recovery shortcuts.

---

## Related software (already in Phase 11C)

- Serial health: `OPEN` / `DEGRADED` / `DISCONNECTED` / `FAULTED`
- Per-pump communication health with configurable timeout thresholds
- Bounded reconnect backoff (0.5s … 15s) without busy-loop or process exit
- Read-only in-process `health_diagnostic_snapshot()` for a future health CLI
- systemd STATUS compact line including serial and pump health summary

---

## Rollback (if service was updated for the session)

```bash
sudo systemctl stop intelipump.service
# Redeploy previous known-good tree / unit, then:
sudo systemctl daemon-reload
sudo systemctl start intelipump.service
systemctl show intelipump -p ActiveState,StatusText,NRestarts
```

---

## After this document

Passive electrical observation is a **manual lab activity**. Automated unit/integration tests must continue to use the simulator and fake transports only. Active Wayne authorization remains a later phase and is out of scope for Phase 11C.
