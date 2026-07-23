# Virtual Wayne DART Pump Simulator (Phase 5)

Software-only dispenser simulator that exercises the DART line layer,
application subset, Phase 4 state machine, and command guards.

**No real serial ports.** Controllers talk to
`SimulatorSession.receive(frame_bytes)` / `advance(ms)`.

## Layout

| Module | Role |
|---|---|
| `clock.py` | Deterministic ms clock, schedule, run_until_idle |
| `config.py` | Sequence policy, filling/pump/dispenser config |
| `encoding.py` | CD/DC application encoders (simulator only) |
| `faults.py` | Protocol fault kinds |
| `models.py` | Snapshots and exchange results |
| `pump.py` | Fueling position + filling engine + guarded commands |
| `session.py` | Multi-pump byte interface (POLL/DATA/ACK/NAK/EOT) |
| `scenarios.py` | Lifecycle scenarios + runner |
| `cli.py` | `intelipump-simulator` |

Default dispenser: two fueling positions (`fp-1` addr 1, `fp-2` addr 2).

## Line behavior

- Invalid CRC → no ACK; `INVALID_CRC` fault
- Unexpected controller TX# → NAK
- Duplicate DATA (same accepted seq) → ACK, no reprocess
- POLL with pending → DATA; else EOT
- ACK advances pump TX# per configurable policy
- Response timeout (default 25 ms simulated) recorded; POLL retransmits

### Sequence policy

- `SPEC_F_TO_1` (default) — documented F→1 wrap
- `OBSERVED_F_TO_0` — seen on merged-bus captures

Neither is claimed proven for direction-separated traffic.

## Application subset

Inbound (controller→sim): CD1 commands, CD3/CD4 presets, CD5 prices, CD101 totals request.

Outbound (sim→controller): DC1 status, DC2 fill, DC3 nozzle/price, DC5 alarm, DC9 identity stub, DC101 totals.

Active commands run **only** inside the simulator and only after Phase 4
guard evaluation with simulator physical-enable / active-commands flags.

## Filling engine

- Decimal flow rate (L/s), update interval, scaled volume/amount
- Amount = volume × unit price (Decimal); never decreases
- Suspend freezes; resume continues; stop completes
- Preset amount/volume stop at threshold
- Duplicate/zero time advances do not double-apply

## CLI

```bash
uv run intelipump-simulator --list-scenarios
uv run intelipump-simulator --scenario normal_sale
uv run intelipump-simulator --all
```

## Scenarios

1. cold_start
2. normal_sale
3. preset_amount_sale
4. preset_volume_sale
5. suspend_resume
6. communication_loss_recovery
7. restart_idle
8. restart_during_filling
9. fault_during_filling
10. price_change_during_filling
