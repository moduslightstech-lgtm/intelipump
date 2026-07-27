# Real-Wayne CD5 price programming review

Status: dry-run available; **single-shot write CLI available for owned lab pump
only** under technician confirmations. RESET/AUTHORIZE remain unavailable.

## Confirmed by DART documentation

- CD5 is Price Update (`TRANS = 0x05`)
- `LNG = 3 ×` number of logical-nozzle prices
- Each price is three-byte packed BCD, MSB first
- PRI1 → logical nozzle 1; PRI2 → logical nozzle 2
- All configured logical nozzles require prices or CD5 may be ignored
- A zeroized pump remains `PUMP_NOT_PROGRAMMED` until a valid price is received
- Price update is separate from RESET and AUTHORIZE
- Documented expected transition after correct programming + price:
  `PUMP_NOT_PROGRAMMED` → `FILLING_COMPLETE` (status code 5)

## Dry-run (no transmit)

```bash
./venv/bin/intelipump-real-wayne-price-dry-run \
  --port /dev/intelipump-controller \
  --address 1 \
  --logical-nozzle-count 2 \
  --price-nozzle-1 1175 \
  --price-nozzle-2 1175 \
  --price-scale-confirmed-by-technician \
  --logical-nozzle-mapping-confirmed-by-technician \
  --evidence-dir data/bench/real-wayne/price-dry-run \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-no-product-connected \
  --confirm-motor-isolated \
  --confirm-valves-isolated \
  --confirm-emergency-isolation-ready \
  --confirm-authorization-disabled \
  --confirm-single-write-plan-reviewed
```

## Real write (single CD5 only)

```bash
./venv/bin/intelipump-real-wayne-price-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --logical-nozzle-count 2 \
  --price-nozzle-1 1175 \
  --price-nozzle-2 1175 \
  --price-scale-confirmed-by-technician \
  --logical-nozzle-mapping-confirmed-by-technician \
  --evidence-dir data/bench/real-wayne/price-write \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-no-product-connected \
  --confirm-motor-isolated \
  --confirm-valves-isolated \
  --confirm-emergency-isolation-ready \
  --confirm-authorization-disabled \
  --confirm-single-write-plan-reviewed \
  --confirm-execute-cd5-write \
  --confirm-post-write-status-verification-required \
  --i-understand-this-transmits-to-owned-lab-pump
```

Wire command for this example (sequence 0):

```text
50 30 05 06 00 11 75 00 11 75 da c7 03 fa
```

Safety model:

- Status polls always allowed.
- CD5 DATA frame allowed only after `authorize_single_cd5_write(exact_frame)` and
  only once.
- No raw-hex / replay / RESET / AUTHORIZE path.
- Evidence records `transmitted` and `cd5WriteCount`.

## Still unproven / caution

- Exact ACK timing on this pump (tool records ACK match or timeout)
- Whether other programming transactions are also required
- Production retry policy (this tool does **not** retry CD5)
