# Real-Wayne CD5 price programming review

Status: **dry-run / candidate-only**. No active write path. Status polls only.

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

## Confirmed by our real capture / lab

- Wire address `0x50` (logical 1)
- Current DC1 status `PUMP_NOT_PROGRAMMED` (0)
- DC2 volume and amount are zero
- Status polling + CRC stable (102/102)
- Pump reports a DC3 block (selected nozzle evidence only)

## Technician-confirmed (required CLI flags)

- Two logical nozzles and ordering
- Display-price / scale interpretation
- No product connected; motor isolated; valves isolated
- Emergency isolation ready; authorization disabled
- Single-write plan reviewed

## Tool

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

Behavior:

1. PermanentSerialReader + status poll only
2. Decode DC1/DC2/DC3; refuse if preconditions fail
3. Build CD5 `05 06 00 11 75 00 11 75` and outer frame **in memory**
4. Write evidence + `price-programming-review.json` with `transmitted=false`
5. Hard refuse any non-poll `serial.write()` via `RealWayneActiveCommandRefusedError`

## Still unproven

- Exact line-layer ACK for CD5 on this pump
- Timing after CD5
- Whether other programming transactions are required
- Meaning of earlier merged-capture Cx frames
- Real accept/reject response to this price block

Do **not** enable a real write milestone until those are proven under a separate reviewed plan.
