# Real-Wayne CD5 price programming review

Status: dry-run + single-shot CD5 write + single-shot CD1 RESET / AUTHORIZE
available for owned lab pump only under technician confirmations.

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
- After `FILLING_COMPLETE` / CLOSED display, CD1 RESET clears to `RESET`
- CD1 AUTHORIZE (from `RESET`) enables live delivery UI (`AUTHORIZED`)

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

Post-write verify retries status polls (default settle 400 ms, up to 8 polls)
until DC1 is `FILLING_COMPLETE`.

## Clear CLOSED display (single CD1 RESET)

Use after CD5 when the pump shows CLOSED / `FILLING_COMPLETE`:

```bash
./venv/bin/intelipump-real-wayne-reset-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --evidence-dir data/bench/real-wayne/cd1-reset \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-no-product-connected \
  --confirm-motor-isolated \
  --confirm-valves-isolated \
  --confirm-emergency-isolation-ready \
  --confirm-authorization-disabled \
  --confirm-single-write-plan-reviewed \
  --confirm-execute-cd1-reset \
  --confirm-post-write-status-verification-required \
  --i-understand-this-transmits-to-owned-lab-pump
```

Wire command (address 1, sequence 0):

```text
50 30 01 01 05 5f 5f 03 fa
```

Expect DC1 `RESET` after verify.

## Enable live volume/amount UI (single CD1 AUTHORIZE)

High-risk. Only with motor + valves isolated and no product. Requires prior
RESET. Does not auto-chain from RESET.

```bash
./venv/bin/intelipump-real-wayne-authorize-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --evidence-dir data/bench/real-wayne/cd1-authorize \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-no-product-connected \
  --confirm-motor-isolated \
  --confirm-valves-isolated \
  --confirm-emergency-isolation-ready \
  --confirm-single-write-plan-reviewed \
  --confirm-execute-cd1-authorize \
  --confirm-post-write-status-verification-required \
  --i-understand-this-transmits-to-owned-lab-pump \
  --i-understand-authorize-enables-live-delivery-ui
```

Wire command (address 1, sequence 0):

```text
50 30 01 01 06 1f 5e 03 fa
```

Expect DC1 `AUTHORIZED` after verify.

## Safety model

- Status polls always allowed.
- Active DATA frame allowed only after
  `authorize_single_active_write(exact_frame, kind=...)` and only once.
- Kinds: `CD5_PRICE`, `CD1_RESET`, `CD1_AUTHORIZE`.
- No raw-hex / replay / generic send path.
- Tools do not auto-chain CD5 → RESET → AUTHORIZE.

## Still unproven / caution

- Lab ACK timing and display lag may vary by pump firmware.
- AUTHORIZE enables delivery UI even when motor/valves are isolated — treat as
  live-path enablement for the dispenser controller.
