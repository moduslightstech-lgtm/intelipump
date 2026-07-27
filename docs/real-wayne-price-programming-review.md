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

## Sequence nibble (important)

Each accepted active DATA write must use the **next** L2 sequence (`0..F`, wrap).
Reusing the prior sequence is treated as a **retransmission**: the pump may ACK
again without applying a new application command.

Lab observation: CD5 with `--sequence 0` then RESET with `--sequence 0` got
`ACK_MATCH` but DC1 stayed `FILLING_COMPLETED`.

| Step | Example `--sequence` | Control byte |
|------|----------------------|--------------|
| CD5 price | `0` | `0x30` |
| CD1 RESET | `1` | `0x31` |
| CD1 AUTHORIZE | `2` | `0x32` |

## Clear CLOSED display (single CD1 RESET)

Use after CD5 when the pump shows CLOSED / `FILLING_COMPLETE`. Advance sequence
after the CD5 write (typically `--sequence 1` if CD5 used `0`):

```bash
./venv/bin/intelipump-real-wayne-reset-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --sequence 1 \
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

Wire command (address 1, sequence 1):

```text
50 31 01 01 05 <crc_lo> <crc_hi> 03 fa
```

Expect DC1 `RESET` after verify.

## Enable live volume/amount UI (single CD1 AUTHORIZE)

High-risk. Only with motor + valves isolated and no product. Requires prior
RESET. Does not auto-chain from RESET. Use next sequence after RESET
(typically `--sequence 2`):

```bash
./venv/bin/intelipump-real-wayne-authorize-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --sequence 2 \
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

Wire command (address 1, sequence 2):

```text
50 32 01 01 06 <crc_lo> <crc_hi> 03 fa
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
- If ACK matches but DC1 is unchanged after a fresh sequence, do **not** keep
  advancing sequence blindly. Lab: seq 0 and seq 1 both ACK'd with DC1 still
  `FILLING_COMPLETED`. Next documented alternate: nozzle **OUT**, then one
  RESET with the next unused sequence (typically `--sequence 2`).
- Confirm the physical display before further TX. Do not AUTHORIZE until
  DC1 is `RESET`.

## CD2 + RESET combined block (lab hypothesis)

Lone CD1 RESET ACK'd without DC1 change on this pump (seq 0–3). Next documented
flow uses **CD2 allowed nozzles then CD1 RESET in one DATA block**. This tool
**refuses TX unless software sees nozzle OUT**.

```bash
export INTELIPUMP_CONTROLLER__MODE=LISTEN_ONLY

# Poll first until NOZIO high-nibble is 1 (OUT), then:
./venv/bin/intelipump-real-wayne-cd2-reset-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --sequence 4 \
  --allowed-nozzle 1 \
  --allowed-nozzle 2 \
  --evidence-dir data/bench/real-wayne/cd2-reset \
  --logical-nozzle-mapping-confirmed-by-technician \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-no-product-connected \
  --confirm-motor-isolated \
  --confirm-valves-isolated \
  --confirm-emergency-isolation-ready \
  --confirm-authorization-disabled \
  --confirm-single-write-plan-reviewed \
  --confirm-nozzle-out-observed \
  --confirm-execute-cd2-and-cd1-reset \
  --confirm-post-write-status-verification-required \
  --i-understand-this-transmits-to-owned-lab-pump
```

Example wire (addr 1, seq 4, nozzles 1+2):

```text
50 34 02 02 01 02 01 01 05 70 ed 03 fa
```
