# Real-Wayne CD5 price programming review

Status: **CD5 proven on owned lab Wayne.** Single-shot CD1 RESET / CD2+RESET /
AUTHORIZE tools exist but **RESET→DC1 `1` is unproven** on this head — park
AUTHORIZE until RESET is proven.

Controller mode: price/reset/authorize CLIs use `LISTEN_ONLY`. Poll-bench uses
`POLL_ONLY_BENCH`.

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
- After `FILLING_COMPLETE` / CLOSED display, CD1 RESET *should* clear to `RESET`
- CD1 AUTHORIZE (from `RESET`) enables live delivery UI (`AUTHORIZED`)

## Lab conclusions (owned lab pump, Jul 2026)

| Observation | Result |
|-------------|--------|
| CD5 two-nozzle `1175`/`1175` | Works. DC1 → `FILLING_COMPLETED/5`; DC3 price `00 11 75` |
| Glass after CD5 | Price + `0.00` may flash, then **CLOSED** (matches DC1 `5`) |
| Glass scale | `1175` wire → **1.175** when DPUNP=3; need DPUNP=0 for glass `1175` |
| CD5 from `FILLING_COMPLETE` | Tool now allows re-price (Pump Interface ex. 4.3); does not fix RESET |
| Post-CD5 verify lag | Tool may `FAULT` while still seeing DC1 `0`; a later poll shows `5` |
| Lone CD1 RESET | TX + often `ACK_MATCH` (sometimes `ACK_TIMEOUT`); **DC1 stays `5`** |
| CD2+RESET (nozzle OUT) | TX + `ACK_MATCH`; **DC1 stays `5`** (when OUT was available earlier) |
| Hang / time | Can leave `5` back to `PUMP_NOT_PROGRAMMED/0` while DC3 may still show price |
| AUTHORIZE | Not attempted while DC1 ≠ `RESET` |

### Lab notes 2026-07-29 (nozzle OUT / CD101)

| Observation | Result |
|-------------|--------|
| Both logical sides CD5 → `DC1=5` | Yes (addr2 needs correct next L2 sequence + longer `--ack-timeout-ms`) |
| Dual POLL + RETURN_STATUS alone | Usually **NOZIO=`01` IN** even when glass reacts |
| ePump first OUT (private capture) | **`0x51`**: `DC1=5` + `NOZIO=11`; preceded by **CD101** (`65 01 01`), not CD2/RESET |
| Gated CD101 tool | `intelipump-real-wayne-cd101-request` — ACK proven on addr2 |
| Lab OUT after CD101 | **Once** on addr2 during dual poll (`…183634…`, hundreds of `11`); **not reliably repeatable** the same day |
| CD2+RESET while chasing OUT | Often `REFUSED` (`nozzle_not_OUT`) if hose hung or OUT not currently reported |
| Offline DC1 analysis | Require `crc_valid` + wire ADR `0x50`/`0x51`; bare `01 01 ..` / mid-frame slices false-flag `RESET` |

**Do not keep bumping `--sequence` for RESET** without live wire **NOZIO OUT** and a plan to verify DC1→`1`. Park AUTHORIZE.

Working hypothesis: CD101 (and/or ePump-like timing) can precede OUT reporting on addr2, but OUT is intermittent; need poll-until-OUT then immediate CD2+RESET without releasing the hose between observe and TX.

### Status grep (avoid false positives)

Prefer:

```bash
grep -oE '03 04 .. .. .. .. 01 01 ..' path/to/poll-bench-*.jsonl | tail -3
```

Example: `03 04 00 11 75 01 01 01 05` → price 1175, nozzle 1 **IN**, DC1 **`05`**.

Do **not** use bare `grep -oE '01 01 ..'` — that can match nozzle byte + DC1 header
and look like DC1=`01` (`RESET`) incorrectly.

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

## Real write (single CD5 only) — proven path

Nozzle **hung (IN)**. Mode `LISTEN_ONLY`. Advance `--sequence` after each accepted
active DATA write.

```bash
export INTELIPUMP_CONTROLLER__MODE=LISTEN_ONLY

./venv/bin/intelipump-real-wayne-price-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --sequence 0 \
  --response-timeout-ms 500 \
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

Wire example (sequence 0):

```text
50 30 05 06 00 11 75 00 11 75 da c7 03 fa
```

Post-write verify defaults: settle **1000 ms**, up to **16** status polls, until
DC1 is `FILLING_COMPLETE`. If the tool still `FAULT`s with ACK but later poll
shows DC1 `5`, treat CD5 as succeeded — do not re-TX the same sequence.

## Sequence nibble (important)

Each accepted active DATA write must use the **next** L2 sequence (`0..F`, wrap).
Reusing the prior sequence is treated as a **retransmission**: the pump may ACK
again without applying a new application command.

| Step | Example `--sequence` | Control byte |
|------|----------------------|--------------|
| CD5 price | `0` | `0x30` |
| Next active (if any) | `1` | `0x31` |
| … | … | … |

Pump status DATA often appears as `50 32 …` (slave TX# independent per spec).

## CD1 RESET — tool available, transition unproven on this lab pump

Requires DC1 `FILLING_COMPLETE`. Documented to clear CLOSED → DC1 `RESET`.
**Lab: not observed** (ACK without DC1 `1`, or ACK timeout).

```bash
export INTELIPUMP_CONTROLLER__MODE=LISTEN_ONLY

./venv/bin/intelipump-real-wayne-reset-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --sequence 1 \
  --response-timeout-ms 500 \
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

Success would be `RESET_VERIFIED` / DC1 `1`. If ACK and DC1 stays `5`, **stop** —
do not spam sequences.

## CD2 + RESET combined block — also unproven here

Documented alternate flow (allowed nozzles + RESET in one DATA block). Tool
**refuses TX unless software sees nozzle OUT**. Lab: with nozzle OUT, ACK without
DC1 `1`.

```bash
export INTELIPUMP_CONTROLLER__MODE=LISTEN_ONLY

./venv/bin/intelipump-real-wayne-cd2-reset-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --sequence N \
  --allowed-nozzle 1 \
  --allowed-nozzle 2 \
  --response-timeout-ms 500 \
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

## CD1 RETURN_STATUS then RESET — capture-matched theory (unproven on lab)

Office merged captures show working controllers do **CD1 RETURN_STATUS**
(`01 01 00`) then lone **CD1 RESET** (`01 01 05`), with DC1 → `RESET` within
~50 ms. Successful examples were on wire **`0x51` (address 2)**. Try address
1 and 2. Nozzle OUT preferred (technician confirm).

`--sequence N` = RETURN_STATUS; RESET uses **`N+1`**.

```bash
export INTELIPUMP_CONTROLLER__MODE=LISTEN_ONLY

./venv/bin/intelipump-real-wayne-return-status-reset-write \
  --port /dev/intelipump-controller \
  --address 2 \
  --sequence N \
  --response-timeout-ms 500 \
  --evidence-dir data/bench/real-wayne/cd1-return-status-reset \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-no-product-connected \
  --confirm-motor-isolated \
  --confirm-valves-isolated \
  --confirm-emergency-isolation-ready \
  --confirm-authorization-disabled \
  --confirm-single-write-plan-reviewed \
  --confirm-nozzle-out-observed \
  --confirm-execute-cd1-return-status-and-reset \
  --confirm-post-write-status-verification-required \
  --i-understand-this-transmits-to-owned-lab-pump
```

Success = `RESET_VERIFIED` / DC1 `1`. If ACK and DC1 stays `5`, **stop**.

## CD1 AUTHORIZE — blocked until RESET proven

High-risk. Requires DC1 `RESET` first. Do **not** run from `FILLING_COMPLETED`.

```bash
./venv/bin/intelipump-real-wayne-authorize-write \
  --port /dev/intelipump-controller \
  --address 1 \
  --sequence N \
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

## Safety model

- Status polls always allowed.
- Active DATA frame allowed only after
  `authorize_single_active_write(exact_frame, kind=...)` and only once.
- Kinds: `CD5_PRICE`, `CD1_RETURN_STATUS`, `CD1_RESET`, `CD1_AUTHORIZE`,
  `CD2_AND_CD1_RESET`, `CD101_REQUEST_TOTALS`.
- RETURN_STATUS→RESET tool uses two sequential single-shot approvals.
- No raw-hex / replay / generic send path.
- Tools do not auto-chain CD5 → RESET → AUTHORIZE.
- Stop `intelipump.service` before bench/active tools; restart when finished.

## Open questions

1. Why this firmware ACKs CD1 RESET / CD2+RESET without DC1 → `RESET` (when OUT was present).
2. Whether RETURN_STATUS before RESET (office capture path) clears CLOSED here.
3. Exact conditions for reliable **NOZIO OUT** on addr2 after CD101 (timing, hose, dual vs single poll).
4. Exact timing bounds for CD5 → DC1 `5` (lab saw multi-second lag; use next sequence after RS burns nibbles).
5. Whether a poll-until-OUT then single CD2+RESET tool would clear CLOSED on this head.
