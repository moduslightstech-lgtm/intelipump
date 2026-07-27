# Wayne price protocol hypothesis

Companion to `docs/real-wayne-price-programming-review.md`.

## Confirmed (documentation)

| Item | Value |
|------|-------|
| CD5 | Price Update |
| TRANS | `0x05` |
| LNG | `3 × N` nozzle prices |
| Price encoding | 3-byte packed BCD MSB first |
| PRI1 / PRI2 | Logical nozzles 1 / 2 |
| Zeroized pump | Remains `PUMP_NOT_PROGRAMMED` until price received |
| After valid price | Documented → `FILLING_COMPLETE` |
| RESET / AUTHORIZE | Separate transactions |

## Confirmed (lab capture)

| Item | Value |
|------|-------|
| Wire | `0x50` |
| DC1 | `PUMP_NOT_PROGRAMMED` |
| DC2 | volume/amount zero |
| Poll transport | PermanentSerialReader; poll-only writes |

## Two-nozzle example (1175 / 1175)

```
05 06 00 11 75 00 11 75
```

Outer frame (sequence 0, CRC computed — not hard-coded):

```
50 30 | 05 06 00 11 75 00 11 75 | CRC_LE | 03 FA
```

## Legacy / hypothesis-only

Earlier two-byte BCD (`11 75`) and merged-capture `3x/Cx` pairing remain
analysis aids only (`encode_price_bcd_2_legacy`). **Documented CD5 construction
always uses three-byte BCD.**

## Unproven

- Direction in ONE_PORT_MERGED captures
- Whether Cx is ACK for CD5
- Full initialization prerequisites beyond CD5
- Production timing / retry
