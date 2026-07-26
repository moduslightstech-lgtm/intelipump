# DART Line Protocol Summary

Verify every item against the authorized source document **and** the captured
ePump / Wayne iGEM traffic for the owned lab profile.

## Captured Wayne iGEM / ePump profile (source of truth for status poll)

Passive merged-bus capture is the source of truth for this profile:

- Logical side/address **1** maps to wire ADR **`0x50`**
- Logical side/address **2** maps to wire ADR **`0x51`**
- Captured status polls are exactly **`50 20 FA`** and **`51 20 FA`**
- Synthetic `01 20 FA` is **invalid** for captured Wayne iGEM V-11.06
- Direction on the passive merged capture is **inferred only**
  (`INFERRED_CONTROLLER_TO_PUMP` / `INFERRED_PUMP_TO_CONTROLLER` /
  `UNKNOWN_DIRECTION`) — never treated as measured
- `C0`–`CF` short frames are classified cautiously as
  `SEQUENCE_CONTROL_OR_ACK` with `sequenceNibble` and
  `possibleAcknowledgement=true` — **not** nozzle lift, authorization, or sale
  state
- Payload field meanings remain **unknown** (`UNKNOWN_PAYLOAD`) until
  correlated with controlled pump events
- This profile does **not** authorize replay of non-poll frames

`build_poll(logical)` encodes only logical 1/2 via the immutable table
`LEGACY_IGEM_WIRE_ADDRESSES`.

```text
Serial: 9600 or 19200 baud
Format: 8 data bits, odd parity, 1 stop bit
Mode: half-duplex
Master: controller
Slave: dispenser

ETX = 0x03
DLE = 0x10
SF  = 0xFA

POLL    = 0x20
DATA    = 0x30-0x3F
IAP     = 0x40
NAK     = 0x50-0x5F   # control high-nibble; distinct from wire ADR 0x50/0x51
EOT     = 0x70-0x7F   # captured short control class SHORT_CONTROL_70
ACK     = 0xC0-0xCF   # captured as SEQUENCE_CONTROL_OR_ACK (semantics unproven)
ACKPOLL = 0xE0-0xEF

DATA:
ADR + CTRL + DATA... + CRC_LOW + CRC_HIGH + ETX + SF

CRC:
CRC-16 (capture-proven dart_ibm_ansi_init_0000 / dart_crc16)
initial value 0x0000
calculated from ADR through final unescaped data byte
inserted DLE bytes excluded

Response deadline: 25 ms
```

Prove the exact CRC bit-processing convention with captured traffic before transmission.
