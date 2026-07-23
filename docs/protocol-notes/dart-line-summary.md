# DART Line Protocol Summary

Verify every item against the authorized source document.

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
NAK     = 0x50-0x5F
EOT     = 0x70-0x7F
ACK     = 0xC0-0xCF
ACKPOLL = 0xE0-0xEF

DATA:
ADR + CTRL + DATA... + CRC_LOW + CRC_HIGH + ETX + SF

CRC:
CRC-16 CCITT
initial value 0x0000
calculated from ADR through final unescaped data byte
inserted DLE bytes excluded

Response deadline: 25 ms
```

Prove the exact CRC bit-processing convention with captured traffic before transmission.
