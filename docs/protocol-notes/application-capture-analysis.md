# Phase 3 Application Capture Analysis

Source of truth: WAYNE EUROPE - Protocol Specification Dart Pump Interface
Revision 2.11 (WM041550 Rev 02).

Captures are `ONE_PORT_MERGED` / `MERGED_BUS`; direction is UNKNOWN unless
payload structure uniquely selects a CD or DC layout.

## Inventory

- DATA frames analyzed: **2098**
- Payloads with trailing undecoded bytes: **0**
- Malformed known transactions: **0**

## Transaction ID frequency (after TRANS+LNG split)

| Wire TRANS | Count |
|---|---|
| `0x01` | 812 |
| `0x02` | 94 |
| `0x03` | 444 |
| `0x05` | 182 |
| `0x65` | 1031 |

## Logical type counts (decoder dispatch)

| Type | Count | Typical LNG |
|---|---|---|
| `AMBIGUOUS_CD1_OR_DC1` | 812 | 1:812 |
| `DC101_TOTAL_COUNTERS` | 517 | 16:517 |
| `CD101_REQUEST_TOTALS` | 514 | 1:514 |
| `DC3_NOZZLE_STATUS_PRICE` | 444 | 4:444 |
| `CD5_PRICE_UPDATE` | 182 | 3:182 |
| `DC2_FILLED_VOLUME_AMOUNT` | 94 | 8:94 |

## Boundary rule

Each application transaction is `TRANS (1) + LNG (1) + DATA (LNG)`.
Multiple transactions may appear in one DATA payload (Pump Interface, page 10).

## Representative samples

### CD101_REQUEST_TOTALS

- raw: `65 01 01`
  - direction: `MASTER_TO_SLAVE` status: `DECODED`
  - body: `{"counter_select": 1, "spec_ref": "Pump Interface Rev 2.11, page 19, CD101"}`
- raw: `65 01 01`
  - direction: `MASTER_TO_SLAVE` status: `DECODED`
  - body: `{"counter_select": 1, "spec_ref": "Pump Interface Rev 2.11, page 19, CD101"}`

### DC2_FILLED_VOLUME_AMOUNT

- raw: `02 08 00 00 00 00 00 00 00 00`
  - direction: `SLAVE_TO_MASTER` status: `DECODED`
  - body: `{"amount": {"decimals": null, "raw_bcd_hex": "00 00 00 00", "raw_scaled": 0, "value": null}, "spec_ref": "Pump Interface Rev 2.11, page 20, DC2", "volume": {"decimals": null, "raw_bcd_hex": "00 00 00 00", "raw_scaled": 0, "value": null}}`
  - warnings: ['Interpreted TRANS 0x02 LNG=8 as DC2 (VOL+AMO). CD2 shares TRANS=0x02 but has a variable nozzle-list layout (Pump Interface pp. 14 and 20).', 'Volume/amount Decimal values omitted: pump decimal parameters (DC7 DPVOL/DPAMO) were not provided.']
- raw: `02 08 00 00 00 00 00 00 00 00`
  - direction: `SLAVE_TO_MASTER` status: `DECODED`
  - body: `{"amount": {"decimals": null, "raw_bcd_hex": "00 00 00 00", "raw_scaled": 0, "value": null}, "spec_ref": "Pump Interface Rev 2.11, page 20, DC2", "volume": {"decimals": null, "raw_bcd_hex": "00 00 00 00", "raw_scaled": 0, "value": null}}`
  - warnings: ['Interpreted TRANS 0x02 LNG=8 as DC2 (VOL+AMO). CD2 shares TRANS=0x02 but has a variable nozzle-list layout (Pump Interface pp. 14 and 20).', 'Volume/amount Decimal values omitted: pump decimal parameters (DC7 DPVOL/DPAMO) were not provided.']

### DC3_NOZZLE_STATUS_PRICE

- raw: `03 04 00 11 75 01`
  - direction: `SLAVE_TO_MASTER` status: `PARTIAL`
  - body: `{"alternate_cd3_volume_view": {"decimals": null, "raw_bcd_hex": "00 11 75 01", "raw_scaled": 117501, "value": null}, "nozio_raw": 1, "nozzle_out": false, "price": {"decimals": null, "raw_bcd_hex": "00 11 75", "raw_scaled": 1175, "value": null}, "selected_logical_nozzle": 1, "spec_ref": "Pump Interface Rev 2.11, page 21, DC3"}`
  - warnings: ['Interpreted TRANS 0x03 LNG=4 as DC3 (PRI+NOZIO). CD3 preset volume shares this wire size (Pump Interface pp. 14 and 21).', 'Price Decimal omitted: pump unit-price decimals (DC7 DPUNP) were not provided.']
- raw: `03 04 00 11 75 01`
  - direction: `SLAVE_TO_MASTER` status: `PARTIAL`
  - body: `{"alternate_cd3_volume_view": {"decimals": null, "raw_bcd_hex": "00 11 75 01", "raw_scaled": 117501, "value": null}, "nozio_raw": 1, "nozzle_out": false, "price": {"decimals": null, "raw_bcd_hex": "00 11 75", "raw_scaled": 1175, "value": null}, "selected_logical_nozzle": 1, "spec_ref": "Pump Interface Rev 2.11, page 21, DC3"}`
  - warnings: ['Interpreted TRANS 0x03 LNG=4 as DC3 (PRI+NOZIO). CD3 preset volume shares this wire size (Pump Interface pp. 14 and 21).', 'Price Decimal omitted: pump unit-price decimals (DC7 DPUNP) were not provided.']

### AMBIGUOUS_CD1_OR_DC1

- raw: `01 01 05`
  - direction: `UNKNOWN` status: `PARTIAL`
  - body: `{"cd1_command": {"dcc": 5, "known": true, "name": "RESET"}, "dc1_pump_status": {"description": "FILLING COMPLETED", "known": true, "name": "FILLING_COMPLETED"}, "raw_code": 5}`
  - warnings: ['TRANS 0x01 LNG=1 is ambiguous: CD1 (command) and DC1 (pump status) share this wire encoding (Pump Interface Rev 2.11, pages 13 and 20). Merged-bus captures cannot distinguish direction; both interpretations are preserved.']
- raw: `01 01 00`
  - direction: `UNKNOWN` status: `PARTIAL`
  - body: `{"cd1_command": {"dcc": 0, "known": true, "name": "RETURN_STATUS"}, "dc1_pump_status": {"description": "PUMP NOT PROGRAMMED", "known": true, "name": "PUMP_NOT_PROGRAMMED"}, "raw_code": 0}`
  - warnings: ['TRANS 0x01 LNG=1 is ambiguous: CD1 (command) and DC1 (pump status) share this wire encoding (Pump Interface Rev 2.11, pages 13 and 20). Merged-bus captures cannot distinguish direction; both interpretations are preserved.']

### DC101_TOTAL_COUNTERS

- raw: `65 10 01 00 00 00 00 02 00 00 00 00 02 00 00 00 00 00`
  - direction: `SLAVE_TO_MASTER` status: `DECODED`
  - body: `{"counter_select": 1, "raw_scaled": {"total_meter1_or_nofill": 2, "total_meter2": 0, "total_value": 2}, "spec_ref": "Pump Interface Rev 2.11, page 25, DC101", "total_meter1_or_nofill": {"decimals": null, "raw_bcd_hex": "00 00 00 00 02", "raw_scaled": 2, "value": null}, "total_meter2": {"decimals": null, "raw_bcd_hex": "00 00 00 00 00", "raw_scaled": 0, "value": null}, "total_value": {"decimals": null, "raw_bcd_hex": "00 00 00 00 02", "raw_scaled": 2, "value": null}}`
  - warnings: ['DC101 Decimal presentation omitted: decimal parameters unknown and/or COUN meaning does not select a provided scale.']
- raw: `65 10 01 00 00 00 00 02 00 00 00 00 02 00 00 00 00 00`
  - direction: `SLAVE_TO_MASTER` status: `DECODED`
  - body: `{"counter_select": 1, "raw_scaled": {"total_meter1_or_nofill": 2, "total_meter2": 0, "total_value": 2}, "spec_ref": "Pump Interface Rev 2.11, page 25, DC101", "total_meter1_or_nofill": {"decimals": null, "raw_bcd_hex": "00 00 00 00 02", "raw_scaled": 2, "value": null}, "total_meter2": {"decimals": null, "raw_bcd_hex": "00 00 00 00 00", "raw_scaled": 0, "value": null}, "total_value": {"decimals": null, "raw_bcd_hex": "00 00 00 00 02", "raw_scaled": 2, "value": null}}`
  - warnings: ['DC101 Decimal presentation omitted: decimal parameters unknown and/or COUN meaning does not select a provided scale.']

### CD5_PRICE_UPDATE

- raw: `05 03 00 11 75`
  - direction: `MASTER_TO_SLAVE` status: `DECODED`
  - body: `{"prices": [{"logical_nozzle": 1, "price": {"decimals": null, "raw_bcd_hex": "00 11 75", "raw_scaled": 1175, "value": null}}], "spec_ref": "Pump Interface Rev 2.11, page 15, CD5"}`
  - warnings: ['Price Decimal omitted: pump unit-price decimals (DC7 DPUNP) were not provided.']
- raw: `05 03 00 11 75`
  - direction: `MASTER_TO_SLAVE` status: `DECODED`
  - body: `{"prices": [{"logical_nozzle": 1, "price": {"decimals": null, "raw_bcd_hex": "00 11 75", "raw_scaled": 1175, "value": null}}], "spec_ref": "Pump Interface Rev 2.11, page 15, CD5"}`
  - warnings: ['Price Decimal omitted: pump unit-price decimals (DC7 DPUNP) were not provided.']

## Likely request/response pairs (inference only)

Merged-bus ordering is not authoritative. Observed co-occurrence patterns:

- `CD101` (`65 01 ..`) often near `DC101` (`65 10 ..`) — totals request/response.
- `CD1/DC1` (`01 01 ..`) often near `DC3` (`03 04 ..`) — status / nozzle+price.
- `CD5` (`05 03 ..`) price updates appear as standalone controller→pump frames.

These pairs are **inference**, not proven direction-separated evidence.

## Scaling / decimals

DC7 defines `DPVOL` / `DPAMO` / `DPUNP`, but DC7 was **not observed** in these
captures. Decoders therefore expose `raw_scaled` integers and leave
`Decimal` values unset unless the caller supplies decimals.

## Unresolved

1. CD1 vs DC1 on TRANS `0x01` (same LNG=1) without direction.
2. CD3 vs DC3 on TRANS `0x03` LNG=4 — prefer DC3 in passive decode.
3. True money/volume decimal places until DC7 parameters are captured.
