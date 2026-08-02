# RS-485 Office Bench Report

- **date**: 2026-07-23
- **bench_name**: us-office-rs485-01
- **physical_hil_status**: `PASS`
- **decision**: `PASS`
- **pi_model**: Raspberry Pi 400 Rev 1.0
- **os**: Linux 6.6.51+rpt-rpi-v8
- **python_version**: 3.14.6
- **controller_version**: 0.1.0
- **environment**: LAB
- **listen_only**: True
- **active_commands_enabled**: False

## Adapters

- **controller**: `/dev/ttyUSB0` (stable=`1a86:7523-1-1.1`)
- **simulator**: `/dev/serial/by-id/usb-1a86_USB_Serial-if00-port0` (stable=`usb-1a86_USB_Serial-if00-port0`)

## Wiring / termination

- automatic_direction_control: True
- termination_enabled: False
- bias_enabled: False
- ground_reference_connected: True

## Serial settings

- baud: 9600
- parity: ODD
- data_bits: 8
- stop_bits: 1
- protocol_target_ms: 25
- configured_bench_timeout_ms: 100
- duration_s: 300.0
- addresses: [1, 2]

## Counters

- poll_count: 8838
- data_count: 4
- eot_count: 8834
- ack_count: 4
- nak_count: 0
- crc_error_count: 0
- timeout_count: 0
- retry_count: 0
- reconnect_events: 0

## Latency

_Primary series (interval 2: POLL write complete → complete response)_

- count: 8838
- min_ms: 14.770854999369476
- max_ms: 515.8770719990571
- mean_ms: 19.561356834011402
- p50_ms: 17.686956998659298
- p95_ms: 23.242503000801662
- p99_ms: 37.967412999933
- jitter_ms: 18.410063616628705
- timeout_count: 0
- protocol_target_ms: 25.0
- configured_bench_timeout_ms: 100.0

### Separate intervals

- **1. POLL write complete → first response byte**: mean=19.300306684087456 p95=22.940223001569393 count=8838
- **2. POLL write complete → complete response**: mean=19.561356834011402 p95=23.242503000801662 count=8838
- **3. DATA receive complete → ACK write start**: mean=0.04747200046040234 p95=0.05518500074686017 count=4
- **4. ACK write complete → next POLL write start**: mean=31.07355775000542 p95=77.52831499965396 count=4

## Fault results

- (none recorded)

## Capture files

- capture_jsonl: `data2/bench/rs485-bench-003.jsonl`
- sanitized_capture: `data2/bench/rs485-bench-003.sanitized.jsonl`
- evidence_json: `data2/bench/rs485-bench-003.json`

## Notes / unresolved observations

- Phase 10 office bench: adapters A/B only; no Wayne dispenser.
- automatic_direction_control=True
- termination_enabled=False
- bias_enabled=False
- ground_reference_connected=True
- no_command_replay
- no_duplicate_transaction_completion_policy
- latency intervals: write_complete→first_byte, write_complete→complete_response, data→ack_start, ack_complete→next_poll; protocol_target_ms and configured_bench_timeout_ms are not part of samples
- transport_read_timeout_s=0.005 (idle wake only; read(1)+drain); ack_to_next_poll includes intentional inter_poll_delay_ms / idle_sleep_ms
