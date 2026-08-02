import struct
import time
import queue
import threading
import logging
from enum import Enum
from typing import List, Dict, Optional
from src.driver.serial_transport import DARTSerialTransport, Level2Frame, TransportDisconnectedError
from src.protocol.crc import calculate_crc16_dart
from src.protocol.bcd import int_to_bcd
from src.protocol.parser import DARTTransactionParser, FrameDirection
from src.core.pump_state import PumpState, SaleLifecycle

class ExchangeResult(Enum):
    LINK_ACKNOWLEDGED = 1
    APPLICATION_CONFIRMED = 2
    TIMED_OUT = 3
    REJECTED = 4

class WayneDartMaster:
    """Master controller managing address queues, exchange correlation, and two-phase state confirmation."""

    def __init__(self, transport: DARTSerialTransport, max_retries: int = 3, 
                 price_format: str = "RAW_BCD_DIGITS", offline_threshold: int = 5,
                 currency_symbol: str = "NGN", configured_nozzles: int = 4,
                 passive_monitoring_mode: bool = False):
        self.transport = transport
        self.max_retries = max_retries
        self.price_format = price_format
        self.offline_threshold = offline_threshold
        self.currency_symbol = currency_symbol
        self.configured_nozzles = configured_nozzles
        self.passive_monitoring_mode = passive_monitoring_mode
        
        self.pumps: Dict[int, PumpState] = {}
        self.locks: Dict[int, threading.Lock] = {}
        self.metrics = {
            "stale_frame_count": 0,
            "missed_poll_count": 0,
            "invalid_bcd_count": 0,
            "invalid_nozzle_count": 0,
            "unrecognized_status_count": 0,
            "truncated_payload_count": 0
        }

    def register_pump(self, addr_hex: int):
        self.pumps[addr_hex] = PumpState(addr_hex)
        self.locks[addr_hex] = threading.Lock()

    def poll_pump(self, addr: int) -> List[Dict]:
        """Executes full multi-frame poll exchange until EOT or deadline timeout."""
        if addr not in self.pumps or not self.transport.dispatcher:
            return []

        with self.locks[addr]:
            pump = self.pumps[addr]
            pump_queue = self.transport.dispatcher.queues[addr]

            poll_frame = bytes([addr, 0x20, 0xFA])
            
            try:
                # Capture actual timestamp from write_frame execution
                poll_write_time = self.transport.write_frame(poll_frame, is_ack=False)
            except TransportDisconnectedError:
                pump.online = False
                return []

            events = []
            valid_bus_response_seen = False
            deadline = poll_write_time + 0.12

            while time.monotonic() - poll_write_time < 0.12:  # Expanded response window for bench measuring
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break

                try:
                    frame: Level2Frame = pump_queue.get(timeout=min(0.01, max(remaining, 0.001)))

                    if frame.first_byte_time < poll_write_time:
                        self.metrics["stale_frame_count"] += 1
                        self._decode_and_process(pump, frame)
                        continue

                    if frame.is_short:
                        if frame.control == 0x70:  # EOT Turnaround = Pump Alive & Online
                            valid_bus_response_seen = True
                            break
                        continue

                    # Log Passive Timing Delays for Field Calibration
                    t_first_byte = (frame.first_byte_time - poll_write_time) * 1000.0
                    t_last_byte = (frame.last_byte_time - poll_write_time) * 1000.0
                    logging.info(f"[TIMING {hex(addr)}] Poll Out -> First Byte: {t_first_byte:.2f}ms | Last Byte: {t_last_byte:.2f}ms")

                    recognized_data_found = self._decode_and_process(pump, frame)
                    if recognized_data_found:
                        valid_bus_response_seen = True
                        
                        ack_ctrl = 0xC0 | (frame.control & 0x0F)
                        try:
                            self.transport.write_frame(bytes([addr, ack_ctrl, 0xFA]), is_ack=True)
                        except TransportDisconnectedError:
                            pump.online = False
                            return []

                        while not pump.event_queue.empty():
                            events.append(pump.event_queue.get())

                except queue.Empty:
                    continue

            # Offline / Synchronization Tracking
            if valid_bus_response_seen:
                pump.online = True
                pump.consecutive_missed_polls = 0
            else:
                pump.consecutive_missed_polls += 1
                self.metrics["missed_poll_count"] += 1
                if pump.consecutive_missed_polls >= self.offline_threshold:
                    if pump.online:
                        logging.warning(f"[OFFLINE] Pump {hex(addr)} marked OFFLINE after {self.offline_threshold} missed polls.")
                    pump.online = False
                    pump.synchronized = False

            return events

    def _decode_and_process(self, pump: PumpState, frame: Level2Frame) -> bool:
        try:
            txs = DARTTransactionParser.parse_payload(
                frame.payload, FrameDirection.PUMP_TO_CONTROLLER,
                configured_nozzles=self.configured_nozzles,
                metrics=self.metrics
            )
            recognized_data_found = False
            rx_time = frame.last_byte_time

            for tx in txs:
                if tx.get("type") == "DC1_STATUS":
                    recognized_data_found = True
                    if pump.update_status(tx["status_label"], rx_time):
                        pump.event_queue.put({"type": "STATUS_CHANGE", "address": pump.address, "status": pump.observed_status})

                elif tx.get("type") == "DC3_NOZZLE_PRICE":
                    recognized_data_found = True
                    if pump.update_nozzle(tx["nozzle_state"], tx["logical_nozzle"], rx_time):
                        pump.event_queue.put({
                            "type": "NOZZLE_TRANSITION", "address": pump.address,
                            "state": pump.nozzle_position, "nozzle": pump.logical_nozzle
                        })

                elif tx.get("type") == "DC2_VOLUME_AMOUNT":
                    recognized_data_found = True
                    pump.filled_volume = tx["volume_raw"] / 100.0
                    pump.filled_amount = tx["amount_raw"] / 100.0

            return recognized_data_found
        except ValueError as e:
            logging.warning(f"Payload parse error from {hex(pump.address)}: {e}")
            return False

    def send_transaction(self, addr: int, payload: bytes) -> ExchangeResult:
        if self.passive_monitoring_mode:
            logging.info(f"[PASSIVE MODE BLOCKED] Suppressed active command payload to {hex(addr)}: {payload.hex()}")
            return ExchangeResult.REJECTED

        if addr not in self.pumps or not self.transport.dispatcher:
            return ExchangeResult.REJECTED

        with self.locks[addr]:
            pump = self.pumps[addr]
            pump_queue = self.transport.dispatcher.queues[addr]
            ctrl = pump.get_reserved_seq_ctrl()

            body = bytes([addr, ctrl]) + payload
            crc = calculate_crc16_dart(body)
            frame = body + struct.pack("<H", crc) + b"\x03\xFA"

            for attempt in range(self.max_retries):
                try:
                    cmd_write_time = self.transport.write_frame(frame, is_ack=False)
                    pump.last_command_time = cmd_write_time
                except TransportDisconnectedError:
                    return ExchangeResult.REJECTED

                while time.monotonic() - cmd_write_time < 0.12:
                    try:
                        rx: Level2Frame = pump_queue.get(timeout=0.01)

                        if rx.first_byte_time < cmd_write_time:
                            self.metrics["stale_frame_count"] += 1
                            self._decode_and_process(pump, rx)
                            continue

                        if not rx.is_short:
                            if self._decode_and_process(pump, rx):
                                try:
                                    ack_ctrl = 0xC0 | (rx.control & 0x0F)
                                    self.transport.write_frame(bytes([addr, ack_ctrl, 0xFA]), is_ack=True)
                                except TransportDisconnectedError:
                                    return ExchangeResult.REJECTED
                            continue

                        if rx.is_short and (rx.control & 0xF0) == 0xC0:
                            if (rx.control & 0x0F) == (ctrl & 0x0F):
                                pump.last_ack_time = time.monotonic()
                                pump.advance_seq_ctrl()
                                return ExchangeResult.LINK_ACKNOWLEDGED

                    except queue.Empty:
                        continue

                time.sleep(0.04)

            logging.warning(f"Transaction to {hex(addr)} timed out after {self.max_retries} attempts.")
            return ExchangeResult.TIMED_OUT

    def set_unit_prices(self, addr: int, prices: List[int]) -> ExchangeResult:
        data = bytearray()
        for p in prices:
            if self.price_format == "RAW_BCD_DIGITS":
                data.extend(int_to_bcd(p, 3))
            elif self.price_format == "CURRENCY_CENTS":
                data.extend(int_to_bcd(int(p), 3))
            elif self.price_format == "MAJOR_UNITS":
                data.extend(int_to_bcd(int(round(p * 100)), 3))
            else:
                raise ValueError(f"Unsupported price_format: {self.price_format}")

        return self.send_transaction(addr, bytes([0x05, len(data)]) + data)

    def reset(self, addr: int, confirm_application: bool = False) -> ExchangeResult:
        res = self.send_transaction(addr, b"\x01\x01\x05")
        if res != ExchangeResult.LINK_ACKNOWLEDGED:
            return res

        if not confirm_application:
            return ExchangeResult.LINK_ACKNOWLEDGED

        pump = self.pumps[addr]
        cmd_time = pump.last_command_time
        start_wait = time.monotonic()
        
        while time.monotonic() - start_wait < 0.6:
            self.poll_pump(addr)
            # Require status to be RESET AND observed AFTER command execution timestamp
            if pump.observed_status == "RESET" and pump.last_status_time > cmd_time:
                return ExchangeResult.APPLICATION_CONFIRMED
            time.sleep(0.04)

        return ExchangeResult.LINK_ACKNOWLEDGED

    def authorize(self, addr: int, allowed_nozzles: Optional[List[int]] = None, confirm_application: bool = False) -> ExchangeResult:
        if allowed_nozzles is None:
            allowed_nozzles = [1]

        pump = self.pumps[addr]
        if pump.authorization_pending:
            logging.warning(f"Authorization already pending for pump {hex(addr)}. Skipping duplicate.")
            return ExchangeResult.REJECTED

        pump.authorization_pending = True

        try:
            cd2_data = bytes([0x02, len(allowed_nozzles)] + allowed_nozzles)
            if self.send_transaction(addr, cd2_data) != ExchangeResult.LINK_ACKNOWLEDGED:
                logging.error(f"CD2 Allowed Nozzles failed for {hex(addr)}. Aborting authorization.")
                return ExchangeResult.REJECTED

            res = self.send_transaction(addr, b"\x01\x01\x06")
            if res != ExchangeResult.LINK_ACKNOWLEDGED:
                return ExchangeResult.REJECTED

            if not confirm_application:
                return ExchangeResult.LINK_ACKNOWLEDGED

            cmd_time = pump.last_command_time
            start_wait = time.monotonic()
            
            while time.monotonic() - start_wait < 0.6:
                self.poll_pump(addr)
                # Require status to be AUTHORIZED AND observed AFTER command execution timestamp
                if pump.observed_status == "AUTHORIZED" and pump.last_status_time > cmd_time:
                    return ExchangeResult.APPLICATION_CONFIRMED
                time.sleep(0.04)

            return ExchangeResult.LINK_ACKNOWLEDGED
        finally:
            pump.authorization_pending = False