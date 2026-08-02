import serial
import threading
import queue
import struct
import time
import logging
from typing import Optional, Dict
from src.protocol.crc import calculate_crc16_dart

class TransportDisconnectedError(Exception):
    pass

class Level2Frame:
    def __init__(self, address: int, control: int, payload: bytes, is_short: bool,
                 crc_valid: bool = True, first_byte_time: float = 0.0, last_byte_time: float = 0.0):
        self.address = address
        self.control = control
        self.payload = payload
        self.is_short = is_short
        self.crc_valid = crc_valid
        self.first_byte_time = first_byte_time or time.monotonic()
        self.last_byte_time = last_byte_time or time.monotonic()

class CentralFrameDispatcher:
    """Routes decoded frames to address-specific queues."""
    
    def __init__(self, allowed_addresses: list[int]):
        self.allowed_addresses = set(allowed_addresses)
        self.queues: Dict[int, queue.Queue] = {addr: queue.Queue() for addr in allowed_addresses}

    def dispatch(self, frame: Level2Frame):
        if frame.address in self.queues:
            self.queues[frame.address].put(frame)

class DARTSerialTransport:
    """Thread-safe RS-485 Transport with First-Byte Timestamps, Sliding Window Recovery, and Differentiated Delays."""

    def __init__(self, port: str, baudrate: int = 9600, parity: str = "ODD",
                 bytesize: int = 8, stopbits: int = 1, read_timeout: float = 0.05,
                 quiet_gap_timeout: float = 0.015, tx_delay_sec: float = 0.035,
                 ack_delay_sec: float = 0.005, max_buffer_size: int = 512):
        
        if parity not in ["ODD", "NONE", "EVEN"]:
            raise ValueError(f"Unsupported serial parity configured: '{parity}'. Allowed: ODD, EVEN, NONE.")
            
        self.port = port
        self.baudrate = baudrate
        if parity == "ODD":
            self.parity = serial.PARITY_ODD
        elif parity == "EVEN":
            self.parity = serial.PARITY_EVEN
        else:
            self.parity = serial.PARITY_NONE

        self.bytesize = bytesize
        self.stopbits = stopbits
        self.read_timeout = read_timeout
        self.quiet_gap_timeout = quiet_gap_timeout
        self.tx_delay_sec = tx_delay_sec
        self.ack_delay_sec = ack_delay_sec
        self.max_buffer_size = max_buffer_size
        
        self.ser: Optional[serial.Serial] = None
        self.running = False
        self.intentional_shutdown = False
        self.dispatcher: Optional[CentralFrameDispatcher] = None
        
        self.buffer = bytearray()
        self.buffer_byte_times = []
        self.rx_error_count = 0
        self.metrics = {"crc_candidate_failure_count": 0, "discarded_byte_count": 0, "quiet_gap_discard_count": 0}
        self._lock = threading.Lock()

    def connect(self, dispatcher: CentralFrameDispatcher):
        self.dispatcher = dispatcher
        try:
            self.ser = serial.Serial(
                port=self.port, baudrate=self.baudrate, parity=self.parity,
                bytesize=self.bytesize, stopbits=self.stopbits, timeout=self.read_timeout
            )
            self.running = True
            self.intentional_shutdown = False
            self.rx_thread = threading.Thread(target=self._rx_worker, daemon=True)
            self.rx_thread.start()
            logging.info(f"Connected to DART RS-485 bus on {self.port}")
        except Exception as e:
            logging.critical(f"Failed to open serial port {self.port}: {e}")
            raise e

    def _rx_worker(self):
        """Continuous stream accumulator and frame parser loop."""
        while self.running:
            try:
                waiting = self.ser.in_waiting

                if waiting > 0:
                    data = self.ser.read(min(waiting, 64))
                else:
                    data = self.ser.read(1)
                now = time.monotonic()

                if data:
                    self.buffer.extend(data)
                    self.buffer_byte_times.extend([now] * len(data))
                    self.rx_error_count = 0
                    
                    if len(self.buffer) > self.max_buffer_size:
                        overflow_bytes = len(self.buffer) - self.max_buffer_size
                        del self.buffer[:overflow_bytes]
                        del self.buffer_byte_times[:overflow_bytes]
                        self.metrics["discarded_byte_count"] += overflow_bytes
                        logging.warning(f"[STREAM] Buffer overflow. Shifted {overflow_bytes} bytes.")
                        
                    self._parse_buffer()
                else:
                    if self.buffer and (now - self.buffer_byte_times[-1] > self.quiet_gap_timeout):
                        discarded = len(self.buffer)
                        leading_hex = self.buffer[:8].hex()
                        duration = now - self.buffer_byte_times[0]
                        self.metrics["discarded_byte_count"] += discarded
                        self.metrics["quiet_gap_discard_count"] += 1
                        logging.debug(f"[STREAM] Quiet gap timeout ({self.quiet_gap_timeout*1000:.1f}ms). "
                                      f"Discarded {discarded} bytes (held {duration*1000:.1f}ms), leading: 0x{leading_hex}")
                        self.buffer.clear()
                        self.buffer_byte_times.clear()

            except serial.SerialException as e:
                if not self.intentional_shutdown:
                    logging.error(f"Serial disconnected unexpectedly: {e}")
                self.running = False
                break
            except Exception as e:
                self.rx_error_count += 1
                logging.error(f"RX Error ({self.rx_error_count}/10): {e}")
                if self.rx_error_count >= 10:
                    logging.critical("Too many consecutive RX failures. Halting transport.")
                    self.running = False
                    break
                time.sleep(0.01)

    def _parse_buffer(self):
        """Candidate-scanning frame assembler with first-byte timestamp propagation."""
        while len(self.buffer) >= 3:
            addr = self.buffer[0]
            
            if not (0x50 <= addr <= 0x6F):
                self.buffer.pop(0)
                self.buffer_byte_times.pop(0)
                self.metrics["discarded_byte_count"] += 1
                continue

            ctrl = self.buffer[1]

            is_valid_short_ctrl = (ctrl == 0x20 or ctrl == 0x70 or (ctrl & 0xF0) == 0xC0)
            if is_valid_short_ctrl and self.buffer[2] == 0xFA:
                frame = Level2Frame(
                    addr, ctrl, payload=b"", is_short=True,
                    first_byte_time=self.buffer_byte_times[0], last_byte_time=self.buffer_byte_times[2]
                )
                if self.dispatcher:
                    self.dispatcher.dispatch(frame)
                del self.buffer[:3]
                del self.buffer_byte_times[:3]
                continue

            candidate_found = False
            search_start = 2
            
            while True:
                idx = self.buffer.find(b"\x03\xFA", search_start)
                if idx == -1:
                    break

                frame_bytes = bytes(self.buffer[: idx + 2])
                if len(frame_bytes) >= 6:
                    body = frame_bytes[:-4]
                    crc_rx = struct.unpack("<H", frame_bytes[-4:-2])[0]
                    
                    if calculate_crc16_dart(body) == crc_rx:
                        frame = Level2Frame(
                            address=addr, control=ctrl, payload=body[2:],
                            is_short=False, crc_valid=True,
                            first_byte_time=self.buffer_byte_times[0],
                            last_byte_time=self.buffer_byte_times[idx + 1]
                        )
                        if self.dispatcher:
                            self.dispatcher.dispatch(frame)
                        del self.buffer[: idx + 2]
                        del self.buffer_byte_times[: idx + 2]
                        candidate_found = True
                        break
                    else:
                        self.metrics["crc_candidate_failure_count"] += 1

                search_start = idx + 1

            if not candidate_found:
                break

    def write_frame(self, frame: bytes, is_ack: bool = False) -> float:
        """Writes frame with differentiated turnaround delays and write-completeness verification."""
        with self._lock:
            if not self.running or not self.ser or not self.ser.is_open:
                raise TransportDisconnectedError("Cannot write frame: Transport disconnected.")
            
            delay = self.ack_delay_sec if is_ack else self.tx_delay_sec
            if delay > 0:
                time.sleep(delay)

            tx_start_time = time.monotonic()
            written = self.ser.write(frame)
            self.ser.flush()
            if written != len(frame):
                raise TransportDisconnectedError(f"Incomplete write: {written}/{len(frame)} bytes sent.")
            return tx_start_time

    def close(self):
        self.intentional_shutdown = True
        self.running = False
        if self.ser and self.ser.is_open:
            self.ser.close()
        if hasattr(self, 'rx_thread') and self.rx_thread.is_alive():
            self.rx_thread.join(timeout=1.0)
        logging.info("Transport cleanly shut down.")