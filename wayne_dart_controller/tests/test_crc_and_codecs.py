import sys
import os
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.protocol.crc import calculate_crc16_dart
from src.protocol.bcd import int_to_bcd, bcd_to_int
from src.protocol.parser import DARTTransactionParser, FrameDirection
from src.driver.serial_transport import DARTSerialTransport, CentralFrameDispatcher, Level2Frame
from src.core.pump_state import PumpState, SaleLifecycle
from src.core.master_controller import WayneDartMaster, ExchangeResult

class TestDARTProtocolSuite(unittest.TestCase):

    def test_crc_calculation(self):
        data = bytes.fromhex("51 30 01 01 00")
        crc = calculate_crc16_dart(data)
        self.assertEqual(hex(crc), "0x9ca2")

    def test_bcd_validation(self):
        self.assertEqual(int_to_bcd(160, 3), bytes.fromhex("000160"))
        self.assertEqual(bcd_to_int(bytes.fromhex("000160")), 160)
        with self.assertRaises(ValueError):
            bcd_to_int(bytes.fromhex("00016F"))

    def test_direction_classification(self):
        payload = bytes.fromhex("01 01 00")
        p2c = DARTTransactionParser.parse_payload(payload, FrameDirection.PUMP_TO_CONTROLLER)
        self.assertEqual(p2c[0]["type"], "DC1_STATUS")

        c2p = DARTTransactionParser.parse_payload(payload, FrameDirection.CONTROLLER_TO_PUMP)
        self.assertEqual(c2p[0]["type"], "CD1_COMMAND")

    def test_truncated_payload_rejection(self):
        payload = bytes.fromhex("02 08 00 00")
        with self.assertRaises(ValueError):
            DARTTransactionParser.parse_payload(payload, FrameDirection.PUMP_TO_CONTROLLER)

    def test_nozzle_range_validation(self):
        payload = bytes.fromhex("03 04 00 01 60 05")
        parsed = DARTTransactionParser.parse_payload(payload, FrameDirection.PUMP_TO_CONTROLLER, configured_nozzles=4)
        self.assertEqual(parsed[0]["type"], "INVALID_NOZZLE_RANGE")

    def test_nozzle_deduplication_and_unknown_initialization(self):
        pump = PumpState(0x50)
        now = time.monotonic()
        self.assertEqual(pump.nozzle_position, "UNKNOWN")

        res1 = pump.update_nozzle("IN", 1, now)
        self.assertFalse(res1)
        self.assertEqual(pump.nozzle_position, "IN")

        res2 = pump.update_nozzle("IN", 1, now)
        self.assertFalse(res2)

        res3 = pump.update_nozzle("OUT", 1, now)
        self.assertTrue(res3)
        self.assertEqual(pump.sale_lifecycle, SaleLifecycle.NOZZLE_LIFTED)

    def test_completion_latch(self):
        pump = PumpState(0x50)
        now = time.monotonic()
        pump.update_nozzle("OUT", 1, now)
        pump.sale_completion_latch = True

        pump.update_status("RESET", now)
        self.assertTrue(pump.sale_completion_latch)

        pump.update_nozzle("IN", 1, now)
        pump.update_nozzle("OUT", 1, now)
        self.assertFalse(pump.sale_completion_latch)

    def test_central_dispatcher_address_isolation(self):
        dispatcher = CentralFrameDispatcher([0x50, 0x51])
        frame50 = Level2Frame(0x50, 0x30, b"", is_short=True)
        frame51 = Level2Frame(0x51, 0x30, b"", is_short=True)

        dispatcher.dispatch(frame50)
        dispatcher.dispatch(frame51)

        self.assertEqual(dispatcher.queues[0x50].qsize(), 1)
        self.assertEqual(dispatcher.queues[0x51].qsize(), 1)

    def test_passive_mode_command_suppression(self):
        master = WayneDartMaster(transport=None, passive_monitoring_mode=True)
        master.register_pump(0x50)
        res = master.send_transaction(0x50, b"\x01\x01\x05")
        self.assertEqual(res, ExchangeResult.REJECTED)

if __name__ == "__main__":
    unittest.main()