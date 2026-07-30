"""DART application transaction constants.

Source: WAYNE EUROPE - Protocol Specification Dart Pump Interface Revision 2.11
(WM041550 Rev 02), pages 10-12 (transaction lists) and section 3 layouts.

Wire TRANS numbers are shared between CD* (controller→pump) and DC*
(pump→controller) families. Direction cannot be inferred from TRANS alone
when both families define the same value; use payload structure and capture
metadata when available.
"""

from __future__ import annotations

from enum import IntEnum, StrEnum


class MessageDirection(StrEnum):
    MASTER_TO_SLAVE = "MASTER_TO_SLAVE"
    SLAVE_TO_MASTER = "SLAVE_TO_MASTER"
    UNKNOWN = "UNKNOWN"


class DecodeStatus(StrEnum):
    DECODED = "DECODED"
    PARTIAL = "PARTIAL"
    UNKNOWN = "UNKNOWN"
    MALFORMED = "MALFORMED"


class TransactionWireId(IntEnum):
    """Wire TRANS byte values (not unique across CD/DC families)."""

    ID_01 = 0x01  # CD1 Command / DC1 Pump status
    ID_02 = 0x02  # CD2 Allowed nozzles / DC2 Filled volume+amount
    ID_03 = 0x03  # CD3 Preset volume / DC3 Nozzle status+price
    ID_04 = 0x04  # CD4 Preset amount
    ID_05 = 0x05  # CD5 Price update / DC5 Alarm
    ID_07 = 0x07  # DC7 Pump parameters
    ID_09 = 0x09  # DC9 Pump identity
    ID_0E = 0x0E  # CD14 Suspend
    ID_0F = 0x0F  # CD15 Resume
    ID_65 = 0x65  # CD101 Request totals / DC101 Total counters
    ID_66 = 0x66  # DC102 IFSF stand-alone
    ID_67 = 0x67  # DC103 Pump unit prices


class TransactionType(StrEnum):
    """Logical transaction type names from the Pump Interface spec."""

    CD1_COMMAND = "CD1_COMMAND"
    CD2_ALLOWED_NOZZLES = "CD2_ALLOWED_NOZZLES"
    CD3_PRESET_VOLUME = "CD3_PRESET_VOLUME"
    CD4_PRESET_AMOUNT = "CD4_PRESET_AMOUNT"
    CD5_PRICE_UPDATE = "CD5_PRICE_UPDATE"
    CD14_SUSPEND = "CD14_SUSPEND"
    CD15_RESUME = "CD15_RESUME"
    CD101_REQUEST_TOTALS = "CD101_REQUEST_TOTALS"
    DC1_PUMP_STATUS = "DC1_PUMP_STATUS"
    DC2_FILLED_VOLUME_AMOUNT = "DC2_FILLED_VOLUME_AMOUNT"
    DC3_NOZZLE_STATUS_PRICE = "DC3_NOZZLE_STATUS_PRICE"
    DC5_ALARM = "DC5_ALARM"
    DC7_PUMP_PARAMETERS = "DC7_PUMP_PARAMETERS"
    DC9_PUMP_IDENTITY = "DC9_PUMP_IDENTITY"
    DC101_TOTAL_COUNTERS = "DC101_TOTAL_COUNTERS"
    DC102_IFSF_STANDALONE = "DC102_IFSF_STANDALONE"
    DC103_PUMP_UNIT_PRICES = "DC103_PUMP_UNIT_PRICES"
    UNKNOWN = "UNKNOWN"
    AMBIGUOUS_CD1_OR_DC1 = "AMBIGUOUS_CD1_OR_DC1"
    AMBIGUOUS_CD3_OR_DC3 = "AMBIGUOUS_CD3_OR_DC3"


# CD1 DCC values — Pump Interface Rev 2.11, page 13
class PumpControlCommand(IntEnum):
    RETURN_STATUS = 0x00
    RETURN_PUMP_PARAMETERS = 0x02
    RETURN_PUMP_IDENTITY = 0x03
    RETURN_FILLING_INFORMATION = 0x04
    RESET = 0x05
    AUTHORIZE = 0x06
    STOP = 0x08
    SWITCH_OFF = 0x0A
    SUSPEND_FUELLING_POINT = 0x0D
    RESUME_FUELLING_POINT = 0x0E
    RETURN_PRICES_OF_ALL_CURRENT_GRADES = 0x0F


HEADER_SIZE: int = 2  # TRANS + LNG
