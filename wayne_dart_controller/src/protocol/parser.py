from typing import List, Dict

class FrameDirection:
    CONTROLLER_TO_PUMP = "C2P"
    PUMP_TO_CONTROLLER = "P2C"
    UNKNOWN = "UNKNOWN"

class DARTTransactionParser:
    """Direction-aware transaction parser with boundary checks and metrics."""

    @staticmethod
    def parse_payload(payload: bytes, direction: str, configured_nozzles: int = 4, metrics: Dict = None) -> List[Dict]:
        transactions = []
        offset = 0

        while offset < len(payload):
            if offset + 2 > len(payload):
                if metrics is not None:
                    metrics["truncated_payload_count"] = metrics.get("truncated_payload_count", 0) + 1
                raise ValueError("Truncated transaction header in payload")

            trans_id = payload[offset]
            length = payload[offset + 1]

            if offset + 2 + length > len(payload):
                if metrics is not None:
                    metrics["truncated_payload_count"] = metrics.get("truncated_payload_count", 0) + 1
                raise ValueError(f"Truncated transaction data for trans_id {hex(trans_id)}")

            data = payload[offset + 2 : offset + 2 + length]
            offset += 2 + length

            parsed = {"trans_id": trans_id, "length": length, "raw_data": data, "type": "UNKNOWN_TRANSACTION"}

            if direction == FrameDirection.PUMP_TO_CONTROLLER:
                if trans_id == 0x01 and length == 1:
                    status_code = data[0]
                    status_map = {
                        0: "PUMP_NOT_PROGRAMMED", 1: "RESET", 2: "AUTHORIZED",
                        4: "FILLING", 5: "FILLING_COMPLETED", 6: "MAX_REACHED",
                        7: "SWITCHED_OFF", 8: "SUSPENDED"
                    }
                    if status_code in status_map:
                        parsed["type"] = "DC1_STATUS"
                        parsed["status_code"] = status_code
                        parsed["status_label"] = status_map[status_code]
                    else:
                        parsed["type"] = "UNRECOGNIZED_DC1_CODE"
                        if metrics is not None:
                            metrics["unrecognized_status_count"] = metrics.get("unrecognized_status_count", 0) + 1

                elif trans_id == 0x02 and length == 8:
                    try:
                        from src.protocol.bcd import bcd_to_int
                        parsed["type"] = "DC2_VOLUME_AMOUNT"
                        parsed["volume_raw"] = bcd_to_int(data[0:4])
                        parsed["amount_raw"] = bcd_to_int(data[4:8])
                    except ValueError:
                        if metrics is not None:
                            metrics["invalid_bcd_count"] = metrics.get("invalid_bcd_count", 0) + 1
                        continue

                elif trans_id == 0x03 and length == 4:
                    try:
                        from src.protocol.bcd import bcd_to_int
                        parsed["type"] = "DC3_NOZZLE_PRICE"
                        parsed["price_raw"] = bcd_to_int(data[0:3])
                        nozio = data[3]
                        logical_nozzle = nozio & 0x0F
                        
                        if 1 <= logical_nozzle <= configured_nozzles:
                            parsed["logical_nozzle"] = logical_nozzle
                            parsed["nozzle_state"] = "OUT" if (nozio & 0x10) else "IN"
                        else:
                            parsed["type"] = "INVALID_NOZZLE_RANGE"
                            if metrics is not None:
                                metrics["invalid_nozzle_count"] = metrics.get("invalid_nozzle_count", 0) + 1
                    except ValueError:
                        if metrics is not None:
                            metrics["invalid_bcd_count"] = metrics.get("invalid_bcd_count", 0) + 1
                        continue

                elif trans_id == 0x65:
                    parsed["type"] = "DC101_TOTAL_COUNTERS_PARTIAL"

            elif direction == FrameDirection.CONTROLLER_TO_PUMP:
                if trans_id == 0x01 and length == 1:
                    parsed["type"] = "CD1_COMMAND"
                    parsed["cmd_code"] = data[0]
                elif trans_id == 0x02:
                    parsed["type"] = "CD2_ALLOWED_NOZZLES"
                elif trans_id == 0x05:
                    parsed["type"] = "CD5_PRICE_UPDATE"

            transactions.append(parsed)

        return transactions