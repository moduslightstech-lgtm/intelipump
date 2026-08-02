import json
import time
import sys
import logging
from src.driver.serial_transport import DARTSerialTransport, CentralFrameDispatcher
from src.core.master_controller import WayneDartMaster, ExchangeResult

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    try:
        with open("config/settings.json", "r") as f:
            config = json.load(f)
    except Exception as e:
        logging.critical(f"Failed to load configuration: {e}")
        sys.exit(1)

    ser_cfg = config["serial"]
    ctrl_cfg = config["controller"]
    pump_addresses = [int(addr, 16) for addr in ctrl_cfg["pumps"]]
    passive_mode = ctrl_cfg.get("passive_monitoring_mode", False)

    dispatcher = CentralFrameDispatcher(pump_addresses)
    
    try:
        transport = DARTSerialTransport(
            port=ser_cfg["port"],
            baudrate=ser_cfg["baudrate"],
            parity=ser_cfg["parity"],
            bytesize=ser_cfg["bytesize"],
            stopbits=ser_cfg["stopbits"],
            read_timeout=ser_cfg["read_timeout_sec"],
            quiet_gap_timeout=ser_cfg["quiet_gap_timeout_sec"],
            tx_delay_sec=ser_cfg["tx_delay_sec"],
            ack_delay_sec=ser_cfg.get("ack_delay_sec", 0.005)
        )
        transport.connect(dispatcher)
    except Exception as e:
        logging.critical(f"Startup aborted due to transport error: {e}")
        sys.exit(1)

    master = WayneDartMaster(
        transport=transport,
        max_retries=ctrl_cfg["max_retries"],
        price_format=ctrl_cfg.get("price_format", "RAW_BCD_DIGITS"),
        offline_threshold=ctrl_cfg.get("offline_missed_poll_threshold", 5),
        currency_symbol=ctrl_cfg.get("currency_symbol", "NGN"),
        configured_nozzles=ctrl_cfg.get("configured_nozzles_per_pump", 4),
        passive_monitoring_mode=passive_mode
    )

    for addr in pump_addresses:
        master.register_pump(addr)

    if passive_mode:
        logging.info("================================================================")
        logging.info("   [PASSIVE MONITORING MODE ACTIVE] All Active Commands Blocked  ")
        logging.info("================================================================")
    else:
        logging.info("================================================================")
        logging.info("   [ACTIVE MASTER CONTROLLER MODE ONLINE]                      ")
        logging.info("   Controller will POLL, ACK, set PRICES, RESET, and AUTHORIZE  ")
        logging.info("================================================================")

    logging.info("[*] Synchronizing session state across pumps...")
    for addr in pump_addresses:
        pump = master.pumps[addr]
        valid_cycles = 0
        
        # Adaptive sync loop: poll up to 30 times (~1.2 sec) to establish baseline state
        for attempt in range(30):
            master.poll_pump(addr)

            # If dispenser responds with EOT but status is UNKNOWN, request status (CD1 Return Status)
            if pump.online and (pump.observed_status == "UNKNOWN" or pump.nozzle_position == "UNKNOWN"):
                if not passive_mode and attempt % 5 == 0:
                    master.send_transaction(addr, b"\x01\x01\x00")  # CD1 DCC=0x00 (RETURN STATUS)

            # Strict Sync Requirement: Online AND known status AND known nozzle state
            if pump.online and pump.observed_status != "UNKNOWN" and pump.nozzle_position != "UNKNOWN":
                valid_cycles += 1
                if valid_cycles >= 3:
                    break
            else:
                valid_cycles = 0  # Reset counter if state drops to UNKNOWN
            time.sleep(0.04)

        # STRICT GUARD: Only synchronize if valid_cycles reached 3 with actual state
        if valid_cycles >= 3:
            pump.synchronized = True
            logging.info(f"    -> Pump {hex(addr)} SYNCHRONIZED. Status: {pump.observed_status}, Nozzle: {pump.nozzle_position}")
            
            # Active mode startup sequence: Program Price & Reset Display
            if ctrl_cfg.get("auto_startup_commands", True) and not passive_mode:
                logging.info(f"    -> [STARTUP] Programming Default Unit Prices {ctrl_cfg['default_prices']} to Pump {hex(addr)}...")
                price_res = master.set_unit_prices(addr, ctrl_cfg["default_prices"])
                logging.info(f"    -> [STARTUP] Price Programming Result: {price_res.name}")
                
                logging.info(f"    -> [STARTUP] Issuing RESET Command to Pump {hex(addr)}...")
                reset_res = master.reset(addr, confirm_application=True)
                logging.info(f"    -> [STARTUP] Reset Result: {reset_res.name}")
        else:
            pump.synchronized = False
            logging.warning(f"    -> Pump {hex(addr)} FAILED synchronization (Status: {pump.observed_status}, Nozzle: {pump.nozzle_position}). Skipping startup commands.")

    logging.info("[*] Entering Main Active Controller Event Loop...")
    try:
        while True:
            for addr in pump_addresses:
                events = master.poll_pump(addr)
                pump = master.pumps[addr]

                if not pump.online or not pump.synchronized:
                    continue

                for ev in events:
                    if ev["type"] == "NOZZLE_TRANSITION":
                        if ev["state"] == "OUT":
                            logging.info(f"[EVENT] Nozzle {ev['nozzle']} lifted on Pump {hex(addr)}. Ensuring RESET before Authorization...")
                            if not passive_mode:
                                # 1. Enforce protocol rule: Pump must be in RESET state before authorization
                                if pump.observed_status != "RESET":
                                    logging.info(f"    -> Pump {hex(addr)} is in status '{pump.observed_status}'. Issuing pre-authorization RESET...")
                                    master.reset(addr, confirm_application=True)
                                    time.sleep(0.05)

                                # 2. Transmit Allowed Nozzles (CD2) & Authorize (CD1)
                                res = master.authorize(addr, allowed_nozzles=[ev['nozzle']], confirm_application=True)
                                logging.info(f"    -> Pump {hex(addr)} Authorization Result: {res.name}")

                    elif ev["type"] == "STATUS_CHANGE":
                        logging.info(f"[EVENT] Status change on Pump {hex(addr)}: {ev['status']}")
                        if ev["status"] == "FILLING_COMPLETED" and not pump.sale_completion_latch:
                            pump.sale_completion_latch = True
                            logging.info(
                                f"    -> Sale Completed on {hex(addr)} "
                                f"({pump.filled_volume}L, {master.currency_symbol} {pump.filled_amount}). Resetting..."
                            )
                            if not passive_mode:
                                res = master.reset(addr, confirm_application=True)
                                logging.info(f"    -> Pump {hex(addr)} Reset Result: {res.name}")

            time.sleep(ctrl_cfg["poll_interval_sec"])

    except KeyboardInterrupt:
        logging.info("Controller stopped by user.")
    finally:
        transport.close()

if __name__ == "__main__":
    main()