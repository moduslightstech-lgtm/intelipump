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
        configured_nozzles=ctrl_cfg.get("configured_nozzles_per_pump", 4)
    )

    for addr in pump_addresses:
        master.register_pump(addr)

    logging.info("[*] Synchronizing session state across pumps...")
    for addr in pump_addresses:
        valid_cycles = 0
        for _ in range(5):
            master.poll_pump(addr)
            pump = master.pumps[addr]
            if pump.online and pump.observed_status != "UNKNOWN" and pump.nozzle_position != "UNKNOWN":
                valid_cycles += 1
            else:
                valid_cycles = 0 # Strictly require consecutive valid polling cycles
            time.sleep(0.04)

        if valid_cycles >= 3:
            pump = master.pumps[addr]
            pump.synchronized = True
            logging.info(f"    -> Pump {hex(addr)} SYNCHRONIZED. Status: {pump.observed_status}, Nozzle: {pump.nozzle_position}")
            if ctrl_cfg.get("auto_startup_commands", False):
                master.set_unit_prices(addr, ctrl_cfg["default_prices"])
                master.reset(addr)

    logging.info("[*] Entering Continuous Event Loop...")
    try:
        while True:
            for addr in pump_addresses:
                events = master.poll_pump(addr)
                pump = master.pumps[addr]

                if not pump.online or not pump.synchronized:
                    continue

                for ev in events:
                    if ev["type"] == "NOZZLE_TRANSITION":
                        now_t = time.monotonic()
                        status_is_recent = (now_t - pump.last_status_time) < 5.0
                        
                        if ev["state"] == "OUT" and pump.observed_status == "RESET" and status_is_recent:
                            logging.info(f"[EVENT] Nozzle {ev['nozzle']} lifted on Pump {hex(addr)}. Authorizing...")
                            res = master.authorize(addr, allowed_nozzles=[ev['nozzle']], confirm_application=True)
                            if res in [ExchangeResult.LINK_ACKNOWLEDGED, ExchangeResult.APPLICATION_CONFIRMED]:
                                logging.info(f"    -> Pump {hex(addr)} Authorization Result: {res.name}")

                    elif ev["type"] == "STATUS_CHANGE":
                        logging.info(f"[EVENT] Status change on Pump {hex(addr)}: {ev['status']}")
                        if ev["status"] == "FILLING_COMPLETED" and not pump.sale_completion_latch:
                            pump.sale_completion_latch = True
                            logging.info(
                                f"    -> Sale Completed on {hex(addr)} "
                                f"({pump.filled_volume}L, {master.currency_symbol} {pump.filled_amount}). Resetting..."
                            )
                            master.reset(addr, confirm_application=True)

            time.sleep(ctrl_cfg["poll_interval_sec"])

    except KeyboardInterrupt:
        logging.info("Controller stopped by user.")
    finally:
        transport.close()

if __name__ == "__main__":
    main()