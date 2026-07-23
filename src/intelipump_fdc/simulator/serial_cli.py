"""CLI: expose Phase-5 simulator on a serial/PTY port."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal

from intelipump_fdc.protocol.dart.transport.serial import SerialConfig, SerialTransport
from intelipump_fdc.simulator.serial_bridge import (
    SerialBridgeConfig,
    SimulatorSerialBridge,
)


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="intelipump-simulator-serial",
        description=(
            "LAB virtual-serial bridge for the Phase-5 Wayne simulator. "
            "Never talks to a real dispenser."
        ),
    )
    parser.add_argument("--port", default="/tmp/dart-pump")
    parser.add_argument("--baud", type=int, default=9600)
    parser.add_argument("--log-frames", action="store_true")
    args = parser.parse_args(argv)

    async def _main() -> None:
        transport = SerialTransport(
            SerialConfig(device=args.port, baud_rate=args.baud)
        )
        bridge = SimulatorSerialBridge(
            transport,
            config=SerialBridgeConfig(log_frames=args.log_frames),
        )

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, bridge.request_stop)

        print(f"simulator-serial listening on {args.port} (LAB only)")
        await bridge.run()
        print("simulator-serial stopped")

    asyncio.run(_main())


if __name__ == "__main__":
    run()
