"""Validate USB-RS485 adapters against DART serial requirements."""

from __future__ import annotations

import asyncio
from typing import Any

from intelipump_fdc.hardware.errors import AdapterValidationError, OddParityUnsupportedError
from intelipump_fdc.hardware.models import AdapterValidationResult
from intelipump_fdc.protocol.dart.transport.serial import SerialConfig, SerialParity


def expected_serial_config(
    device: str,
    *,
    baud_rate: int = 9600,
    exclusive_open: bool = True,
    read_timeout_s: float = 0.05,
    write_timeout_s: float = 2.0,
    read_chunk_size: int = 64,
) -> SerialConfig:
    return SerialConfig(
        device=device,
        baud_rate=baud_rate,
        data_bits=8,
        parity=SerialParity.ODD,
        stop_bits=1,
        read_chunk_size=read_chunk_size,
        read_timeout_s=read_timeout_s,
        write_timeout_s=write_timeout_s,
        exclusive_open=exclusive_open,
        flow_control=False,
    )


def validate_config_fields(config: SerialConfig) -> None:
    """Static validation — never allow silent 8N1 downgrade."""
    if config.parity is not SerialParity.ODD:
        raise OddParityUnsupportedError(
            f"parity must be ODD; refusing {config.parity.value}"
        )
    if config.flow_control:
        raise AdapterValidationError("flow control must be disabled for DART RS-485")
    config.validate()


async def validate_adapter_open(config: SerialConfig) -> AdapterValidationResult:
    """Open the port with required settings and confirm odd parity sticks."""
    validate_config_fields(config)
    errors: list[str] = []
    warnings: list[str] = []
    exclusive_ok: bool | None = None

    def _probe() -> tuple[bool | None, list[str], list[str]]:
        import serial  # type: ignore[import-untyped]

        local_errors: list[str] = []
        local_warnings: list[str] = []
        exclusive: bool | None = None
        kwargs: dict[str, Any] = {
            "port": config.device,
            "baudrate": config.baud_rate,
            "bytesize": config.data_bits,
            "parity": "O",
            "stopbits": config.stop_bits,
            "timeout": config.read_timeout_s,
            "write_timeout": config.write_timeout_s,
            "xonxoff": False,
            "rtscts": False,
            "dsrdtr": False,
        }
        if config.exclusive_open:
            kwargs["exclusive"] = True
        try:
            ser = serial.Serial(**kwargs)
        except TypeError:
            # Older pyserial without exclusive=
            kwargs.pop("exclusive", None)
            local_warnings.append("exclusive_open not supported by pyserial/OS")
            try:
                ser = serial.Serial(**kwargs)
                exclusive = False
            except Exception as exc:  # pragma: no cover - hardware dependent
                raise OddParityUnsupportedError(
                    f"failed to open {config.device} with odd parity: {exc}"
                ) from exc
        except Exception as exc:
            msg = str(exc).lower()
            if "parity" in msg or "odd" in msg:
                raise OddParityUnsupportedError(
                    f"odd parity unsupported on {config.device}: {exc}"
                ) from exc
            raise AdapterValidationError(
                f"failed to open {config.device}: {exc}"
            ) from exc
        else:
            exclusive = bool(kwargs.get("exclusive", False))

        try:
            if getattr(ser, "parity", None) not in {"O", serial.PARITY_ODD}:
                local_errors.append(
                    f"port parity after open is {getattr(ser, 'parity', None)!r}; "
                    "expected ODD"
                )
            if ser.baudrate != config.baud_rate:
                local_errors.append(
                    f"baudrate mismatch: got {ser.baudrate}, expected {config.baud_rate}"
                )
            if int(ser.bytesize) != config.data_bits:
                local_errors.append(
                    f"data bits mismatch: got {ser.bytesize}, expected {config.data_bits}"
                )
            if float(ser.stopbits) != float(config.stop_bits):
                local_errors.append(
                    f"stop bits mismatch: got {ser.stopbits}, expected {config.stop_bits}"
                )
            if getattr(ser, "xonxoff", False) or getattr(ser, "rtscts", False):
                local_errors.append("flow control must remain disabled")
        finally:
            ser.close()
        return exclusive, local_errors, local_warnings

    try:
        exclusive_ok, errors, warnings = await asyncio.to_thread(_probe)
    except (OddParityUnsupportedError, AdapterValidationError):
        raise
    except Exception as exc:  # pragma: no cover
        raise AdapterValidationError(str(exc)) from exc

    ok = not errors
    return AdapterValidationResult(
        device_path=config.device,
        ok=ok,
        baud_rate=config.baud_rate,
        data_bits=config.data_bits,
        parity=SerialParity.ODD.value,
        stop_bits=config.stop_bits,
        flow_control_disabled=True,
        exclusive_open=exclusive_ok,
        read_timeout_s=config.read_timeout_s,
        write_timeout_s=config.write_timeout_s,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )
