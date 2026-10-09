"""Publish controller meter-read-result.json to sync_queue / MQTT (cloud-sync)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.controller.meter_read_request import read_meter_read_result
from intelipump_fdc.core.config import MeterReadingSettings
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services import meter_reading as meter_svc

logger = structlog.get_logger(__name__)


class MeterResultPublisher:
    """Idempotent: each correlationId published at most once per process."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        station_id: str,
        device_id: str,
        meter_settings: MeterReadingSettings,
    ) -> None:
        self._factory = session_factory
        self._station_id = station_id
        self._device_id = device_id
        self._meter = meter_settings
        self._seen: set[str] = set()

    async def publish_new_results(self) -> int:
        result = read_meter_read_result()
        if not result:
            return 0
        corr = str(result.get("correlationId") or "").strip()
        if not corr or corr in self._seen:
            return 0
        status = str(result.get("status") or "").upper()
        if status not in {
            "CAPTURED",
            "UNSUPPORTED",
            "ERROR",
            "RATE_LIMITED",
            "DEFERRED",
        }:
            return 0

        raw = result.get("cumulativeVolumeRaw")
        if raw is None and isinstance(result.get("rawScaled"), dict):
            raw = result["rawScaled"].get("total_value")
        decimals = self._meter.volume_decimals
        if decimals is None and result.get("volumeDecimals") is not None:
            try:
                decimals = int(result["volumeDecimals"])
            except (TypeError, ValueError):
                decimals = None
        liters = result.get("volumeLiters")
        if liters is None:
            liters = meter_svc.liters_from_raw_scaled(
                int(raw) if raw is not None else None, decimals
            )

        nozzle_id = (
            str(result.get("nozzleHint") or "").strip()
            or (
                (result.get("channelMap") or {}).get("nozzle_id")
                if isinstance(result.get("channelMap"), dict)
                else None
            )
            or "nozzle-1"
        )
        pump_id = (
            result.get("pumpId")
            or result.get("pump_id")
            or (
                (result.get("channelMap") or {}).get("pump_id")
                if isinstance(result.get("channelMap"), dict)
                else None
            )
            or "pump-unknown"
        )

        event = "METER_READING" if status == "CAPTURED" else "METER_READING_UNSUPPORTED"
        payload: dict[str, Any] = {
            "stationId": self._station_id,
            "deviceId": self._device_id,
            "pumpId": pump_id,
            "nozzleId": nozzle_id,
            "dartAddress": result.get("dartAddress"),
            "status": status,
            "source": (
                "STARTUP_OPENING"
                if result.get("startupOpening")
                or str(result.get("slot") or "").upper() == "OPENING"
                else "READ_NOW"
            ),
            "slot": (
                str(result.get("slot") or "").upper()
                if result.get("slot")
                else (
                    "OPENING"
                    if result.get("startupOpening")
                    else "AD_HOC"
                )
            ),
            "requestedAt": result.get("requestedAt"),
            "capturedAt": result.get("capturedAt") or datetime.now(UTC).isoformat(),
            "cumulativeVolumeRaw": int(raw) if raw is not None else None,
            "volumeLiters": liters,
            "volumeDecimals": decimals,
            "units": "liters",
            "correlationId": corr,
            "errorCode": result.get("errorCode"),
            "errorMessage": result.get("errorMessage"),
            "flags": {
                "hardware_cd101": True,
                "counter_select": result.get("counterSelect"),
            },
            "softwareVersion": meter_svc.SOFTWARE_VERSION,
            "rawEvidence": {
                "kind": "hardware_cd101",
                "requestPayloadHex": result.get("requestPayloadHex"),
                "responseFrameHex": result.get("responseFrameHex"),
                "decoded": result.get("decoded"),
                "channelMap": result.get("channelMap"),
            },
        }
        dedupe = f"meter-result:{self._station_id}:{corr}:{nozzle_id}:{status}"
        async with unit_of_work(self._factory) as uow:
            # Intake inserts PENDING_CONTROLLER; resolve it or max_pending (default 2)
            # permanently blocks further dashboard Read now on this pump.
            await uow.meter_readings.resolve_pending_by_correlation(
                station_id=self._station_id,
                correlation_id=corr,
                status=status,
                error_code=(
                    str(result["errorCode"]) if result.get("errorCode") else None
                ),
                error_message=(
                    str(result["errorMessage"]) if result.get("errorMessage") else None
                ),
            )
            await uow.sync_queue.enqueue_checked(
                entity_type="meter_reading",
                entity_id=corr,
                event_type=event,
                payload=payload,
                deduplication_key=dedupe,
            )
            await uow.meter_readings.create(
                station_id=self._station_id,
                device_id=self._device_id,
                pump_id=str(pump_id),
                nozzle_id=str(nozzle_id),
                dart_address=result.get("dartAddress"),
                source=str(payload.get("source") or "READ_NOW"),
                status=status,
                slot=str(payload.get("slot") or "AD_HOC"),
                deduplication_key=dedupe,
                correlation_id=corr,
                cumulative_volume_raw=int(raw) if raw is not None else None,
                volume_decimals=int(decimals) if decimals is not None else 2,
                volume_liters=str(liters) if liters is not None else None,
                captured_at=(
                    datetime.fromisoformat(str(result["capturedAt"]).replace("Z", "+00:00"))
                    if result.get("capturedAt")
                    else None
                ),
                requested_at=datetime.now(UTC),
                raw_evidence=payload.get("rawEvidence"),
                software_version=meter_svc.SOFTWARE_VERSION,
                flags=payload.get("flags"),
                error_code=result.get("errorCode"),
                error_message=result.get("errorMessage"),
            )
        self._seen.add(corr)
        logger.info(
            "meter_result_queued_for_cloud",
            correlationId=corr,
            status=status,
            nozzleId=nozzle_id,
            raw=raw,
            litres=liters,
        )
        return 1
