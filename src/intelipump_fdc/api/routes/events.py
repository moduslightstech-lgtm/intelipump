"""SSE and WebSocket live event routes."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse

from intelipump_fdc.api.dependencies import get_app_state
from intelipump_fdc.api.errors import ApiError
from intelipump_fdc.api.state import AppState
from intelipump_fdc.events.models import EventFilter, LiveEventType
from intelipump_fdc.events.subscription import SubscriberLimitError

router = APIRouter(tags=["events"])


def _parse_types(raw: str | None) -> frozenset[LiveEventType] | None:
    if not raw:
        return None
    out: set[LiveEventType] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        out.add(LiveEventType(part))
    return frozenset(out) if out else None


def _filter_from_query(
    *,
    station_id: str | None,
    pump_id: str | None,
    event_type: str | None,
    severity: str | None,
    simulated: bool | None,
) -> EventFilter:
    return EventFilter(
        station_id=station_id,
        pump_id=pump_id,
        event_types=_parse_types(event_type),
        severity=severity,
        simulated=simulated,
    )


def _sse_format(*, event: str, event_id: str, data: str) -> str:
    return f"id: {event_id}\nevent: {event}\ndata: {data}\n\n"


@router.get("/events/stream")
async def sse_stream(
    request: Request,
    state: AppState = Depends(get_app_state),
    station_id: str | None = None,
    pump_id: str | None = None,
    event_type: str | None = None,
    severity: str | None = None,
    simulated: bool | None = None,
) -> StreamingResponse:
    try:
        sub = await state.broker.subscribe(
            kind="sse",
            event_filter=_filter_from_query(
                station_id=station_id,
                pump_id=pump_id,
                event_type=event_type,
                severity=severity,
                simulated=simulated,
            ),
        )
    except SubscriberLimitError as exc:
        raise ApiError(
            code="STREAM_SUBSCRIBER_LIMIT",
            message=str(exc),
            status_code=429,
        ) from exc

    keepalive = state.settings.api.stream_keepalive_seconds

    async def gen() -> AsyncIterator[str]:
        connected = state.broker.make_event(
            LiveEventType.CONNECTED,
            station_id=state.settings.controller.station_id,
            environment=state.settings.environment,
            simulated=state.settings.api.simulated,
            payload={"subscription_id": sub.subscription_id},
        )
        yield _sse_format(
            event=connected.event_type.value,
            event_id=connected.event_id,
            data=json.dumps(connected.to_dict()),
        )
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(sub.queue.get(), timeout=keepalive)
                except TimeoutError:
                    hb = state.broker.make_event(
                        LiveEventType.HEARTBEAT,
                        station_id=state.settings.controller.station_id,
                        environment=state.settings.environment,
                        simulated=state.settings.api.simulated,
                    )
                    yield _sse_format(
                        event=hb.event_type.value,
                        event_id=hb.event_id,
                        data=json.dumps(hb.to_dict()),
                    )
                    continue
                if item is None:
                    break
                yield _sse_format(
                    event=item.event_type.value,
                    event_id=item.event_id,
                    data=json.dumps(item.to_dict()),
                )
        finally:
            await state.broker.unsubscribe(sub.subscription_id)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.websocket("/events/ws")
async def ws_stream(
    websocket: WebSocket,
    station_id: str | None = Query(None),
    pump_id: str | None = Query(None),
    event_type: str | None = Query(None),
    severity: str | None = Query(None),
    simulated: bool | None = Query(None),
) -> None:
    await websocket.accept()
    app_state: AppState | None = getattr(websocket.app.state, "app_state", None)
    if app_state is None:
        await websocket.close(code=1011)
        return
    try:
        sub = await app_state.broker.subscribe(
            kind="ws",
            event_filter=_filter_from_query(
                station_id=station_id,
                pump_id=pump_id,
                event_type=event_type,
                severity=severity,
                simulated=simulated,
            ),
        )
    except SubscriberLimitError:
        await websocket.close(code=1013)
        return

    connected = app_state.broker.make_event(
        LiveEventType.CONNECTED,
        station_id=app_state.settings.controller.station_id,
        environment=app_state.settings.environment,
        simulated=app_state.settings.api.simulated,
        payload={"subscription_id": sub.subscription_id},
    )
    await websocket.send_json(connected.to_dict())
    keepalive = app_state.settings.api.stream_keepalive_seconds
    try:
        while True:
            try:
                item = await asyncio.wait_for(sub.queue.get(), timeout=keepalive)
            except TimeoutError:
                hb = app_state.broker.make_event(
                    LiveEventType.HEARTBEAT,
                    station_id=app_state.settings.controller.station_id,
                    environment=app_state.settings.environment,
                    simulated=app_state.settings.api.simulated,
                )
                await websocket.send_json(hb.to_dict())
                continue
            if item is None:
                break
            await websocket.send_json(item.to_dict())
    except WebSocketDisconnect:
        pass
    finally:
        await app_state.broker.unsubscribe(sub.subscription_id)
