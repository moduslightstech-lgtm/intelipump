"""Controller status route (lightweight)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from intelipump_fdc.api.dependencies import get_app_state
from intelipump_fdc.api.state import AppState

router = APIRouter(tags=["controller"])


@router.get("/controller/status")
async def controller_status(state: AppState = Depends(get_app_state)) -> dict[str, object]:
    return {
        "environment": state.settings.environment,
        "mode": state.settings.controller.mode.value,
        "loop_running": (
            state.controller_task is not None and not state.controller_task.done()
        ),
        "accepting_commands": state.metrics.accepting_commands,
        "shutting_down": state.shutting_down,
        "uptime_seconds": state.uptime_seconds,
    }
