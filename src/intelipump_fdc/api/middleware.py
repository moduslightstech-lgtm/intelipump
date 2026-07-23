"""Correlation ID middleware."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from uuid import uuid4

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from intelipump_fdc.api.errors import error_json

_CORR_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
CORRELATION_HEADER = "X-Correlation-ID"


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        incoming = request.headers.get(CORRELATION_HEADER)
        if incoming is None or incoming.strip() == "":
            correlation_id = str(uuid4())
        else:
            candidate = incoming.strip()
            if not _CORR_RE.fullmatch(candidate):
                return error_json(
                    code="INVALID_CORRELATION_ID",
                    message=(
                        "X-Correlation-ID must be 1-128 chars of "
                        "[A-Za-z0-9._:-]"
                    ),
                    correlation_id=None,
                    status_code=400,
                )
            correlation_id = candidate

        request.state.correlation_id = correlation_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
        response = await call_next(request)
        response.headers[CORRELATION_HEADER] = correlation_id
        return response


def get_correlation_id(request: Request) -> str:
    value = getattr(request.state, "correlation_id", None)
    if isinstance(value, str) and value:
        return value
    return str(uuid4())
