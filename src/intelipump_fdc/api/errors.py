"""Structured API errors."""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException


class ErrorBody(BaseModel):
    code: str
    message: str
    correlation_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorBody


class ApiError(Exception):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(message)


def error_json(
    *,
    code: str,
    message: str,
    correlation_id: str | None,
    details: dict[str, Any] | None = None,
    status_code: int = 400,
) -> JSONResponse:
    payload = ErrorResponse(
        error=ErrorBody(
            code=code,
            message=message,
            correlation_id=correlation_id,
            details=details or {},
        )
    )
    return JSONResponse(status_code=status_code, content=payload.model_dump())


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    corr = getattr(request.state, "correlation_id", None)
    return error_json(
        code=exc.code,
        message=exc.message,
        correlation_id=corr,
        details=exc.details,
        status_code=exc.status_code,
    )


async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    corr = getattr(request.state, "correlation_id", None)
    detail = exc.detail
    if isinstance(detail, dict) and "code" in detail:
        return error_json(
            code=str(detail.get("code")),
            message=str(detail.get("message", exc.detail)),
            correlation_id=corr,
            details=dict(detail.get("details") or {}),
            status_code=exc.status_code,
        )
    return error_json(
        code="HTTP_ERROR",
        message=str(detail),
        correlation_id=corr,
        status_code=exc.status_code,
    )


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    corr = getattr(request.state, "correlation_id", None)
    return error_json(
        code="VALIDATION_ERROR",
        message="Request validation failed",
        correlation_id=corr,
        details={"errors": exc.errors()},
        status_code=422,
    )
