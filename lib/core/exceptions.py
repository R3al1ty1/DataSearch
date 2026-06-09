from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from lib.core.error_codes import ErrorCode


class DataSearchError(Exception):
    def __init__(
        self,
        message: str = "Internal server error",
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.details = details
        self.headers = headers
        super().__init__(message)


DataSearchBaseException = DataSearchError


class ResourceNotFound(DataSearchError):
    def __init__(
        self,
        resource: str,
        identifier: str,
        error_code: ErrorCode = ErrorCode.RESOURCE_NOT_FOUND,
    ) -> None:
        super().__init__(
            f"{resource} not found",
            status.HTTP_404_NOT_FOUND,
            error_code,
            {"resource": resource, "identifier": identifier},
        )


class ExternalServiceError(DataSearchError):
    def __init__(
        self,
        service: str,
        details: str,
        status_code: int = status.HTTP_502_BAD_GATEWAY,
    ) -> None:
        super().__init__(
            f"Error communicating with {service}",
            status_code,
            ErrorCode.EXTERNAL_SERVICE_ERROR,
            {"service": service, "details": details},
        )


class InvalidSearchQuery(DataSearchError):
    def __init__(self, message: str = "Invalid search query") -> None:
        super().__init__(message, status.HTTP_400_BAD_REQUEST, ErrorCode.INVALID_SEARCH_QUERY)


def error_body(
    error_code: ErrorCode,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "error_code": error_code.value,
        "message": message,
        "details": details,
    }


def _http_error_code(status_code: int) -> ErrorCode:
    if status_code == status.HTTP_401_UNAUTHORIZED:
        return ErrorCode.AUTHENTICATION_ERROR
    if status_code == status.HTTP_403_FORBIDDEN:
        return ErrorCode.INSUFFICIENT_PERMISSIONS
    if status_code == status.HTTP_404_NOT_FOUND:
        return ErrorCode.RESOURCE_NOT_FOUND
    if status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        return ErrorCode.RATE_LIMIT_EXCEEDED
    if status.HTTP_400_BAD_REQUEST <= status_code < status.HTTP_500_INTERNAL_SERVER_ERROR:
        return ErrorCode.VALIDATION_ERROR
    return ErrorCode.INTERNAL_ERROR


def _request_id(request: Request) -> str | None:
    state_request_id = getattr(request.state, "request_id", None)
    if state_request_id:
        return str(state_request_id)
    return (
        request.headers.get("X-Request-ID")
        or request.headers.get("X-Correlation-ID")
    )


def _error_log_message(
    request: Request,
    error_code: ErrorCode,
    status_code: int,
    message: str,
) -> str:
    request_id = _request_id(request) or "none"
    return (
        "API error: "
        f"error_code={error_code.value}, status_code={status_code}, "
        f"method={request.method}, path={request.url.path}, "
        f"request_id={request_id}, message={message}"
    )


def _log_api_error(
    logger: logging.Logger,
    request: Request,
    error_code: ErrorCode,
    status_code: int,
    message: str,
    exc_info: bool = False,
) -> None:
    log_message = _error_log_message(request, error_code, status_code, message)
    if status_code >= status.HTTP_500_INTERNAL_SERVER_ERROR:
        logger.error(log_message, exc_info=exc_info)
    else:
        logger.warning(log_message)


def _response_headers(
    request: Request,
    headers: dict[str, str] | None = None,
) -> dict[str, str] | None:
    request_id = _request_id(request)
    if not request_id:
        return headers
    response_headers = dict(headers or {})
    response_headers.setdefault("X-Request-ID", request_id)
    return response_headers


def register_exception_handlers(app: FastAPI, logger: logging.Logger) -> None:
    @app.exception_handler(DataSearchError)
    async def datasearch_error_handler(
        request: Request,
        exc: DataSearchError,
    ) -> JSONResponse:
        _log_api_error(
            logger,
            request,
            exc.error_code,
            exc.status_code,
            exc.message,
            exc_info=exc.status_code >= status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=jsonable_encoder(error_body(exc.error_code, exc.message, exc.details)),
            headers=_response_headers(request, exc.headers),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        fields = [
            {
                "field": ".".join(str(loc) for loc in error["loc"]),
                "message": error["msg"],
            }
            for error in exc.errors()
        ]
        _log_api_error(
            logger,
            request,
            ErrorCode.VALIDATION_ERROR,
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"Request validation failed for {len(fields)} field(s)",
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content=jsonable_encoder(
                error_body(
                    ErrorCode.VALIDATION_ERROR,
                    "Request validation failed",
                    {"fields": fields},
                )
            ),
            headers=_response_headers(request),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(
        request: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        message = str(exc.detail) if exc.detail else "HTTP error"
        error_code = _http_error_code(exc.status_code)
        _log_api_error(
            logger,
            request,
            error_code,
            exc.status_code,
            message,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=jsonable_encoder(
                error_body(error_code, message)
            ),
            headers=_response_headers(request, exc.headers),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        _log_api_error(
            logger,
            request,
            ErrorCode.INTERNAL_ERROR,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "An unexpected error occurred",
            exc_info=True,
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=jsonable_encoder(
                error_body(ErrorCode.INTERNAL_ERROR, "An unexpected error occurred")
            ),
            headers=_response_headers(request),
        )
