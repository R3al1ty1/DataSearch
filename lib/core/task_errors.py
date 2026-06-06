import logging
from typing import Any

from lib.core.error_codes import ErrorCode
from lib.core.exceptions import DataSearchError


def serialize_error(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, DataSearchError):
        return {
            "error_code": exc.error_code.value,
            "message": exc.message,
            "details": exc.details,
        }
    return {
        "error_code": ErrorCode.INTERNAL_ERROR.value,
        "message": "An unexpected error occurred",
        "details": None,
    }


def task_error_result(task_name: str, exc: Exception) -> dict[str, Any]:
    return {
        "status": "error",
        "task_name": task_name,
        "error": serialize_error(exc),
    }


def log_task_error(logger: logging.Logger, task_name: str, exc: Exception) -> None:
    error = serialize_error(exc)
    logger.error(
        f"{task_name} failed: "
        f"error_code={error['error_code']}, message={error['message']}, "
        f"details={error['details']}"
    )
