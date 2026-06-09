from fastapi import status

from lib.core.error_codes import ErrorCode
from lib.core.exceptions import DataSearchError
from lib.core.task_errors import serialize_error, task_error_result


def test_serialize_error_preserves_datasearch_error_contract():
    exc = DataSearchError(
        message="External source unavailable",
        status_code=status.HTTP_502_BAD_GATEWAY,
        error_code=ErrorCode.EXTERNAL_SERVICE_ERROR,
        details={"source": "kaggle"},
    )

    assert serialize_error(exc) == {
        "error_code": "EXTERNAL_SERVICE_ERROR",
        "message": "External source unavailable",
        "details": {"source": "kaggle"},
    }


def test_serialize_error_returns_safe_internal_error_for_unexpected_exception():
    result = serialize_error(RuntimeError("database password leaked"))

    assert result == {
        "error_code": "INTERNAL_ERROR",
        "message": "An unexpected error occurred",
        "details": None,
    }


def test_task_error_result_wraps_serialized_error():
    result = task_error_result("example.task", RuntimeError("raw failure"))

    assert result == {
        "status": "error",
        "task_name": "example.task",
        "error": {
            "error_code": "INTERNAL_ERROR",
            "message": "An unexpected error occurred",
            "details": None,
        },
    }
