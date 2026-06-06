from fastapi import status

from lib.core.error_codes import ErrorCode
from lib.core.exceptions import DataSearchError


class TaskQueueError(DataSearchError):
    def __init__(self, task_name: str, details: str) -> None:
        super().__init__(
            "Failed to queue background task",
            status.HTTP_503_SERVICE_UNAVAILABLE,
            ErrorCode.TASK_QUEUE_ERROR,
            {"task_name": task_name, "details": details},
        )
