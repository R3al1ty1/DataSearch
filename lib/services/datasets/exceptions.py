from uuid import UUID

from fastapi import status

from lib.core.error_codes import ErrorCode
from lib.core.exceptions import DataSearchError, ResourceNotFound


class DatasetNotFound(ResourceNotFound):
    def __init__(self, dataset_id: UUID | str) -> None:
        super().__init__("Dataset", str(dataset_id), ErrorCode.DATASET_NOT_FOUND)


class DatasetConflict(DataSearchError):
    def __init__(self, source_name: str, external_id: str) -> None:
        super().__init__(
            "Dataset already exists",
            status.HTTP_409_CONFLICT,
            ErrorCode.DATASET_CONFLICT,
            {"source_name": source_name, "external_id": external_id},
        )


class SearchLogNotFound(ResourceNotFound):
    def __init__(self, search_log_id: UUID | str) -> None:
        super().__init__("SearchLog", str(search_log_id), ErrorCode.SEARCH_LOG_NOT_FOUND)
