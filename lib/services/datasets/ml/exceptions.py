from fastapi import status

from lib.core.error_codes import ErrorCode
from lib.core.exceptions import DataSearchError


class EmbeddingError(DataSearchError):
    def __init__(
        self,
        message: str,
        error_code: ErrorCode,
        details: dict | None = None,
    ) -> None:
        super().__init__(message, status.HTTP_503_SERVICE_UNAVAILABLE, error_code, details)


class EmbeddingModelLoadError(EmbeddingError):
    def __init__(self, model_name: str, reason: str) -> None:
        super().__init__(
            "Embedding model could not be loaded",
            ErrorCode.EMBEDDING_MODEL_LOAD_FAILED,
            {"model_name": model_name, "reason": reason},
        )


class EmbeddingEncodingError(EmbeddingError):
    def __init__(self, text_count: int, reason: str) -> None:
        super().__init__(
            "Embedding encoding failed",
            ErrorCode.EMBEDDING_ENCODING_FAILED,
            {"text_count": text_count, "reason": reason},
        )


class EmbeddingPersistenceError(EmbeddingError):
    def __init__(self, dataset_id: str, reason: str) -> None:
        super().__init__(
            "Embedding could not be saved",
            ErrorCode.EMBEDDING_PERSISTENCE_FAILED,
            {"dataset_id": dataset_id, "reason": reason},
        )
