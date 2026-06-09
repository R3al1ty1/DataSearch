from typing import Any

import httpx
from fastapi import status

from lib.core.error_codes import ErrorCode
from lib.core.exceptions import DataSearchError


class EnrichmentError(DataSearchError):
    def __init__(
        self,
        message: str,
        source: str,
        stage: str,
        error_code: ErrorCode,
        status_code: int = status.HTTP_502_BAD_GATEWAY,
        details: dict[str, Any] | None = None,
    ) -> None:
        context = {"source": source, "stage": stage}
        if details:
            context.update(details)
        super().__init__(message, status_code, error_code, context)
        self.source = source
        self.stage = stage


class EnrichmentSourceError(EnrichmentError):
    def __init__(self, source: str, stage: str, reason: str) -> None:
        super().__init__(
            f"{source} enrichment source failed",
            source,
            stage,
            ErrorCode.ENRICHMENT_SOURCE_ERROR,
            details={"reason": reason},
        )


class EnrichmentRateLimited(EnrichmentError):
    def __init__(self, source: str, stage: str, retry_after: int | None = None) -> None:
        details = {"retry_after": retry_after} if retry_after is not None else None
        super().__init__(
            f"{source} enrichment source rate limited",
            source,
            stage,
            ErrorCode.ENRICHMENT_RATE_LIMITED,
            status.HTTP_429_TOO_MANY_REQUESTS,
            details,
        )


class EnrichmentProcessingError(EnrichmentError):
    def __init__(self, source: str, stage: str, reason: str) -> None:
        super().__init__(
            f"{source} enrichment processing failed",
            source,
            stage,
            ErrorCode.ENRICHMENT_PROCESSING_ERROR,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            {"reason": reason},
        )


def is_rate_limit_error(exc: Exception) -> bool:
    if isinstance(exc, EnrichmentRateLimited):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
    return "429" in str(exc) or "rate" in str(exc).lower()


def to_enrichment_error(source: str, stage: str, exc: Exception) -> EnrichmentError:
    if isinstance(exc, EnrichmentError):
        return exc
    if is_rate_limit_error(exc):
        return EnrichmentRateLimited(source, stage)
    if isinstance(exc, httpx.HTTPError):
        return EnrichmentSourceError(source, stage, str(exc))
    return EnrichmentProcessingError(source, stage, str(exc))
