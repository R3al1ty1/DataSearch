import httpx

from lib.core.error_codes import ErrorCode
from lib.services.datasets.enrichment.exceptions import (
    EnrichmentRateLimited,
    EnrichmentSourceError,
    to_enrichment_error,
)
from lib.services.datasets.ml.exceptions import EmbeddingEncodingError


def test_enrichment_http_429_maps_to_rate_limited_error():
    request = httpx.Request("GET", "https://example.test/datasets")
    response = httpx.Response(429, request=request)
    source_error = httpx.HTTPStatusError(
        "Too many requests",
        request=request,
        response=response,
    )

    error = to_enrichment_error("zenodo", "fetch_and_store", source_error)

    assert isinstance(error, EnrichmentRateLimited)
    assert error.status_code == 429
    assert error.error_code == ErrorCode.ENRICHMENT_RATE_LIMITED
    assert error.details == {"source": "zenodo", "stage": "fetch_and_store"}


def test_enrichment_http_error_maps_to_source_error():
    source_error = httpx.ConnectError("connection failed")

    error = to_enrichment_error("datagov", "fetch_and_store", source_error)

    assert isinstance(error, EnrichmentSourceError)
    assert error.status_code == 502
    assert error.error_code == ErrorCode.ENRICHMENT_SOURCE_ERROR
    assert error.details == {
        "source": "datagov",
        "stage": "fetch_and_store",
        "reason": "connection failed",
    }


def test_embedding_encoding_error_has_standard_contract_fields():
    error = EmbeddingEncodingError(text_count=3, reason="model unavailable")

    assert error.status_code == 503
    assert error.error_code == ErrorCode.EMBEDDING_ENCODING_FAILED
    assert error.details == {"text_count": 3, "reason": "model unavailable"}
