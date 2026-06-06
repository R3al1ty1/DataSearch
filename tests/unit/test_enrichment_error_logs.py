import asyncio
from types import SimpleNamespace
from uuid import uuid4

from lib.services.datasets.enrichment.exceptions import (
    EnrichmentRateLimited,
    EnrichmentSourceError,
)
from lib.services.datasets.enrichment.kaggle_parser.processor import KaggleProcessor


class DummyDatasets:
    def __init__(self):
        self.failed = None

    async def mark_failed(self, dataset_id, error_message):
        self.failed = (dataset_id, error_message)


class DummyEnrichmentLogs:
    def __init__(self):
        self.entry = None

    async def log_enrichment(self, **kwargs):
        self.entry = kwargs


class DummyUnitOfWork:
    def __init__(self):
        self.datasets = DummyDatasets()
        self.enrichment_logs = DummyEnrichmentLogs()
        self.committed = False

    async def commit(self):
        self.committed = True


def make_dataset():
    return SimpleNamespace(
        id=uuid4(),
        external_id="owner/dataset",
        enrichment_attempts=2,
    )


def make_processor():
    return KaggleProcessor(kaggle_client=SimpleNamespace())


def test_mark_as_failed_logs_datasearch_error_code():
    uow = DummyUnitOfWork()
    dataset = make_dataset()
    error = EnrichmentSourceError("kaggle", "api_metadata", "upstream failed")

    asyncio.run(make_processor()._mark_as_failed(uow, dataset, error))

    assert uow.datasets.failed == (dataset.id, "kaggle enrichment source failed")
    assert uow.enrichment_logs.entry["error_type"] == "ENRICHMENT_SOURCE_ERROR"
    assert uow.enrichment_logs.entry["error_message"] == (
        "kaggle enrichment source failed"
    )
    assert uow.committed is True


def test_log_rate_limit_uses_stable_error_code():
    uow = DummyUnitOfWork()
    dataset = make_dataset()
    error = EnrichmentRateLimited("kaggle", "api_metadata")

    asyncio.run(make_processor()._log_rate_limit(uow, dataset, error))

    assert uow.enrichment_logs.entry["error_type"] == "ENRICHMENT_RATE_LIMITED"
    assert uow.enrichment_logs.entry["error_message"] == (
        "kaggle enrichment source rate limited"
    )
    assert uow.committed is True
