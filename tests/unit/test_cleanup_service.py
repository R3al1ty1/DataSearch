"""Unit tests for CleanupService."""
import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from lib.services.datasets.cleanup.service import CleanupBatchResult, CleanupService
from lib.services.datasets.models import EnrichmentResult, EnrichmentStage
from lib.services.datasets.validation.link_checker import LinkCheckResult


def _make_result(dataset_id=None, is_reachable=True, error_type=None):
    return LinkCheckResult(
        dataset_id=dataset_id or uuid4(),
        url="https://example.com/data",
        is_reachable=is_reachable,
        http_status=200 if is_reachable else 404,
        error_type=error_type,
        duration_ms=50,
    )


def _make_service(datasets=None, check_results=None):
    dataset_repo = MagicMock()
    enrichment_log_repo = MagicMock()
    link_checker = MagicMock()

    dataset_repo.get_stale_for_validation = AsyncMock(return_value=datasets or [])
    dataset_repo.bulk_update_check_results = AsyncMock(
        return_value=(len(check_results or []), sum(1 for r in (check_results or []) if not r.is_reachable))
    )
    enrichment_log_repo.log_enrichment = AsyncMock()
    link_checker.check_batch = AsyncMock(return_value=check_results or [])

    svc = CleanupService(
        dataset_repo=dataset_repo,
        enrichment_log_repo=enrichment_log_repo,
        link_checker=link_checker,
        logger=MagicMock(),
    )
    return svc, dataset_repo, enrichment_log_repo, link_checker


class TestRunCleanupBatch:
    def test_returns_zero_when_no_stale_datasets(self):
        svc, *_ = _make_service(datasets=[])

        async def _run():
            session = AsyncMock()
            session.commit = AsyncMock()
            return await svc.run_cleanup_batch(session)

        result = asyncio.run(_run())
        assert result == CleanupBatchResult(checked=0, deactivated=0, errors=0)

    def test_counts_deactivated_correctly(self):
        d1, d2, d3 = MagicMock(), MagicMock(), MagicMock()
        d1.id, d2.id, d3.id = uuid4(), uuid4(), uuid4()
        d1.url = d2.url = d3.url = "https://example.com/data"

        results = [
            _make_result(d1.id, is_reachable=True),
            _make_result(d2.id, is_reachable=False, error_type="http_error"),
            _make_result(d3.id, is_reachable=False, error_type="timeout"),
        ]
        svc, repo, _, checker = _make_service(datasets=[d1, d2, d3], check_results=results)

        async def _run():
            session = AsyncMock()
            session.commit = AsyncMock()
            return await svc.run_cleanup_batch(session)

        result = asyncio.run(_run())
        assert result.checked == 3
        assert result.deactivated == 2

    def test_calls_bulk_update_with_all_results(self):
        dataset = MagicMock()
        dataset.id = uuid4()
        dataset.url = "https://example.com/data"
        check_result = _make_result(dataset.id, is_reachable=False, error_type="http_error")

        svc, repo, _, checker = _make_service(datasets=[dataset], check_results=[check_result])

        async def _run():
            session = AsyncMock()
            session.commit = AsyncMock()
            await svc.run_cleanup_batch(session)
            return session

        asyncio.run(_run())
        repo.bulk_update_check_results.assert_called_once()

    def test_logs_enrichment_for_each_result(self):
        datasets = [MagicMock() for _ in range(3)]
        for d in datasets:
            d.id = uuid4()
            d.url = "https://example.com/data"

        results = [_make_result(d.id) for d in datasets]
        svc, _, log_repo, _ = _make_service(datasets=datasets, check_results=results)

        async def _run():
            session = AsyncMock()
            session.commit = AsyncMock()
            await svc.run_cleanup_batch(session)

        asyncio.run(_run())
        assert log_repo.log_enrichment.call_count == 3

    def test_failed_result_logged_with_failed_enrichment_result(self):
        dataset = MagicMock()
        dataset.id = uuid4()
        dataset.url = "https://example.com/data"
        check_result = _make_result(dataset.id, is_reachable=False, error_type="dns_error")

        svc, _, log_repo, _ = _make_service(datasets=[dataset], check_results=[check_result])

        async def _run():
            session = AsyncMock()
            session.commit = AsyncMock()
            await svc.run_cleanup_batch(session)

        asyncio.run(_run())
        call_kwargs = log_repo.log_enrichment.call_args.kwargs
        assert call_kwargs["result"] == EnrichmentResult.FAILED
        assert call_kwargs["stage"] == EnrichmentStage.LINK_VALIDATION
        assert call_kwargs["error_message"] == "dns_error"


class TestDeactivateDataset:
    def test_calls_bulk_update_and_log(self):
        svc, repo, log_repo, _ = _make_service()
        dataset_id = uuid4()
        check_result = _make_result(dataset_id, is_reachable=False, error_type="http_error")

        async def _run():
            session = AsyncMock()
            session.commit = AsyncMock()
            await svc.deactivate_dataset(session, dataset_id, check_result)

        asyncio.run(_run())
        repo.bulk_update_check_results.assert_called_once()
        log_repo.log_enrichment.assert_called_once()

    def test_logs_with_failed_result(self):
        svc, _, log_repo, _ = _make_service()
        dataset_id = uuid4()
        check_result = _make_result(dataset_id, is_reachable=False, error_type="timeout")

        async def _run():
            session = AsyncMock()
            session.commit = AsyncMock()
            await svc.deactivate_dataset(session, dataset_id, check_result)

        asyncio.run(_run())
        call_kwargs = log_repo.log_enrichment.call_args.kwargs
        assert call_kwargs["result"] == EnrichmentResult.FAILED
        assert call_kwargs["error_message"] == "timeout"
