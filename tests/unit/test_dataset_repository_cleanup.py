"""Unit tests for DatasetRepository cleanup methods."""
import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from lib.services.datasets.repository import DatasetRepository
from lib.services.datasets.validation.link_checker import LinkCheckResult


def _make_repo() -> DatasetRepository:
    return DatasetRepository()


def _make_link_result(dataset_id=None, is_reachable=True, error_type=None):
    return LinkCheckResult(
        dataset_id=dataset_id or uuid4(),
        url="https://example.com/data",
        is_reachable=is_reachable,
        http_status=200 if is_reachable else 404,
        error_type=error_type,
        duration_ms=50,
    )


class TestGetStaleForValidation:
    def test_query_filters_active_and_stale(self):
        repo = _make_repo()

        async def _run():
            session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = []
            session.execute = AsyncMock(return_value=mock_result)
            return await repo.get_stale_for_validation(session, batch_size=50, stale_after_hours=24)

        result = asyncio.run(_run())
        assert result == []

    def test_returns_list_of_datasets(self):
        repo = _make_repo()
        fake_datasets = [MagicMock(), MagicMock()]

        async def _run():
            session = AsyncMock()
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = fake_datasets
            session.execute = AsyncMock(return_value=mock_result)
            return await repo.get_stale_for_validation(session, batch_size=10)

        result = asyncio.run(_run())
        assert len(result) == 2


class TestBulkUpdateCheckResults:
    def test_returns_correct_counts(self):
        repo = _make_repo()
        results = [
            _make_link_result(is_reachable=True),
            _make_link_result(is_reachable=False, error_type="http_error"),
            _make_link_result(is_reachable=False, error_type="timeout"),
        ]

        async def _run():
            session = AsyncMock()
            session.execute = AsyncMock()
            session.flush = AsyncMock()
            return await repo.bulk_update_check_results(session, results)

        total, deactivated = asyncio.run(_run())
        assert total == 3
        assert deactivated == 2

    def test_no_deactivated_when_all_reachable(self):
        repo = _make_repo()
        results = [_make_link_result(is_reachable=True) for _ in range(3)]

        async def _run():
            session = AsyncMock()
            session.execute = AsyncMock()
            session.flush = AsyncMock()
            return await repo.bulk_update_check_results(session, results)

        total, deactivated = asyncio.run(_run())
        assert total == 3
        assert deactivated == 0

    def test_all_deactivated_when_none_reachable(self):
        repo = _make_repo()
        results = [_make_link_result(is_reachable=False, error_type="http_error") for _ in range(4)]

        async def _run():
            session = AsyncMock()
            session.execute = AsyncMock()
            session.flush = AsyncMock()
            return await repo.bulk_update_check_results(session, results)

        total, deactivated = asyncio.run(_run())
        assert total == 4
        assert deactivated == 4

    def test_calls_session_execute_for_last_checked_update(self):
        repo = _make_repo()
        results = [_make_link_result(is_reachable=True)]

        async def _run():
            session = AsyncMock()
            session.execute = AsyncMock()
            session.flush = AsyncMock()
            await repo.bulk_update_check_results(session, results)
            return session.execute.call_count

        call_count = asyncio.run(_run())
        assert call_count >= 1

    def test_deactivated_triggers_second_execute(self):
        repo = _make_repo()
        results = [_make_link_result(is_reachable=False, error_type="http_error")]

        async def _run():
            session = AsyncMock()
            session.execute = AsyncMock()
            session.flush = AsyncMock()
            await repo.bulk_update_check_results(session, results)
            return session.execute.call_count

        call_count = asyncio.run(_run())
        assert call_count == 2
