"""Unit tests for LinkCheckerService."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx

from lib.services.datasets.validation.link_checker import (
    LinkCheckerService,
    LinkCheckResult,
)


def _make_settings(
    timeout: float = 10.0,
    concurrency: int = 20,
    domain_rps: dict | None = None,
    default_rps: float = 1.0,
):
    s = MagicMock()
    s.LINK_CHECK_TIMEOUT_SECONDS = timeout
    s.LINK_CHECK_MAX_CONCURRENCY = concurrency
    s.LINK_CHECK_DOMAIN_RPS = domain_rps or {"huggingface.co": 0.5, "kaggle.com": 0.3}
    s.LINK_CHECK_DEFAULT_RPS = default_rps
    return s


def _make_service(**kwargs) -> LinkCheckerService:
    return LinkCheckerService(settings=_make_settings(**kwargs), logger=MagicMock())


def _mock_response(status_code: int) -> MagicMock:
    r = MagicMock()
    r.status_code = status_code
    return r


class TestCheckUrl:
    def test_200_is_reachable(self):
        svc = _make_service()
        dataset_id = uuid4()

        async def _run():
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.head = AsyncMock(return_value=_mock_response(200))
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client
                return await svc.check_url(dataset_id, "https://example.com/data")

        result = asyncio.run(_run())
        assert result.is_reachable is True
        assert result.http_status == 200
        assert result.error_type is None

    def test_301_redirect_is_reachable(self):
        svc = _make_service()

        async def _run():
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.head = AsyncMock(return_value=_mock_response(301))
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client
                return await svc.check_url(uuid4(), "https://example.com/data")

        result = asyncio.run(_run())
        assert result.is_reachable is True
        assert result.error_type is None

    def test_404_is_not_reachable(self):
        svc = _make_service()

        async def _run():
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.head = AsyncMock(return_value=_mock_response(404))
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client
                return await svc.check_url(uuid4(), "https://example.com/data")

        result = asyncio.run(_run())
        assert result.is_reachable is False
        assert result.error_type == "http_error"
        assert result.http_status == 404

    def test_500_is_not_reachable(self):
        svc = _make_service()

        async def _run():
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.head = AsyncMock(return_value=_mock_response(500))
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client
                return await svc.check_url(uuid4(), "https://example.com/data")

        result = asyncio.run(_run())
        assert result.is_reachable is False
        assert result.error_type == "http_error"

    def test_429_is_reachable(self):
        svc = _make_service()

        async def _run():
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.head = AsyncMock(return_value=_mock_response(429))
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client
                return await svc.check_url(uuid4(), "https://huggingface.co/datasets/test")

        result = asyncio.run(_run())
        assert result.is_reachable is True
        assert result.error_type is None

    def test_timeout_returns_not_reachable(self):
        svc = _make_service()

        async def _run():
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.head = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client
                return await svc.check_url(uuid4(), "https://example.com/data")

        result = asyncio.run(_run())
        assert result.is_reachable is False
        assert result.error_type == "timeout"
        assert result.http_status is None

    def test_connect_error_returns_dns_error(self):
        svc = _make_service()

        async def _run():
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.head = AsyncMock(side_effect=httpx.ConnectError("failed"))
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client
                return await svc.check_url(uuid4(), "https://nonexistent.example.com/data")

        result = asyncio.run(_run())
        assert result.is_reachable is False
        assert result.error_type == "dns_error"

    def test_result_contains_dataset_id_and_url(self):
        svc = _make_service()
        dataset_id = uuid4()
        url = "https://example.com/dataset"

        async def _run():
            with patch("httpx.AsyncClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.head = AsyncMock(return_value=_mock_response(200))
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client
                return await svc.check_url(dataset_id, url)

        result = asyncio.run(_run())
        assert result.dataset_id == dataset_id
        assert result.url == url
        assert result.duration_ms >= 0


class TestCheckBatch:
    def test_returns_result_for_each_dataset(self):
        svc = _make_service()
        datasets = [(uuid4(), f"https://example.com/{i}") for i in range(5)]

        async def _run():
            with patch.object(svc, "check_url", new_callable=AsyncMock) as mock_check:
                mock_check.side_effect = lambda did, url: LinkCheckResult(
                    dataset_id=did, url=url, is_reachable=True,
                    http_status=200, error_type=None, duration_ms=50
                )
                return await svc.check_batch(datasets)

        results = asyncio.run(_run())
        assert len(results) == 5

    def test_semaphore_limits_concurrency(self):
        svc = _make_service(concurrency=2)
        active_count = {"current": 0, "max": 0}

        async def slow_check(did, url):
            active_count["current"] += 1
            active_count["max"] = max(active_count["max"], active_count["current"])
            await asyncio.sleep(0.01)
            active_count["current"] -= 1
            return LinkCheckResult(
                dataset_id=did, url=url, is_reachable=True,
                http_status=200, error_type=None, duration_ms=10
            )

        datasets = [(uuid4(), f"https://example.com/{i}") for i in range(6)]

        async def _run():
            with patch.object(svc, "check_url", side_effect=slow_check):
                return await svc.check_batch(datasets)

        asyncio.run(_run())
        assert active_count["max"] <= 2
