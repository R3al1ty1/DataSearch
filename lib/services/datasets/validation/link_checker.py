import asyncio
import logging
import time
from dataclasses import dataclass
from urllib.parse import urlparse
from uuid import UUID

import httpx

from lib.core.config import Settings


@dataclass
class LinkCheckResult:
    dataset_id: UUID
    url: str
    is_reachable: bool
    http_status: int | None
    error_type: str | None  # "timeout" | "dns_error" | "http_error" | None
    duration_ms: int


class _DomainThrottle:
    """Per-domain rate limiter using min-interval between requests."""

    def __init__(self):
        self._last_request: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, domain: str) -> asyncio.Lock:
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()
        return self._locks[domain]

    async def wait(self, domain: str, rps: float) -> None:
        min_interval = 1.0 / rps
        async with self._lock(domain):
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request.get(domain, 0)
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self._last_request[domain] = asyncio.get_event_loop().time()


class LinkCheckerService:
    def __init__(self, settings: Settings, logger: logging.Logger):
        self._timeout = settings.LINK_CHECK_TIMEOUT_SECONDS
        self._max_concurrency = settings.LINK_CHECK_MAX_CONCURRENCY
        self._domain_rps = settings.LINK_CHECK_DOMAIN_RPS
        self._default_rps = settings.LINK_CHECK_DEFAULT_RPS
        self._logger = logger
        self._throttle = _DomainThrottle()

    def _domain_rps_for(self, url: str) -> float:
        domain = urlparse(url).netloc
        return self._domain_rps.get(domain, self._default_rps)

    async def check_url(self, dataset_id: UUID, url: str) -> LinkCheckResult:
        domain = urlparse(url).netloc
        rps = self._domain_rps_for(url)
        await self._throttle.wait(domain, rps)

        start = time.monotonic()
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(self._timeout),
                follow_redirects=True,
            ) as client:
                response = await client.head(url)
            duration_ms = int((time.monotonic() - start) * 1000)

            if response.status_code == 429:
                return LinkCheckResult(
                    dataset_id=dataset_id,
                    url=url,
                    is_reachable=True,
                    http_status=429,
                    error_type=None,
                    duration_ms=duration_ms,
                )
            if response.status_code < 400:
                return LinkCheckResult(
                    dataset_id=dataset_id,
                    url=url,
                    is_reachable=True,
                    http_status=response.status_code,
                    error_type=None,
                    duration_ms=duration_ms,
                )
            return LinkCheckResult(
                dataset_id=dataset_id,
                url=url,
                is_reachable=False,
                http_status=response.status_code,
                error_type="http_error",
                duration_ms=duration_ms,
            )

        except (httpx.TimeoutException, asyncio.TimeoutError):
            duration_ms = int((time.monotonic() - start) * 1000)
            self._logger.warning(f"Link check failed: dataset_id={dataset_id}, url={url}, error=timeout")
            return LinkCheckResult(
                dataset_id=dataset_id,
                url=url,
                is_reachable=False,
                http_status=None,
                error_type="timeout",
                duration_ms=duration_ms,
            )
        except httpx.ConnectError:
            duration_ms = int((time.monotonic() - start) * 1000)
            self._logger.warning(f"Link check failed: dataset_id={dataset_id}, url={url}, error=dns_error")
            return LinkCheckResult(
                dataset_id=dataset_id,
                url=url,
                is_reachable=False,
                http_status=None,
                error_type="dns_error",
                duration_ms=duration_ms,
            )
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            self._logger.warning(f"Link check failed: dataset_id={dataset_id}, url={url}, error={e}")
            return LinkCheckResult(
                dataset_id=dataset_id,
                url=url,
                is_reachable=False,
                http_status=None,
                error_type="http_error",
                duration_ms=duration_ms,
            )

    async def check_batch(self, datasets: list[tuple[UUID, str]]) -> list[LinkCheckResult]:
        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def _check(dataset_id: UUID, url: str) -> LinkCheckResult:
            async with semaphore:
                return await self.check_url(dataset_id, url)

        return list(await asyncio.gather(*[_check(did, url) for did, url in datasets]))
