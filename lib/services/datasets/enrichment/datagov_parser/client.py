import asyncio
from datetime import datetime, timezone
from typing import AsyncGenerator

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from lib.core.constants import ExternalAPIUrls
from lib.core.container import container


class DataGovClient:
    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = 30.0,
        min_interval_seconds: float = 1.0,
    ):
        self.headers = {"User-Agent": "TDR-Dataset-Discovery/1.0"}
        self.timeout = timeout
        self.min_interval_seconds = min_interval_seconds
        self._logger = container.logger

        key = api_key or container.settings.DATAGOV_API_KEY
        if key:
            self.headers["X-Api-Key"] = key

    async def fetch_latest_datasets(
        self,
        limit: int = 1000,
        batch_size: int = 100,
        min_harvested: datetime | None = None,
        query: str = "",
    ) -> AsyncGenerator[list[dict], None]:
        page_size = self._page_size(batch_size)
        fetched_count = 0
        after = None

        async with httpx.AsyncClient() as client:
            while fetched_count < limit:
                payload = await self._fetch_page(
                    client,
                    query=query,
                    page_size=page_size,
                    after=after,
                )
                records = self._extract_records(payload)
                if not records:
                    break

                batch, should_stop = self._filter_records(records, min_harvested)
                if batch:
                    remaining = limit - fetched_count
                    batch = batch[:remaining]
                    yield batch
                    fetched_count += len(batch)

                after = self._next_cursor(payload)
                if should_stop or not after or fetched_count >= limit:
                    break

                await asyncio.sleep(self.min_interval_seconds)

    def _page_size(self, batch_size: int) -> int:
        return min(max(batch_size, 1), 100)

    def _extract_records(self, payload: object) -> list[dict]:
        if not isinstance(payload, dict):
            return []
        results = payload.get("results")
        if not isinstance(results, list):
            return []
        return [item for item in results if isinstance(item, dict)]

    def _next_cursor(self, payload: object) -> str | None:
        if not isinstance(payload, dict):
            return None
        after = payload.get("after")
        return after if isinstance(after, str) and after else None

    def _filter_records(
        self,
        records: list[dict],
        min_harvested: datetime | None,
    ) -> tuple[list[dict], bool]:
        if not min_harvested:
            return records, False

        batch = []
        for record in records:
            harvested_at = self._record_harvested_at(record)
            if harvested_at and harvested_at < min_harvested:
                return batch, True
            batch.append(record)

        return batch, False

    def _record_harvested_at(self, record: dict) -> datetime | None:
        return self._parse_datetime(record.get("last_harvested_date"))

    def _parse_datetime(self, value: object) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        query: str,
        page_size: int,
        after: str | None,
    ) -> object:
        params = {
            "q": query,
            "sort": "last_harvested_date",
            "per_page": page_size,
        }
        if after:
            params["after"] = after

        self._logger.info(
            f"Fetching Data.gov datasets: size={page_size}, after={after}"
        )
        response = await client.get(
            ExternalAPIUrls.DATAGOV_CATALOG_SEARCH,
            params=params,
            headers=self.headers,
            timeout=self.timeout,
        )
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                await asyncio.sleep(int(retry_after))
        response.raise_for_status()
        return response.json()
