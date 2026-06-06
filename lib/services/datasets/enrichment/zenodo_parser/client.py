import asyncio
from datetime import datetime, timezone
from typing import AsyncGenerator

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from lib.core.constants import ExternalAPIUrls
from lib.core.container import container


class ZenodoClient:
    def __init__(
        self,
        token: str | None = None,
        timeout: float = 30.0,
        min_interval_seconds: float = 1.0,
    ):
        self.headers = {"User-Agent": "TDR-Dataset-Discovery/1.0"}
        self.timeout = timeout
        self.min_interval_seconds = min_interval_seconds
        self._logger = container.logger

        api_token = token or container.settings.ZENODO_ACCESS_TOKEN
        if api_token:
            self.headers["Authorization"] = f"Bearer {api_token}"
        else:
            self._logger.warning("No Zenodo token found. Anonymous page size is limited to 25.")

    async def fetch_latest_datasets(
        self,
        limit: int = 1000,
        batch_size: int = 100,
        min_updated: datetime | None = None,
    ) -> AsyncGenerator[list[dict], None]:
        page_size = self._page_size(batch_size)
        page = 1
        fetched_count = 0

        async with httpx.AsyncClient() as client:
            while fetched_count < limit:
                payload = await self._fetch_page(client, page=page, page_size=page_size)
                records = self._extract_records(payload)
                if not records:
                    break

                batch, should_stop = self._filter_records(records, min_updated)
                if batch:
                    remaining = limit - fetched_count
                    batch = batch[:remaining]
                    yield batch
                    fetched_count += len(batch)

                if should_stop or len(records) < page_size or fetched_count >= limit:
                    break

                page += 1
                await asyncio.sleep(self.min_interval_seconds)

    def _page_size(self, batch_size: int) -> int:
        max_page_size = 100 if "Authorization" in self.headers else 25
        return min(max(batch_size, 1), max_page_size)

    def _extract_records(self, payload: object) -> list[dict]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []

        hits = payload.get("hits")
        if isinstance(hits, dict):
            hit_items = hits.get("hits")
            if isinstance(hit_items, list):
                return [item for item in hit_items if isinstance(item, dict)]

        records = payload.get("records")
        if isinstance(records, list):
            return [item for item in records if isinstance(item, dict)]

        return []

    def _filter_records(
        self,
        records: list[dict],
        min_updated: datetime | None,
    ) -> tuple[list[dict], bool]:
        if not min_updated:
            return records, False

        batch = []
        for record in records:
            updated = self._record_updated_at(record)
            if updated and updated < min_updated:
                return batch, True
            batch.append(record)

        return batch, False

    def _record_updated_at(self, record: dict) -> datetime | None:
        value = record.get("updated") or record.get("modified")
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
        page: int,
        page_size: int,
    ) -> object:
        params = {
            "type": "dataset",
            "sort": "mostrecent",
            "page": page,
            "size": page_size,
        }
        self._logger.info(f"Fetching Zenodo page: page={page}, size={page_size}")
        response = await client.get(
            ExternalAPIUrls.ZENODO_RECORDS,
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
