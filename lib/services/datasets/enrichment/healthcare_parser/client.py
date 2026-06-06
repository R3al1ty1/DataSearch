import asyncio
from typing import AsyncGenerator

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from lib.core.constants import ExternalAPIUrls
from lib.core.container import container
from lib.services.datasets.schemas import DataHealthcareDatasetDTO


class DataHealthcareClient:
    def __init__(self, timeout: float = 30.0, dictionary_concurrency: int = 3):
        self.headers = {"User-Agent": "TDR-Dataset-Discovery/1.0"}
        self.timeout = timeout
        self._logger = container.logger
        self._dictionary_semaphore = asyncio.Semaphore(dictionary_concurrency)

    async def fetch_datasets(
        self,
        batch_size: int = 100,
        include_data_dictionaries: bool = True,
    ) -> AsyncGenerator[list[DataHealthcareDatasetDTO], None]:
        raw_items = await self._fetch_catalog_items()

        for offset in range(0, len(raw_items), batch_size):
            batch_items = raw_items[offset:offset + batch_size]
            if include_data_dictionaries:
                async with httpx.AsyncClient() as client:
                    await self._enrich_column_names(client, batch_items)

            batch = [
                dto for item in batch_items
                if (dto := self._parse_item(item)) is not None
            ]
            if batch:
                yield batch

    async def _fetch_catalog_items(self) -> list[dict]:
        async with httpx.AsyncClient() as client:
            catalog = await self._get_json(
                client,
                ExternalAPIUrls.DATA_HEALTHCARE_GOV_CATALOG,
            )

        datasets = catalog.get("dataset", [])
        if isinstance(datasets, dict):
            return [item for item in datasets.values() if isinstance(item, dict)]
        if isinstance(datasets, list):
            return [item for item in datasets if isinstance(item, dict)]
        return []

    async def _enrich_column_names(
        self,
        client: httpx.AsyncClient,
        items: list[dict],
    ) -> None:
        tasks = [
            self._fetch_column_names(client, url)
            for item in items
            if (url := self._first_data_dictionary_url(item))
        ]
        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        result_iter = iter(results)

        for item in items:
            if not self._first_data_dictionary_url(item):
                continue
            result = next(result_iter)
            if isinstance(result, Exception):
                self._logger.warning(f"Failed to fetch healthcare data dictionary: {result}")
                continue
            if result:
                item["column_names"] = result

    async def _fetch_column_names(
        self,
        client: httpx.AsyncClient,
        url: str,
    ) -> list[str]:
        if not url.startswith("https://data.healthcare.gov/api/1/metastore/schemas/data-dictionary/items/"):
            return []

        async with self._dictionary_semaphore:
            payload = await self._get_json(client, url)
        data = payload.get("data", payload)
        fields = data.get("fields", []) if isinstance(data, dict) else []
        return [
            str(name)
            for field in fields
            if isinstance(field, dict)
            if (name := field.get("name") or field.get("title"))
        ]

    def _first_data_dictionary_url(self, item: dict) -> str | None:
        distributions = item.get("distribution") or []
        if not isinstance(distributions, list):
            return None

        for distribution in distributions:
            if not isinstance(distribution, dict):
                continue
            url = distribution.get("describedBy")
            if isinstance(url, str):
                return url
        return None

    def _parse_item(self, item: dict) -> DataHealthcareDatasetDTO | None:
        try:
            return DataHealthcareDatasetDTO.model_validate(item)
        except Exception as e:
            item_id = item.get("identifier", "unknown")
            self._logger.error(f"Failed to parse Data.Healthcare.gov dataset {item_id}: {e}")
            return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _get_json(
        self,
        client: httpx.AsyncClient,
        url: str,
    ) -> dict:
        response = await client.get(
            url,
            headers=self.headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()
