import asyncio
from datetime import datetime, timezone
from typing import AsyncGenerator

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from lib.core.constants import ExternalAPIUrls
from lib.core.container import container


class WorldBankDDHClient:
    def __init__(
        self,
        timeout: float = 30.0,
        min_interval_seconds: float = 1.0,
        max_schema_resources: int = 3,
    ):
        self.headers = {"User-Agent": "TDR-Dataset-Discovery/1.0"}
        self.timeout = timeout
        self.min_interval_seconds = min_interval_seconds
        self.max_schema_resources = max(max_schema_resources, 0)
        self._logger = container.logger

    async def fetch_latest_datasets(
        self,
        limit: int = 1000,
        batch_size: int = 100,
        min_updated: datetime | None = None,
    ) -> AsyncGenerator[list[dict], None]:
        page_size = self._page_size(batch_size)
        fetched_count = 0
        skip = 0

        async with httpx.AsyncClient() as client:
            while fetched_count < limit:
                payload = await self._fetch_datasets_page(client, skip=skip, top=page_size)
                records = self._extract_records(payload)
                if not records:
                    break

                batch = []
                should_stop = False
                for record in records:
                    updated = self._record_updated_at(record)
                    if min_updated and updated and updated < min_updated:
                        should_stop = True
                        break

                    enriched = await self._enrich_record(client, record)
                    batch.append(enriched)

                    if len(batch) + fetched_count >= limit:
                        break

                if batch:
                    yield batch
                    fetched_count += len(batch)

                if should_stop or len(records) < page_size or fetched_count >= limit:
                    break

                skip += page_size
                await asyncio.sleep(self.min_interval_seconds)

    def _page_size(self, batch_size: int) -> int:
        return min(max(batch_size, 1), 100)

    def _extract_records(self, payload: object) -> list[dict]:
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if not isinstance(data, list):
            return []
        return [item for item in data if isinstance(item, dict)]

    def _record_updated_at(self, record: dict) -> datetime | None:
        return self._parse_datetime(
            record.get("modified_on") or record.get("last_updated_date")
        )

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

    async def _enrich_record(
        self,
        client: httpx.AsyncClient,
        record: dict,
    ) -> dict:
        dataset_unique_id = record.get("dataset_unique_id")
        if not isinstance(dataset_unique_id, str) or not dataset_unique_id:
            return record

        detail = await self._fetch_dataset_detail(client, dataset_unique_id)
        if not isinstance(detail, dict):
            return record

        resources_payload = await self._fetch_resources(client, dataset_unique_id)
        resources = self._extract_records(resources_payload)
        if resources:
            detail["resources"] = resources

        schemas = {}
        for resource in resources[: self.max_schema_resources]:
            if not self._supports_resource_metadata(resource):
                continue
            resource_unique_id = resource.get("resource_unique_id")
            if not isinstance(resource_unique_id, str) or not resource_unique_id:
                continue
            metadata = await self._fetch_resource_metadata(client, resource_unique_id)
            if isinstance(metadata, list):
                schemas[resource_unique_id] = [
                    item for item in metadata if isinstance(item, dict)
                ]

        if schemas:
            detail["resource_schemas"] = schemas

        return detail

    def _supports_resource_metadata(self, resource: dict) -> bool:
        format_value = self._resource_format(resource)
        if format_value in {"csv", "json", "jsonl", "tsv", "xls", "xlsx"}:
            return True

        distribution = resource.get("distribution")
        if not isinstance(distribution, dict):
            return False

        is_directory = distribution.get("is_directory")
        return is_directory is True

    def _resource_format(self, resource: dict) -> str | None:
        distribution = resource.get("distribution")
        if not isinstance(distribution, dict):
            distribution = {}

        for value in (
            resource.get("format"),
            distribution.get("format"),
            distribution.get("distribution_format"),
        ):
            if isinstance(value, str) and value.strip():
                normalized = value.strip().lower()
                if "/" in normalized:
                    normalized = normalized.rsplit("/", 1)[1]
                return normalized.removeprefix("vnd.").removesuffix("+json")

        return None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _fetch_datasets_page(
        self,
        client: httpx.AsyncClient,
        skip: int,
        top: int,
    ) -> object:
        self._logger.info(f"Fetching World Bank DDH datasets: skip={skip}, top={top}")
        response = await client.get(
            f"{ExternalAPIUrls.WORLD_BANK_DDH}/datasets",
            params={"skip": skip, "top": top},
            headers=self.headers,
            timeout=self.timeout,
        )
        await self._sleep_on_rate_limit(response)
        response.raise_for_status()
        return response.json()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _fetch_dataset_detail(
        self,
        client: httpx.AsyncClient,
        dataset_unique_id: str,
    ) -> object:
        response = await client.get(
            f"{ExternalAPIUrls.WORLD_BANK_DDH}/datasets/{dataset_unique_id}",
            headers=self.headers,
            timeout=self.timeout,
        )
        await self._sleep_on_rate_limit(response)
        response.raise_for_status()
        return response.json()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _fetch_resources(
        self,
        client: httpx.AsyncClient,
        dataset_unique_id: str,
    ) -> object:
        response = await client.get(
            f"{ExternalAPIUrls.WORLD_BANK_DDH}/resources",
            params={"dataset_unique_id": dataset_unique_id, "skip": 0, "top": 100},
            headers=self.headers,
            timeout=self.timeout,
        )
        await self._sleep_on_rate_limit(response)
        response.raise_for_status()
        return response.json()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _fetch_resource_metadata(
        self,
        client: httpx.AsyncClient,
        resource_unique_id: str,
    ) -> object:
        response = await client.get(
            f"{ExternalAPIUrls.WORLD_BANK_DDH}/resources/{resource_unique_id}/metadata",
            headers=self.headers,
            timeout=self.timeout,
        )
        await self._sleep_on_rate_limit(response)
        if response.status_code in {400, 404, 417}:
            self._logger.warning(
                "Skipping World Bank DDH resource metadata: "
                f"resource_unique_id={resource_unique_id}, "
                f"status_code={response.status_code}"
            )
            return None
        response.raise_for_status()
        return response.json()

    async def _sleep_on_rate_limit(self, response: httpx.Response) -> None:
        if response.status_code != 429:
            return
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            await asyncio.sleep(int(retry_after))
