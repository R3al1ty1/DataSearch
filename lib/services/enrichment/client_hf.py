import httpx
import asyncio
from datetime import datetime
from typing import AsyncGenerator
from tenacity import (
    retry, stop_after_attempt, wait_exponential, retry_if_exception_type
)

from lib.core.container import container
from lib.core.constants import ExternalAPIUrls
from lib.schemas.dataset import HFDatasetDTO


class HuggingFaceClient:
    def __init__(self, token: str | None = None, timeout: float = 30.0):
        self.headers = {"User-Agent": "TDR-Dataset-Discovery/1.0"}
        self._logger = container.logger

        api_token = token or container.settings.HF_TOKEN

        if api_token:
            self.headers["Authorization"] = f"Bearer {api_token}"
        else:
            self._logger.warning("No HF Token found! Rate limits will be strict.")

        self.timeout = timeout

    async def fetch_latest_datasets(
        self,
        limit: int = 100,
        batch_size: int = 1000,
        min_last_modified: datetime | None = None
    ) -> AsyncGenerator[list[HFDatasetDTO], None]:
        """Fetches latest datasets from HuggingFace in batches."""
        offset = 0
        fetched_count = 0
        base_params = self._build_base_params(batch_size)

        async with httpx.AsyncClient() as client:
            while fetched_count < limit:
                raw_data = await self._fetch_single_batch(client, base_params, offset)

                if raw_data is None:
                    break

                if self._should_stop_pagination(raw_data, batch_size):
                    break

                batch_dtos, should_stop = self._process_raw_batch(raw_data, min_last_modified)

                if batch_dtos:
                    yield batch_dtos
                    fetched_count += len(batch_dtos)
                elif raw_data:
                    self._logger.warning("All items in batch failed validation. Moving to next page.")

                if should_stop:
                    break

                offset += batch_size
                await asyncio.sleep(0.3)

    def _build_base_params(self, batch_size: int) -> dict:
        """Builds base query parameters for HuggingFace API."""
        return {
            "sort": "lastModified",
            "direction": -1,
            "full": "true",
            "limit": batch_size
        }

    def _process_raw_batch(
        self,
        raw_data: list[dict],
        min_last_modified: datetime | None
    ) -> tuple[list[HFDatasetDTO], bool]:
        """Processes a batch of raw dataset items."""
        batch_dtos = []

        for item in raw_data:
            dto = self._parse_raw_item(item)

            if dto is None:
                continue

            if self._is_dataset_too_old(dto, min_last_modified):
                self._logger.info(f"Reached old dataset ({dto.get_update_time()}). Stopping.")
                return batch_dtos, True

            batch_dtos.append(dto)

        return batch_dtos, False

    def _should_stop_pagination(self, raw_data: list[dict], batch_size: int) -> bool:
        """Checks if pagination should stop based on response size."""
        return not raw_data or len(raw_data) < batch_size

    async def _fetch_single_batch(
        self,
        client: httpx.AsyncClient,
        base_params: dict,
        offset: int
    ) -> list[dict] | None:
        """Fetches a single batch of datasets from HuggingFace API."""
        current_params = base_params.copy()
        current_params["offset"] = offset

        self._logger.info(f"Fetching HF page: offset={offset}")

        try:
            return await self._fetch_page(client, current_params)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self._logger.info("Received 404, no more data available.")
                return None
            raise

    def _parse_raw_item(self, item: dict) -> HFDatasetDTO | None:
        """Parses a single raw dataset item into DTO."""
        try:
            return HFDatasetDTO.model_validate(item)
        except Exception as e:
            item_id = item.get("id", "unknown")
            self._logger.error(f"Failed to parse {item_id}: {e}")
            return None

    def _is_dataset_too_old(
        self, dto: HFDatasetDTO, min_last_modified: datetime | None
    ) -> bool:
        """Checks if dataset is older than the minimum required date."""
        if not min_last_modified:
            return False

        update_time = dto.get_update_time()
        return update_time < min_last_modified

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)),
        reraise=True
    )
    async def _fetch_page(self, client: httpx.AsyncClient, params: dict) -> list[dict]:
        response = await client.get(
            ExternalAPIUrls.HUGGINGFACE_DATASETS,
            params=params,
            headers=self.headers,
            timeout=self.timeout
        )

        if "RateLimit" in response.headers:
            self._logger.debug(f"RateLimit: {response.headers['RateLimit']}")

        response.raise_for_status()
        return response.json()
