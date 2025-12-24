import asyncio
from typing import AsyncGenerator

from lib.core.container import container
from lib.schemas.dataset import KaggleEnrichedDatasetDTO
from ..models import APIConsts
from ..utils import initialize_kaggle_api


class KaggleAPIClient:
    """Handles Kaggle API operations for detailed metadata."""

    def __init__(
        self, throttle_delay: float = APIConsts.DEFAULT_THROTTLE_DELAY
    ):
        self._logger = container.logger
        self.throttle_delay = throttle_delay
        self.api = initialize_kaggle_api()

    async def fetch_single_dataset(
        self, dataset_ref: str
    ) -> KaggleEnrichedDatasetDTO | None:
        """Fetches single dataset with full metadata."""
        try:
            dataset_obj = await self._search_dataset_by_ref(dataset_ref)

            if dataset_obj:
                return self._convert_to_dto(dataset_obj)

            return None

        except Exception as e:
            self._logger.error(f"Failed to fetch {dataset_ref}: {e}")
            return None

    async def fetch_latest_datasets(
        self,
        limit: int,
        sort_by: str = 'updated'
    ) -> AsyncGenerator[list[KaggleEnrichedDatasetDTO], None]:
        """Fetches latest datasets with full metadata."""
        fetched_count = 0
        page = 1

        while fetched_count < limit:
            datasets_page = await self._fetch_dataset_list_page(
                page=page, sort_by=sort_by
            )

            if not datasets_page:
                break

            batch = await self._enrich_datasets_batch(
                datasets_page=datasets_page,
                max_count=limit - fetched_count
            )

            if batch:
                fetched_count += len(batch)
                yield batch

            page += 1

            if len(datasets_page) < APIConsts.DEFAULT_PAGE_SIZE:
                break

    async def _search_dataset_by_ref(self, dataset_ref: str) -> object | None:
        """Searches for a dataset by exact ref match."""
        loop = asyncio.get_event_loop()

        dataset_list = await loop.run_in_executor(
            None,
            lambda: self.api.dataset_list(search=dataset_ref, page=1)
        )

        if not dataset_list:
            return None

        for dataset in dataset_list:
            if dataset.ref == dataset_ref:
                return dataset

        return None

    async def _fetch_dataset_list_page(self, page: int, sort_by: str) -> list[dict]:
        """Fetches single page of dataset list."""
        self._logger.info(f"Fetching page {page}")

        loop = asyncio.get_event_loop()

        try:
            datasets = await loop.run_in_executor(
                None,
                lambda: self.api.dataset_list(sort_by=sort_by, page=page)
            )
            return datasets or []

        except Exception as e:
            self._logger.error(f"Error fetching page {page}: {e}")
            return []

    async def _enrich_datasets_batch(
        self,
        datasets_page: list[dict],
        max_count: int
    ) -> list[KaggleEnrichedDatasetDTO]:
        """Enriches a batch of datasets with full metadata."""
        batch = []

        for dataset in datasets_page:
            if len(batch) >= max_count:
                break

            try:
                dto = self._convert_to_dto(dataset)
                batch.append(dto)
            except Exception as e:
                self._logger.warning(f"Error converting {dataset.ref}: {e}")

            await asyncio.sleep(self.throttle_delay)

        return batch

    def _convert_to_dto(self, dataset_obj: object) -> KaggleEnrichedDatasetDTO:
        """Converts Kaggle API dataset object to DTO."""
        files_list = self._extract_files_metadata(dataset_obj)

        return KaggleEnrichedDatasetDTO(
            ref=dataset_obj.ref,
            title=dataset_obj.title,
            subtitle=getattr(dataset_obj, 'subtitle', None),
            creatorName=getattr(dataset_obj, 'creator_name', None),
            totalBytes=getattr(dataset_obj, 'total_bytes', 0),
            url=dataset_obj.url,
            createdDate=None,
            lastUpdated=getattr(dataset_obj, 'last_updated', None),
            downloadCount=getattr(dataset_obj, 'download_count', 0),
            voteCount=getattr(dataset_obj, 'vote_count', 0),
            viewCount=getattr(dataset_obj, 'view_count', 0),
            licenseName=getattr(dataset_obj, 'license_name', None),
            description=getattr(dataset_obj, 'description', None),
            data=files_list
        )

    def _extract_files_metadata(self, dataset_obj: object) -> list[dict]:
        """Extracts file metadata from dataset object."""
        if not hasattr(dataset_obj, 'files') or not dataset_obj.files:
            return []

        files_list = []
        for file in dataset_obj.files:
            file_dict = {
                'name': getattr(file, 'name', 'unknown'),
                'size': getattr(file, 'total_bytes', 0),
                'creationDate': getattr(file, 'creation_date', None),
            }

            if hasattr(file, 'columns') and file.columns:
                file_dict['columns'] = [
                    col.name if hasattr(col, 'name') else str(col)
                    for col in file.columns
                ]

            files_list.append(file_dict)

        return files_list
