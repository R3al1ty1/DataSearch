from datetime import datetime
from typing import AsyncGenerator

from lib.core.container import container
from lib.schemas.dataset import KaggleMetaDatasetDTO, KaggleEnrichedDatasetDTO
from .services.meta_parser import MetaKaggleParser
from .services.api_parser import KaggleAPIClient
from .models import MetaKaggleConsts, APIConsts, ParsingConsts
from .utils import get_csv_path


class KaggleClient:
    """Main orchestrator for Kaggle dataset ingestion."""

    def __init__(
        self,
        cache_dir: str = MetaKaggleConsts.DEFAULT_CACHE_DIR,
        api_throttle_delay: float = APIConsts.DEFAULT_THROTTLE_DELAY
    ):
        """Initializes Kaggle client."""
        self._logger = container.logger
        self.meta_parser = MetaKaggleParser(cache_dir=cache_dir)
        self.api_client = KaggleAPIClient(throttle_delay=api_throttle_delay)
        self._logger.info("KaggleClient initialized")

    async def fetch_initial_seed(
        self,
        batch_size: int = ParsingConsts.DEFAULT_BATCH_SIZE,
        force_redownload: bool = False,
        min_last_activity: datetime | None = None
    ) -> AsyncGenerator[list[KaggleMetaDatasetDTO], None]:
        """Fetches initial seed data from Meta Kaggle CSV."""
        self._logger.info("Starting Phase 1: Initial seed from Meta Kaggle CSV")

        await self.meta_parser.download_if_needed(force=force_redownload)

        total_count = await self.meta_parser.get_total_count()
        self._logger.info(f"Total datasets in Meta Kaggle: {total_count}")

        async for batch in self.meta_parser.parse_csv_batches(
            batch_size=batch_size,
            min_last_activity=min_last_activity
        ):
            yield batch

        self._logger.info("Phase 1 completed: Initial seed ingestion done")

    async def fetch_latest_datasets(
        self,
        limit: int = 100,
        sort_by: str = 'updated'
    ) -> AsyncGenerator[list[KaggleEnrichedDatasetDTO], None]:
        """Fetches latest datasets from Kaggle API."""
        self._logger.info(
            f"Starting Phase 3: Fetching latest {limit} datasets via API "
            f"(sorted by {sort_by})"
        )

        async for batch in self.api_client.fetch_latest_datasets(
            limit=limit,
            sort_by=sort_by
        ):
            yield batch

        self._logger.info("Phase 3 completed: Latest datasets fetched")

    async def enrich_dataset_by_ref(
        self,
        dataset_ref: str
    ) -> KaggleEnrichedDatasetDTO | None:
        """Enriches a single dataset with detailed metadata."""
        self._logger.debug(f"Enriching dataset: {dataset_ref}")

        return await self.api_client.fetch_single_dataset(
            dataset_ref=dataset_ref
        )

    async def check_meta_kaggle_updates(
        self,
        last_download_time: datetime
    ) -> bool:
        """Checks if Meta Kaggle CSV has been updated."""
        return await self.meta_parser.check_for_updates(
            last_download=last_download_time
        )

    async def get_meta_kaggle_stats(self) -> dict:
        """Returns statistics about Meta Kaggle cache."""
        total_count = await self.meta_parser.get_total_count()
        csv_path = get_csv_path(self.meta_parser.cache_dir)

        stats = {
            'total_datasets': total_count,
            'csv_cached': csv_path.exists(),
            'csv_path': str(csv_path),
            'cache_dir': str(self.meta_parser.cache_dir)
        }

        if csv_path.exists():
            stats['csv_size_mb'] = csv_path.stat().st_size / (1024 * 1024)
            stats['csv_modified'] = datetime.fromtimestamp(
                csv_path.stat().st_mtime
            )

        return stats
