from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container
from lib.repositories.dataset import DatasetRepository
from lib.services.enrichment.hf_parser.client_hf import HuggingFaceClient
from lib.services.enrichment.hf_parser.mapper import map_hf_to_dataset


class HFProcessor:
    """Processes HuggingFace dataset fetching and storage."""

    def __init__(
        self,
        hf_client: HuggingFaceClient,
        dataset_repo: DatasetRepository
    ):
        self.hf_client = hf_client
        self.dataset_repo = dataset_repo
        self.logger = container.logger

    async def fetch_and_store(
        self,
        session: AsyncSession,
        limit: int = 1000,
        min_last_modified: datetime | None = None
    ) -> tuple[int, int]:
        """Fetchse datasets from HuggingFace and store in database."""
        total_fetched = 0
        total_inserted = 0

        async for batch in self.hf_client.fetch_latest_datasets(
            limit=limit,
            batch_size=1000,
            min_last_modified=min_last_modified
        ):
            datasets = [map_hf_to_dataset(dto) for dto in batch]
            inserted = await self.dataset_repo.bulk_upsert(session, datasets)
            await self.dataset_repo.commit(session)

            total_fetched += len(batch)
            total_inserted += inserted

            self.logger.info(
                f"Processed batch: {len(batch)} datasets, "
                f"inserted/updated: {inserted}"
            )

        return total_fetched, total_inserted
