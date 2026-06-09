from datetime import datetime

from lib.core.container import container
from lib.core.uow import UnitOfWork
from lib.services.datasets.enrichment.exceptions import to_enrichment_error
from lib.services.datasets.enrichment.hf_parser.client_hf import HuggingFaceClient
from lib.services.datasets.enrichment.hf_parser.mapper import map_hf_to_dataset

class HFProcessor:
    """Processes HuggingFace dataset fetching and storage."""

    def __init__(
        self,
        hf_client: HuggingFaceClient,
    ):
        self.hf_client = hf_client
        self.logger = container.logger

    async def fetch_and_store(
        self,
        uow: UnitOfWork,
        limit: int = 1000,
        min_last_modified: datetime | None = None
    ) -> tuple[int, int]:
        """Fetchse datasets from HuggingFace and store in database."""
        total_fetched = 0
        total_inserted = 0

        try:
            async for batch in self.hf_client.fetch_latest_datasets(
                limit=limit,
                batch_size=1000,
                min_last_modified=min_last_modified
            ):
                datasets = [map_hf_to_dataset(dto) for dto in batch]
                inserted = await uow.datasets.bulk_upsert(datasets)
                await uow.commit()

                total_fetched += len(batch)
                total_inserted += inserted

                self.logger.info(
                    f"Processed batch: {len(batch)} datasets, "
                    f"inserted/updated: {inserted}"
                )
        except Exception as e:
            raise to_enrichment_error("huggingface", "fetch_and_store", e) from e

        return total_fetched, total_inserted
