from datetime import datetime

from lib.core.container import container
from lib.core.uow import UnitOfWork
from lib.services.datasets.enrichment.exceptions import to_enrichment_error
from lib.services.datasets.enrichment.zenodo_parser.client import ZenodoClient
from lib.services.datasets.enrichment.zenodo_parser.mapper import map_zenodo_to_dataset


class ZenodoProcessor:
    def __init__(self, client: ZenodoClient):
        self.client = client
        self.logger = container.logger

    async def fetch_and_store(
        self,
        uow: UnitOfWork,
        limit: int = 1000,
        batch_size: int = 100,
        min_updated: datetime | None = None,
    ) -> tuple[int, int]:
        total_fetched = 0
        total_saved = 0

        try:
            async for batch in self.client.fetch_latest_datasets(
                limit=limit,
                batch_size=batch_size,
                min_updated=min_updated,
            ):
                datasets = [map_zenodo_to_dataset(record) for record in batch]
                saved = await uow.datasets.bulk_upsert(datasets)
                await uow.commit()

                total_fetched += len(batch)
                total_saved += saved

                self.logger.info(
                    f"Processed Zenodo batch: {len(batch)} datasets, "
                    f"inserted/updated: {saved}"
                )
        except Exception as e:
            raise to_enrichment_error("zenodo", "fetch_and_store", e) from e

        return total_fetched, total_saved
