from datetime import datetime

from lib.core.container import container
from lib.core.uow import UnitOfWork
from lib.services.datasets.enrichment.datagov_parser.client import DataGovClient
from lib.services.datasets.enrichment.datagov_parser.mapper import (
    map_datagov_to_dataset,
)


class DataGovProcessor:
    def __init__(self, client: DataGovClient):
        self.client = client
        self.logger = container.logger

    async def fetch_and_store(
        self,
        uow: UnitOfWork,
        limit: int = 1000,
        batch_size: int = 100,
        min_harvested: datetime | None = None,
        query: str = "",
    ) -> tuple[int, int]:
        total_fetched = 0
        total_saved = 0

        async for batch in self.client.fetch_latest_datasets(
            limit=limit,
            batch_size=batch_size,
            min_harvested=min_harvested,
            query=query,
        ):
            datasets = [map_datagov_to_dataset(record) for record in batch]
            saved = await uow.datasets.bulk_upsert(datasets)
            await uow.commit()

            total_fetched += len(batch)
            total_saved += saved

            self.logger.info(
                f"Processed Data.gov batch: {len(batch)} datasets, "
                f"inserted/updated: {saved}"
            )

        return total_fetched, total_saved
