from datetime import datetime

from lib.core.container import container
from lib.core.uow import UnitOfWork
from lib.services.datasets.enrichment.world_bank_ddh_parser.client import (
    WorldBankDDHClient,
)
from lib.services.datasets.enrichment.world_bank_ddh_parser.mapper import (
    map_world_bank_ddh_to_dataset,
)


class WorldBankDDHProcessor:
    def __init__(self, client: WorldBankDDHClient):
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

        async for batch in self.client.fetch_latest_datasets(
            limit=limit,
            batch_size=batch_size,
            min_updated=min_updated,
        ):
            datasets = [map_world_bank_ddh_to_dataset(record) for record in batch]
            saved = await uow.datasets.bulk_upsert(datasets)
            await uow.commit()

            total_fetched += len(batch)
            total_saved += saved

            self.logger.info(
                f"Processed World Bank DDH batch: {len(batch)} datasets, "
                f"inserted/updated: {saved}"
            )

        return total_fetched, total_saved
