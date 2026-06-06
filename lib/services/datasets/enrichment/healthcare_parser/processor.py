from lib.core.container import container
from lib.core.uow import UnitOfWork
from lib.services.datasets.enrichment.exceptions import to_enrichment_error
from lib.services.datasets.enrichment.healthcare_parser.client import (
    DataHealthcareClient,
)
from lib.services.datasets.enrichment.healthcare_parser.mapper import (
    map_healthcare_to_dataset,
)


class DataHealthcareProcessor:
    def __init__(self, client: DataHealthcareClient):
        self.client = client
        self.logger = container.logger

    async def refresh_catalog(
        self,
        uow: UnitOfWork,
        batch_size: int = 100,
        include_data_dictionaries: bool = True,
    ) -> tuple[int, int]:
        total_fetched = 0
        total_saved = 0

        try:
            async for batch in self.client.fetch_datasets(
                batch_size=batch_size,
                include_data_dictionaries=include_data_dictionaries,
            ):
                datasets = [map_healthcare_to_dataset(dto) for dto in batch]
                saved = await uow.datasets.bulk_upsert(datasets)
                await uow.commit()

                total_fetched += len(batch)
                total_saved += saved

                self.logger.info(
                    f"Processed Data.Healthcare.gov batch: {len(batch)} datasets, "
                    f"inserted/updated: {saved}"
                )
        except Exception as e:
            raise to_enrichment_error("data_healthcare", "refresh_catalog", e) from e

        return total_fetched, total_saved
