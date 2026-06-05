from datetime import datetime

from lib.core.container import container
from lib.core.uow import UnitOfWork
from lib.services.datasets.models import EnrichmentStage, EnrichmentResult
from lib.services.datasets.enrichment.kaggle_parser.client_kaggle import KaggleClient
from lib.services.datasets.enrichment.kaggle_parser.mapper import (
    map_meta_to_dataset,
    map_enriched_to_dataset
)


class KaggleProcessor:
    """Processes Kaggle dataset fetching and enrichment."""

    def __init__(
        self,
        kaggle_client: KaggleClient,
    ):
        self.kaggle_client = kaggle_client
        self.logger = container.logger

    async def seed_from_csv(
        self,
        uow: UnitOfWork,
        batch_size: int = 1000,
        force_redownload: bool = False
    ) -> tuple[int, int]:
        """Phase 1: Seeds database from Meta Kaggle CSV."""
        total_processed = 0
        total_inserted = 0

        async for batch in self.kaggle_client.fetch_initial_seed(
            batch_size=batch_size,
            force_redownload=force_redownload
        ):
            datasets = [map_meta_to_dataset(dto) for dto in batch]
            inserted = await uow.datasets.bulk_upsert(datasets)
            await uow.commit()

            total_processed += len(batch)
            total_inserted += inserted

            self.logger.info(
                f"Processed batch: {len(batch)} datasets, "
                f"inserted/updated: {inserted}"
            )

        return total_processed, total_inserted

    async def enrich_pending(
        self,
        uow: UnitOfWork,
        batch_size: int = 50
    ) -> tuple[int, int]:
        """Phase 2: Enriches pending datasets via Kaggle API."""
        pending = await uow.datasets.get_pending_for_enrichment(
            source_name='kaggle',
            limit=batch_size
        )

        if not pending:
            self.logger.info("No pending datasets found")
            return 0, 0

        self.logger.info(f"Found {len(pending)} datasets to enrich")

        total_enriched = 0
        total_failed = 0

        for dataset in pending:
            start_time = datetime.utcnow()

            try:
                await uow.datasets.mark_enriching(dataset.id)
                await uow.commit()

                ref = self._extract_dataset_ref(dataset)
                enriched_dto = await self.kaggle_client.enrich_dataset_by_ref(
                    ref
                )

                if enriched_dto:
                    total_enriched += 1
                    await self._save_enriched_dataset(
                        uow, dataset, enriched_dto, start_time
                    )
                else:
                    total_failed += 1
                    await self._mark_as_failed(
                        uow, dataset, "Failed to fetch from API"
                    )

            except Exception as e:
                error_msg = str(e)

                if "429" in error_msg or "rate" in error_msg.lower():
                    await self._log_rate_limit(uow, dataset, error_msg)
                    self.logger.warning(
                        f"Rate limited on {dataset.external_id}, stopping"
                    )
                    break
                else:
                    total_failed += 1
                    await self._mark_as_failed(uow, dataset, error_msg)

            await self._rate_limit_delay()

        return total_enriched, total_failed

    async def fetch_latest(
        self,
        uow: UnitOfWork,
        limit: int = 100,
        sort_by: str = 'updated'
    ) -> tuple[int, int]:
        """Phase 3: Fetches latest datasets from Kaggle API."""
        total_processed = 0
        total_inserted = 0

        async for batch in self.kaggle_client.fetch_latest_datasets(
            limit=limit,
            sort_by=sort_by
        ):
            datasets = [map_enriched_to_dataset(dto) for dto in batch]
            inserted = await uow.datasets.bulk_upsert(datasets)
            await uow.commit()

            total_processed += len(batch)
            total_inserted += inserted

            self.logger.info(
                f"Processed batch: {len(batch)} datasets, "
                f"inserted/updated: {inserted}"
            )

        return total_processed, total_inserted

    def _extract_dataset_ref(self, dataset) -> str:
        """Extracts dataset reference from metadata."""
        ref = dataset.source_meta.get('ref')
        if not ref:
            csv_id = dataset.source_meta.get('csv_id')
            ref = str(csv_id) if csv_id else dataset.external_id
        return ref

    async def _mark_as_failed(
        self,
        uow: UnitOfWork,
        dataset,
        error_message: str
    ) -> None:
        """Marks dataset as failed and log error."""
        await uow.datasets.mark_failed(
            dataset.id,
            error_message
        )

        await uow.enrichment_logs.log_enrichment(
            dataset_id=dataset.id,
            stage=EnrichmentStage.API_METADATA,
            result=EnrichmentResult.FAILED,
            attempt_number=dataset.enrichment_attempts + 1,
            error_message=error_message,
            error_type=type(error_message).__name__
        )

        await uow.commit()

        self.logger.warning(f"Failed to enrich {dataset.external_id}")

    async def _log_rate_limit(
        self,
        uow: UnitOfWork,
        dataset,
        error_msg: str
    ) -> None:
        """Logs rate limit error."""
        await uow.enrichment_logs.log_enrichment(
            dataset_id=dataset.id,
            stage=EnrichmentStage.API_METADATA,
            result=EnrichmentResult.RATE_LIMITED,
            attempt_number=dataset.enrichment_attempts + 1,
            error_message=error_msg,
            error_type="RateLimitError"
        )
        await uow.commit()

    async def _rate_limit_delay(self) -> None:
        """Rate limiting delay between requests."""
        import asyncio
        await asyncio.sleep(1.0)

    async def _save_enriched_dataset(
        self,
        uow: UnitOfWork,
        original_dataset,
        enriched_dto,
        start_time: datetime
    ) -> None:
        """Saves enriched dataset and log success."""
        enriched_dataset = map_enriched_to_dataset(enriched_dto)
        enriched_dataset.id = original_dataset.id

        await uow.datasets.upsert(enriched_dataset)
        await uow.datasets.mark_enriched(original_dataset.id)

        duration_ms = int(
            (datetime.utcnow() - start_time).total_seconds() * 1000
        )

        await uow.enrichment_logs.log_enrichment(
            dataset_id=original_dataset.id,
            stage=EnrichmentStage.API_METADATA,
            result=EnrichmentResult.SUCCESS,
            attempt_number=original_dataset.enrichment_attempts + 1,
            duration_ms=duration_ms
        )

        await uow.commit()

        self.logger.info(
            f"Enriched dataset {original_dataset.external_id} ({duration_ms}ms)"
        )
