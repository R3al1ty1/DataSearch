from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.container import container
from lib.models import EnrichmentStage, EnrichmentResult
from lib.repositories.dataset import DatasetRepository
from lib.repositories.enrichment_log import EnrichmentLogRepository
from lib.services.enrichment.kaggle_parser.client_kaggle import KaggleClient
from lib.services.enrichment.kaggle_parser.mapper import (
    map_meta_to_dataset,
    map_enriched_to_dataset
)


class KaggleProcessor:
    """Processes Kaggle dataset fetching and enrichment."""

    def __init__(
        self,
        kaggle_client: KaggleClient,
        dataset_repo: DatasetRepository,
        log_repo: EnrichmentLogRepository
    ):
        self.kaggle_client = kaggle_client
        self.dataset_repo = dataset_repo
        self.log_repo = log_repo
        self.logger = container.logger

    async def seed_from_csv(
        self,
        session: AsyncSession,
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
            inserted = await self.dataset_repo.bulk_upsert(session, datasets)
            await self.dataset_repo.commit(session)

            total_processed += len(batch)
            total_inserted += inserted

            self.logger.info(
                f"Processed batch: {len(batch)} datasets, "
                f"inserted/updated: {inserted}"
            )

        return total_processed, total_inserted

    async def enrich_pending(
        self,
        session: AsyncSession,
        batch_size: int = 50
    ) -> tuple[int, int]:
        """Phase 2: Enriches pending datasets via Kaggle API."""
        pending = await self.dataset_repo.get_pending_for_enrichment(
            session,
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
                await self.dataset_repo.mark_enriching(session, dataset.id)
                await self.dataset_repo.commit(session)

                ref = self._extract_dataset_ref(dataset)
                enriched_dto = await self.kaggle_client.enrich_dataset_by_ref(
                    ref
                )

                if enriched_dto:
                    total_enriched += 1
                    await self._save_enriched_dataset(
                        session, dataset, enriched_dto, start_time
                    )
                else:
                    total_failed += 1
                    await self._mark_as_failed(
                        session, dataset, "Failed to fetch from API"
                    )

            except Exception as e:
                error_msg = str(e)

                if "429" in error_msg or "rate" in error_msg.lower():
                    await self._log_rate_limit(session, dataset, error_msg)
                    self.logger.warning(
                        f"Rate limited on {dataset.external_id}, stopping"
                    )
                    break
                else:
                    total_failed += 1
                    await self._mark_as_failed(session, dataset, error_msg)

            await self._rate_limit_delay()

        return total_enriched, total_failed

    async def fetch_latest(
        self,
        session: AsyncSession,
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
            inserted = await self.dataset_repo.bulk_upsert(session, datasets)
            await self.dataset_repo.commit(session)

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
        session: AsyncSession,
        dataset,
        error_message: str
    ) -> None:
        """Marks dataset as failed and log error."""
        await self.dataset_repo.mark_failed(
            session,
            dataset.id,
            error_message
        )

        await self.log_repo.log_enrichment(
            session,
            dataset_id=dataset.id,
            stage=EnrichmentStage.API_METADATA,
            result=EnrichmentResult.FAILED,
            attempt_number=dataset.enrichment_attempts + 1,
            error_message=error_message,
            error_type=type(error_message).__name__
        )

        await self.dataset_repo.commit(session)

        self.logger.warning(f"Failed to enrich {dataset.external_id}")

    async def _log_rate_limit(
        self,
        session: AsyncSession,
        dataset,
        error_msg: str
    ) -> None:
        """Logs rate limit error."""
        await self.log_repo.log_enrichment(
            session,
            dataset_id=dataset.id,
            stage=EnrichmentStage.API_METADATA,
            result=EnrichmentResult.RATE_LIMITED,
            attempt_number=dataset.enrichment_attempts + 1,
            error_message=error_msg,
            error_type="RateLimitError"
        )
        await self.dataset_repo.commit(session)

    async def _rate_limit_delay(self) -> None:
        """Rate limiting delay between requests."""
        import asyncio
        await asyncio.sleep(1.0)

    async def _save_enriched_dataset(
        self,
        session: AsyncSession,
        original_dataset,
        enriched_dto,
        start_time: datetime
    ) -> None:
        """Saves enriched dataset and log success."""
        enriched_dataset = map_enriched_to_dataset(enriched_dto)
        enriched_dataset.id = original_dataset.id

        await self.dataset_repo.upsert(session, enriched_dataset)
        await self.dataset_repo.mark_enriched(session, original_dataset.id)

        duration_ms = int(
            (datetime.utcnow() - start_time).total_seconds() * 1000
        )

        await self.log_repo.log_enrichment(
            session,
            dataset_id=original_dataset.id,
            stage=EnrichmentStage.API_METADATA,
            result=EnrichmentResult.SUCCESS,
            attempt_number=original_dataset.enrichment_attempts + 1,
            duration_ms=duration_ms
        )

        await self.dataset_repo.commit(session)

        self.logger.info(
            f"Enriched dataset {original_dataset.external_id} ({duration_ms}ms)"
        )
