import asyncio
from datetime import datetime

from celery import shared_task

from lib.core.container import container
from lib.repositories import DatasetRepository, EnrichmentLogRepository
from lib.models import EnrichmentStage, EnrichmentResult
from lib.services.enrichment.kaggle_parser import (
    KaggleClient,
    map_meta_to_dataset,
    map_enriched_to_dataset
)


@shared_task(name="kaggle.seed_initial")
def seed_initial_datasets(
    batch_size: int = 1000,
    force_redownload: bool = False
):
    """
    Phase 1: Seed database with minimal metadata from Meta Kaggle CSV.

    This task downloads the Meta Kaggle CSV and populates the database
    with minimal dataset information (MINIMAL status).
    Should be run once initially, then occasionally to refresh.
    """
    logger = container.logger
    logger.info(
        f"Starting Kaggle seed: batch_size={batch_size}, "
        f"force={force_redownload}"
    )

    async def _seed():
        kaggle_client = KaggleClient()
        total_processed = 0
        total_inserted = 0

        async with container.db.get_session_generator() as session:
            dataset_repo = DatasetRepository(session)

            async for batch in kaggle_client.fetch_initial_seed(
                batch_size=batch_size,
                force_redownload=force_redownload
            ):
                datasets = [map_meta_to_dataset(dto) for dto in batch]
                inserted = await dataset_repo.bulk_upsert(datasets)
                await dataset_repo.commit()

                total_processed += len(batch)
                total_inserted += inserted

                logger.info(
                    f"Processed batch: {len(batch)} datasets, "
                    f"inserted/updated: {inserted}"
                )

        return total_processed, total_inserted

    processed, inserted = asyncio.run(_seed())
    logger.info(
        f"Seed completed: {processed} processed, {inserted} inserted/updated"
    )

    return {
        "total_processed": processed,
        "total_inserted": inserted,
        "source": "kaggle_meta_csv"
    }


@shared_task(name="kaggle.enrich_pending")
def enrich_pending_datasets(batch_size: int = 50):
    """
    Phase 2: Enrich pending datasets with detailed metadata from Kaggle API.

    This task fetches datasets with MINIMAL or PENDING status and enriches
    them with full metadata from the Kaggle API.
    Should be run periodically (e.g., every hour).
    """
    logger = container.logger
    logger.info(f"Starting Kaggle enrichment: batch_size={batch_size}")

    async def _enrich():
        kaggle_client = KaggleClient()
        total_enriched = 0
        total_failed = 0

        async with container.db.get_session_generator() as session:
            dataset_repo = DatasetRepository(session)
            log_repo = EnrichmentLogRepository(session)

            pending = await dataset_repo.get_pending_for_enrichment(
                source_name='kaggle',
                limit=batch_size
            )

            if not pending:
                logger.info("No pending datasets found")
                return 0, 0

            logger.info(f"Found {len(pending)} datasets to enrich")

            for dataset in pending:
                start_time = datetime.utcnow()

                try:
                    await dataset_repo.mark_enriching(dataset.id)
                    await dataset_repo.commit()

                    ref = dataset.source_meta.get('ref')
                    if not ref:
                        csv_id = dataset.source_meta.get('csv_id')
                        ref = str(csv_id) if csv_id else dataset.external_id

                    enriched_dto = await kaggle_client.enrich_dataset_by_ref(
                        ref
                    )

                    if enriched_dto:
                        enriched_dataset = map_enriched_to_dataset(
                            enriched_dto
                        )
                        enriched_dataset.id = dataset.id

                        await dataset_repo.upsert(enriched_dataset)
                        await dataset_repo.mark_enriched(dataset.id)

                        duration_ms = int(
                            (datetime.utcnow() - start_time).total_seconds()
                            * 1000
                        )

                        await log_repo.log_enrichment(
                            dataset_id=dataset.id,
                            stage=EnrichmentStage.API_METADATA,
                            result=EnrichmentResult.SUCCESS,
                            attempt_number=dataset.enrichment_attempts + 1,
                            duration_ms=duration_ms
                        )

                        await dataset_repo.commit()
                        total_enriched += 1

                        logger.info(
                            f"Enriched dataset {dataset.external_id} "
                            f"({duration_ms}ms)"
                        )
                    else:
                        await dataset_repo.mark_failed(
                            dataset.id,
                            "Failed to fetch from API"
                        )

                        await log_repo.log_enrichment(
                            dataset_id=dataset.id,
                            stage=EnrichmentStage.API_METADATA,
                            result=EnrichmentResult.FAILED,
                            attempt_number=dataset.enrichment_attempts + 1,
                            error_message="Failed to fetch from API",
                            error_type="FetchError"
                        )

                        await dataset_repo.commit()
                        total_failed += 1

                        logger.warning(
                            f"Failed to enrich {dataset.external_id}"
                        )

                except Exception as e:
                    error_msg = str(e)
                    error_type = type(e).__name__

                    if "429" in error_msg or "rate" in error_msg.lower():
                        await log_repo.log_enrichment(
                            dataset_id=dataset.id,
                            stage=EnrichmentStage.API_METADATA,
                            result=EnrichmentResult.RATE_LIMITED,
                            attempt_number=dataset.enrichment_attempts + 1,
                            error_message=error_msg,
                            error_type=error_type
                        )
                        await dataset_repo.commit()

                        logger.warning(
                            f"Rate limited on {dataset.external_id}, "
                            f"stopping batch"
                        )
                        break
                    else:
                        await dataset_repo.mark_failed(
                            dataset.id,
                            error_msg
                        )

                        await log_repo.log_enrichment(
                            dataset_id=dataset.id,
                            stage=EnrichmentStage.API_METADATA,
                            result=EnrichmentResult.FAILED,
                            attempt_number=dataset.enrichment_attempts + 1,
                            error_message=error_msg,
                            error_type=error_type
                        )

                        await dataset_repo.commit()
                        total_failed += 1

                        logger.error(
                            f"Error enriching {dataset.external_id}: {e}"
                        )

                await asyncio.sleep(1.0)

        return total_enriched, total_failed

    enriched, failed = asyncio.run(_enrich())
    logger.info(
        f"Enrichment completed: {enriched} enriched, {failed} failed"
    )

    return {
        "enriched": enriched,
        "failed": failed,
        "source": "kaggle"
    }


@shared_task(name="kaggle.fetch_latest")
def fetch_latest_datasets(limit: int = 100, sort_by: str = 'updated'):
    """
    Phase 3: Fetch latest datasets from Kaggle API for incremental updates.

    This task fetches the newest/updated datasets directly from the API
    and adds them to the database.
    Should be run daily for incremental updates.
    """
    logger = container.logger
    logger.info(
        f"Starting Kaggle latest fetch: limit={limit}, sort_by={sort_by}"
    )

    async def _fetch_latest():
        kaggle_client = KaggleClient()
        total_processed = 0
        total_inserted = 0

        async with container.db.get_session_generator() as session:
            dataset_repo = DatasetRepository(session)

            async for batch in kaggle_client.fetch_latest_datasets(
                limit=limit,
                sort_by=sort_by
            ):
                datasets = [
                    map_enriched_to_dataset(dto) for dto in batch
                ]
                inserted = await dataset_repo.bulk_upsert(datasets)
                await dataset_repo.commit()

                total_processed += len(batch)
                total_inserted += inserted

                logger.info(
                    f"Processed batch: {len(batch)} datasets, "
                    f"inserted/updated: {inserted}"
                )

        return total_processed, total_inserted

    processed, inserted = asyncio.run(_fetch_latest())
    logger.info(
        f"Latest fetch completed: {processed} processed, "
        f"{inserted} inserted/updated"
    )

    return {
        "total_processed": processed,
        "total_inserted": inserted,
        "source": "kaggle_api"
    }
