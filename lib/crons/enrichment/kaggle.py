import asyncio

from celery import shared_task

from lib.core.container import container
from lib.core.task_errors import log_task_error, task_error_result


@shared_task(name="kaggle.seed_initial")
def seed_initial(batch_size: int = 1000, force_redownload: bool = False):
    """Seeds database with minimal metadata from Meta Kaggle CSV."""
    logger = container.logger
    logger.info(
        f"Starting Kaggle seed: batch_size={batch_size}, "
        f"force={force_redownload}"
    )

    async def _process():
        async with container.uow() as uow:
            return await container.kaggle_processor.seed_from_csv(
                uow,
                batch_size=batch_size,
                force_redownload=force_redownload
            )

    try:
        processed, inserted = asyncio.run(_process())
    except Exception as exc:
        log_task_error(logger, "Kaggle seed", exc)
        return task_error_result("kaggle.seed_initial", exc)

    logger.info(
        f"Kaggle seed completed: {processed} processed, {inserted} saved"
    )

    return {
        "total_processed": processed,
        "total_inserted": inserted,
        "source": "kaggle_meta_csv"
    }

@shared_task(name="kaggle.enrich_pending")
def enrich_pending(batch_size: int = 50):
    """Enriches pending datasets with detailed metadata from Kaggle API."""
    logger = container.logger
    logger.info(f"Starting Kaggle enrichment: batch_size={batch_size}")

    async def _process():
        async with container.uow() as uow:
            return await container.kaggle_processor.enrich_pending(
                uow, batch_size=batch_size
            )

    try:
        enriched, failed = asyncio.run(_process())
    except Exception as exc:
        log_task_error(logger, "Kaggle enrichment", exc)
        return task_error_result("kaggle.enrich_pending", exc)

    logger.info(
        f"Kaggle enrichment completed: {enriched} enriched, {failed} failed"
    )

    return {
        "enriched": enriched,
        "failed": failed,
        "source": "kaggle"
    }

@shared_task(name="kaggle.fetch_latest")
def fetch_latest(limit: int = 100, sort_by: str = 'updated'):
    """Fetches latest datasets from Kaggle API for incremental updates."""
    logger = container.logger
    logger.info(
        f"Starting Kaggle latest fetch: limit={limit}, sort_by={sort_by}"
    )

    async def _process():
        async with container.uow() as uow:
            return await container.kaggle_processor.fetch_latest(
                uow,
                limit=limit,
                sort_by=sort_by
            )

    try:
        processed, inserted = asyncio.run(_process())
    except Exception as exc:
        log_task_error(logger, "Kaggle latest fetch", exc)
        return task_error_result("kaggle.fetch_latest", exc)

    logger.info(
        f"Kaggle latest fetch completed: {processed} processed, {inserted} saved"
    )

    return {
        "total_processed": processed,
        "total_inserted": inserted,
        "source": "kaggle_api"
    }
