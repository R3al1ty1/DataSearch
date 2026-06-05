import asyncio

from celery import shared_task

from lib.core.container import container


@shared_task(
    name="search.compute_static_scores",
    autoretry_for=(Exception,),
    max_retries=3,
    default_retry_delay=60,
)
def compute_static_scores():
    """Recomputes static_score and component sub-scores for all active enriched datasets."""
    logger = container.logger
    logger.info("Starting static score computation")

    async def _run():
        async with container.uow() as uow:
            return await container.static_score_service.compute_all(uow)

    updated = asyncio.run(_run())
    logger.info(f"Static score computation completed: {updated} datasets updated")
    return {"updated": updated}
