import asyncio

from celery import shared_task

from lib.core.container import container
from lib.core.task_errors import log_task_error, task_error_result


@shared_task(name="search.compute_static_scores")
def compute_static_scores():
    """Recomputes static_score and component sub-scores for all active enriched datasets."""
    logger = container.logger
    logger.info("Starting static score computation")

    async def _run():
        async with container.uow() as uow:
            return await container.static_score_service.compute_all(uow)

    try:
        updated = asyncio.run(_run())
    except Exception as exc:
        log_task_error(logger, "Static score computation", exc)
        return task_error_result("search.compute_static_scores", exc)

    logger.info(f"Static score computation completed: {updated} datasets updated")
    return {"updated": updated}
