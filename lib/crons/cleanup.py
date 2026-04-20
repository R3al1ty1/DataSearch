import asyncio

from celery import shared_task

from lib.core.container import container


@shared_task(
    name="cleanup.check_inactive_datasets",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    soft_time_limit=3000,
    time_limit=3600,
)
def check_inactive_datasets(self, batch_size: int = 200, stale_after_hours: int = 48) -> dict:
    """Checks active dataset URLs and deactivates unreachable ones."""
    logger = container.logger
    logger.info(f"Starting dataset cleanup: batch_size={batch_size}, stale_after_hours={stale_after_hours}")

    async def _process():
        async with container.db.get_session() as session:
            return await container.cleanup_service.run_cleanup_batch(session, batch_size, stale_after_hours)

    try:
        result = asyncio.run(_process())
        return {"checked": result.checked, "deactivated": result.deactivated, "errors": result.errors}
    except Exception as exc:
        logger.error(f"Cleanup task failed, retrying: attempt={self.request.retries}, error={exc}")
        raise self.retry(exc=exc)
