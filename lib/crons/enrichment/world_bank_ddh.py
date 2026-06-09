import asyncio
from datetime import datetime, timedelta, timezone

from celery import shared_task

from lib.core.container import container
from lib.core.task_errors import log_task_error, task_error_result


@shared_task(name="world_bank_ddh.fetch_datasets")
def fetch_datasets(
    limit: int = 1000,
    days_back: int = 1,
    batch_size: int = 100,
):
    logger = container.logger
    logger.info(
        "Starting World Bank DDH fetch: "
        f"limit={limit}, days_back={days_back}, batch_size={batch_size}"
    )

    min_updated = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
        if days_back > 0
        else None
    )

    async def _process():
        async with container.uow() as uow:
            return await container.world_bank_ddh_processor.fetch_and_store(
                uow,
                limit=limit,
                batch_size=batch_size,
                min_updated=min_updated,
            )

    try:
        fetched, saved = asyncio.run(_process())
    except Exception as exc:
        log_task_error(logger, "World Bank DDH fetch", exc)
        return task_error_result("world_bank_ddh.fetch_datasets", exc)

    logger.info(f"World Bank DDH fetch completed: {fetched} fetched, {saved} saved")

    return {
        "total_fetched": fetched,
        "total_saved": saved,
        "source": "world_bank_ddh",
    }
