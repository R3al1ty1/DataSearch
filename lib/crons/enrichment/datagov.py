import asyncio
from datetime import datetime, timedelta, timezone

from celery import shared_task

from lib.core.container import container
from lib.core.task_errors import log_task_error, task_error_result


@shared_task(name="datagov.fetch_datasets")
def fetch_datasets(
    limit: int = 1000,
    days_back: int = 1,
    batch_size: int = 100,
    query: str = "",
):
    logger = container.logger
    logger.info(
        "Starting Data.gov fetch: "
        f"limit={limit}, days_back={days_back}, "
        f"batch_size={batch_size}, query={query!r}"
    )

    min_harvested = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
        if days_back > 0
        else None
    )

    async def _process():
        async with container.uow() as uow:
            return await container.datagov_processor.fetch_and_store(
                uow,
                limit=limit,
                batch_size=batch_size,
                min_harvested=min_harvested,
                query=query,
            )

    try:
        fetched, saved = asyncio.run(_process())
    except Exception as exc:
        log_task_error(logger, "Data.gov fetch", exc)
        return task_error_result("datagov.fetch_datasets", exc)

    logger.info(f"Data.gov fetch completed: {fetched} fetched, {saved} saved")

    return {
        "total_fetched": fetched,
        "total_saved": saved,
        "source": "datagov",
    }
