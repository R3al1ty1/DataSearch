import asyncio

from celery import shared_task

from lib.core.container import container


@shared_task(name="healthcare.refresh_catalog")
def refresh_catalog(
    batch_size: int = 100,
    include_data_dictionaries: bool = True,
):
    logger = container.logger
    logger.info(
        "Starting Data.Healthcare.gov refresh: "
        f"batch_size={batch_size}, "
        f"include_data_dictionaries={include_data_dictionaries}"
    )

    async def _process():
        async with container.uow() as uow:
            return await container.data_healthcare_processor.refresh_catalog(
                uow,
                batch_size=batch_size,
                include_data_dictionaries=include_data_dictionaries,
            )

    fetched, saved = asyncio.run(_process())
    logger.info(
        f"Data.Healthcare.gov refresh completed: {fetched} fetched, {saved} saved"
    )

    return {
        "total_fetched": fetched,
        "total_saved": saved,
        "source": "data_healthcare_gov",
    }
