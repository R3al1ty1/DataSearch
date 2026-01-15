import asyncio
from datetime import datetime, timedelta

from celery import shared_task

from lib.core.container import container


@shared_task(name="hf.fetch_datasets")
def fetch_datasets(limit: int = 1000, days_back: int = 1):
    """Fetches datasets from HuggingFace."""
    logger = container.logger
    logger.info(
        f"Starting HuggingFace fetch: limit={limit}, days_back={days_back}"
    )

    min_date = (
        datetime.utcnow() - timedelta(days=days_back) if days_back > 0 else None
    )

    async def _process():
        async with container.db.get_session() as session:
            return await container.hf_processor.fetch_and_store(
                session,
                limit=limit,
                min_last_modified=min_date
            )

    fetched, inserted = asyncio.run(_process())
    logger.info(
        f"HuggingFace fetch completed: {fetched} fetched, {inserted} saved"
    )

    return {
        "total_fetched": fetched,
        "total_inserted": inserted,
        "source": "huggingface"
    }
