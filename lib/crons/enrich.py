"""Enrichment tasks for dataset ingestion and embedding generation."""
import asyncio
from datetime import datetime, timedelta

from celery import shared_task

from lib.core.container import container


@shared_task(name="enrich.fetch_hf_datasets")
def fetch_hf_datasets(limit: int = 1000, days_back: int = 1):
    """
    Fetch latest datasets from HuggingFace.

    Args:
        limit: Maximum number of datasets to fetch
        days_back: Fetch datasets modified in the last N days
    """
    logger = container.logger
    logger.info(f"Starting HuggingFace dataset fetch: limit={limit}, days_back={days_back}")

    min_date = datetime.utcnow() - timedelta(days=days_back)

    async def _fetch():
        hf_client = container.hf_client
        total_fetched = 0

        async for batch in hf_client.fetch_latest_datasets(
            limit=limit,
            batch_size=1000,
            min_last_modified=min_date
        ):
            logger.info(f"Received batch of {len(batch)} datasets from HuggingFace")
            total_fetched += len(batch)

        return total_fetched

    total = asyncio.run(_fetch())
    logger.info(f"Completed HuggingFace fetch: {total} datasets retrieved")
    return {"total_fetched": total, "source": "huggingface"}


@shared_task(name="enrich.fetch_kaggle_seed")
def fetch_kaggle_seed(batch_size: int = 1000, force_redownload: bool = False):
    """
    DEPRECATED: Use kaggle.seed_initial instead.

    Kept for backwards compatibility.
    """
    from lib.services.enrichment.kaggle_parser.background import (
        seed_initial_datasets
    )
    return seed_initial_datasets(batch_size, force_redownload)


@shared_task(name="enrich.fetch_kaggle_latest")
def fetch_kaggle_latest(limit: int = 100, sort_by: str = 'updated'):
    """
    DEPRECATED: Use kaggle.fetch_latest instead.

    Kept for backwards compatibility.
    """
    from lib.services.enrichment.kaggle_parser.background import (
        fetch_latest_datasets
    )
    return fetch_latest_datasets(limit, sort_by)


@shared_task(name="enrich.enrich_kaggle_datasets")
def enrich_kaggle_datasets(batch_size: int = 50):
    """
    DEPRECATED: Use kaggle.enrich_pending instead.

    Kept for backwards compatibility.
    """
    from lib.services.enrichment.kaggle_parser.background import (
        enrich_pending_datasets
    )
    return enrich_pending_datasets(batch_size)


@shared_task(name="enrich.generate_embeddings")
def generate_embeddings(dataset_ids: list[str]):
    """
    Generate embeddings for datasets.
    """
    logger = container.logger
    logger.info(f"Generating embeddings for {len(dataset_ids)} datasets")

    # TODO: Implement embedding generation logic

    logger.info("Embedding generation completed")
    return {"processed": len(dataset_ids)}
