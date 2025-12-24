import asyncio

from celery import shared_task

from lib.core.container import container


@shared_task(name="enrich.generate_embeddings")
def generate_embeddings(batch_size: int = 100):
    """
    Generates embeddings for datasets without them.

    Finds datasets with status ENRICHED but no embedding vector,
    then generates and saves embeddings using the EmbeddingProcessor service.
    Works for datasets from all sources (HuggingFace, Kaggle, etc).
    """
    from lib.services.ml.embedding_processor import EmbeddingProcessor

    logger = container.logger
    logger.info(f"Starting embedding generation: batch_size={batch_size}")

    async def _process():
        processor = EmbeddingProcessor(
            dataset_repo=container.dataset_repo,
            embedder=container.embedder,
            logger=logger
        )

        async with container.db.get_session() as session:
            return await processor.process_batch(session, batch_size)

    processed, failed = asyncio.run(_process())
    logger.info(
        f"Embedding generation completed: "
        f"{processed} processed, {failed} failed"
    )

    return {"processed": processed, "failed": failed}
