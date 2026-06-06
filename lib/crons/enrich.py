import asyncio

from celery import shared_task

from lib.core.container import container
from lib.core.task_errors import log_task_error, task_error_result


@shared_task(name="enrich.generate_embeddings")
def generate_embeddings(batch_size: int = 100):
    """
    Generates embeddings for datasets without them.

    Finds datasets with status ENRICHED but no embedding vector,
    then generates and saves embeddings using the EmbeddingProcessor service.
    Works for datasets from all sources (HuggingFace, Kaggle, etc).
    """
    logger = container.logger
    logger.info(f"Starting embedding generation: batch_size={batch_size}")

    async def _process():
        async with container.uow() as uow:
            return await container.embedding_processor.process_batch(
                uow, batch_size
            )

    try:
        processed, failed = asyncio.run(_process())
    except Exception as exc:
        log_task_error(logger, "Embedding generation", exc)
        return task_error_result("enrich.generate_embeddings", exc)

    logger.info(
        f"Embedding generation completed: "
        f"{processed} processed, {failed} failed"
    )
    return {"processed": processed, "failed": failed}
