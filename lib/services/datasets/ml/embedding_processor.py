from uuid import UUID

from lib.core.container import container
from lib.core.uow import UnitOfWork
from lib.services.datasets.ml.embedder import EmbeddingService
from lib.services.datasets.ml.exceptions import EmbeddingError, EmbeddingPersistenceError

class EmbeddingProcessor:
    """Handles batch processing of dataset embeddings."""

    def __init__(
        self,
        embedder: EmbeddingService
    ):
        self.embedder = embedder
        self.logger = container.logger

    async def process_batch(
        self, uow: UnitOfWork, batch_size: int = 100
    ) -> tuple[int, int]:
        """Processes a batch of datasets to generate embeddings."""
        datasets = await uow.datasets.get_for_embedding_generation(limit=batch_size)

        if not datasets:
            self.logger.info("No datasets found for embedding generation")
            return 0, 0

        self.logger.info(f"Found {len(datasets)} datasets to process")

        dataset_metadata = []
        dataset_ids = []

        for dataset in datasets:
            try:
                dataset_metadata.append((
                    dataset.title,
                    dataset.description
                ))
                dataset_ids.append(dataset.id)
            except Exception as e:
                self.logger.error(
                    f"Error preparing dataset {dataset.id}: {e}"
                )

        if not dataset_metadata:
            return 0, len(datasets)

        try:
            self.logger.info(f"Encoding {len(dataset_metadata)} datasets...")
            embeddings = self.embedder.batch_encode_datasets(
                dataset_metadata,
                batch_size=32
            )

            return await self._save_embeddings(
                uow, dataset_ids, embeddings
            )

        except EmbeddingError as e:
            self.logger.error(f"Batch encoding failed: {e}")
            return 0, len(dataset_metadata)

    async def _save_embeddings(
        self,
        uow: UnitOfWork,
        dataset_ids: list[UUID],
        embeddings: list[list[float]]
    ) -> tuple[int, int]:
        """Saves generated embeddings to database."""
        processed = 0
        failed = 0

        for dataset_id, embedding in zip(dataset_ids, embeddings):
            try:
                await uow.datasets.mark_enriched(
                    dataset_id,
                    embedding=embedding
                )
                processed += 1

            except Exception as e:
                error = EmbeddingPersistenceError(str(dataset_id), str(e))
                self.logger.error(f"Error saving embedding for {dataset_id}: {error}")
                failed += 1

        self.logger.info(f"Batch complete: {processed} saved, {failed} failed")

        return processed, failed
