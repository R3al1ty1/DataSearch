"""
Test script for embedding generation.

Usage:
    uv run python lib/scripts/test_embedding_task.py
"""
import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from lib.core.container import container


async def check_datasets_status():
    """Check datasets needing embeddings and overall stats."""
    dataset_repo = container.dataset_repo

    async with container.db.get_session() as session:
        datasets = await dataset_repo.get_for_embedding_generation(
            session, limit=1000
        )
        print(f"\nDatasets needing embeddings: {len(datasets)}")

        if datasets:
            print("\nFirst 5 datasets:")
            for ds in datasets[:5]:
                print(
                    f"  - [{ds.source_name}] {ds.external_id}: "
                    f"{ds.title[:50]}..."
                )


async def test_embedding_processor():
    """Test EmbeddingProcessor directly."""
    from lib.services.ml.embedding_processor import EmbeddingProcessor

    print("\n=== Testing EmbeddingProcessor ===")

    processor = EmbeddingProcessor(
        dataset_repo=container.dataset_repo,
        embedder=container.embedder,
        logger=container.logger
    )

    async with container.db.get_session() as session:
        processed, failed = await processor.process_batch(session, batch_size=5)
        print(f"Processed: {processed}, Failed: {failed}")


def test_celery_task():
    """Test Celery task."""
    from lib.crons.enrich import generate_embeddings

    print("\n=== Testing Celery Task ===")
    result = generate_embeddings(batch_size=10)
    print(f"Result: {result}")


if __name__ == "__main__":
    print("=" * 60)
    print("EMBEDDING GENERATION TEST")
    print("=" * 60)

    print("\n[1] Initial Status")
    asyncio.run(check_datasets_status())

    print("\n[2] Testing Processor")
    asyncio.run(test_embedding_processor())

    print("\n[3] Testing Celery Task")
    test_celery_task()

    print("\n[4] Final Status")
    asyncio.run(check_datasets_status())
