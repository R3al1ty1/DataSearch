"""
Test script for embedding generation.

Usage:
    uv run python -m lib.scripts.run_embedding_task
"""
import asyncio
import os

os.environ.setdefault('POSTGRES_HOST', 'localhost')
os.environ.setdefault('POSTGRES_PORT', '5434')
os.environ.setdefault('POSTGRES_DB', 'datasearch_db')
os.environ['DEBUG'] = 'false'

from lib.core.container import container


async def check_datasets_status():
    """Check datasets needing embeddings and overall stats."""
    dataset_repo = container.dataset_repo

    async with container.db.begin_session() as session:
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
    from lib.services.datasets.ml.embedding_processor import EmbeddingProcessor

    print("\n=== Testing EmbeddingProcessor ===")

    processor = EmbeddingProcessor(
        dataset_repo=container.dataset_repo,
        embedder=container.embedder
    )

    async with container.db.begin_session() as session:
        processed, failed = await processor.process_batch(session, batch_size=5)
        print(f"Processed: {processed}, Failed: {failed}")


async def main():
    container.db.init()

    try:
        print("\n[1] Initial Status")
        await check_datasets_status()

        print("\n[2] Testing Processor")
        await test_embedding_processor()

        print("\n[3] Final Status")
        await check_datasets_status()
    finally:
        await container.db.close()


if __name__ == "__main__":
    print("=" * 60)
    print("EMBEDDING GENERATION TEST")
    print("=" * 60)

    asyncio.run(main())
