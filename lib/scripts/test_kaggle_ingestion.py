import asyncio
import os
from sqlalchemy import select, func

os.environ.setdefault('POSTGRES_HOST', 'localhost')
os.environ.setdefault('POSTGRES_PORT', '5434')
os.environ.setdefault('POSTGRES_DB', 'datasearch_db')

from lib.core.container import container
from lib.models.dataset import Dataset


async def test_kaggle_ingestion():
    """Test Kaggle dataset ingestion and verify what gets saved to DB."""

    logger = container.logger
    logger.info("=== Testing Kaggle Dataset Ingestion ===")
    logger.info(f"Database URL: {container.settings.SQLALCHEMY_DATABASE_URI}")

    container.db.init()

    async with container.db._session_factory() as session:
        try:
            count_before = await session.execute(
                select(func.count(Dataset.id)).where(Dataset.source_name == 'kaggle')
            )
            total_before = count_before.scalar_one()
            logger.info(f"Kaggle datasets before: {total_before}")

            logger.info("Fetching latest 10 datasets from Kaggle API...")
            fetched, inserted = await container.kaggle_processor.fetch_latest(
                session,
                limit=10,
                sort_by='updated'
            )

            logger.info(f"Fetch completed: {fetched} fetched, {inserted} inserted/updated")

            count_after = await session.execute(
                select(func.count(Dataset.id)).where(Dataset.source_name == 'kaggle')
            )
            total_after = count_after.scalar_one()
            logger.info(f"Kaggle datasets after: {total_after}")

            result = await session.execute(
                select(Dataset)
                .where(Dataset.source_name == 'kaggle')
                .order_by(Dataset.created_at.desc())
                .limit(3)
            )
            latest_datasets = result.scalars().all()

            logger.info("\n=== Sample of saved datasets ===")
            for i, dataset in enumerate(latest_datasets, 1):
                logger.info(f"\nDataset {i}:")
                logger.info(f"  ID: {dataset.id}")
                logger.info(f"  External ID: {dataset.external_id}")
                logger.info(f"  Title: {dataset.title}")
                logger.info(f"  URL: {dataset.url}")
                logger.info(f"  Description: {dataset.description[:100] if dataset.description else 'N/A'}...")
                logger.info(f"  Tags: {dataset.tags[:5] if dataset.tags else []}")
                logger.info(f"  License: {dataset.license}")
                logger.info(f"  Downloads: {dataset.download_count}")
                logger.info(f"  Likes: {dataset.like_count}")
                logger.info(f"  Views: {dataset.view_count}")
                logger.info(f"  Source Created: {dataset.source_created_at}")
                logger.info(f"  Source Updated: {dataset.source_updated_at}")
                logger.info(f"  Enrichment Status: {dataset.enrichment_status}")
                logger.info(f"  Has Embedding: {dataset.embedding is not None}")
                logger.info(f"  Is Active: {dataset.is_active}")
                logger.info(f"  Column Names: {dataset.column_names[:5] if dataset.column_names else []}")
                logger.info(f"  Row Count: {dataset.row_count}")
                logger.info(f"  File Formats: {dataset.file_formats}")
                logger.info(f"  Total Size: {dataset.total_size_bytes}")
                logger.info(f"  Source Meta Keys: {list(dataset.source_meta.keys()) if dataset.source_meta else 'N/A'}")

            logger.info("\n=== Test completed successfully ===")

        except Exception as e:
            logger.error(f"Error during test: {e}", exc_info=True)
            raise
        finally:
            await container.db.close()


if __name__ == "__main__":
    asyncio.run(test_kaggle_ingestion())
