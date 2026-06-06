import asyncio
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from lib.core.container import container
from lib.core.exceptions import DataSearchError
from lib.services.datasets.models import Dataset

os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5434")
os.environ.setdefault("POSTGRES_DB", "datasearch_db")


async def test_zenodo_ingestion():
    """Test Zenodo dataset ingestion and verify what gets saved to DB."""

    logger = container.logger
    logger.info("=== Testing Zenodo Dataset Ingestion ===")
    logger.info(f"Database URL: {container.settings.SQLALCHEMY_DATABASE_URI}")

    limit = int(os.getenv("ZENODO_LIMIT", "10"))
    days_back = int(os.getenv("ZENODO_DAYS_BACK", "0"))
    batch_size = int(os.getenv("ZENODO_BATCH_SIZE", "100"))
    min_updated = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
        if days_back > 0
        else None
    )

    container.db.init()

    async with container.uow() as uow:
        try:
            count_before = await uow.session.execute(
                select(func.count(Dataset.id)).where(Dataset.source_name == "zenodo")
            )
            total_before = count_before.scalar_one()
            logger.info(f"Zenodo datasets before: {total_before}")

            logger.info(
                "Fetching Zenodo datasets "
                f"(limit={limit}, days_back={days_back}, batch_size={batch_size})..."
            )
            fetched, saved = await container.zenodo_processor.fetch_and_store(
                uow,
                limit=limit,
                batch_size=batch_size,
                min_updated=min_updated,
            )

            logger.info(f"Fetch completed: {fetched} fetched, {saved} inserted/updated")

            count_after = await uow.session.execute(
                select(func.count(Dataset.id)).where(Dataset.source_name == "zenodo")
            )
            total_after = count_after.scalar_one()
            logger.info(f"Zenodo datasets after: {total_after}")

            result = await uow.session.execute(
                select(Dataset)
                .where(Dataset.source_name == "zenodo")
                .order_by(Dataset.source_updated_at.desc().nulls_last())
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

        except DataSearchError as e:
            logger.error(
                "Domain error during test: "
                f"error_code={e.error_code.value}, message={e.message}, "
                f"details={e.details}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(f"Error during test: {e}", exc_info=True)
            raise
        finally:
            await container.db.close()


if __name__ == "__main__":
    asyncio.run(test_zenodo_ingestion())
