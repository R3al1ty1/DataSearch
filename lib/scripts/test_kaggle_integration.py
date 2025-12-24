#!/usr/bin/env python3
"""
Manual test script for Kaggle integration.

Tests the entire Kaggle data flow:
1. API authentication
2. Fetching latest datasets
3. Enriching with detailed metadata
4. Mapping to Dataset model

Usage:
    uv run python lib/scripts/test_kaggle_integration.py [--limit N]
"""
import asyncio
import sys
import logging
from pathlib import Path
from argparse import ArgumentParser

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from lib.schemas.dataset import KaggleEnrichedDatasetDTO


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Colors:
    """Terminal colors for pretty output."""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    print(f"\n{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.END}\n")


def print_section(text: str):
    print(f"\n{Colors.BOLD}{Colors.CYAN}▸ {text}{Colors.END}")


def print_success(text: str):
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_error(text: str):
    print(f"{Colors.RED}✗ {text}{Colors.END}")


def print_info(key: str, value):
    print(f"  {Colors.BLUE}{key}:{Colors.END} {value}")


def display_dataset_dto(dto: KaggleEnrichedDatasetDTO, index: int):
    """Display enriched dataset DTO."""
    print(f"\n{Colors.BOLD}Dataset #{index + 1}: {dto.title}{Colors.END}")
    print(f"{Colors.YELLOW}{'─'*80}{Colors.END}")

    print_info("Ref", dto.ref)
    print_info("URL", dto.url)
    print_info("Creator", dto.creatorName or "N/A")
    print_info("Size", f"{dto.totalBytes / (1024**2):.2f} MB" if dto.totalBytes else "N/A")
    print_info("Downloads", f"{dto.downloadCount:,}")
    print_info("Views", f"{dto.viewCount:,}")
    print_info("Votes", f"{dto.voteCount:,}")
    print_info("License", dto.licenseName or "N/A")
    print_info("Created", dto.createdDate.strftime("%Y-%m-%d") if dto.createdDate else "N/A")
    print_info("Updated", dto.lastUpdated.strftime("%Y-%m-%d") if dto.lastUpdated else "N/A")

    if dto.subtitle:
        print_info("Subtitle", dto.subtitle)

    if dto.description:
        desc_preview = dto.description[:150].replace('\n', ' ')
        print_info("Description", f"{desc_preview}..." if len(dto.description) > 150 else desc_preview)

    if dto.data:
        print_info("Files", f"{len(dto.data)} file(s)")
        for i, file in enumerate(dto.data[:3], 1):
            file_name = file.get('name', 'unknown')
            file_size = file.get('size', 0)
            columns = file.get('columns', [])
            print(f"    {i}. {file_name} ({file_size / (1024**2):.2f} MB)")
            if columns:
                print(f"       Columns ({len(columns)}): {', '.join(columns[:5])}")

    if dto.column_names:
        print_info("Total Columns", f"{len(dto.column_names)}")
        print(f"       {', '.join(dto.column_names[:10])}")


def display_dataset_model(dataset):
    """Display mapped Dataset model."""
    print(f"\n{Colors.BOLD}Mapped Dataset Model{Colors.END}")
    print(f"{Colors.YELLOW}{'─'*80}{Colors.END}")

    print_info("Source", dataset.source_name)
    print_info("External ID", dataset.external_id)
    print_info("Title", dataset.title)
    print_info("URL", dataset.url)
    print_info("License", dataset.license or "N/A")
    print_info("Total Size", f"{dataset.total_size_bytes / (1024**2):.2f} MB" if dataset.total_size_bytes else "N/A")
    print_info("File Formats", ', '.join(dataset.file_formats) if dataset.file_formats else "N/A")
    print_info("Column Names", f"{len(dataset.column_names)} columns" if dataset.column_names else "N/A")
    print_info("Downloads", f"{dataset.download_count:,}" if dataset.download_count else "0")
    print_info("Views", f"{dataset.view_count:,}" if dataset.view_count else "0")
    print_info("Likes", f"{dataset.like_count:,}" if dataset.like_count else "0")
    print_info("Enrichment Status", dataset.enrichment_status)
    print_info("Is Active", dataset.is_active)


async def test_kaggle_api(limit: int = 3):
    """Test Kaggle API integration."""
    from lib.services.enrichment.kaggle_parser.services.api_parser import KaggleAPIClient
    from lib.services.enrichment.kaggle_parser.mapper import map_enriched_to_dataset

    print_header("Kaggle Integration Test")

    print_section("Step 1: Initialize Kaggle API Client")
    try:
        client = KaggleAPIClient(throttle_delay=1.0)
        print_success("Kaggle API client initialized successfully")
        print_info("Throttle Delay", f"{client.throttle_delay}s")
    except Exception as e:
        print_error(f"Failed to initialize Kaggle API: {e}")
        logger.exception("Initialization error")
        return False

    print_section(f"Step 2: Fetch Latest {limit} Datasets")
    print_info("Sort By", "updated")
    print_info("Limit", limit)

    datasets_collected = []
    batch_count = 0

    try:
        async for batch in client.fetch_latest_datasets(limit=limit, sort_by='updated'):
            batch_count += 1
            print_success(f"Batch {batch_count}: Received {len(batch)} dataset(s)")
            datasets_collected.extend(batch)

            for idx, dto in enumerate(batch):
                display_dataset_dto(dto, len(datasets_collected) - len(batch) + idx)

        if not datasets_collected:
            print_error("No datasets were fetched")
            return False

        print_success(f"Total datasets collected: {len(datasets_collected)}")

    except Exception as e:
        print_error(f"Error fetching datasets: {e}")
        logger.exception("Fetch error")
        return False

    print_section("Step 3: Test Dataset Mapping")
    try:
        first_dto = datasets_collected[0]
        dataset_model = map_enriched_to_dataset(first_dto)
        print_success("Successfully mapped DTO to Dataset model")
        display_dataset_model(dataset_model)

    except Exception as e:
        print_error(f"Error mapping dataset: {e}")
        logger.exception("Mapping error")
        return False

    print_section("Step 4: Test Single Dataset Fetch")
    try:
        test_ref = datasets_collected[0].ref
        print_info("Fetching", test_ref)

        single_dto = await client.fetch_single_dataset(test_ref)

        if single_dto:
            print_success(f"Successfully fetched single dataset: {single_dto.title}")
            print_info("Files", f"{len(single_dto.data)} file(s)" if single_dto.data else "0")
        else:
            print_error("Failed to fetch single dataset")
            return False

    except Exception as e:
        print_error(f"Error fetching single dataset: {e}")
        logger.exception("Single fetch error")
        return False

    print_header("Test Summary")
    print_success(f"All tests passed! Processed {len(datasets_collected)} dataset(s)")

    print(f"\n{Colors.BOLD}Key Findings:{Colors.END}")
    print_info("Total Datasets", len(datasets_collected))
    print_info("Total Batches", batch_count)

    total_downloads = sum(d.downloadCount for d in datasets_collected)
    total_views = sum(d.viewCount for d in datasets_collected)
    total_votes = sum(d.voteCount for d in datasets_collected)

    print_info("Total Downloads", f"{total_downloads:,}")
    print_info("Total Views", f"{total_views:,}")
    print_info("Total Votes", f"{total_votes:,}")

    with_columns = sum(1 for d in datasets_collected if d.column_names)
    print_info("Datasets with Columns", f"{with_columns}/{len(datasets_collected)}")

    with_license = sum(1 for d in datasets_collected if d.licenseName)
    print_info("Datasets with License", f"{with_license}/{len(datasets_collected)}")

    return True


def main():
    parser = ArgumentParser(description="Test Kaggle integration")
    parser.add_argument(
        '--limit',
        type=int,
        default=3,
        help='Number of datasets to fetch (default: 3)'
    )
    args = parser.parse_args()

    try:
        success = asyncio.run(test_kaggle_api(limit=args.limit))
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Test interrupted by user{Colors.END}")
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == '__main__':
    main()
