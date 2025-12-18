from lib.models.dataset import Dataset, EnrichmentStatus
from lib.schemas.dataset import HFDatasetDTO


def map_hf_to_dataset(dto: HFDatasetDTO) -> Dataset:
    """
    Convert HuggingFace DTO to Dataset model.

    HuggingFace provides enriched data by default.
    Status: PENDING (needs embedding generation).
    """
    file_formats = _extract_file_formats_from_tags(dto.tags)
    task_categories = _extract_task_categories(dto.tags)

    return Dataset(
        source_name='huggingface',
        external_id=dto.id,
        title=dto.title,
        url=f"https://huggingface.co/datasets/{dto.id}",
        description=dto.description,
        tags=task_categories if task_categories else None,
        license=dto.license,
        file_formats=file_formats if file_formats else None,
        total_size_bytes=None,
        column_names=None,
        row_count=None,
        download_count=dto.downloads,
        view_count=0,
        like_count=dto.likes,
        source_created_at=dto.created_at,
        source_updated_at=dto.last_modified,
        embedding=None,
        static_score=None,
        is_active=True,
        enrichment_status=EnrichmentStatus.PENDING.value,
        enrichment_attempts=0,
        last_enrichment_error=None,
        last_enriched_at=None,
        last_checked_at=None,
        source_meta={
            'sha': dto.sha,
            'card_data': dto.card_data,
            'dataset_info': dto.dataset_info,
            'tags': dto.tags,
            'enrichment_source': 'api'
        }
    )


def _extract_file_formats_from_tags(tags: list[str]) -> list[str]:
    """Extract file formats from HuggingFace tags."""
    format_prefixes = ['parquet', 'csv', 'json', 'text', 'arrow', 'webdataset']
    formats = []

    for tag in tags:
        tag_lower = tag.lower()
        for fmt in format_prefixes:
            if fmt in tag_lower:
                formats.append(fmt)
                break

    return sorted(list(set(formats)))


def _extract_task_categories(tags: list[str]) -> list[str]:
    """Extract task categories from tags."""
    task_tags = [
        tag for tag in tags
        if tag.startswith('task_categories:') or tag.startswith('task_ids:')
    ]

    categories = []
    for tag in task_tags:
        if ':' in tag:
            category = tag.split(':', 1)[1]
            categories.append(category)

    return sorted(list(set(categories)))
