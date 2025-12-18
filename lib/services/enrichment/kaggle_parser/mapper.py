from lib.models.dataset import Dataset, EnrichmentStatus
from lib.schemas.dataset import KaggleMetaDatasetDTO, KaggleEnrichedDatasetDTO


def map_meta_to_dataset(dto: KaggleMetaDatasetDTO) -> Dataset:
    """
    Convert Kaggle Meta CSV DTO to Dataset model.

    This creates a minimal dataset record from CSV data.
    Status: MINIMAL (needs API enrichment).
    """
    return Dataset(
        source_name='kaggle',
        external_id=dto.external_id,
        title=f"Kaggle Dataset {dto.Id}",
        url=f"https://www.kaggle.com/datasets/{dto.Id}",
        description=None,
        tags=None,
        license=None,
        file_formats=None,
        total_size_bytes=None,
        column_names=None,
        row_count=None,
        download_count=dto.TotalDownloads,
        view_count=dto.TotalViews,
        like_count=dto.TotalVotes,
        source_created_at=dto.CreationDate,
        source_updated_at=dto.LastActivityDate,
        embedding=None,
        static_score=None,
        is_active=True,
        enrichment_status=EnrichmentStatus.MINIMAL.value,
        enrichment_attempts=0,
        last_enrichment_error=None,
        last_enriched_at=None,
        last_checked_at=None,
        source_meta={
            'csv_id': dto.Id,
            'creator_user_id': dto.CreatorUserId,
            'owner_user_id': dto.OwnerUserId,
            'owner_organization_id': dto.OwnerOrganizationId,
            'current_dataset_version_id': dto.CurrentDatasetVersionId,
            'current_datasource_version_id': dto.CurrentDatasourceVersionId,
            'forum_id': dto.ForumId,
            'type': dto.Type,
            'total_kernels': dto.TotalKernels,
            'enrichment_source': 'csv'
        }
    )


def map_enriched_to_dataset(dto: KaggleEnrichedDatasetDTO) -> Dataset:
    """
    Convert Kaggle API enriched DTO to Dataset model.

    This creates a fully enriched dataset record from API data.
    Status: PENDING (needs embedding generation).
    """
    file_formats = _extract_file_formats(dto.data) if dto.data else None

    return Dataset(
        source_name='kaggle',
        external_id=dto.ref,
        title=dto.title,
        url=dto.url,
        description=dto.description,
        tags=None,
        license=dto.licenseName,
        file_formats=file_formats,
        total_size_bytes=dto.totalBytes,
        column_names=dto.column_names if dto.column_names else None,
        row_count=None,
        download_count=dto.downloadCount,
        view_count=dto.viewCount,
        like_count=dto.voteCount,
        source_created_at=dto.createdDate,
        source_updated_at=dto.lastUpdated,
        embedding=None,
        static_score=None,
        is_active=True,
        enrichment_status=EnrichmentStatus.PENDING.value,
        enrichment_attempts=0,
        last_enrichment_error=None,
        last_enriched_at=None,
        last_checked_at=None,
        source_meta={
            'ref': dto.ref,
            'creator_name': dto.creatorName,
            'subtitle': dto.subtitle,
            'files': dto.data,
            'enrichment_source': 'api'
        }
    )


def _extract_file_formats(files: list[dict]) -> list[str]:
    """Extract unique file formats from file list."""
    formats = set()
    for file in files:
        if 'name' in file:
            ext = file['name'].split('.')[-1].lower()
            if ext and len(ext) <= 10:
                formats.add(ext)
    return sorted(list(formats))
