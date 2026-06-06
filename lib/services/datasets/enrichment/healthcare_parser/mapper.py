import html
import re

from lib.core.constants import ExternalAPIUrls
from lib.services.datasets.models import Dataset, EnrichmentStatus
from lib.services.datasets.schemas import DataHealthcareDatasetDTO


def map_healthcare_to_dataset(dto: DataHealthcareDatasetDTO) -> Dataset:
    return Dataset(
        source_name="data_healthcare_gov",
        external_id=dto.identifier,
        title=dto.title,
        url=f"{ExternalAPIUrls.DATA_HEALTHCARE_GOV_DATASET}/{dto.identifier}",
        description=_clean_html(dto.description),
        tags=dto.tags,
        license=_normalize_license(dto.license),
        file_formats=dto.file_formats,
        total_size_bytes=None,
        column_names=dto.column_names,
        row_count=None,
        download_count=0,
        view_count=0,
        like_count=0,
        source_created_at=dto.issued,
        source_updated_at=dto.modified,
        embedding=None,
        static_score=None,
        is_active=True,
        enrichment_status=EnrichmentStatus.ENRICHED.value,
        enrichment_attempts=0,
        last_enrichment_error=None,
        last_enriched_at=None,
        last_checked_at=None,
        source_meta={
            "access_level": dto.access_level,
            "accrual_periodicity": dto.accrual_periodicity,
            "publisher": dto.publisher,
            "contact_point": dto.contact_point,
            "distributions": dto.distribution,
            "bureau_code": dto.bureau_code,
            "program_code": dto.program_code,
            "enrichment_source": "data_json",
        },
    )


def _clean_html(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _normalize_license(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if "usa.gov/publicdomain" in normalized or "usa.gov/government-works" in normalized:
        return "public domain"
    return value
