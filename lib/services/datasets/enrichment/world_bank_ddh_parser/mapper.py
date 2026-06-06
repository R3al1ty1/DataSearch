import html
import re
from datetime import datetime, timezone
from pathlib import PurePosixPath

from lib.core.constants import ExternalAPIUrls
from lib.services.datasets.models import Dataset, EnrichmentStatus


def map_world_bank_ddh_to_dataset(record: dict) -> Dataset:
    identification = _dict(record.get("identification"))
    constraints = _dict(record.get("constraints"))
    resources = _resources(record)
    external_id = str(record.get("dataset_unique_id") or record.get("dataset_id") or "unknown")

    return Dataset(
        source_name="world_bank_ddh",
        external_id=external_id,
        title=_string(identification.get("title")) or _string(record.get("name")) or external_id,
        url=_dataset_url(record, external_id, resources),
        description=_clean_html(_string(identification.get("description"))),
        tags=_tags(record, identification),
        license=_normalize_license(_license_value(constraints, resources)),
        file_formats=_file_formats(resources),
        total_size_bytes=_total_size(resources),
        column_names=_column_names(record),
        row_count=None,
        download_count=_download_count(resources),
        view_count=0,
        like_count=0,
        source_created_at=(
            _parse_datetime(record.get("first_published"))
            or _parse_datetime(record.get("created_on"))
        ),
        source_updated_at=(
            _parse_datetime(record.get("modified_on"))
            or _parse_datetime(record.get("last_updated_date"))
        ),
        embedding=None,
        static_score=None,
        is_active=True,
        enrichment_status=EnrichmentStatus.ENRICHED.value,
        enrichment_attempts=0,
        last_enrichment_error=None,
        last_enriched_at=None,
        last_checked_at=None,
        source_meta={
            "dataset_id": record.get("dataset_id"),
            "archive_nid": record.get("archive_nid"),
            "source": record.get("source"),
            "version_no": record.get("version_no"),
            "identification": identification,
            "keywords": record.get("keywords"),
            "constraints": constraints,
            "data_quality": record.get("data_quality"),
            "geographical_extent": record.get("geographical_extent"),
            "temporal_extent": record.get("temporal_extent"),
            "temporal_resolution": record.get("temporal_resolution"),
            "spatial_resolution": record.get("spatial_resolution"),
            "reference_system": record.get("reference_system"),
            "lineage": record.get("lineage"),
            "maintenance_information": record.get("maintenance_information"),
            "indicators": record.get("indicators"),
            "resources": resources,
            "resource_schemas": record.get("resource_schemas"),
            "license_raw": _license_value(constraints, resources),
            "enrichment_source": "ddh_openapi",
        },
    )


def _dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _string(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _clean_html(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _tags(record: dict, identification: dict) -> list[str] | None:
    values = []
    for item in record.get("keywords") or []:
        if isinstance(item, str):
            values.append(item)
        elif isinstance(item, dict):
            values.extend(
                str(item.get(key)).strip()
                for key in ("name", "value", "keyword")
                if item.get(key)
            )

    for topic in identification.get("topics") or []:
        if isinstance(topic, str):
            values.append(topic)
        elif isinstance(topic, dict):
            values.extend(
                str(topic.get(key)).strip()
                for key in ("name", "value", "topic")
                if topic.get(key)
            )

    unique = sorted({value.strip() for value in values if value.strip()})
    return unique or None


def _resources(record: dict) -> list[dict]:
    resources = record.get("resources")
    if isinstance(resources, list):
        return [item for item in resources if isinstance(item, dict)]
    return []


def _dataset_url(record: dict, external_id: str, resources: list[dict]) -> str:
    for resource in resources:
        distribution = _dict(resource.get("distribution"))
        for value in (
            distribution.get("website_url"),
            resource.get("url"),
            distribution.get("url"),
        ):
            if isinstance(value, str) and value.strip():
                return value

    return f"{ExternalAPIUrls.WORLD_BANK_DDH}/datasets/{external_id}"


def _license_value(constraints: dict, resources: list[dict]) -> str | None:
    dataset_license = _dict(constraints.get("license"))
    for key in ("license_id", "custom_license_information", "license_reference"):
        value = dataset_license.get(key)
        if isinstance(value, str) and value.strip():
            return value

    for resource in resources:
        resource_license = _dict(_dict(resource.get("constraints")).get("license"))
        for key in ("license_id", "custom_license_information", "license_reference"):
            value = resource_license.get(key)
            if isinstance(value, str) and value.strip():
                return value
    return None


def _normalize_license(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    if normalized in {"creative commons attribution 4.0", "creative commons 4.0 by attribution"}:
        return "cc-by-4.0"
    if "creativecommons.org/licenses/by/4.0" in normalized:
        return "cc-by-4.0"
    if normalized in {"mit license", "mit license with world bank igo rider"}:
        return "mit"
    if normalized == "license specified externally":
        return normalized
    return normalized


def _file_formats(resources: list[dict]) -> list[str] | None:
    formats = set()
    for resource in resources:
        distribution = _dict(resource.get("distribution"))
        for value in (
            resource.get("format"),
            distribution.get("format"),
            distribution.get("distribution_format"),
        ):
            if isinstance(value, str) and value.strip():
                formats.add(_normalize_format(value))
                break
        else:
            ext = _format_from_url(resource, distribution)
            if ext:
                formats.add(ext)

    return sorted(formats) or None


def _normalize_format(value: str) -> str:
    normalized = value.strip().lower()
    if "/" in normalized:
        normalized = normalized.rsplit("/", 1)[1]
    return normalized.removeprefix("vnd.").removesuffix("+json")


def _format_from_url(resource: dict, distribution: dict) -> str | None:
    for value in (
        resource.get("url"),
        distribution.get("url"),
        distribution.get("website_url"),
        distribution.get("file_name"),
    ):
        if not isinstance(value, str):
            continue
        path = PurePosixPath(value.split("?", 1)[0])
        suffixes = path.suffixes
        if suffixes[-2:] == [".tar", ".gz"]:
            return "tar.gz"
        if suffixes:
            ext = suffixes[-1].lstrip(".").lower()
            if ext and len(ext) <= 10:
                return ext
    return None


def _total_size(resources: list[dict]) -> int | None:
    total = 0
    for resource in resources:
        distribution = _dict(resource.get("distribution"))
        value = distribution.get("distribution_size")
        if isinstance(value, int):
            total += value
        elif isinstance(value, str) and value.isdigit():
            total += int(value)
    return total or None


def _column_names(record: dict) -> list[str] | None:
    schemas = record.get("resource_schemas")
    if not isinstance(schemas, dict):
        return None

    names = []
    for fields in schemas.values():
        if not isinstance(fields, list):
            continue
        for field in fields:
            if not isinstance(field, dict):
                continue
            name = field.get("attribute_name")
            if isinstance(name, str) and name.strip():
                names.append(name.strip())

    unique = sorted(set(names))
    return unique or None


def _download_count(resources: list[dict]) -> int:
    total = 0
    for resource in resources:
        value = resource.get("download_count")
        if isinstance(value, int):
            total += value
    return total


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
