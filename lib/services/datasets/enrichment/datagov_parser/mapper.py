import html
import re
from datetime import datetime, timezone
from pathlib import PurePosixPath

from lib.services.datasets.models import Dataset, EnrichmentStatus


def map_datagov_to_dataset(record: dict) -> Dataset:
    dcat = _dict(record.get("dcat"))
    distributions = _distributions(dcat)
    external_id = _external_id(record, dcat)

    return Dataset(
        source_name="datagov",
        external_id=external_id,
        title=_string(dcat.get("title")) or _string(record.get("title")) or external_id,
        url=_dataset_url(record, dcat),
        description=_clean_html(
            _string(dcat.get("description")) or _string(record.get("description"))
        ),
        tags=_tags(record, dcat),
        license=_normalize_license(_string(dcat.get("license"))),
        file_formats=_file_formats(distributions),
        total_size_bytes=_total_size(distributions),
        column_names=None,
        row_count=None,
        download_count=0,
        view_count=0,
        like_count=0,
        source_created_at=_parse_datetime(dcat.get("issued")),
        source_updated_at=(
            _parse_datetime(dcat.get("modified"))
            or _parse_datetime(record.get("last_harvested_date"))
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
            "slug": record.get("slug"),
            "publisher": record.get("publisher"),
            "access_level": dcat.get("accessLevel") or record.get("accessLevel"),
            "theme": dcat.get("theme") or record.get("theme"),
            "organization": record.get("organization"),
            "popularity": record.get("popularity"),
            "has_spatial": record.get("has_spatial"),
            "spatial_centroid": record.get("spatial_centroid"),
            "spatial_shape": record.get("spatial_shape"),
            "last_harvested_date": record.get("last_harvested_date"),
            "distribution_titles": record.get("distribution_titles"),
            "distributions": distributions,
            "dcat": dcat,
            "harvest_record": record.get("harvest_record"),
            "harvest_record_raw": record.get("harvest_record_raw"),
            "enrichment_source": "catalog_api_search",
        },
    )


def _dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _string(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _external_id(record: dict, dcat: dict) -> str:
    value = record.get("identifier") or dcat.get("identifier") or record.get("slug")
    return str(value or record.get("title") or "unknown")


def _dataset_url(record: dict, dcat: dict) -> str:
    for value in (
        dcat.get("landingPage"),
        record.get("landingPage"),
        record.get("harvest_record"),
    ):
        if isinstance(value, str) and value.strip():
            return value

    slug = record.get("slug")
    if isinstance(slug, str) and slug.strip():
        return f"https://catalog.data.gov/dataset/{slug}"

    return "https://catalog.data.gov"


def _clean_html(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _tags(record: dict, dcat: dict) -> list[str] | None:
    values = []
    for source in (dcat, record):
        for key in ("keyword", "theme"):
            items = source.get(key)
            if not isinstance(items, list):
                continue
            values.extend(str(item).strip() for item in items if str(item).strip())

    unique = sorted(set(values))
    return unique or None


def _normalize_license(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if "usa.gov/publicdomain" in normalized or "usa.gov/government-works" in normalized:
        return "public domain"
    if "creativecommons.org/licenses/by/4.0" in normalized:
        return "cc-by-4.0"
    if "creativecommons.org/publicdomain/zero" in normalized:
        return "cc0-1.0"
    return normalized


def _distributions(dcat: dict) -> list[dict]:
    value = dcat.get("distribution")
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _file_formats(distributions: list[dict]) -> list[str] | None:
    formats = set()
    for distribution in distributions:
        for key in ("format", "mediaType"):
            value = distribution.get(key)
            if isinstance(value, str) and value.strip():
                formats.add(_normalize_format(value))
                break
        else:
            ext = _format_from_url(distribution)
            if ext:
                formats.add(ext)

    return sorted(formats) or None


def _normalize_format(value: str) -> str:
    normalized = value.strip().lower()
    if "/" in normalized:
        normalized = normalized.rsplit("/", 1)[1]
    return normalized.removeprefix("vnd.").removesuffix("+json")


def _format_from_url(distribution: dict) -> str | None:
    for key in ("downloadURL", "accessURL"):
        value = distribution.get(key)
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


def _total_size(distributions: list[dict]) -> int | None:
    total = 0
    for distribution in distributions:
        size = distribution.get("byteSize")
        if isinstance(size, int):
            total += size
        elif isinstance(size, str) and size.isdigit():
            total += int(size)
    return total or None


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
