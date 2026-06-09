import html
import re
from datetime import datetime, timezone
from pathlib import PurePosixPath

from lib.services.datasets.models import Dataset, EnrichmentStatus


def map_zenodo_to_dataset(record: dict) -> Dataset:
    metadata = _dict(record.get("metadata"))
    stats = _dict(record.get("stats"))
    files = _files(record)
    record_id = str(record.get("id") or record.get("recid"))

    return Dataset(
        source_name="zenodo",
        external_id=record_id,
        title=str(metadata.get("title") or record.get("title") or record_id),
        url=_record_url(record, record_id),
        description=_clean_html(_string(metadata.get("description") or record.get("description"))),
        tags=_tags(metadata),
        license=_license(metadata),
        file_formats=_file_formats(files),
        total_size_bytes=_total_size(record, files),
        column_names=None,
        row_count=None,
        download_count=_stat(stats, "downloads"),
        view_count=_stat(stats, "views"),
        like_count=0,
        source_created_at=_parse_datetime(record.get("created")),
        source_updated_at=_parse_datetime(record.get("updated") or record.get("modified")),
        embedding=None,
        static_score=None,
        is_active=True,
        enrichment_status=EnrichmentStatus.ENRICHED.value,
        enrichment_attempts=0,
        last_enrichment_error=None,
        last_enriched_at=None,
        last_checked_at=None,
        source_meta={
            "doi": record.get("doi") or metadata.get("doi"),
            "conceptdoi": record.get("conceptdoi"),
            "conceptrecid": record.get("conceptrecid"),
            "version": metadata.get("version") or record.get("version"),
            "publication_date": metadata.get("publication_date") or record.get("publication_date"),
            "resource_type": metadata.get("resource_type") or record.get("resource_type"),
            "access_right": metadata.get("access_right") or record.get("access_right"),
            "creators": metadata.get("creators"),
            "contributors": metadata.get("contributors"),
            "communities": metadata.get("communities") or record.get("communities"),
            "subjects": metadata.get("subjects") or record.get("subjects"),
            "related_identifiers": metadata.get("related_identifiers"),
            "references": metadata.get("references"),
            "grants": metadata.get("grants"),
            "license_raw": metadata.get("license"),
            "files": files,
            "stats": stats,
            "links": record.get("links"),
            "enrichment_source": "api",
        },
    )


def _dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _clean_html(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _tags(metadata: dict) -> list[str] | None:
    values = []
    for key in ("keywords", "subjects"):
        items = metadata.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, str):
                values.append(item)
            elif isinstance(item, dict):
                term = item.get("term") or item.get("identifier")
                if isinstance(term, str):
                    values.append(term)
    unique = sorted({value.strip() for value in values if value.strip()})
    return unique or None


def _license(metadata: dict) -> str | None:
    value = metadata.get("license")
    if isinstance(value, dict):
        identifier = value.get("id") or value.get("identifier")
        return str(identifier).lower() if identifier else None
    if isinstance(value, str):
        return value.lower()
    return None


def _files(record: dict) -> list[dict]:
    files = record.get("files")
    if isinstance(files, list):
        return [item for item in files if isinstance(item, dict)]
    if isinstance(files, dict):
        entries = files.get("entries")
        if isinstance(entries, dict):
            return [item for item in entries.values() if isinstance(item, dict)]
        if isinstance(entries, list):
            return [item for item in entries if isinstance(item, dict)]
    return []


def _file_formats(files: list[dict]) -> list[str] | None:
    formats = set()
    for file in files:
        value = file.get("type") or file.get("filetype")
        if isinstance(value, str) and value.strip():
            formats.add(value.strip().lower())
            continue

        name = file.get("key") or file.get("filename") or file.get("name")
        if not isinstance(name, str):
            continue
        suffixes = PurePosixPath(name).suffixes
        if suffixes[-2:] == [".tar", ".gz"]:
            formats.add("tar.gz")
        elif suffixes:
            formats.add(suffixes[-1].lstrip(".").lower())

    return sorted(formats) or None


def _total_size(record: dict, files: list[dict]) -> int | None:
    direct = record.get("size")
    if isinstance(direct, int):
        return direct
    total = 0
    for file in files:
        size = file.get("size") or file.get("filesize")
        if isinstance(size, int):
            total += size
    return total or None


def _stat(stats: dict, key: str) -> int:
    value = stats.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, dict):
        for nested_key in ("total", "unique"):
            nested = value.get(nested_key)
            if isinstance(nested, int):
                return nested
    return 0


def _record_url(record: dict, record_id: str) -> str:
    links = _dict(record.get("links"))
    html_url = links.get("html") or links.get("latest_html")
    if isinstance(html_url, str):
        return html_url
    return f"https://zenodo.org/records/{record_id}"


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
