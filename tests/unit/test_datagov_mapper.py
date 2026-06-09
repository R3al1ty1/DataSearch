from lib.services.datasets.enrichment.datagov_parser.mapper import (
    map_datagov_to_dataset,
)
from lib.services.datasets.models import EnrichmentStatus


def test_map_datagov_to_dataset_maps_core_fields():
    record = {
        "identifier": "catalog-id",
        "slug": "example-dataset",
        "title": "Example dataset",
        "description": "<p>Short catalog description.</p>",
        "keyword": ["water"],
        "theme": ["environment"],
        "publisher": "Example Agency",
        "popularity": 42,
        "last_harvested_date": "2026-01-03T10:00:00",
        "organization": {"slug": "example-agency"},
        "harvest_record": "https://catalog.data.gov/harvest_record/123",
        "harvest_record_raw": "https://catalog.data.gov/harvest_record/123/raw",
        "dcat": {
            "identifier": "source-id",
            "title": "DCAT title",
            "description": "<p>Useful dataset&nbsp;description.</p>",
            "keyword": ["climate", "water"],
            "theme": ["science"],
            "license": "https://creativecommons.org/licenses/by/4.0/",
            "issued": "2026-01-01",
            "modified": "2026-01-02T10:00:00Z",
            "landingPage": "https://agency.gov/datasets/example",
            "accessLevel": "public",
            "distribution": [
                {
                    "format": "CSV",
                    "downloadURL": "https://agency.gov/data.csv",
                    "byteSize": "100",
                },
                {
                    "mediaType": "application/json",
                    "accessURL": "https://agency.gov/api",
                    "byteSize": 200,
                },
            ],
        },
    }

    dataset = map_datagov_to_dataset(record)

    assert dataset.source_name == "datagov"
    assert dataset.external_id == "catalog-id"
    assert dataset.title == "DCAT title"
    assert dataset.url == "https://agency.gov/datasets/example"
    assert dataset.description == "Useful dataset description."
    assert dataset.tags == ["climate", "environment", "science", "water"]
    assert dataset.license == "cc-by-4.0"
    assert dataset.file_formats == ["csv", "json"]
    assert dataset.total_size_bytes == 300
    assert dataset.column_names is None
    assert dataset.row_count is None
    assert dataset.download_count == 0
    assert dataset.view_count == 0
    assert dataset.like_count == 0
    assert dataset.enrichment_status == EnrichmentStatus.ENRICHED.value
    assert dataset.source_meta["popularity"] == 42
    assert dataset.source_meta["dcat"] == record["dcat"]


def test_map_datagov_to_dataset_handles_minimal_record():
    dataset = map_datagov_to_dataset(
        {
            "slug": "minimal",
            "dcat": {
                "title": "Minimal",
                "identifier": "minimal-id",
            },
        }
    )

    assert dataset.external_id == "minimal-id"
    assert dataset.title == "Minimal"
    assert dataset.url == "https://catalog.data.gov/dataset/minimal"
    assert dataset.tags is None
    assert dataset.license is None
    assert dataset.file_formats is None
    assert dataset.total_size_bytes is None
