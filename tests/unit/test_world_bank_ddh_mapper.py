from lib.services.datasets.enrichment.world_bank_ddh_parser.mapper import (
    map_world_bank_ddh_to_dataset,
)
from lib.services.datasets.models import EnrichmentStatus


def test_map_world_bank_ddh_to_dataset_maps_core_fields():
    record = {
        "dataset_id": "dataset-guid",
        "dataset_unique_id": "0067097",
        "archive_nid": "139859",
        "name": "Fallback title",
        "source": "FINANCE",
        "version_no": "2",
        "first_published": "2026-06-01T10:00:00+00:00",
        "modified_on": "2026-06-03T18:13:28+00:00",
        "identification": {
            "title": "World Bank example dataset",
            "description": "<p>Useful&nbsp;metadata.</p>",
            "citation": "World Bank citation",
            "topics": [{"name": "Climate"}, "Finance"],
            "language_supported": [{"code": "EN", "name": "English"}],
        },
        "keywords": [{"name": "weather"}, "economy"],
        "constraints": {
            "license": {
                "license_id": "Creative Commons Attribution 4.0",
                "license_reference": "https://creativecommons.org/licenses/by/4.0/",
            }
        },
        "resources": [
            {
                "resource_unique_id": "DR1",
                "download_count": 12,
                "format": "CSV",
                "distribution": {
                    "distribution_size": "100",
                    "url": "https://example.org/data.csv",
                    "file_name": "data.csv",
                },
            },
            {
                "resource_unique_id": "DR2",
                "download_count": 8,
                "distribution": {
                    "distribution_format": "application/json",
                    "distribution_size": 200,
                    "website_url": "https://example.org/dataset",
                },
            },
        ],
        "resource_schemas": {
            "DR1": [
                {"attribute_name": "country", "data_type": "string"},
                {"attribute_name": "year", "data_type": "integer"},
            ],
            "DR2": [{"attribute_name": "country", "data_type": "string"}],
        },
    }

    dataset = map_world_bank_ddh_to_dataset(record)

    assert dataset.source_name == "world_bank_ddh"
    assert dataset.external_id == "0067097"
    assert dataset.title == "World Bank example dataset"
    assert dataset.url == "https://example.org/data.csv"
    assert dataset.description == "Useful metadata."
    assert dataset.tags == ["Climate", "Finance", "economy", "weather"]
    assert dataset.license == "cc-by-4.0"
    assert dataset.file_formats == ["csv", "json"]
    assert dataset.total_size_bytes == 300
    assert dataset.column_names == ["country", "year"]
    assert dataset.row_count is None
    assert dataset.download_count == 20
    assert dataset.view_count == 0
    assert dataset.like_count == 0
    assert dataset.enrichment_status == EnrichmentStatus.ENRICHED.value
    assert dataset.source_meta["dataset_id"] == "dataset-guid"
    assert dataset.source_meta["resources"] == record["resources"]
    assert dataset.source_meta["resource_schemas"] == record["resource_schemas"]


def test_map_world_bank_ddh_to_dataset_handles_minimal_record():
    dataset = map_world_bank_ddh_to_dataset(
        {
            "dataset_unique_id": "0067000",
            "name": "Minimal",
        }
    )

    assert dataset.external_id == "0067000"
    assert dataset.title == "Minimal"
    assert dataset.url == "https://ddh-openapi.worldbank.org/datasets/0067000"
    assert dataset.tags is None
    assert dataset.license is None
    assert dataset.file_formats is None
    assert dataset.total_size_bytes is None
    assert dataset.column_names is None
    assert dataset.download_count == 0
