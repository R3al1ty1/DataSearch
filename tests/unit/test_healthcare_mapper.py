from lib.services.datasets.enrichment.healthcare_parser.mapper import (
    map_healthcare_to_dataset,
)
from lib.services.datasets.models import EnrichmentStatus
from lib.services.datasets.schemas import DataHealthcareDatasetDTO


def test_healthcare_dto_extracts_tags_and_formats():
    dto = DataHealthcareDatasetDTO.model_validate(
        {
            "title": "Rate PUF",
            "identifier": "rate-2026",
            "keyword": ["healthcare", "Rate"],
            "theme": ["Marketplace PUF"],
            "distribution": [
                {"format": "CSV"},
                {"mediaType": "application/pdf"},
                {"downloadURL": "https://data.healthcare.gov/file/archive.zip"},
            ],
        }
    )

    assert dto.tags == ["Marketplace PUF", "Rate", "healthcare"]
    assert dto.file_formats == ["csv", "pdf", "zip"]


def test_map_healthcare_to_dataset_normalizes_core_fields():
    dto = DataHealthcareDatasetDTO.model_validate(
        {
            "title": "Agent Broker Registration Tracker",
            "identifier": "e4rr-zk4i",
            "description": "<p>Dataset for broker registration.</p>",
            "license": "https://www.usa.gov/publicdomain/label/1.0/",
            "keyword": ["healthcare"],
            "distribution": [{"format": "csv"}],
            "column_names": ["agent_id", "registration_status"],
        }
    )

    dataset = map_healthcare_to_dataset(dto)

    assert dataset.source_name == "data_healthcare_gov"
    assert dataset.external_id == "e4rr-zk4i"
    assert dataset.url == "https://data.healthcare.gov/dataset/e4rr-zk4i"
    assert dataset.description == "Dataset for broker registration."
    assert dataset.license == "public domain"
    assert dataset.file_formats == ["csv"]
    assert dataset.column_names == ["agent_id", "registration_status"]
    assert dataset.download_count == 0
    assert dataset.enrichment_status == EnrichmentStatus.ENRICHED.value
