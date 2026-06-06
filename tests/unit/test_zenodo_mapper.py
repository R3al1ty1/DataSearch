from lib.services.datasets.enrichment.zenodo_parser.mapper import map_zenodo_to_dataset


def test_map_zenodo_to_dataset_maps_core_fields():
    record = {
        "id": 123,
        "created": "2026-01-01T10:00:00Z",
        "updated": "2026-01-02T10:00:00Z",
        "doi": "10.5281/zenodo.123",
        "conceptdoi": "10.5281/zenodo.120",
        "conceptrecid": "120",
        "links": {"html": "https://zenodo.org/records/123"},
        "metadata": {
            "title": "Example dataset",
            "description": "<p>Useful dataset&nbsp;description.</p>",
            "keywords": ["health", "csv"],
            "subjects": [{"term": "clinical"}],
            "license": {"id": "cc-by-4.0"},
            "version": "v1",
            "resource_type": {"type": "dataset"},
            "access_right": "open",
        },
        "files": [
            {"key": "data.csv", "size": 100, "checksum": "md5:abc"},
            {"key": "archive.tar.gz", "size": 200},
        ],
        "stats": {
            "downloads": {"total": 12, "unique": 10},
            "views": 30,
        },
    }

    dataset = map_zenodo_to_dataset(record)

    assert dataset.source_name == "zenodo"
    assert dataset.external_id == "123"
    assert dataset.title == "Example dataset"
    assert dataset.url == "https://zenodo.org/records/123"
    assert dataset.description == "Useful dataset description."
    assert dataset.tags == ["clinical", "csv", "health"]
    assert dataset.license == "cc-by-4.0"
    assert dataset.file_formats == ["csv", "tar.gz"]
    assert dataset.total_size_bytes == 300
    assert dataset.download_count == 12
    assert dataset.view_count == 30
    assert dataset.like_count == 0
    assert dataset.column_names is None
    assert dataset.row_count is None
    assert dataset.source_meta["doi"] == "10.5281/zenodo.123"
    assert dataset.source_meta["files"] == record["files"]


def test_map_zenodo_to_dataset_handles_minimal_record():
    dataset = map_zenodo_to_dataset({"id": 456, "metadata": {"title": "Minimal"}})

    assert dataset.external_id == "456"
    assert dataset.title == "Minimal"
    assert dataset.url == "https://zenodo.org/records/456"
    assert dataset.tags is None
    assert dataset.license is None
    assert dataset.file_formats is None
    assert dataset.total_size_bytes is None
    assert dataset.download_count == 0
    assert dataset.view_count == 0
