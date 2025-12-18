# Kaggle Parser Module

## Overview

This module handles data ingestion from Kaggle, using a three-phase approach:
1. **Phase 1 (Seed)**: Bulk import from Meta Kaggle CSV
2. **Phase 2 (Enrichment)**: Background enrichment via Kaggle API
3. **Phase 3 (Incremental)**: Daily updates for newest datasets

## Background Tasks

### `kaggle.seed_initial`

Seeds database with minimal metadata from Meta Kaggle CSV (~50,000 datasets).

```python
# Run once initially
from lib.services.enrichment.kaggle_parser.background import seed_initial_datasets

result = seed_initial_datasets.delay(batch_size=1000, force_redownload=False)
```

**When to run:** Once initially, then monthly to refresh.

### `kaggle.enrich_pending`

Enriches datasets with MINIMAL status using Kaggle API.

```python
# Run periodically (hourly)
from lib.services.enrichment.kaggle_parser.background import enrich_pending_datasets

result = enrich_pending_datasets.delay(batch_size=50)
```

**When to run:** Hourly, to gradually enrich all datasets.

### `kaggle.fetch_latest`

Fetches newest/updated datasets directly from Kaggle API.

```python
# Run daily
from lib.services.enrichment.kaggle_parser.background import fetch_latest_datasets

result = fetch_latest_datasets.delay(limit=100, sort_by='updated')
```

**When to run:** Daily, for incremental updates.

## Database Flow

```
Phase 1: CSV → map_meta_to_dataset() → Repository.bulk_upsert()
         Status: MINIMAL

Phase 2: Get MINIMAL datasets → API → map_enriched_to_dataset() → Repository.upsert()
         Status: MINIMAL → ENRICHING → PENDING

Phase 3: API latest → map_enriched_to_dataset() → Repository.bulk_upsert()
         Status: PENDING (if new)
```

## Enrichment Logging

All enrichment attempts are logged to `dataset_enrichment_logs` table:
- **SUCCESS**: Dataset enriched successfully
- **FAILED**: Critical error (dataset marked as failed)
- **RATE_LIMITED**: 429 error (will retry later)

## Error Handling

- **Rate limits (429)**: Task stops, logs RATE_LIMITED, keeps dataset in queue
- **Not found (404)**: Dataset marked as FAILED, is_active=False
- **Other errors**: Logged, dataset marked as FAILED after max attempts (3)

## Usage Example

```python
# In Celery Beat schedule (docker-compose.yml or code)
from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    'kaggle-enrich-hourly': {
        'task': 'kaggle.enrich_pending',
        'schedule': crontab(minute=0),  # Every hour
        'args': (50,)  # batch_size
    },
    'kaggle-latest-daily': {
        'task': 'kaggle.fetch_latest',
        'schedule': crontab(hour=2, minute=0),  # 2 AM daily
        'args': (100, 'updated')
    },
}
```
