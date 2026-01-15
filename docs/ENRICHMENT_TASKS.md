# Source-Specific Enrichment Tasks

This directory contains Celery tasks organized by data source.

## Structure

```
lib/crons/enrichment/
├── __init__.py
├── hf.py       # HuggingFace tasks
├── kaggle.py   # Kaggle tasks
└── README.md
```

## HuggingFace Tasks (`hf.py`)

### `hf.fetch_datasets`

Fetches datasets from HuggingFace API.

**Args:**
- `limit` (int): Maximum number of datasets to fetch
- `days_back` (int): Fetch datasets modified in last N days (0 = all)

**Returns:**
```python
{
    "total_fetched": 1234,
    "total_inserted": 1100,
    "source": "huggingface"
}
```

**Usage:**
```python
# Via Celery
from lib.crons.enrichment.hf import fetch_datasets
result = fetch_datasets.delay(limit=1000, days_back=1)

# Direct call
result = fetch_datasets(limit=1000, days_back=0)  # Fetch all
```

**Scheduled:** Daily (86400s) via Celery Beat

---

## Kaggle Tasks (`kaggle.py`)

Kaggle uses a 3-phase architecture:

### Phase 1: `kaggle.seed_initial`

Seeds database with minimal metadata from Meta Kaggle CSV.

**Args:**
- `batch_size` (int): Datasets per batch (default: 1000)
- `force_redownload` (bool): Force CSV re-download (default: False)

**Returns:**
```python
{
    "total_processed": 5000,
    "total_inserted": 4800,
    "source": "kaggle_meta_csv"
}
```

**Usage:**
```python
from lib.crons.enrichment.kaggle import seed_initial

# Initial seed
result = seed_initial.delay(batch_size=1000, force_redownload=False)

# Force refresh
result = seed_initial(batch_size=1000, force_redownload=True)
```

**Run:** Once initially, then occasionally to refresh

---

### Phase 2: `kaggle.enrich_pending`

Enriches datasets with MINIMAL/PENDING status via Kaggle API.

**Args:**
- `batch_size` (int): Maximum datasets to process (default: 50)

**Returns:**
```python
{
    "enriched": 45,
    "failed": 5,
    "source": "kaggle"
}
```

**Features:**
- Rate limiting: 1 req/sec
- Stops on 429 (rate limit exceeded)
- Logs enrichment attempts and errors
- Individual error handling per dataset

**Usage:**
```python
from lib.crons.enrichment.kaggle import enrich_pending

result = enrich_pending.delay(batch_size=50)
```

**Scheduled:** Hourly (3600s) via Celery Beat

---

### Phase 3: `kaggle.fetch_latest`

Fetches latest/updated datasets for incremental updates.

**Args:**
- `limit` (int): Maximum datasets to fetch (default: 100)
- `sort_by` (str): Sort order - 'updated', 'hottest', 'votes' (default: 'updated')

**Returns:**
```python
{
    "total_processed": 100,
    "total_inserted": 95,
    "source": "kaggle_api"
}
```

**Usage:**
```python
from lib.crons.enrichment.kaggle import fetch_latest

# Fetch updated datasets
result = fetch_latest.delay(limit=100, sort_by='updated')

# Fetch hottest datasets
result = fetch_latest(limit=50, sort_by='hottest')
```

**Scheduled:** Daily (86400s) via Celery Beat

---

## Task Registration

Tasks are registered in `lib/worker.py`:

```python
include=[
    "lib.crons.enrich",           # Cross-source tasks (embeddings)
    "lib.crons.cleanup",          # Infrastructure tasks
    "lib.crons.enrichment.hf",    # HuggingFace
    "lib.crons.enrichment.kaggle", # Kaggle
]
```

## Viewing Registered Tasks

```bash
celery -A lib.worker.celery_app inspect registered
```

Expected output:
```
- enrich.generate_embeddings
- hf.fetch_datasets
- kaggle.seed_initial
- kaggle.enrich_pending
- kaggle.fetch_latest
- cleanup.check_broken_links
- cleanup.remove_old_cache
```

## Migration from Old Structure

**OLD** (deprecated):
```python
from lib.services.enrichment.kaggle_parser.background import (
    seed_initial_datasets,
    enrich_pending_datasets,
    fetch_latest_datasets
)
```

**NEW** (current):
```python
from lib.crons.enrichment.kaggle import (
    seed_initial,
    enrich_pending,
    fetch_latest
)

from lib.crons.enrichment.hf import fetch_datasets
```

## Adding New Sources

To add a new data source (e.g., Zenodo):

1. Create `lib/crons/enrichment/zenodo.py`
2. Define tasks with `@shared_task` decorator
3. Register in `lib/worker.py` includes
4. Add to beat schedule if needed

Example:
```python
# lib/crons/enrichment/zenodo.py
from celery import shared_task
from lib.core.container import container

@shared_task(name="zenodo.fetch_datasets")
def fetch_datasets(limit: int = 100):
    # Implementation
    pass
```

Then in `lib/worker.py`:
```python
include=[
    # ...
    "lib.crons.enrichment.zenodo",
]
```
