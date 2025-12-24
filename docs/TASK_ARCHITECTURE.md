# Background Tasks Architecture

## Overview

Background tasks are organized by domain, with clear separation between task orchestration (Celery) and business logic (services).

## Task Organization

### 1. HuggingFace Tasks (`lib/crons/enrich.py`)

**Location:** `lib/crons/enrich.py`

**Tasks:**
- `enrich.fetch_hf_datasets` - Fetch datasets from HuggingFace API
- `enrich.generate_embeddings` - Generate embeddings for all datasets

**Why here:**
- HuggingFace is simple, single-phase ingestion
- Embedding generation is cross-source (works for all datasets)

### 2. Kaggle Tasks (`lib/services/enrichment/kaggle_parser/background.py`)

**Location:** `lib/services/enrichment/kaggle_parser/background.py`

**Tasks:**
- `kaggle.seed_initial` - Seed from Meta Kaggle CSV
- `kaggle.enrich_pending` - Enrich datasets via Kaggle API
- `kaggle.fetch_latest` - Fetch latest datasets incrementally

**Why separate file:**
- Kaggle has 3-phase architecture (seed → enrich → incremental)
- Complex domain-specific logic
- Lives close to Kaggle client and mappers

### 3. Cleanup Tasks (`lib/crons/cleanup.py`)

**Location:** `lib/crons/cleanup.py`

**Tasks:**
- `cleanup.check_broken_links` - Janitor loop for link validation
- `cleanup.remove_old_cache` - Redis cache maintenance

**Why here:**
- Infrastructure/maintenance tasks
- Not source-specific

## Architecture Principles

### Separation of Concerns

```
┌─────────────────────────────────────────┐
│ Celery Task (lib/crons/*.py)            │
│ - Thin wrapper                          │
│ - Parameter validation                  │
│ - Logging coordination                  │
│ - asyncio.run() bridge                  │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Service Layer (lib/services/*)          │
│ - Business logic                        │
│ - Batch processing                      │
│ - Error handling                        │
│ - Transaction management                │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Repository Layer (lib/repositories/*)   │
│ - Database operations                   │
│ - Query building                        │
└─────────────────────────────────────────┘
```

### Example: Embedding Generation

**Bad (before):**
```python
# All logic in task
@shared_task
def generate_embeddings(batch_size):
    # 100 lines of logic here
    datasets = repo.get(...)
    texts = [prepare(d) for d in datasets]
    embeddings = model.encode(texts)
    for emb in embeddings:
        repo.save(emb)
```

**Good (now):**
```python
# Task: thin wrapper
@shared_task
def generate_embeddings(batch_size):
    processor = EmbeddingProcessor(...)
    async with db.get_session() as session:
        return await processor.process_batch(session, batch_size)

# Service: business logic
class EmbeddingProcessor:
    async def process_batch(self, session, batch_size):
        datasets = await self.repo.get_for_embedding_generation(...)
        embeddings = self.embedder.batch_encode_datasets(...)
        return await self._save_embeddings(...)
```

## Task Discovery

### Registered Tasks

```bash
# View all registered tasks
celery -A lib.worker.celery_app inspect registered

# Expected output:
- enrich.fetch_hf_datasets
- enrich.generate_embeddings
- kaggle.seed_initial
- kaggle.enrich_pending
- kaggle.fetch_latest
- cleanup.check_broken_links
- cleanup.remove_old_cache
```

### Scheduled Tasks (Celery Beat)

Configured in `lib/worker.py:31`:

| Task | Schedule | Batch Size |
|------|----------|------------|
| `enrich.generate_embeddings` | 30 min | 100 |
| `enrich.fetch_hf_datasets` | 24 hrs | 1000 |
| `kaggle.enrich_pending` | 1 hr | 50 |
| `kaggle.fetch_latest` | 24 hrs | 100 |

## Service Layer Structure

### Embedding Services

**`lib/services/ml/embedder.py`**
- Low-level ML operations
- Model loading and inference
- Single/batch encoding

**`lib/services/ml/embedding_processor.py`**
- High-level business logic
- Dataset batch processing
- Database integration
- Error handling and recovery

### Enrichment Services

**`lib/services/enrichment/hf_parser/`**
- `client_hf.py` - HuggingFace API client
- `mapper.py` - DTO → Dataset mapping

**`lib/services/enrichment/kaggle_parser/`**
- `client_kaggle.py` - Kaggle API client
- `mapper.py` - DTO → Dataset mapping
- `background.py` - Kaggle-specific tasks
- `services/api_parser.py` - API response parsing
- `services/meta_parser.py` - CSV parsing

## Adding New Tasks

### 1. Simple Task (Single Source)

If task is source-specific (e.g., Zenodo):

```python
# lib/services/enrichment/zenodo_parser/background.py
@shared_task(name="zenodo.fetch_datasets")
def fetch_datasets(limit: int = 100):
    async def _fetch():
        client = ZenodoClient()
        # ... logic
    return asyncio.run(_fetch())
```

Then register in `lib/worker.py`:
```python
include=[
    "lib.services.enrichment.zenodo_parser.background"
]
```

### 2. Complex Task (Cross-Source)

If task affects multiple sources (e.g., Static Score):

1. Create service: `lib/services/scoring/static_score_calculator.py`
2. Create task: `lib/crons/scoring.py`
3. Register in worker

```python
# lib/services/scoring/static_score_calculator.py
class StaticScoreCalculator:
    async def calculate_batch(self, session, datasets):
        # Business logic
        pass

# lib/crons/scoring.py
@shared_task(name="scoring.calculate_static")
def calculate_static_scores(batch_size: int = 100):
    calculator = StaticScoreCalculator(...)
    # ... orchestration
```

## Testing

### Unit Test (Service Layer)

```python
import pytest
from lib.services.ml.embedding_processor import EmbeddingProcessor

@pytest.mark.asyncio
async def test_process_batch(mock_repo, mock_embedder):
    processor = EmbeddingProcessor(mock_repo, mock_embedder, logger)
    processed, failed = await processor.process_batch(session, 10)
    assert processed > 0
```

### Integration Test (Task)

```python
from lib.crons.enrich import generate_embeddings

def test_task_execution():
    result = generate_embeddings(batch_size=5)
    assert result["processed"] >= 0
    assert result["failed"] >= 0
```

### Manual Test Script

```bash
uv run python lib/scripts/test_embedding_task.py
```

## Common Patterns

### Pattern 1: Async Wrapper

```python
@shared_task
def my_task(batch_size: int):
    async def _process():
        async with container.db.get_session() as session:
            # async logic
            pass
    return asyncio.run(_process())
```

### Pattern 2: Service Injection

```python
@shared_task
def my_task(batch_size: int):
    service = MyService(
        repo=container.my_repo,
        client=container.my_client,
        logger=container.logger
    )
    async def _process():
        async with container.db.get_session() as session:
            return await service.process_batch(session, batch_size)
    return asyncio.run(_process())
```

### Pattern 3: Error Recovery

```python
class MyProcessor:
    async def process_batch(self, session, batch_size):
        items = await self.repo.get_pending(session, batch_size)

        for item in items:
            try:
                await self._process_item(session, item)
                self.processed += 1
            except Exception as e:
                self.logger.error(f"Failed {item.id}: {e}")
                self.failed += 1

        await self.repo.commit(session)
        return self.processed, self.failed
```

## Migration Notes

### Deprecated Tasks Removed

The following tasks were **removed** from `lib/crons/enrich.py`:
- `enrich.fetch_kaggle_seed` → use `kaggle.seed_initial`
- `enrich.fetch_kaggle_latest` → use `kaggle.fetch_latest`
- `enrich.enrich_kaggle_datasets` → use `kaggle.enrich_pending`

These were just wrappers around the real tasks in `kaggle_parser/background.py`.

### If You Have Scheduled Jobs

Update your `celery_app.conf.beat_schedule` to use the correct task names:

```python
# Before
'my-kaggle-seed': {
    'task': 'enrich.fetch_kaggle_seed',  # REMOVED
}

# After
'my-kaggle-seed': {
    'task': 'kaggle.seed_initial',  # Direct task
}
```
