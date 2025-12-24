# Embedding Generation Tasks

## Overview

The embedding generation system provides background tasks for creating vector embeddings for dataset metadata. These embeddings enable semantic search capabilities.

## Architecture

### Component Layers

```
┌─────────────────────────────────────────┐
│ Celery Task                             │
│ lib/crons/enrich.py:generate_embeddings │
│ - Orchestration                         │
│ - Logging                               │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Business Logic                          │
│ lib/services/ml/embedding_processor.py  │
│ - Batch processing                      │
│ - Error handling                        │
│ - Transaction management                │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ ML Layer                                │
│ lib/services/ml/embedder.py             │
│ - Model loading                         │
│ - Batch encoding                        │
│ - Vector generation                     │
└─────────────────────────────────────────┘
```

### Processing Flow

```
Datasets (status=ENRICHED, embedding=NULL)
    ↓
EmbeddingProcessor.process_batch()
    ↓
EmbeddingService.batch_encode_datasets()
    ↓
SentenceTransformer (all-MiniLM-L6-v2)
    ↓
DatasetRepository.mark_enriched(embedding=...)
    ↓
Datasets ready for search (is_ready_for_search=True)
```

### Selection Criteria

Datasets are selected when:
- `enrichment_status = 'enriched'`
- `embedding IS NULL`
- `is_active = true`

### Processing Details

**EmbeddingProcessor** ([lib/services/ml/embedding_processor.py](lib/services/ml/embedding_processor.py)):
1. Fetches datasets in batches (default: 100)
2. Prepares (title, description) tuples
3. Calls `EmbeddingService.batch_encode_datasets()`
4. Saves embeddings individually with error handling
5. Commits transaction

**EmbeddingService** ([lib/services/ml/embedder.py](lib/services/ml/embedder.py)):
- Combines: `"{title} {title} {description}"` (title weighted 2x)
- Batch encodes using sentence-transformers
- Returns 384-dimensional vectors as Python lists

## Running Tasks

### 1. Automatic (Celery Beat)

Embeddings are generated automatically every 30 minutes via Celery Beat scheduler.

**Schedule Configuration** (`lib/worker.py:31`):
```python
'generate-embeddings-every-30min': {
    'task': 'enrich.generate_embeddings',
    'schedule': 1800.0,  # 30 minutes
    'args': (100,)       # batch_size
}
```

**Start Celery Beat:**
```bash
# In docker-compose
docker-compose up worker

# Manual
celery -A lib.worker.celery_app beat --loglevel=info
```

### 2. Manual Trigger (API)

**Endpoint:** `POST /api/tasks/generate-embeddings`

**Request Body:**
```json
{
  "batch_size": 100
}
```

**Example:**
```bash
curl -X POST "http://localhost:8000/api/tasks/generate-embeddings" \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 50}'
```

**Response:**
```json
{
  "task_name": "enrich.generate_embeddings",
  "status": "queued",
  "message": "Task queued with ID: abc123..."
}
```

### 3. Direct Execution (Python)

**Test Script:** `lib/scripts/test_embedding_task.py`

```bash
uv run python lib/scripts/test_embedding_task.py
```

**Direct Call:**
```python
from lib.crons.enrich import generate_embeddings

result = generate_embeddings(batch_size=100)
print(result)
# {'processed': 95, 'failed': 5}
```

### 4. Celery CLI

```bash
# Queue task via Celery
celery -A lib.worker.celery_app call enrich.generate_embeddings --args='[100]'

# Monitor task
celery -A lib.worker.celery_app inspect active
```

## Monitoring

### Check Datasets Without Embeddings

```python
from lib.core.container import container
import asyncio

async def check():
    async with container.db.get_session() as session:
        datasets = await container.dataset_repo.get_for_embedding_generation(
            session, limit=1000
        )
        print(f"Datasets needing embeddings: {len(datasets)}")

asyncio.run(check())
```

### Check Task Status

```bash
# View active tasks
celery -A lib.worker.celery_app inspect active

# View scheduled tasks
celery -A lib.worker.celery_app inspect scheduled

# View registered tasks
celery -A lib.worker.celery_app inspect registered
```

### Logs

```bash
# Docker
docker-compose logs -f worker

# Check for embedding generation
docker-compose logs worker | grep "embedding generation"
```

## Configuration

### Embedding Model

Set in `.env`:
```bash
EMBEDDING_MODEL=all-MiniLM-L6-v2  # Default, 384 dimensions
```

**Alternative Models:**
- `all-mpnet-base-v2` (768 dim, better quality, slower)
- `paraphrase-MiniLM-L6-v2` (384 dim, paraphrase optimized)

**Note:** Changing the model requires:
1. Update database schema (Vector dimension)
2. Regenerate all embeddings

### Batch Size

Adjust based on:
- **GPU Memory:** Larger batches if GPU available
- **Worker Memory:** Reduce if OOM errors occur
- **Processing Speed:** Balance throughput vs. latency

**Recommended:**
- CPU: 32-64
- GPU: 128-256

## Performance

### Benchmarks (all-MiniLM-L6-v2)

| Hardware | Batch Size | Speed | Notes |
|----------|-----------|-------|-------|
| CPU (4 cores) | 32 | ~50 datasets/min | Default |
| GPU (T4) | 128 | ~500 datasets/min | CUDA enabled |

### Optimization Tips

1. **Increase Batch Size** (if memory allows):
```python
generate_embeddings(batch_size=200)
```

2. **GPU Acceleration:**
```bash
# Ensure PyTorch with CUDA is installed
pip install torch --index-url https://download.pytorch.org/whl/cu118
```

3. **Parallel Workers:**
```bash
# Run multiple workers
celery -A lib.worker.celery_app worker --concurrency=4
```

## Troubleshooting

### Task Not Running

**Check Worker:**
```bash
docker-compose ps worker
celery -A lib.worker.celery_app inspect ping
```

**Check Redis:**
```bash
docker-compose ps redis
redis-cli ping
```

### Out of Memory

**Reduce batch size:**
```python
generate_embeddings(batch_size=10)
```

**Check worker logs:**
```bash
docker-compose logs worker | grep -i "memory\|oom"
```

### Model Loading Fails

**Check model cache:**
```bash
ls ~/.cache/torch/sentence_transformers/
```

**Re-download model:**
```python
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('all-MiniLM-L6-v2')
```

### No Datasets Found

**Check enrichment status:**
```sql
SELECT
    enrichment_status,
    COUNT(*)
FROM datasets
WHERE is_active = true
GROUP BY enrichment_status;
```

**Run enrichment first:**
```bash
# For Kaggle
celery -A lib.worker.celery_app call kaggle.enrich_pending --args='[50]'

# For HuggingFace
celery -A lib.worker.celery_app call enrich.fetch_hf_datasets --args='[1000, 1]'
```

## Integration with Search

After embeddings are generated, datasets become searchable:

```python
from lib.models.dataset import Dataset

# Check if ready for search
dataset.is_ready_for_search
# True if: is_active=True AND enrichment_status='enriched' AND embedding IS NOT NULL
```

**Search Flow:**
1. User query → Generate query embedding
2. Vector similarity search (`<=>` operator)
3. Combine with keyword search and static_score
4. Return ranked results

## Related Tasks

- **`enrich.fetch_hf_datasets`** - Fetch datasets from HuggingFace
- **`kaggle.seed_initial`** - Seed from Kaggle Meta CSV
- **`kaggle.enrich_pending`** - Enrich Kaggle datasets via API
- **`kaggle.fetch_latest`** - Fetch latest Kaggle datasets

## Next Steps

After implementing embedding generation:
1. ✅ Implement Static Score calculation
2. ✅ Implement semantic search endpoint
3. ✅ Add keyword + semantic hybrid search
4. ✅ Implement time decay factor
