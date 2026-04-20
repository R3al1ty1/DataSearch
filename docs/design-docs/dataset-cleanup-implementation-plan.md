# Implementation Plan: Dataset Cleanup Feature

**Design Doc:** [dataset-cleanup.md](./dataset-cleanup.md)  
**Branch:** `feature/cleanup-datasets`  
**Date:** 2026-04-17

---

## Порядок выполнения

```
Шаг 1 (migration)
  → Шаг 2 (LinkCheckerService)
    → Шаг 3 (repo methods)
      → Шаг 4 (CleanupService)
        → Шаг 5 (schemas)
          → Шаг 6 (admin router)
          → Шаг 7 (visit endpoint)
          → Шаг 8 (celery task) → Шаг 9 (beat + container)
Шаг 10 (reactivation) — параллельно с 3–9
Шаг 11 (tests) — после 2–9
```

> **Ресёрч по rate limits завершён.** Результаты зафиксированы ниже и в дизайн-документе (секция 4.4 / Performance).

---

## Шаг 1 — DB Migration

**Файл:** `lib/services/datasets/models.py`

Добавить в `Dataset.__table_args__`:

```python
Index(
    "idx_datasets_active_last_checked",
    "is_active",
    "last_checked_at",
    postgresql_where=(text("is_active = true"))
)
```

**Миграция Alembic** (новый файл):
```python
op.execute("""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_active_last_checked
    ON datasets (is_active, last_checked_at NULLS FIRST)
    WHERE is_active = true
""")
```

**Откат:**
```python
op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_datasets_active_last_checked")
```

**Зависимости:** нет.

---

## Шаг 2 — `LinkCheckerService`

**Файл:** `lib/services/datasets/validation/link_checker.py` (сейчас пустой)

### Rate limit findings (результаты ресёрча)

| Источник | Лимит (официальный) | Рекомендуемый RPS | Комментарий |
|---|---|---|---|
| **HuggingFace** | Pages: 100 req / 5 min (anon), 200 / 5 min (free) | **0.5 req/s** | Официальная документация. Dataset URL — это "Pages" bucket. При 429 — заголовок `RateLimit` содержит секунды до сброса |
| **Kaggle** | Не задокументировано | **0.3 req/s** | Пользователи получают 429 при интенсивных запросах. Официального числа нет. Консервативно: 1 запрос в ~3 сек |
| **OpenML** | Не задокументировано | **0.5 req/s** | Академический open-source проект; rate limiting не описан в docs. Консервативно: 1 запрос в ~2 сек |

**Добавить в `Settings` (`lib/core/config.py`):**
```python
LINK_CHECK_TIMEOUT_SECONDS: float = 10.0
LINK_CHECK_MAX_CONCURRENCY: int = 20
LINK_CHECK_DOMAIN_RPS: dict = {
    "huggingface.co": 0.5,
    "kaggle.com": 0.3,
    "openml.org": 0.5,
}
LINK_CHECK_DEFAULT_RPS: float = 1.0  # для неизвестных доменов
```

### Что реализовать

```python
@dataclass
class LinkCheckResult:
    dataset_id: UUID
    url: str
    is_reachable: bool
    http_status: int | None
    error_type: str | None  # "timeout" | "dns_error" | "http_error" | None
    duration_ms: int

class LinkCheckerService:
    def __init__(self, settings: Settings, logger: Logger): ...

    async def check_url(self, dataset_id: UUID, url: str) -> LinkCheckResult:
        """HEAD-запрос с таймаутом. HTTP 429 -> is_reachable=True."""

    async def check_batch(
        self, datasets: list[tuple[UUID, str]]
    ) -> list[LinkCheckResult]:
        """asyncio.gather + Semaphore(max_concurrency) + per-domain token bucket."""
```

**Per-domain throttling:** `dict[str, asyncio.Lock]` + timestamp последнего запроса на домен. Домен — `urllib.parse.urlparse(url).netloc`.

**Маппинг ответов:**
| Ситуация | `is_reachable` | `error_type` |
|---|---|---|
| HTTP 2xx / 3xx | `True` | `None` |
| HTTP 429 | `True` | `None` (временная блокировка, не деактивируем) |
| HTTP 4xx / 5xx | `False` | `"http_error"` |
| `TimeoutException` | `False` | `"timeout"` |
| `ConnectError` | `False` | `"dns_error"` |

**Зависимости:** нет (кроме `httpx`, уже в зависимостях).

---

## Шаг 3 — Новые методы `DatasetRepository`

**Файл:** `lib/services/datasets/repository.py`

Добавить в класс `DatasetRepository`:

```python
async def get_stale_for_validation(
    self,
    session: AsyncSession,
    batch_size: int = 200,
    stale_after_hours: int = 48,
) -> list[Dataset]:
    """
    WHERE is_active=true
      AND (last_checked_at IS NULL OR last_checked_at < now() - interval)
    ORDER BY last_checked_at ASC NULLS FIRST
    LIMIT batch_size
    """

async def bulk_update_check_results(
    self,
    session: AsyncSession,
    results: list[LinkCheckResult],
) -> tuple[int, int]:
    """
    Один UPDATE на все результаты (не построчно).
    Обновляет is_active и last_checked_at=now() для всех проверенных.
    Возвращает (total_updated, deactivated_count).
    """
```

**Зависимости:** Шаг 1 (индекс), Шаг 2 (тип `LinkCheckResult`).

---

## Шаг 4 — `CleanupService`

**Файл:** новый `lib/services/datasets/cleanup_service.py`

```python
@dataclass
class CleanupBatchResult:
    checked: int
    deactivated: int
    errors: int

class CleanupService:
    def __init__(
        self,
        dataset_repo: DatasetRepository,
        enrichment_log_repo: EnrichmentLogRepository,
        link_checker: LinkCheckerService,
        logger: Logger,
    ): ...

    async def run_cleanup_batch(
        self,
        session: AsyncSession,
        batch_size: int = 200,
        stale_after_hours: int = 48,
    ) -> CleanupBatchResult: ...

    async def deactivate_dataset(
        self,
        session: AsyncSession,
        dataset_id: UUID,
        check_result: LinkCheckResult,
    ) -> None: ...

    async def get_stats(
        self, session: AsyncSession, hours: int = 48
    ) -> CleanupStatsResponse: ...
```

**Зависимости:** Шаги 2, 3.

---

## Шаг 5 — Схемы

**Файл:** новый `lib/services/datasets/cleanup_schemas.py`

```python
class CleanupTriggerRequest(BaseModel):
    batch_size: int = 200
    stale_after_hours: int = 48

class CleanupTriggerResponse(BaseModel):
    task_id: str
    status: str
    message: str

class CleanupErrorBreakdown(BaseModel):
    error_type: str
    count: int

class CleanupStatsResponse(BaseModel):
    period_hours: int
    total_checked: int
    total_deactivated: int
    total_errors: int
    deactivation_rate_percent: float
    last_run_at: datetime | None
    breakdown_by_error_type: list[CleanupErrorBreakdown]
```

**Зависимости:** нет.

---

## Шаг 6 — Admin Router

**Файл:** новый `lib/api/handlers/admin.py`

```python
router = APIRouter(prefix="/admin", tags=["Admin"])

@router.post("/datasets/cleanup/trigger", response_model=CleanupTriggerResponse, status_code=202)
async def trigger_cleanup(
    body: CleanupTriggerRequest,
    current_user: Annotated[User, Depends(get_current_active_user)],
    db: AsyncSession = Depends(container.db.get_session),
): ...
# Проверить current_user.is_superuser, иначе 403
# celery_app.send_task("cleanup.check_inactive_datasets", kwargs={...})

@router.get("/datasets/cleanup/stats", response_model=CleanupStatsResponse)
async def get_cleanup_stats(
    hours: int = 48,
    current_user: Annotated[User, Depends(get_current_active_user)],
    db: AsyncSession = Depends(container.db.get_session),
): ...
```

Зарегистрировать в `lib/router.py`:
```python
from lib.api.handlers.admin import router as admin_router
api_router.include_router(admin_router)
```

**Зависимости:** Шаги 4, 5.

---

## Шаг 7 — Inline-проверка в `visit_dataset`

**Файл:** `lib/services/search/router.py`

Добавить после получения датасета из репозитория:

```python
try:
    check_result = await container.link_checker.check_url(dataset.id, dataset.url)
    if not check_result.is_reachable:
        await container.cleanup_service.deactivate_dataset(db, dataset.id, check_result)
        raise HTTPException(status_code=404, detail="Dataset is no longer available")
except asyncio.TimeoutError:
    pass  # при таймауте самой проверки — не деактивируем, делаем редирект
```

**Зависимости:** Шаг 4.

---

## Шаг 8 — Celery-задача

**Файл:** `lib/crons/cleanup.py`

Заменить оба stub-а (`check_broken_links`, `remove_old_cache`) реальными реализациями. `check_broken_links` → `check_inactive_datasets`:

```python
@shared_task(
    name="cleanup.check_inactive_datasets",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    soft_time_limit=3000,
    time_limit=3600,
)
def check_inactive_datasets(self, batch_size: int = 200, stale_after_hours: int = 48) -> dict:
    logger = container.logger
    logger.info(f"Starting dataset cleanup: batch_size={batch_size}, stale_after_hours={stale_after_hours}")

    async def _process():
        async with container.db.get_session() as session:
            return await container.cleanup_service.run_cleanup_batch(
                session, batch_size, stale_after_hours
            )

    try:
        result = asyncio.run(_process())
        logger.info(f"Cleanup completed: {result}")
        return {"checked": result.checked, "deactivated": result.deactivated, "errors": result.errors}
    except Exception as exc:
        logger.error(f"Cleanup task failed, retrying: attempt={self.request.retries}, error={exc}")
        raise self.retry(exc=exc)
```

**Зависимости:** Шаг 4.

---

## Шаг 9 — Beat Schedule + Container

**Файл 1:** `lib/worker.py` — добавить в `beat_schedule`, удалить старый ключ `check_broken_links` если был:

```python
'check-inactive-datasets-every-2d': {
    'task': 'cleanup.check_inactive_datasets',
    'schedule': 172800.0,
    'kwargs': {'batch_size': 200, 'stale_after_hours': 48}
},
```

**Файл 2:** `lib/core/container.py` — добавить два `@cached_property`:

```python
@cached_property
def link_checker(self) -> LinkCheckerService:
    from lib.services.datasets.validation.link_checker import LinkCheckerService
    return LinkCheckerService(settings=self.settings, logger=self.logger)

@cached_property
def cleanup_service(self) -> CleanupService:
    from lib.services.datasets.cleanup_service import CleanupService
    return CleanupService(
        dataset_repo=self.dataset_repo,
        enrichment_log_repo=self.enrichment_log_repo,
        link_checker=self.link_checker,
        logger=self.logger,
    )
```

**Зависимости:** Шаги 2, 4.

---

## Шаг 10 — Реактивация в Enrichment Pipeline

**Файл:** `lib/services/datasets/repository.py` — метод `bulk_upsert`

Проверить, что `is_active` не входит в `DatasetFieldsExclude.ON_UPDATE` (сейчас не входит — поведение правильное).

Явно закрепить намерение: убедиться, что маппер HF (`mapper.py`) и Kaggle (`mapper.py`) не выставляют `is_active=False` при создании `Dataset` объекта. Если не выставляют — добавить явный `is_active: bool = True` в маппер, чтобы поведение было намеренным, а не случайным (default-значение).

**Зависимости:** нет (изолированное изменение).

---

## Шаг 11 — Тесты

**Файлы:**

| Файл | Что тестируем |
|---|---|
| `tests/unit/test_link_checker.py` | `check_url`: все типы ответов (200, 301, 404, 500, 429, timeout, DNS); per-domain throttling |
| `tests/unit/test_cleanup_service.py` | `run_cleanup_batch`: правильный подсчёт deactivated; `deactivate_dataset`: вызовы repo + log_repo |
| `tests/unit/test_dataset_repository_cleanup.py` | `get_stale_for_validation`: фильтрация по `last_checked_at` и `is_active`; `bulk_update_check_results`: только недоступные помечаются `is_active=False` |
| `tests/integration/test_cleanup_flow.py` | Полный флоу с тестовой БД: создать датасеты → mock httpx → `run_cleanup_batch` → проверить `is_active` и записи в `enrichment_logs` |

**Зависимости:** Шаги 2–9.
