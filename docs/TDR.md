
# TDR: Dataset Discovery & Search Engine

**Version:** 1.0
**Date:** December 2025
**Status:** Approved for Implementation

---

## 1. Executive Summary
Проектируемый сервис — это агрегатор и интеллектуальный поисковик датасетов для ML-инженеров.
**Ключевая особенность:** Сервис не хранит петабайты данных (файлы), а индексирует метаданные из внешних источников (Kaggle, HuggingFace, Zenodo), создавая унифицированный поисковый индекс.
**Бизнес-цель:** Предоставить пользователю топ-5 наиболее релевантных датасетов с учетом семантики, качества данных и юридической чистоты.

---

## 2. High-Level Architecture

Мы используем архитектуру **"Smart Monolith"** с асинхронными воркерами. Это обеспечивает минимальную задержку при поиске и надежность при сборе данных.

### Компоненты системы
1.  **API Gateway (FastAPI):** Единая точка входа. Обрабатывает поисковые запросы, клики (редиректы) и отдачу метаданных.
2.  **Search Engine & Storage (PostgreSQL + pgvector):** Единое хранилище. Отвечает и за хранение метаданных, и за векторный поиск, и за фильтрацию.
3.  **Background Workers (Celery):**
    *   *Ingestion Workers:* Сбор данных из внешних API.
    *   *Enrichment Workers:* Вычисление эмбеддингов и Static Score.
    *   *Janitor Workers:* Фоновая проверка битых ссылок.
4.  **Cache Layer (Redis):** Снижение нагрузки на БД и ML-модель.

---

## 3. Data Storage Schema (PostgreSQL 16)

Отказываемся от ElasticSearch и Qdrant в пользу **PostgreSQL** для упрощения инфраструктуры и транзакционной целостности.

### Основная таблица `datasets`

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | UUID (PK) | Внутренний идентификатор |
| `source_name` | VARCHAR | Источник (kaggle, hf, uci) |
| `external_id` | VARCHAR | ID на источнике (для дедупликации) |
| `title` | TEXT | Название (индексируется GIN) |
| `description` | TEXT | Описание |
| `column_names` | TEXT[] | Список колонок (индексируется GIN) |
| `meta` | JSONB | Специфика: license, file_format, size_bytes |
| `embedding` | VECTOR(384) | Вектор от `all-MiniLM-L6-v2` |
| `static_score` | FLOAT | **Качество датасета (0.0 - 1.0)** |
| `created_at` | TIMESTAMP | Дата создания оригинала |
| `last_checked_at`| TIMESTAMP | Дата последней проверки доступности |
| `is_active` | BOOLEAN | Soft Delete флаг |

**Индексы:**
1.  **HNSW (`embedding`):** Для быстрого векторного поиска (ANN).
2.  **GIN (`title`, `column_names`):** Для полнотекстового поиска (Keywords).
3.  **B-Tree (`is_active`, `source_name`):** Для быстрых фильтров.

---

## 4. Search & Ranking Logic (Core Algorithm)

Мы используем двухступенчатую модель ранжирования.

### 4.1. Static Score (Offline Calculation)
Вычисляется один раз при добавлении датасета. Определяет "индекс здоровья".
*   **Формула:** `(FormatWeight + DocumentationWeight + LicenseWeight + Log(Popularity)) / MaxScore`
*   **Пример весов:**
    *   `Format`: Parquet/CSV = 1.0, PDF = 0.1.
    *   `License`: MIT = 1.0, Unknown = 0.5.
    *   `Documentation`: Есть описание колонок = 1.0, нет = 0.0.

### 4.2. Relevance Score (Online Calculation)
Вычисляется в SQL при запросе.
*   **Semantic Similarity:** `1 - (embedding <=> query_vector)`.
*   **Keyword Match:** `ts_rank` (Postgres FTS) по названию и колонкам.
*   **Time Decay:** Штраф за старость данных (>3 лет).

### 4.3. Итоговый SQL запрос
```sql
ORDER BY (
    (
       (1 - (embedding <=> :query_vec)) * 0.7 +  -- Семантика (Главный фактор)
       LEAST(ts_rank(...) * 0.3, 0.5)            -- Ключевые слова (Уточняющий)
    )
    * static_score                               -- Фильтр качества (Умножение!)
    * (1.0 / (1.0 + 0.1 * age_years))            -- Свежесть
) DESC
```

---

## 5. Ingestion & Validation Strategy

### 5.1. Обход лимитов (Rate Limiting)
*   Использовать библиотеку `tenacity` для **Exponential Backoff** (при 429 ошибке ждать 2s, 4s, 8s...).
*   Лимиты в воркерах: 1 запрос в секунду на домен.
*   Режим работы: **Incremental**. Спрашиваем у API только `datasets created > yesterday`.

### 5.2. Валидация доступности (3 Loops)
1.  **Ingestion Loop:** При добавлении проверяем, что ссылка жива.
2.  **User Loop (Smart Redirect):** Ссылка в UI ведет на `/api/visit/{id}` -> Бэкенд делает `HEAD` запрос -> Если OK, редирект 302. Если 404, помечаем `is_active=False`.
3.  **Janitor Loop:** Cron-задача раз в ночь проверяет 5% самых старых записей методом `HEAD`.

---

## 6. Caching Strategy (Redis)

1.  **Query Cache (TTL: 30 min):**
    *   Key: `search:{hash(normalized_query)}`
    *   Value: Готовый JSON списка датасетов.
    *   *Зачем:* Снять нагрузку с ML-инференса на популярных запросах.

2.  **Dataset Cache (TTL: 24 hours):**
    *   Key: `dataset:{uuid}`
    *   Value: JSON карточки датасета.
    *   *Зачем:* Мгновенная загрузка страницы деталей.

---

## 7. Implementation Roadmap

### Phase 1: MVP (Недели 1-2)
*   **Infra:** Docker Compose (App + Postgres + Redis).
*   **Data:** Реализация ETL только для **HuggingFace** (самый удобный API).
*   **Search:** Простой векторный поиск + SQL фильтры.
*   **UI:** Простой Swagger API.

### Phase 2: Quality & Logic (Недели 3-4)
*   **Ranking:** Внедрение формулы Static Score и Keyword Search.
*   **Sources:** Подключение Kaggle (требует API key) и Zenodo.
*   **Validation:** Реализация "Smart Redirect".

### Phase 3: Production Hardening (Месяц 2+)
*   **Deploy:** Вынос БД на отдельный сервер/Managed Service.
*   **Features:** Добавление фильтров по типам задач (Computer Vision, NLP) через классификацию на этапе сбора.

---

## 8. Stack Recommendation Summary
*   **Language:** Python 3.11
*   **Web Framework:** FastAPI
*   **DB:** PostgreSQL 16 + pgvector
*   **Broker/Cache:** Redis
*   **ORM:** SQLAlchemy (Async) + Psycopg3
*   **NLP:** `sentence-transformers` (Model: `all-MiniLM-L6-v2`) (надо посмотреть, какие еще варианты можно использовать)
