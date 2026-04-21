# Search — проектирование RAG-поиска

**Статус:** Draft
**Дата:** 2026-04-20

---

## 1. Проблема

Текущая реализация в [lib/services/datasets/repository.py:270](../../lib/services/datasets/repository.py#L270) и [lib/services/search/search_service.py](../../lib/services/search/search_service.py) — pure dense retrieval:

```
query → embedder.encode() → pgvector cosine → топ-N → post-rank
post-rank: final = 0.7 * semantic + 0.3 * static_score  (аддитивно)
```

**Пробелы относительно TDR §4.2 / §4.3:**

1. Нет keyword-компоненты (ts_rank, Postgres FTS).
2. Нет time decay (штраф за старость).
3. `static_score` используется как слагаемое, а не множитель — не отсекает мусор.

**Проблемы одного dense retrieval как такового:**

- Короткие запросы ("titanic") → embedding размыт, точные совпадения названия могут не попасть в топ.
- Редкие термины, имена собственные, версии моделей — embedding их "не знает".
- Длинные семантические запросы ("medical imaging for lung cancer detection") — работают отлично.

RAG-система должна сочетать оба подхода.

---

## 2. Целевая архитектура — Hybrid Search

Классическая RAG-пайплайн в 2-3 этапа.

```
query
  │
  ├─► Stage 1: Retrieval (parallel)
  │       ├─ Dense  (pgvector HNSW, cosine) → топ-100
  │       └─ Sparse (Postgres FTS, ts_rank) → топ-100
  │
  ├─► Stage 2: Fusion (RRF)
  │       объединяем два списка → топ-50 кандидатов
  │
  ├─► Stage 3: Re-ranking (финальная формула TDR §4.3)
  │       final = (α*semantic + β*keyword) * static_score * freshness_decay
  │       → топ-N (N=5 для UI)
  │
  └─► [опц.] Stage 4: Cross-encoder re-ranker
          топ-20 → cross-encoder(query, doc) → топ-5
```

---

## 3. Stage 1 — Retrieval

### 3.1. Dense retrieval (как сейчас)

- Модель: `all-MiniLM-L6-v2`, 384-dim
- Индекс: HNSW (`embedding <=> query_vec`)
- Выход: топ-100 по cosine distance
- Фильтры (`source_name`, `license`, `file_formats` и т.п.) применяются в `WHERE`

### 3.2. Sparse retrieval — новое

**Postgres FTS через `tsvector`.** Материализованная колонка в `datasets`:

```sql
ALTER TABLE datasets ADD COLUMN search_tsv tsvector
  GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(array_to_string(tags, ' '), '')), 'B') ||
    setweight(to_tsvector('english', coalesce(array_to_string(column_names, ' '), '')), 'B') ||
    setweight(to_tsvector('english', coalesce(description, '')), 'C')
  ) STORED;

CREATE INDEX idx_datasets_search_tsv ON datasets USING GIN (search_tsv);
```

**Веса по полям:**
- `A` (title) — максимум
- `B` (tags, column_names) — средне-высоко (структурные сигналы)
- `C` (description) — низко (может быть "водянистым")

**Запрос:**

```sql
SELECT id, ts_rank_cd(search_tsv, plainto_tsquery('english', :query)) AS score
FROM datasets
WHERE is_active AND search_tsv @@ plainto_tsquery('english', :query)
ORDER BY score DESC
LIMIT 100;
```

**Важно:** фильтры (`source_name`, `license` и т.п.) применяются в **обоих** retrieval-ветках одинаково.

### 3.3. Язык токенизатора

- Для MVP: `'english'` (основная масса HF/Kaggle на английском).
- Для ру-контента: в будущем `'russian'` + multilingual embedder (например `paraphrase-multilingual-MiniLM-L12-v2` или `BGE-M3`).
- Смешанный контент: можно держать **две** `tsvector`-колонки (en + ru) и объединять через `||`.

---

## 4. Stage 2 — Fusion (RRF)

**Reciprocal Rank Fusion** — простой и мощный метод, не требующий нормализации скоров разных шкал.

```
score_rrf(d) = Σ  1 / (k + rank_i(d))
              i∈{dense, sparse}

k = 60  (стандартное значение из литературы)
rank_i(d) = позиция d в списке i (1..100), или ∞ если d отсутствует
```

**Преимущества:**
- Не нужно нормализовать cosine distance и ts_rank.
- Устойчив к выбросам.
- Работает лучше, чем weighted sum скоров в большинстве публичных бенчмарков (BEIR, MS MARCO).

**Выход:** топ-50 кандидатов с RRF-скором.

---

## 5. Stage 3 — Re-ranking

Применяем формулу TDR §4.3 к каждому из 50 кандидатов:

```
final_score = (α * semantic + β * keyword) * static_score * freshness_decay

α = 0.7, β = 0.3
semantic        = 1 - cosine_distance               ∈ [0, 1]
keyword         = min(ts_rank_cd / T_max, 1.0)      ∈ [0, 1]  (T_max калибруется)
static_score    = уже в [0, 1] (см. static-score-design.md)
freshness_decay = 1 / (1 + 0.1 * age_years)         ∈ (0, 1]
  где age_years = (now - source_created_at) / 365.25
```

**Почему умножение на `static_score` и `freshness_decay`:**
- "Некачественный" датасет (низкий static_score) не должен всплывать даже при идеальной релевантности.
- Старые датасеты (>5 лет) получают `~0.67`, >10 лет — `~0.5`. Мягкий, но ощутимый штраф.

**Что мы храним:**
- `semantic` и `keyword` достаются из Stage 1 (уже посчитаны).
- `static_score` — из БД.
- `freshness_decay` — из `source_created_at`, считается на лету.

Выход: топ-5 (или сколько запросил пользователь).

---

## 6. Stage 4 (опционально) — Cross-Encoder re-rank

Если после Stage 3 качество всё ещё недостаточно.

**Модель:** `cross-encoder/ms-marco-MiniLM-L-6-v2` или `BAAI/bge-reranker-base`.

**Принцип:** cross-encoder оценивает пару `(query, doc)` одним forward pass. Точнее bi-encoder embeddings, но медленнее — O(N) инференсов, не один.

**Схема:**
- Stage 3 выдаёт топ-20 (вместо топ-5).
- Cross-encoder пересчитывает релевантность для каждой пары.
- Берём топ-5 по cross-encoder score.

**Стоимость:** ~50-200 мс на 20 пар на CPU, <50 мс на GPU. Приемлемо.

**Выигрыш:** +5-15% к NDCG@5 по нашим ожиданиям (из публичных MS MARCO бенчмарков).

**Когда делать:** только если простая Stage 3 даёт недостаточную точность на реальных запросах. Сначала собираем данные (SearchLog), потом решаем.

---

## 7. Подготовка документа под embedding (важно)

Сейчас нужно проверить, как [EmbeddingProcessor](../../lib/services/datasets/ml/embedding_processor.py) собирает текст для embedding.

**Рекомендация:** структурированный текст с field-префиксами.

```python
f"title: {title}. tags: {', '.join(tags or [])}. "
f"description: {description or ''}. "
f"columns: {', '.join(column_names or [])}."
```

**Обоснование:**
- Современные retrieval-модели (BGE, E5) обучены на instruction-aware текстах с маркерами полей.
- Даже MiniLM выигрывает от явной структуры — меньше ambiguity между "titanic" (название) и "titanic" (слово в описании).
- **Критично:** включать `column_names` в embedding — они содержат главный сигнал того, "о чём" датасет (ml-термины, названия фичей).

**Query-side:** для запросов оставляем как есть (пользователь пишет естественным языком). Можно добавить префикс `"query: "` для моделей E5-семейства, но MiniLM в этом не нуждается.

---

## 8. Embedding-модель — стоит ли менять

| Модель | Dim | Качество (MTEB avg) | Скорость | Комментарий |
|---|---|---|---|---|
| `all-MiniLM-L6-v2` (текущая) | 384 | ~56 | ★★★ | MVP-tier, быстрая |
| `intfloat/e5-base-v2` | 768 | ~62 | ★★ | Хороший upgrade |
| `BAAI/bge-base-en-v1.5` | 768 | ~63 | ★★ | SOTA-tier |
| `BAAI/bge-large-en-v1.5` | 1024 | ~64 | ★ | Больше, медленнее |
| `BGE-M3` | 1024 | ~63 (multi-lang) | ★ | Для ру-контента |

**Рекомендация:** не трогать сейчас. Смена = пересчёт всех embeddings + изменение размерности `Vector(384)` → миграция БД + переиндексация HNSW. Большой рефакторинг.

**Когда менять:** после внедрения hybrid search и cross-encoder, если метрики на реальном трафике всё ещё недостаточны.

---

## 9. Кэширование (TDR §6)

Ортогонально к пайплайну, но важно для latency.

- **Query Cache** (`search:{hash(normalized_query + filters)}`, TTL 30 мин): кэш финального топ-5 JSON. Снимает нагрузку с embedder + retrieval + re-ranker на популярных запросах.
- **Dataset Cache** (`dataset:{uuid}`, TTL 24 ч): кэш полной карточки датасета для детальной страницы.
- **Embedding Cache** (опц.): кэш `embedder.encode(query)` для повторяющихся запросов. Хотя MiniLM быстр, экономит ~30-50 мс.

Это отдельная задача (см. TDR §6), не связана с логикой ранжирования.

---

## 10. Метрики и оценка

Без метрик невозможно понять, работает ли hybrid search лучше baseline.

**Offline:**
- Собрать golden set: 30-50 запросов с ручной разметкой релевантных датасетов.
- Метрики: NDCG@5, MRR@10, Recall@50.
- Сравнение: baseline (dense-only) vs hybrid vs hybrid+cross-encoder.

**Online (из SearchLog):**
- CTR@1, CTR@5 (кликнул ли пользователь в топ-1 / топ-5).
- Позиция первого клика.
- Zero-result rate (доля запросов без результатов).

---

## 11. План внедрения

1. **Миграция: добавить `search_tsv` + GIN-индекс.** Без даунтайма (`GENERATED ALWAYS ... STORED`).
2. **Sparse retrieval в `DatasetRepository`.** Новый метод `fts_search`.
3. **Hybrid + RRF в `SearchService`.** Параллельный вызов dense и sparse + fusion.
4. **Re-rank по формуле TDR §4.3** (freshness + множитель static).
5. **Проверить и исправить подготовку документа в `EmbeddingProcessor`.**
6. **Метрики CTR из SearchLog** (уже логируем).
7. **[опц.] Cross-encoder re-ranker** — по результатам метрик.
8. **[опц.] Кэш поиска в Redis** — отдельная задача.

---

## 12. Открытые вопросы

1. **Калибровка α, β.** TDR предлагает 0.7 / 0.3 — начать с них, потом тюнить по NDCG на golden set.
2. **Калибровка `T_max` для нормализации ts_rank.** Зависит от корпуса — замерить эмпирически перцентиль 95%.
3. **Нужен ли query expansion?** (LLM-синонимы) — скорее всего избыточно на этом этапе. Hybrid search покрывает большую часть проблемы.
4. **Мультиязычный контент.** Пока откладываем, но архитектура позволяет добавить без переписывания.
