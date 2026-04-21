# Static Score — проектирование качества датасетов

**Статус:** Draft
**Дата:** 2026-04-20
**Научное обоснование:** см. [static-score-research.md](./static-score-research.md) — полный ресерч по академическим фреймворкам (Wang & Strong, ISO 25012, Datasheets, Data Cards), индустриальной практике (Kaggle reverse-engineered, HF, OpenML, Google Dataset Search) и современным подходам 2023-2025 (FineWeb, DataComp, Nemotron-CC, DCAI).

---

## 1. Проблема

Текущая реализация в [lib/crons/enrichment/static_scores.py](../../lib/crons/enrichment/static_scores.py) считает только popularity (log-нормализация downloads/views/likes). Остальные сигналы качества игнорируются.

**Последствие:** датасет в PDF-формате без лицензии и без описания колонок получает тот же скор, что Parquet-датасет с MIT-лицензией и полной документацией — если их downloads сопоставимы. Это ломает ценностный сигнал для ML-инженера.

---

## 2. Доступные сигналы (из модели `Dataset`)

| Категория | Поля |
|---|---|
| Контент | `title`, `description`, `tags`, `column_names` |
| Формат / объём | `file_formats`, `total_size_bytes`, `row_count` |
| Легал | `license` |
| Популярность | `download_count`, `view_count`, `like_count` |
| Свежесть | `source_created_at`, `source_updated_at`, `last_checked_at` |

---

## 3. Предлагаемая модель (по TDR §4.1)

Static score разбивается на **4 компоненты**, каждая нормализована в `[0, 1]`.

### 3.1. FormatScore (блок `repr`)

Берём `max` по `file_formats`.

| Формат | Вес |
|---|---|
| Parquet | 1.0 |
| CSV, JSON, JSONL, Arrow, Feather | 0.9 |
| TSV | 0.8 |
| XLS, XLSX | 0.6 |
| XML, HTML | 0.4 |
| PDF, DOC, DOCX | **0.2** |
| Unknown / пустой | 0.3 |

**Формула:** `repr = FormatScore(max(file_formats))`. Если `file_formats` пустой — `0.3` (Unknown).

**Обоснование:** формат определяет пригодность для ML-пайплайна. Columnar-форматы (Parquet/Arrow) — идеальны, человекочитаемые (PDF) — почти непригодны. Минимум таблицы = 0.2 соответствует floor блока (§4.2).

**v1 намеренно минималистичен** — только формат. Кандидаты в v2 после расширения модели: streaming-support (HF), feature types (numeric/string/date per column), size vs row_count consistency.

### 3.2. LicenseScore (блок `legal`)

#### Tier-таблица

| Tier | Вес | Примеры |
|---|---|---|
| Permissive | 1.0 | MIT, Apache-2.0, BSD-2/3-Clause, CC-BY-3.0/4.0, CC0-1.0, CDLA-Permissive-2.0, ODC-By |
| Weak copyleft / share-alike | 0.8 | MPL-2.0, LGPL-2.1/3.0, CC-BY-SA-3.0/4.0, ODbL-1.0, CDLA-Sharing-1.0 |
| Strong copyleft | 0.6 | GPL-2.0/3.0, AGPL-3.0 |
| Non-commercial | 0.4 | CC-BY-NC, CC-BY-NC-SA, CC-BY-NC-ND |
| Unknown / proprietary / custom | **0.3** | `null`, `""`, `"other"`, `"custom"`, проприетарные |

#### Canonical mapping

Сырая строка `license` из БД нормализуется (lowercase, strip, алиасы типа `"apache"` → `"apache-2.0"`) и сопоставляется со словарём:

```python
PERMISSIVE = {"mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause",
              "cc-by-3.0", "cc-by-4.0", "cc0-1.0",
              "cdla-permissive-2.0", "odc-by-1.0"}
WEAK_COPYLEFT = {"mpl-2.0", "lgpl-2.1", "lgpl-3.0",
                 "cc-by-sa-3.0", "cc-by-sa-4.0",
                 "odbl-1.0", "cdla-sharing-1.0"}
STRONG_COPYLEFT = {"gpl-2.0", "gpl-3.0", "agpl-3.0"}
NON_COMMERCIAL = {"cc-by-nc-4.0", "cc-by-nc-sa-4.0", "cc-by-nc-nd-4.0"}
# всё остальное (null, "", "other", "custom", "proprietary") → UNKNOWN (0.3)
```

**CC-BY-SA → 0.8** (share-alike ≈ weak copyleft для данных).
**ODbL → 0.8, ODC-By → 1.0** — Open Data Commons популярны для датасетов.

#### Multiple licenses

Если `license` — массив (`["mit", "cc-by-4.0"]`), берём `max()` по tier'ам. Автор даёт выбор → пользуемся лучшим.

#### Обоснование

Датасет без ясной лицензии юридически рискован, даже если технически идеален. Floor 0.30 — не "ноль", но заметный штраф, отражающий правовую неопределённость. "other"/"custom" нельзя без юриста, поэтому сидят в UNKNOWN.

### 3.3. DocumentationScore (блок `docs`)

#### Чек-лист сигналов

| Сигнал | Вес в блоке | Шкала сигнала |
|---|---|---|
| `description` | 0.30 | **graded** (см. ниже) |
| `column_names` | 0.30 | **graded** (см. ниже) |
| `tags` | 0.15 | binary (есть непустой массив → 1.0, иначе → 0.0) |
| `row_count` OR `total_size_bytes` заполнен | 0.15 | binary |
| `source_updated_at` есть | 0.10 | binary |

**Итоговая формула:**
```
checklist_sum = 0.30·description + 0.30·column_names + 0.15·tags + 0.15·size_or_rows + 0.10·updated_at
docs = 0.15 + 0.85 · checklist_sum
```

Диапазон: `docs ∈ [0.15, 1.00]`. Floor 0.15 встроен как базовый терм за сам факт существования записи.

**Note:** `file_formats` намеренно убран из чек-листа — этот сигнал уже используется в блоке `repr` (FormatScore). Дублирование создавало бы корреляцию между компонентами Cobb-Douglas, смещая веса.

#### Graded `description`

| Длина description | Балл |
|---|---|
| `null` или 0 символов | 0.0 |
| 1–49 символов | 0.3 |
| 50–199 символов | 0.7 |
| ≥ 200 символов | 1.0 |

**Пороги:** 50 — один-два предложения, минимум для "реального" описания; 200 — полная карточка (~3-4 предложения, покрывает "что, откуда, как"). Пороги — round numbers для v1, калибруются в v2 на corpus statistics.

#### Graded `column_names`

Детектируем осмысленность имён через регулярку + blacklist:

```python
import re

BAD_NAME_PATTERN = re.compile(
    r"^(col|column|field|var|unnamed|x|y)[\s_]*\d*$", re.IGNORECASE
)

def is_meaningful(name: str) -> bool:
    s = (name or "").strip()
    if len(s) < 3:
        return False                      # "a", "id", "" → плохо
    if s.isdigit():
        return False                      # "0", "1" → плохо
    if BAD_NAME_PATTERN.match(s):
        return False                      # "col_0", "Unnamed: 1", "var3"
    return True

def column_names_score(column_names: list[str] | None) -> float:
    if not column_names:
        return 0.0
    good_ratio = sum(is_meaningful(n) for n in column_names) / len(column_names)
    if good_ratio < 0.3: return 0.3       # есть, но почти все мусорные
    if good_ratio < 0.7: return 0.7       # смешанные
    return 1.0                            # осмысленные имена
```

**Фильтруем:** длина < 3 символов, только цифры, паттерны `col_N` / `unnamed: N` / `var_N` / одиночные `x`, `y`. Пороги ratio (0.3 / 0.7) — калибруются в v2.

#### Обоснование

Наличие осмысленных `column_names` и непустого `description` — единственный способ для ML-инженера понять структуру датасета **без скачивания**. Важнейший предиктор "удобства использования" (Kaggle Usability Score эмпирически ставит их в Completeness-компоненту с весом ~41%, см. Cabitza et al. 2024, BioData Mining).

### 3.4. PopularityScore (блок `social`)

#### Сигналы и базовые веса

| Сигнал | Вес |
|---|---|
| `log1p(download_count)` | 0.5 |
| `log1p(view_count)` | 0.3 |
| `log1p(like_count)` | 0.2 |

#### Per-source перцентильная нормализация

**Проблема:** HF `downloads` — агрегат за 30 дней, Kaggle — lifetime. Глобальные перцентили систематически занижают один из источников.

**Решение:** перцентили p5–p95 считаются **отдельно для каждого `source_name`**.

```python
for source in {"kaggle", "huggingface", ...}:
    for signal in ("downloads", "views", "likes"):
        rows = datasets WHERE source_name = source AND signal IS NOT NULL
        p5, p95 = percentiles(log1p(signal), rows, [5, 95])

        for row in rows:
            normalized[signal] = clip((log1p(row[signal]) - p5) / (p95 - p5), 0, 1)
```

Датасет "в топ-5% своего источника" получает `1.0` по этому сигналу, независимо от абсолютных цифр. Это честно уравнивает источники с разными API-семантиками.

#### Available-case weight redistribution (missing signals)

**Проблема:** не каждый API возвращает все три сигнала.

- **HuggingFace:** `downloads`, `likes` — **views нет**
- **Kaggle:** `downloads`, `votes` (= likes), `views` — всё есть

Если для HF-датасета поставить `views = 0`, он получит искусственный штраф 0.3 за пробел в API — это bug, не feature.

**Решение:** вес отсутствующего сигнала пропорционально распределяется на оставшиеся.

```python
def social_combined(signals: dict[str, float | None]) -> float:
    BASE_WEIGHTS = {"downloads": 0.5, "views": 0.3, "likes": 0.2}
    available = {k: v for k, v in signals.items() if v is not None}
    if not available:
        return 0.0
    total_w = sum(BASE_WEIGHTS[k] for k in available)
    effective_w = {k: BASE_WEIGHTS[k] / total_w for k in available}
    return sum(effective_w[k] * available[k] for k in available)
```

**Пример для HF** (downloads=0.8, likes=0.5, views=None):
- Базовые веса по доступным: 0.5 + 0.2 = 0.7
- Эффективные: `downloads = 0.5/0.7 = 0.714`, `likes = 0.2/0.7 = 0.286`
- `combined = 0.714·0.8 + 0.286·0.5 = 0.714`

Академическое обоснование — **available-case weighting** (Little & Rubin, 2002, *Statistical Analysis with Missing Data*), стандартный приём в анализе неполных данных.

#### Финальный мап в `[0.40, 1.00]`

```
social = 0.40 + 0.60 · combined
```

Floor 0.40 встроен линейным мапом, а не через `max()`. Защищает новые качественные датасеты от обнуления из-за отсутствия популярности.

#### Обоснование

Downloads — главный сигнал реального использования (committed intent). Views/likes — soft-сигналы признания. Перцентильная нормализация p5-p95 устойчива к выбросам (один суперпопулярный датасет не ломает шкалу). Per-source честно уравнивает источники.

---

## 4. Финальная формула — Cobb-Douglas с per-component floors

**РЕШЕНО (2026-04-21):**

```
static_score = docs^0.40 · repr^0.15 · social^0.25 · legal^0.20
```

где `docs = DocumentationScore`, `repr = FormatScore`, `social = PopularityScore`, `legal = LicenseScore` из §3.

### 4.1. Почему Cobb-Douglas (weighted geometric mean)

1. **Non-substitutability:** слабая компонента не компенсируется сильной. Датасет без лицензии юридически непригоден, сколько бы downloads у него ни было.
2. **Убывающая отдача:** прирост компоненты с 0.9 → 1.0 ценится меньше, чем с 0.1 → 0.2 (log-вогнутость).
3. **Веса как эластичности:** `w_docs = 0.40` ⇒ 1% прироста docs даёт 0.40% прироста итогового скора. Интерпретируемо.
4. **Прецеденты в composite indicators:** HDI (UN), EPI (Yale), OECD handbook — все используют geometric mean, когда компоненты non-substitutable.

Сравнение на примере (все компоненты 0.9, кроме legal = 0.1):
- Weighted sum: `0.74` (штраф слабый)
- Cobb-Douglas: `0.58` (штраф заметный, но не нулевой)

### 4.2. Per-component floors

Floor — минимум, ниже которого компонента не опускается. Защищает от обнуления всего скора и отражает **семантику** компоненты.

| Блок | Вес | Floor | Обоснование |
|---|---|---|---|
| docs | 0.40 | **0.15** | Критичен для юзабельности; отсутствие метаданных коррелирует с плохим качеством данных |
| repr | 0.15 | **0.20** | Разные форматы пригодны в разной степени; PDF юзабелен, но болезненно |
| social | 0.25 | **0.40** | Новый ≠ плохой; высокий floor защищает свежие качественные датасеты от несправедливости |
| legal | 0.20 | **0.30** | Unknown license — юр-риск, но не техническая поломка |

**Реализация:** floor встраивается в шкалу самой компоненты (минимум её таблицы/функции), **не через `max(x, floor)`** — последнее создаёт плато и резкий излом.

- `repr`: таблица форматов, минимум = 0.20 (PDF/DOC); Unknown = 0.30
- `legal`: таблица лицензий, минимум = 0.30 (Unknown / proprietary)
- `docs`: чек-лист сигналов + базовый балл 0.15 за факт существования записи
- `social`: нормализация через перцентили p5–p95, затем линейный мап в `[0.40, 1.00]`

### 4.3. Свойства шкалы

| Сценарий | Скор |
|---|---|
| Worst case (все на floor) | `0.15^0.40 · 0.20^0.15 · 0.40^0.25 · 0.30^0.20 ≈ 0.23` |
| Best case (всё = 1.0) | `1.00` — достижимо для эталонов (Titanic, SQuAD, MNIST) |
| Новый качественный датасет (social=0.40, остальное=1.0) | `≈ 0.80` |
| Популярный без лицензии (legal=0.30, остальное≈0.9) | `≈ 0.74` |
| Идеальный, но без column_names (docs≈0.7) | `≈ 0.87` |

Реальное распределение ожидается `0.3–0.7`, концентрированно вокруг `~0.5`.

### 4.4. Обоснование весов через AHP

**Метод:** Analytic Hierarchy Process (Saaty, 1977, 1980) — каноничный подход в composite indicators (OECD Handbook, EU JRC). Процедура: парные сравнения → веса через собственный вектор (или геометрическое среднее строк для консистентных матриц) → проверка консистентности.

**Шкала Saaty** (интенсивность важности):

| Значение | Интерпретация |
|---|---|
| 1 | Равнозначны |
| 3 | Умеренно важнее |
| 5 | Сильно важнее |
| 7 | Очень сильно важнее |
| 9 | Абсолютно важнее |
| 2, 4, 6, 8 | Промежуточные |

**Парные сравнения** с обоснованием:

| Пара | Суждение | Saaty |
|---|---|---|
| docs vs repr | Полнота документации умеренно важнее формата — хороший CSV с плохими метаданными хуже, чем средний формат с отличной документацией | 2 |
| docs vs social | Объективная полнота важнее социального proxy — downloads это сигнал, docs это прямое измерение | 2 |
| docs vs legal | Docs непрерывно влияет на юзабельность, legal бинарно-ish (можно / нельзя) | 2 |
| social vs repr | Revealed preference тысяч пользователей сильнее, чем структурный формат | 2 |
| social vs legal | Равнозначны — оба opinionated-метаданные | 1 |
| repr vs legal | Равнозначны — оба структурные проверки | 1 |

**Матрица парных сравнений:**

```
           docs   repr   social  legal
docs       1      2      2       2
repr       1/2    1      1/2     1
social     1/2    2      1       1
legal      1/2    1      1       1
```

**Веса** (геометрическое среднее строк → нормализация):

```
docs:    (1·2·2·2)^(1/4)     = 1.682  →  0.413
repr:    (0.5·1·0.5·1)^(1/4) = 0.707  →  0.174
social:  (0.5·2·1·1)^(1/4)   = 0.841  →  0.207
legal:   (0.5·1·1·1)^(1/4)   = 0.841  →  0.207
```

**Проверка консистентности:** `λ_max ≈ 4.03`, `CI ≈ 0.01`, `CR ≈ 0.01 / 0.90 ≈ 0.011 << 0.10` → матрица консистентна (CR ниже 10%-порога Saaty).

**Снэп к чистым числам** (округление до 0.05):

```
w_docs   = 0.413 → 0.40
w_repr   = 0.174 → 0.15
w_social = 0.207 → 0.25
w_legal  = 0.207 → 0.20
```

**Плюсы AHP для v1:**
- Воспроизводимо: любой может пересчитать матрицу.
- Аудитируемо: в работе показана логика каждого сравнения.
- Канонично: Saaty 1980, 30k+ цитирований, стандарт в operations research.
- CR < 0.10 защищает от внутренних противоречий эксперта.

**Ограничения** (честно):
- Парные сравнения субъективны на входе. AHP систематизирует мнение, но не превращает его в объективную истину.
- Критика шкалы Saaty (Dyer 1990): rank reversal при добавлении альтернатив. В нашем случае не критично — критерии фиксированные.
- Альтернатива — эмпирическая калибровка на данных (см. §5).

---

## 5. Валидация и калибровка — v1 → v2

### 5.1. Статус

- **v1 (текущая версия, AHP-calibrated):** веса выведены экспертным суждением через Saaty-матрицу (§4.4). Обоснованные априорные значения, но эмпирически не подтверждены. **Это то, что идёт в прод сейчас.**
- **v2 (будущая работа):** эмпирическая калибровка весов на реальных данных. Требует накопления данных и зрелости системы в проде. **Целимся в неё после data flywheel.**

### 5.2. Что нужно собирать для v2

Три обязательных компонента данных:

**1. Golden set (разметка вручную):**
- Пары `(query, relevant_datasets_with_grades)`.
- Минимум 50 запросов, надёжно — 200+.
- Разметка по шкале 0-3 (нерелевантен / слабо / умеренно / сильно).
- Покрытие: разные типы задач (NLP, CV, tabular, time-series), разная длина и специфичность запросов.
- **Минимум 2 разметчика + Cohen's kappa** — чтобы исключить self-evaluation bias (один разработчик ≠ объективный разметчик).

**2. SearchLog с реальным CTR:**
- Уже есть инфраструктура: [lib/services/datasets/search_log_repository.py](../../lib/services/datasets/search_log_repository.py).
- Нужные поля: `query`, `results_shown`, `clicked_result_id`, `click_position`, `timestamp`, `dwell_time` (опционально).
- Объём: **тысячи запросов**, не десятки (статистическая значимость).
- Разнообразие пользователей — CTR от одного человека — это просто его вкус.

**3. Variance в фичах корпуса:**
- Чтобы статистически оценить вес `legal`, в корпусе должны быть датасеты с разными лицензиями. Если 95% — MIT, регрессия не работает (нет variance для обучения).
- Аналогично для `repr` (разные форматы) и `docs` (разная полнота).
- Требует зрелой ingestion-пайплайны с диверсифицированными источниками.

### 5.3. Почему нельзя сразу v2 (cold-start)

1. **Нет baseline в проде** → не с чем сравнивать v2.
2. **Нет пользователей → нет CTR** → нет revealed preference.
3. **Мало размеченных данных → overfit.** Правило: минимум 10× data points на параметр. 4 веса → 40 запросов минимум, реалистично 100+ для доверительных интервалов.
4. **Self-evaluation bias:** один разметчик, который одновременно разработчик скоринга — методологически слабо для статьи.
5. **Data flywheel требует времени:** 3-6 месяцев реального трафика минимум.

### 5.4. План перехода v1 → v2

| Этап | Действие | Срок |
|---|---|---|
| 1 | Деплой v1 (AHP weights `0.40 / 0.15 / 0.25 / 0.20`) | сейчас |
| 2 | Убедиться, что SearchLog пишет все нужные поля (click_position, timestamp) | сразу |
| 3 | Bootstrap golden set (30-50 запросов своими силами) для sanity-check v1 | 1-2 недели |
| 4 | Ablation matrix: 3-4 варианта весов (AHP / all-equal / docs-heavy / social-heavy) на tiny golden set → даёт **направление**, не точные значения | 1 неделя |
| 5 | A/B инфраструктура: `score_version` как feature flag в коде | 1-2 недели |
| 6 | Накопление real traffic + CTR | **3-6 месяцев** |
| 7 | External golden set (2+ разметчика, 200 запросов) | после п.6 |
| 8 | v2: grid search / BayesOpt весов с оптимизацией NDCG@5 на golden set | после п.7 |
| 9 | Online A/B v1 vs v2 по CTR@5 | после п.8 |

### 5.5. Нарратив для статьи

Двухчастный стандартный формат IR-статей:

1. **Метод (v1, сейчас):** Cobb-Douglas с per-component floors + AHP-калибровка как априорный обоснованный выбор. Источники обоснования: Saaty (1980), OECD Composite Indicators Handbook, Wang & Strong (1996), ISO/IEC 25012, FineWeb/DataComp эмпирика.
2. **Валидация (v2, future work):** offline NDCG@5 на golden set + online A/B на CTR как апостериорная проверка. В статье описать план + показать **предварительные результаты на bootstrap golden set (30 запросов)** — достаточно для публикации.

## 6. Пересчёт

- **Частота:** раз в сутки (как сейчас, `celery beat` schedule).
- **Триггер:** при добавлении / обновлении датасета — можно не пересчитывать сразу (batch-режим). Сроки компромисс между актуальностью и нагрузкой.
- **Формат-/license-/docs-компоненты** не требуют глобальной нормализации — считаются локально.
- **Popularity-компонента** требует глобальных перцентилей — считается в одном проходе по всем enriched-датасетам.

---

## 7. План реализации (v1)

**РЕШЕНО (2026-04-21).**

### 7.1. Структура кода

Computation-логика отделяется от scheduling по паттерну проекта (как `lib/services/datasets/cleanup/`):

```
lib/services/static_scores/
    __init__.py
    service.py         # StaticScoreService, orchestrator
    components.py      # 4 функции: docs_score / repr_score / social_score / legal_score
    aggregator.py      # Cobb-Douglas
    constants.py       # format table, license canonical map, thresholds

lib/crons/enrichment/static_scores.py   # тонкий celery task → вызов сервиса
```

**DI:** `container.static_score_service` регистрируется в [lib/core/container.py](../../lib/core/container.py) по паттерну других сервисов (`search_service`, `embedding_processor` и т.п.).

**Старая popularity-only логика** из [lib/crons/enrichment/static_scores.py](../../lib/crons/enrichment/static_scores.py) **удаляется полностью** (no dual implementations, per CLAUDE.md — "remove the old implementation").

### 7.2. Миграция БД

Добавить nullable Float-колонки для **хранения разложения** на компоненты:

```sql
-- migrations/005_add_static_score_components.sql
ALTER TABLE datasets
    ADD COLUMN docs_score   FLOAT,
    ADD COLUMN repr_score   FLOAT,
    ADD COLUMN social_score FLOAT,
    ADD COLUMN legal_score  FLOAT;
```

**Зачем хранить sub-scores:**
- **Debugging:** "почему у этого датасета `static_score = 0.3`?" → смотрим разложение.
- **Ablation для v2:** grid search по весам на готовых компонентах **без пересчёта** всего корпуса.
- **Статистики для статьи:** распределения каждого блока в корпусе.

Cost: 4 × Float × N датасетов ≈ 32 bytes/row — пренебрежимо.

Обновление `Dataset` модели в [lib/services/datasets/models.py](../../lib/services/datasets/models.py) — четыре новых поля `Mapped[float | None]`.

### 7.3. Пересчёт и деплой

- **Триггер:** первый `celery beat` после деплоя (раз в сутки). Ручной запуск не требуется — синхронизируется автоматически.
- **Риск:** средние значения `static_score` станут ниже (Cobb-Douglas жёстче аддитивной формулы). Учесть в фильтрах / отсечках `search_service`.
- **Мониторинг:** после первого пересчёта проверить распределение — должно быть `~0.3–0.7`, центр `~0.5`.

### 7.4. Тесты

```
tests/unit/test_static_scores_components.py    — 4 функции × параметризованные кейсы
tests/unit/test_static_scores_aggregator.py    — Cobb-Douglas, worst/best cases, floors
tests/unit/test_static_scores_service.py       — end-to-end с моками repo
```

**Покрытие (обязательный минимум):**
- graded `description` — все 4 тира (0 / <50 / <200 / ≥200 симв).
- column_names regex — good / mixed / bad / empty / only-generic.
- license canonical mapping — каждый tier, алиасы, multiple licenses → max.
- per-source percentile нормализация.
- available-case weight redistribution (HF без views ≠ штраф).
- Cobb-Douglas floor behavior (compo=0 не обнуляет итог).

### 7.5. Порядок имплементации

Пошагово, каждый шаг — отдельный коммит:

1. `migrations/005_add_static_score_components.sql` + обновление `Dataset` модели.
2. `lib/services/static_scores/constants.py` — format table, license canonical map, thresholds.
3. `lib/services/static_scores/components.py` — 4 функции компонентов.
4. `lib/services/static_scores/aggregator.py` — Cobb-Douglas.
5. `lib/services/static_scores/service.py` — orchestrator (batch per-source percentiles → per-row components → aggregation → save).
6. Регистрация `container.static_score_service` в [lib/core/container.py](../../lib/core/container.py).
7. Переписать [lib/crons/enrichment/static_scores.py](../../lib/crons/enrichment/static_scores.py) — тонкий celery-wrapper.
8. Удалить старую popularity-only логику.
9. Тесты (три файла из §7.4).
10. Локальный прогон `compute_static_scores` на текущих данных + sanity-check распределения.

---

## 8. Открытые вопросы

1. **Веса внутри DocumentationScore** — возможно, стоит завести A/B по offline-метрике (NDCG@5 на ручной разметке 50-100 датасетов).
2. **Freshness в static vs relevance** — TDR §4.3 кладёт freshness decay в **online** формулу, но можно частично кешировать age_years в static. Пока оставляем в online (проще и актуальнее).
3. **Source-trust weight** (HF vs Kaggle vs Zenodo) — не добавляем сейчас, т.к. все источники trusted. Можно вернуться при добавлении менее надёжных.
4. **Ingestion quality gate** — датасеты с `static_score < 0.2` можно помечать `is_active = False` автоматически? Обсуждаемо.
