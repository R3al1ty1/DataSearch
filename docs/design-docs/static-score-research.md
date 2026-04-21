# Dataset Quality Scoring — Research Report

**Статус:** Research, source material for article
**Дата:** 2026-04-20
**Область:** Как правильно измерять и считать качество датасета для поисковой системы, которая агрегирует датасеты из внешних источников (Kaggle, HuggingFace, OpenML, Zenodo и т.д.).

---

## 0. Цели ресерча и структура

Для компонента `static_score` нужно научно обоснованное решение:
1. **Какие измерения качества существуют** — полный список.
2. **Какие из них применимы** к нашему контексту (metadata-only аггрегатор).
3. **Как их комбинировать** — взвешенная сумма, умножение, геометрическое среднее, learned classifier.
4. **Как валидировать** — что наш скор действительно предсказывает полезность.
5. **Что подходит для разных ML-задач** — quality относительно.

Ресерч выполнен по трём направлениям:
- **Часть I.** Академические фреймворки качества данных (Wang & Strong, DAMA, ISO, Datasheets, Data Cards, etc.)
- **Часть II.** Индустриальная практика (Kaggle Usability Score, HF, OpenML, Papers with Code, Google Dataset Search, data-quality tools)
- **Часть III.** Современные подходы 2023-2025 (LLM pretraining filters, Data-Centric AI, task-specific signals, learned quality classifiers, LLM-as-judge)

Финальные разделы — синтез и рекомендация.

---

# Часть I. Академические фреймворки

## 1.1. Таблица ключевых фреймворков

| Фреймворк | Год | Ключевой вклад | Релевантность к dataset scoring |
|---|---|---|---|
| **Wang & Strong — "Beyond Accuracy"** | 1996 | 15 измерений качества данных в 4 категориях (intrinsic / contextual / representational / accessibility), основано на опросе 118 атрибутов у потребителей данных и факторном анализе | Основополагающая таксономия; contextual + representational дают прямой маппинг на metadata-сигналы |
| **Redman — "Data Quality for the Information Age"** | 1996-2001 | Трёхуровневая модель (data model / data values / representation) с ~26 измерениями | Дополнительная детализация оси "представления" (format, portability, interpretability) |
| **DAMA-DMBOK (+ DDQ v1.2)** | 2017 / 2020 | Industry-consensus 6 "core" измерений: **Accuracy, Completeness, Consistency, Timeliness, Uniqueness, Validity** (+ Integrity, Reasonability, Currency в DMBOK 2.0) | De-facto operational vocabulary; 4 из 6 требуют row-level данных |
| **ISO/IEC 25012:2008** | 2008 | Международный стандарт: 15 характеристик разделены на inherent vs system-dependent | Citeable vocabulary; inherent / system-dependent separation даёт защищённость выбора измерений |
| **Datasheets for Datasets** (Gebru et al.) | 2018 / CACM 2021 | 57 вопросов в 7 секциях: Motivation, Composition, Collection, Preprocessing, Uses, Distribution, Maintenance | Канонический стандарт "что должно быть задокументировано"; completeness → scorable |
| **Data Statements for NLP** (Bender & Friedman) | 2018 | Curation rationale + language variety + speaker/annotator demographics | NLP-specific representational bias signals |
| **Dataset Nutrition Label** (Holland et al., MIT) | 2018 | 7 модулей: Metadata, Provenance, Variables, Statistics, Non-conformity, Pair Plots, Probabilistic Model | Ближайший аналог автоматизируемой "карточки качества" |
| **Data Readiness Levels** (Lawrence) | 2017 | Bands A/B/C по аналогии с NASA TRL: C (exists, hearsay) → B (electronically faithful) → A (task-ready) | Stage abstraction: C→B = electronic availability, B→A = task context. Отлично ложится на "task-conditional quality" |
| **Data Cards** (Pushkarna et al., Google) | 2022 | 31-aspect human-centred template; дизайн по lifecycle stage / stakeholder | Мотивирует **purposeful, audience-aware** шкалирование |
| **Dataset Cartography** (Swayamdipta et al.) | EMNLP 2020 | "Data map" (confidence × variability) → easy / ambiguous / hard / mislabelled regions | Learning-theoretic per-example quality; обосновывает label-noise как ось качества |
| **Confident Learning / cleanlab** (Northcutt et al.) | NeurIPS 2021 / JAIR | Класс-conditional noise estimation; ~100k label-errors в ImageNet | Empirical proof, что label noise сдвигает accuracy; оправдывает label-quality как first-class axis |
| **Data Shapley** (Ghorbani & Zou) | ICML 2019 | Shapley-based per-sample data valuation, удовлетворяет null-player / symmetry / additivity axioms | Теоретическое обоснование, что "data value" well-defined, not subjective |
| **CleanML** (Li, Rao et al.) | 2019/2021 | Бенчмарк: 13 datasets × 5 error types × 7 ML models, с hypothesis testing | Empirical evidence, что разные ошибки дают разный downstream-impact → обосновывает dimension-specific weights |
| **HoloClean** (Rekatsinas et al., VLDB) | 2017 | Probabilistic inference для holistic repair, ~90% precision / ~76% recall | "Cleanability" как ось качества |

---

## 1.2. Унифицированный список измерений (по всем фреймворкам)

Аннотируем по доступности в нашем metadata-only контексте: `[M]` — измеряется напрямую, `[P]` — partial proxy, `[N]` — требует доступа к содержимому.

### Intrinsic dimensions (собственные свойства данных)
- **Accuracy** — соответствие реальному миру (Wang & Strong, ISO 25012, DAMA). `[N]`
- **Objectivity** — беспристрастность (Wang & Strong). `[P]` — proxy через source/author reputation
- **Believability / Credibility** (Wang & Strong, ISO 25012). `[P]` — provenance + license + popularity
- **Reputation** — репутация источника (Wang & Strong). `[M]` — curator/organisation, downloads
- **Consistency (internal)** — отсутствие противоречий между записями (DAMA, ISO 25012). `[N]`
- **Uniqueness / deduplication** — отсутствие дубликатов (DAMA). `[N]` на внутреннем уровне, `[P]` на уровне cross-dataset
- **Validity** — соответствие schema/business rules (DAMA). `[P]` — proxy через наличие column_names
- **Integrity** — referential integrity (DAMA 2.0). `[N]`

### Contextual dimensions (относительно задачи)
- **Relevancy** — к задаче (Wang & Strong). `[P]` — только относительно query, не самостоятельно
- **Value-Added** (Wang & Strong). `[P]` — popularity / citation signals
- **Timeliness / Currentness** (Wang & Strong; DAMA; ISO 25012). `[M]` — last-updated timestamp
- **Completeness (metadata)** (Datasheets, Data Cards). `[M]` — прямая проверка заполненности полей
- **Completeness (data)** — нет missing values на уровне строк (DAMA). `[N]`
- **Appropriate Amount of Data** (Wang & Strong). `[M]` — row_count, size

### Representational dimensions (представление)
- **Interpretability** (Wang & Strong; Redman). `[M]` — column_names осмысленны, description читаем
- **Ease of Understanding** (Wang & Strong). `[M]` — description length/readability
- **Representational Consistency** — совместимый формат (Wang & Strong). `[M]` — стандартные file_formats
- **Concise Representation** (Wang & Strong). `[P]`
- **Format precision / flexibility / portability** (Redman). `[M]` — file_formats
- **Understandability** (ISO 25012). `[M]`

### Accessibility / system-dependent
- **Accessibility** (Wang & Strong; ISO 25012). `[M]` — link-checker
- **Access Security / Confidentiality** (Wang & Strong; ISO 25012). `[M]` — license, gated-access flag
- **Availability** (ISO 25012). `[M]` — link checker
- **Compliance** — license/legal constraints (ISO 25012). `[M]` — license field
- **Traceability** — provenance/audit (ISO 25012). `[M]` — source URL, creator, version
- **Efficiency** — storage/access (ISO 25012; Redman). `[M]`

### ML-specific (из documentation frameworks)
- **Motivation disclosed** (Datasheets §1). `[M]`
- **Collection process disclosed** (Datasheets §3, Data Statements curation rationale). `[M]`
- **Preprocessing disclosed** (Datasheets §4). `[P]`
- **Intended uses / out-of-scope uses** (Datasheets §5, Data Cards). `[P]`
- **Maintenance commitment** (Datasheets §7). `[M]` — update cadence
- **Speaker/annotator demographics** (Data Statements). `[P]` — tag presence
- **License clarity** (Datasheets §6, Data Cards). `[M]` — SPDX-valid vs Unknown

### Learning-theoretic
- **Label quality / noise rate** (Confident Learning). `[N]`
- **Training-dynamics difficulty distribution** (Cartography). `[N]`
- **Per-sample value** (Data Shapley). `[N]`
- **Cleanability / repair cost** (HoloClean, CleanML). `[N]`

---

## 1.3. Критические пробелы академических фреймворков

1. **Query-conditional quality.** Все фреймворки кроме Data Readiness Levels (Band A) рассматривают качество как абсолютное свойство датасета. Поисковик нуждается в *query-conditional* оценке — один и тот же датасет хорош для одной задачи и плох для другой.
2. **Popularity / network effects.** Downloads/stars широко используются на практике (HF, Kaggle), но ни один академический фреймворк их не формализует. Ближайшее — Wang & Strong "Reputation", но это про *источник*, не артефакт.
3. **Benchmark / usage record.** Использовался ли датасет в публикациях? Ближайший — Data Cards "Uses" и Datasheets §5, но без quantitative signal.
4. **License compatibility с downstream use.** Все фреймворки учитывают *presence* лицензии, но не *permissiveness* (CC-BY vs CC-BY-NC vs Unknown) — критичный decision-relevant сигнал.
5. **Schema richness.** Ни один фреймворк не скорит "columns с именами vs anonymous col_0, col_1" — один из самых actionable metadata-сигналов для tabular.
6. **Reproducibility of collection.** Нет скоринга "воспроизводим ли процесс сбора".
7. **Conceptual drift.** Timeliness — про currency, не про semantic drift.
8. **Size-vs-quality trade-off.** CleanML/Cartography показывают, что *больше данных* может маскировать плохое качество, но prescriptive guidance отсутствует.
9. **Cross-dataset deduplication.** Не адресуется.

---

## 1.4. Теоретическое обоснование выбора весов

### AHP (Analytic Hierarchy Process)

Saaty (1980) — стандартный метод elicitation весов через pairwise comparisons. Eigenvector сравнительной матрицы = вектор весов. Consistency Ratio < 0.1 — критерий валидности.

Есть прямо релевантный precedent: **Caballero et al.** применяли AHP к измерениям ISO/IEC 25012 — это можно цитировать как обоснование метода.

### Aggregation functions — теория

Ключевой теоретический вопрос: каким образом агрегировать компоненты в общий скор?

1. **Weighted arithmetic mean** (`Σ wᵢ·xᵢ`) — *fully compensatory*. Датасет может иметь ноль по accuracy и всё равно получить высокий скор, если другие компоненты компенсируют. Прост, но смысловой проблемой — не отражает эссенциальность некоторых осей.
2. **Weighted geometric mean / Cobb-Douglas** (`∏ xᵢ^wᵢ`) — *partially compensatory*. При любом нулевом компоненте скор схлопывается к нулю. Соответствует интуиции: датасет без license или с broken download не должен "усредниться" до хорошей оценки.
3. **Product (TDR §4.1 style)** — частный случай geometric mean c равными весами; тот же эффект.

**Precedent:** UNDP в 2010 году сменила HDI (Human Development Index) с arithmetic на geometric mean именно по этой причине — что нельзя компенсировать нулевой уровень образования высокой продолжительностью жизни. Это сильный precedent для нашей статьи — аналогия HDI-compositional-index с dataset-quality-composite.

Экономическая analogy — Cobb-Douglas production function: predполагает, что inputs *essential* (labor не может заместить нулевой capital).

**Вывод:** geometric mean / Cobb-Douglas предпочтительна когда некоторые измерения veto-like (отсутствие лицензии делает датасет юридически непригодным, сколько бы хорош он ни был по остальным). Это теоретически обоснованный выбор для нас.

---

# Часть II. Индустриальная практика

## 2.1. Kaggle Usability Score

Kaggle — **единственная** mainstream-платформа с публично отображаемым числовым usability score (0.0-10.0).

### Три категории (официальная декомпозиция)

- **Completeness** — subtitle, tags, description, cover image
- **Credibility** — source/provenance, public notebook (kernel), update frequency
- **Compatibility** — license, file format, file description, column description

### Reverse-engineered веса (Cabitza et al., *BioData Mining* 2024, "Venus Score" paper)

| Категория (общий вес) | Поле | Вес |
|---|---|---|
| Completeness (~41.0%) | Subtitle | 1.17 |
|  | Tag | 1.17 |
|  | Description | 1.17 |
|  | Cover Image | 0.59 |
| Credibility (~17.7%) | Source / Provenance | 0.59 |
|  | Public Notebook | 0.59 |
|  | Update Frequency | 0.59 |
| Compatibility (~41.3%) | License | 1.18 |
|  | File Format | 1.18 |
|  | File Description | 1.18 |
|  | Column Description | 0.59 |
|  | **Total** | **10.00** |

### Критические наблюдения

- **Kaggle НЕ оценивает содержимое.** Score — это metadata-completeness checklist. Датасет со случайным шумом в CSV получит 10/10, если все поля заполнены.
- Community engagement (upvotes, medals) **не** входит в usability — это отдельный сигнал (Dataset Progression Bronze/Silver/Gold).
- Column Description пропускается для non-tabular датасетов, веса ренормализуются.

**Lessons for us:** формула Kaggle — очень простая взвешенная сумма бинарных признаков присутствия. Работает в продакшене 10 лет. Но это нижний bar, не upper bar.

## 2.2. HuggingFace

Не публикует usability score. Использует:
- **downloads / downloadsAllTime / likes** (прямые signals)
- **trendingScore** — undocumented, предположительно likes за последние ~7 дней (velocity)
- **Dataset Card** (YAML front-matter + markdown sections) — 86.0% top-100 downloaded датасетов имеют все recommended секции vs 7.9% на zero-downloads датасетах (ICLR 2024, Yang et al., "Navigating Dataset Documentations")

**Lessons for us:** HF полностью полагается на popularity signals + community engagement. Нет прямого quality score, но есть машиночитаемые метаданные (YAML tags) → можно строить свой score сверху.

## 2.3. OpenML

Противоположный подход: **auto-computed statistical meta-features** (~145 штук), не judgmental score. Шесть категорий:
1. **Simple** (12): N instances, N features, N classes, missing values, etc.
2. **Statistical** (48): mean, std, skewness, kurtosis, correlations
3. **Information-theoretic** (13): class entropy, mutual information, noise-to-signal
4. **Model-based** (24): decision-tree fingerprint (depth, leaves, entropy)
5. **Landmarking** (14): accuracy cheap baselines (NaiveBayes, 1NN, DecisionStump)
6. **Complexity** (34): Ho & Basu measures

Нет числового quality score как такового. "Хорошие" датасеты surfaced через: (a) membership в курируемых benchmark suites (CC-18, CTR-23), (b) количество runs и tasks.

**Lessons for us:** OpenML демонстрирует, что **content-level signals** (на самой data) очень богаты, но требуют доступа к данным. Для metadata-only аггрегатора большая часть недоступна. Но: мы можем использовать simple family (row_count, N features) как прокси.

## 2.4. Papers with Code

Ранжирование через (Task, Dataset, Metric) triples. Social-proof aggregator:
- Benchmarked status (binary)
- Paper citations per year
- SOTA plateau как сигнал "исчерпанности"
- GitHub stars linked implementations

Нет explicit quality thresholds. **Community momentum = ranking.**

## 2.5. Google Dataset Search

Не оценивает датасеты напрямую — использует Google web-search authority + schema.org completeness.

### Ranking signals (из Google Research blog)
1. Web-ranking baseline (PageRank landing page)
2. Metadata completeness (schema.org/Dataset fields)
3. Citation signals из Google Scholar
4. Dataset replica consolidation через `sameAs`
5. Authority of publishing organization

**Required schema fields:** `name`, `description` (50-5000 chars)
**Recommended:** `creator`, `license`, `distribution`, `identifier` (DOI), `keywords`, `temporalCoverage`, `spatialCoverage`, `variableMeasured`, `citation`, `funder`, `sameAs`

## 2.6. Data Quality Tools

Определяют *vocabulary* измеримых сигналов, даже если не оценивают.

| Tool | Ключевые измерения |
|---|---|
| **Great Expectations** | Completeness, Consistency, Integrity, Timeliness, Uniqueness, Validity; 300+ expectations |
| **Deequ (AWS)** | Completeness, Uniqueness, Compliance, Mutual Info, Entropy, Correlation, ApproxCountDistinct; **ConstraintSuggestionRunner** auto-проposes правила |
| **Soda Core / SodaCL** | Numeric (row-count, percentiles), Missing (null %), Validity (regex), Freshness, Schema, Reference, Anomaly detection |
| **Monte Carlo / Datafold** | 5-pillar: Freshness, Volume, Schema, Distribution, Lineage |

Все 4 converge на DAMA/ISO-8000 dimensions. Никто не выдаёт single scalar — per-check pass/fail.

## 2.7. Academic-Industrial Hybrids

- **DataPerf** (MLCommons, NeurIPS 2023) — бенчмарк для data-centric algorithms, не для датасетов.
- **DCBench** (Stanford, SIGMOD 2022) — benchmark interventions (sample selection, slice discovery).
- **Dynabench** (Meta AI) — **adversarial quality** = модели продолжают ошибаться.
- **Venus Score** (Cabitza et al., 2024, biomedical) — 10 yes/no вопросов; научный counterweight к Kaggle usability.

### Venus Score questions (буквально)
1. Defined origin/context/purpose?
2. Data-protection + license described?
3. Devices/centers/periods identified?
4. Variables explained?
5. Protected attributes included (age/sex/ethnicity)?
6. Inaccuracy sources characterized?
7. Noise info included?
8. Preparation/cleaning/annotation described?
9. Peer-reviewed publication exists?
10. Available online + global ID?

## 2.8. Comparison table: platform × dimension

| Dimension | Kaggle | HF | OpenML | PwC | Google | Venus |
|---|---|---|---|---|---|---|
| Completeness of description | Yes (~41%) | Yes | No | No | Yes | Yes |
| License clarity | Yes (1.18) | YAML | No | No | Yes | Yes |
| File format | Yes (1.18) | Implicit | Yes (ARFF) | No | Yes | No |
| Column/schema docs | Yes (0.59+1.18) | Optional | Auto (ARFF) | No | variableMeasured | Yes |
| Statistical meta-features | No | No | **Yes** (~145) | No | No | No |
| Provenance | Yes (0.59) | Card | Yes | Yes | Yes | Yes |
| Update frequency | Yes (0.59) | lastModified | Version | Paper recency | dateModified | No |
| Popularity | Medals (не в score) | downloads, likes, trending | Runs | Papers/year | Web authority | No |
| Citations | No | paperswithcode_id | OpenML pubs | Central signal | Yes (Scholar) | Yes (Q9) |
| Ethics/bias docs | No | "Considerations" (2%) | No | No | No | Yes (Q5) |
| Benchmark hardness | No | No | No | Implicit (SOTA) | No | No |
| **Numeric 0-10 score** | **Yes** | No | No | No | No | **Yes** |

## 2.9. Ключевые outtakes индустрии

1. **Industry оценивает что дёшево проверить, не что действительно важно.** Kaggle — metadata audit; content random — всё равно 10/10.
2. **Academia оценивает content, industry — metadata.** OpenML считает 100+ дескрипторов байтов; Kaggle — галочки.
3. **Конвергенция на 6 осях:** (a) documentation completeness, (b) license clarity, (c) schema/column docs, (d) freshness, (e) popularity/citations, (f) provenance. Новая платформа, покрывающая эти шесть, будет directly comparable.
4. **Popularity overwhelms usability в поиске.** На всех UI (HF trending, PwC, Google citations, Kaggle medals) popularity-weighted signal побеждает static usability при ranking. **Usability обычно фильтр, не primary sort.**
5. **Benchmark saturation как quality signal underused.** Только Dynabench (adversarial) и PwC (SOTA plateau) делают что-то с difficulty.
6. **Machine-readable metadata > narrative.** schema.org/HF YAML/OpenML ARFF > free-text Kaggle. Google явно вознаграждает schema completeness.
7. **Что industry НЕ измеряет, но должна:** row-level null rates, label noise, demographic coverage, license reliability (beyond "упомянута"), train/test leakage.

---

# Часть III. Современные подходы (2023-2025)

## 3.1. LLM training quality filters — concrete heuristics

Эти pipelines — единственный источник *battle-tested* quality signals в продакшене (скорят триллионы документов). Большинство эвристик напрямую переносятся на metadata-scoring (применяем "mean word length 3-10" к description вместо raw web text).

### 3.1.1. Gopher / MassiveText (DeepMind, 2021)

Канонический набор правил. Документный уровень:

- **Word count:** 50 - 100,000 (RedPajama implementation: 50 - 10,000)
- **Mean word length:** 3 - 10 chars
- **Symbol-to-word ratio:** < 0.1
- **Bullet lines:** не более 90% lines начинаются с bullet
- **Ellipsis lines:** не более 30% lines заканчиваются `...`
- **Alphabetic ratio:** ≥ 80% слов содержат хоть одну букву
- **Stop-word ratio:** ≥ 2 из {the, be, to, of, and, that, have, with} — crude language-purity check
- **Repetition filters:** fraction duplicated lines ≤ 0.2; top 2-gram ≤ 0.2; 3-gram ≤ 0.18; 4-gram ≤ 0.16

Reference: Rae et al. 2021, arXiv:2112.11446.

### 3.1.2. C4 / T5 (Raffel et al. 2020)

- Discard lines без terminal punctuation (. ! ? ")
- Discard pages с < 5 предложений
- Lines должны иметь ≥ 3 слова
- Drop containing "Javascript", "lorem ipsum"
- Drop на basis ~400-word ["List of Dirty, Naughty, Obscene..." wordlist]
- Drop pages с `{`, `}` (crude code leakage filter)

### 3.1.3. CCNet (Facebook 2019) — perplexity as quality proxy

Train 5-gram KenLM на Wikipedia, scoring каждого документа по perplexity. Buckets: head/middle/tail. Low perplexity = Wikipedia-like writing. Эта идея (KenLM perplexity как сигнал качества) **до сих пор** в RedPajama, Dolma.

Reference: Wenzek et al. 2019, arXiv:1911.00359.

### 3.1.4. The Pile (EleutherAI 2020)

- **fastText quality classifier** (curated vs raw Common Crawl) по примеру GPT-3
- **MinHash-LSH deduplication** на document level

### 3.1.5. RedPajama-v2 (Together AI, 2023) — архитектурный сдвиг

**НЕ фильтрует**, а **precomputes 40+ quality signals per document**. Downstream users применяют свои thresholds.

Signals:
- Все Gopher эвристики
- C4 эвристики
- CCNet perplexity (KenLM per language)
- fastText classifier scores (Wikipedia-like, DCLM-like)
- Line-start/line-end patterns
- Domain quality (URL blocklists)
- All-caps / digits / unique words fractions

**Это critical insight для нас:** precompute rich signals once → lazy filter. NeurIPS 2024 paper.

### 3.1.6. FineWeb / FineWeb-Edu (HuggingFace 2024)

**Стандартный pipeline:**
1. URL filtering
2. Trafilatura HTML extraction
3. fastText language classifier (English, threshold 0.65)
4. Gopher quality + repetition filters
5. Per-dump MinHash deduplication
6. C4 filters **кроме** terminal-punctuation (too aggressive)
7. Три кастомных из ablation:
   - Remove, где ≥ 1/3 lines ends in punctuation
   - Remove, где ≥ 12% lines duplicated
   - Remove, где ≥ 30% short lines (<30 chars)

**FineWeb-Edu classifier — ключевое нововведение:**
- Llama-3-70B-Instruct scored 500k samples на educational quality (0-5 scale)
- Linear regression head на Snowflake-arctic-embed embeddings обучен на 450k таких scores
- Filter @ score ≥ 3 удалил **92%** FineWeb, оставил 1.3T tokens
- Эта подвыборка **побеждает full FineWeb** на MMLU, ARC, OpenBookQA

**Pattern:** LLM labels sample → small classifier learns labeling → scales. Это самый мощный современный паттерн.

### 3.1.7. Dolma (AI2, 2024)

Использовался для OLMo. Stack: Gopher + C4 + RedPajama + StarCoder + extras:
- Paragraph-level terminal punctuation check
- fastText на Jigsaw Toxic Comments для hate/NSFW tags
- High-precision regex PII (emails, IPs, phones)
- Code-specific: drop JSON/CSV, overly long lines, mostly numeric
- Paragraph-level filtering (не только document-level)

### 3.1.8. Nemotron-CC (NVIDIA, 2024-2025)

Следующий шаг эволюции: **classifier ensembling + synthetic rephrasing**.

- Ансамбль нескольких quality classifiers (один имитирует FineWeb-Edu rubric)
- Per-document scores → quality decile
- High-quality → LLM rephrasing для synthetic variants
- Low-quality → rephrasing в Q&A/summary для salvage

Результат: 6.3T tokens; 8B модель +5 MMLU vs Llama-3.1-8B.

**NVIDIA framing:** shift от static heuristics к learned flywheel — better data → better models → better classifiers → better data.

## 3.2. Data-Centric AI movement

### 3.2.1. Принципы

- Andrew Ng 2021, NeurIPS DCAI workshop
- "Fix the data, not the model"
- Treat datasets as living infrastructure (version, measure, debug)
- Quality = task-relative
- Iterative: label → train → find failures → relabel

### 3.2.2. Landmark papers

- **Zha et al. 2023** "Data-centric AI: Perspectives and Challenges", SDM 2023 — три pillars (training / inference / maintenance data development)
- **Zha et al. 2023** Survey, ACM Computing Surveys 2025
- **LIMA** (Zhou et al., 2023) — 1000 curated examples на LLaMA-65B побеждают RLHF'd DaVinci-003 и 52K Alpaca. Quantity < Quality × Diversity.
- **LESS** (Xia et al., ICML 2024) — gradient-based selection; top-5% beats full dataset.

### 3.2.3. Lessons for dataset search engine

- Quality task-relative → let users re-weight per query
- Diversity ≠ size → measure independently
- Version datasets, track which revision trained which model
- Precompute expensive signals once, cheap queries re-rank

## 3.3. Task-specific quality matrix

Разные ML задачи sensitive к разным свойствам.

| Task | Dominant quality signals | Representative methods |
|------|------|------|
| **LLM pretraining (text)** | Perplexity vs reference LM, Gopher/C4 heuristics, educational classifier, dedup ratio | KenLM perplexity, FineWeb-Edu score 0-5, MinHash-LSH, fastText |
| **Instruction tuning / RLHF** | Response quality, instruction diversity, format consistency | LLM-judge (AlpacaEval, MT-Bench), log-det distance diversity, LESS influence |
| **Computer Vision (classification/detection)** | Label noise, class balance, resolution distribution, annotation consistency, demographic diversity | cleanlab confident learning, Gini of class freq, mean resolution, annotator agreement |
| **Vision-Language (CLIP, VLM)** | Image-text CLIP similarity, caption length, image size/aspect, CLIP-image-embedding coverage | DataComp filter: basic + CLIP-L/14 top-30% + image-based intersection |
| **NLP supervised (classification, NER)** | IAA, label consistency per class, text length distribution, language purity, train/test leakage | Cohen's κ, Fleiss' κ, Krippendorff's α, MinHash overlap |
| **Tabular / classical ML** | Missing-value rate per column, class imbalance, feature-type consistency, outliers, distribution shift | Imputation sensitivity, SHAP-outlier, schema-validation hash |
| **Time series** | Timestamp regularity, sampling-rate consistency, missing pattern (MCAR/MAR/MNAR), seasonality presence | Expected-vs-observed counts, autocorrelation at seasonal lag, gap histogram |
| **Recommender systems** | User-item sparsity, cold-start ratio, long-tail coverage, rating skew | Sparsity = 1 - nnz/(|U|·|I|), Gini of item popularity |

**Takeaway:** инфер task из tags/modality, применяй только релевантные signals. Tabular с 60% missing — бесполезен; time-series с 60% missing timestamps может быть fine, если MCAR.

## 3.4. DataComp findings — does filtering actually work?

DataComp (Gadre et al., NeurIPS 2023) — самый важный empirical результат по "работает ли filtering?"

Бенчмарк: 12.8B image-text pairs, participants фильтруют, train CLIP, scored на 38 downstream.

### Strategies evaluated

- **No filter** (baseline)
- **Basic filter** — English-only, caption 2-32 tokens, image width ≥ 200px
- **CLIP score filter** — top X% по CLIP cosine similarity
- **Text-based** — caption contains ImageNet-21k class name
- **Image-based** — image embedding в радиусе ImageNet-train

### Winner

**Image-based ∩ CLIP-L/14 top-30%** побеждает каждый individual filter на medium / large / xlarge. Этот recipe = **DataComp-1B**; при равном compute → **+3.7 ImageNet zero-shot** над OpenAI CLIP.

Thresholds: **0.243 для CLIP-L/14**, 0.281 для B/32.

### Follow-ups

- **Data Filtering Networks (DFN, ICLR 2024)** — beat DataComp-1B с меньшим network
- **Fine-tuned MLLMs as filters** (2024) — beats CLIP score alone
- **DataComp-LM (DCLM, 2024)** — text analog; **model-based filtering is dominant lever**. DCLM-Baseline (7B, 2T tokens) → 63% MMLU, +6 над MAP-Neo с половиной compute

**Key takeaway:** **learned, lightweight quality classifiers beat any hand-crafted heuristic**. Ensembling beats single classifier.

## 3.5. LLM-as-Judge — current state

### Where it works
- FineWeb-Edu: Llama-3-70B as annotator → distill в cheap classifier. **Reliable pattern.**
- AlpacaEval, MT-Bench: GPT-4 judge для instruction tuning; reasonable correlation с humans.

### Documented biases (2024)
- **Position bias** — prefer first candidate in pairwise
- **Verbosity bias** — prefer longer responses
- **Self-enhancement** — LLM rates own outputs higher
- **Scoring bias** — same-content в разном ordering даёт разные scores
- Surveys identify ~12 bias categories

### Mitigations
- Randomize position, run both orders, average
- Multi-sample, McDonald's omega / agreement rate
- Constrain outputs to rubric-driven scalars
- **Distill judge → small classifier** (FineWeb-Edu pattern) для scale

### Application to dataset cards / metadata

Никто не публиковал canonical "dataset-card quality score" ещё, но ingredients есть:
1. Strong LLM scores dataset cards на rubric (completeness, task clarity, license clarity, examples, limitations)
2. Collect ~1-5K labels
3. Train cheap embedding-based head
4. Use as ranking signal

**Cheap baseline:** embedding semantic coherence между title/tags/description — если семантически рассинхронизированы → low quality.

## 3.6. Does quality scoring predict downstream performance?

Critical question. Если quality score не коррелирует с accuracy — это шум.

### Evidence YES

- **DataComp-1B vs OpenAI CLIP** — same compute, +3.7 ImageNet
- **FineWeb-Edu vs FineWeb** — same tokens, big gains на MMLU/ARC/OpenBookQA
- **DCLM-Baseline vs MAP-Neo** — +6 MMLU, half compute
- **Nemotron-CC vs Llama-3.1-8B** — +5 MMLU чисто от curation
- **LIMA** — 1K curated beats 52K Alpaca
- **LESS** — 5% of data beats 100%
- **"Improving Pretraining Data Using Perplexity Correlations"** (2024) — per-document LM loss → benchmark correlation используется для data selection без обучения
- **"Predictive Data Selection"** — PreSelect uses fastText scorer обученный на "LM-loss predicts downstream" signal
- **"A Pretrainer's Guide to Training Data"** — nearly every Gopher/C4 filter net-positive

### Evidence noisy

- DIScore (per-doc influence) correlation as low as 0.16 при cheap proxy models
- Instruction-tuning diversity metrics плохо изучены
- LLM-judge disagreement с humans 15-30% на open-ended

### Synthesis

Pretraining-scale filtering **демонстрируемо улучшает** downstream accuracy — иногда на 3-6 MMLU points при равном compute, это **огромный** эффект.

Dataset-level metadata quality (то, что делаем мы) имеет меньше прямой empirical literature, но principles transfer: *anything a user would reject as spammy when browsing — broken titles, missing descriptions, no license — correlates with the dataset content being low-quality*, потому что publication hygiene и content hygiene делят общую причину (care).

## 3.7. Deduplication как first-class quality signal

Lee et al. 2022 ("Deduplicating Training Data Makes Language Models Better"):
- >1% unprompted outputs — verbatim training text
- >4% validation sets affected by train-test leakage
- Dedup'd models → fewer train steps для same accuracy, 10× меньше memorized text

**Two methods (both standard):**
1. **Suffix array** — exact substring dedup
2. **MinHash (NearDup)** — LSH over shingles; document-level near-dup

Для нас: MinHash на dataset-card text + content hash на payload ловит near-duplicate uploads. 50-я копия Titanic хуже первой.

---

# Часть IV. Синтез — что критически важно

## 4.1. Единая модель качества (интегрированная)

На основе всех трёх частей, для dataset-search-engine context:

### Блок A. Documentation / Metadata completeness
*источники:* Datasheets, Data Cards, Kaggle Usability, HF Cards, Venus
- Title + description present, длина descriptива
- Tags/keywords
- License SPDX-valid (не Unknown)
- Column names + descriptions (для tabular)
- Intended uses / limitations disclosed
- Collection process described
- Update frequency / maintenance
- Cover image / examples (weak signal)

### Блок B. Technical / Representational quality
*источники:* ISO 25012, Redman, OpenML, DataComp
- File format (Parquet/CSV > XLSX > PDF)
- Size / row count sensible (не 0 rows, не 10 GB XML)
- Schema declared
- Accessible URL (link checker)
- Standardized identifier (DOI / HF id / Kaggle id)
- Dedup uniqueness (не копия популярного датасета)

### Блок C. Social / Usage signals
*источники:* HF trending, PwC citations, Google Scholar, Nemotron-CC flywheel
- Downloads / views / likes
- Citations в published papers
- Benchmark/leaderboard membership
- Community activity (notebooks, discussions)
- Source reputation (publishing org)

### Блок D. Content quality (proxy)
*источники:* Kaggle provenance, Dolma PII, CleanML, Dataset Cartography
- **Только проксируемо без доступа к данным:**
  - Claim of peer review / publication (Venus Q9)
  - Annotator/demographic disclosure (Data Statements)
  - Known limitations/ethics disclosed
  - Preprocessing described
- Direct content quality (label noise, duplicates, missing values) — требует data access

### Блок E. Task-appropriateness
*источники:* DCAI, Data Readiness Level Band A, query-conditional
- **Computable ТОЛЬКО относительно query:**
  - Modality match (user ищет image → is image dataset)
  - Task tag match
  - Size expectations
  - License-compatible с declared use

**Важное наблюдение:** Блоки A-D — absolute quality (static_score territory). Блок E — relative, должен быть в search ranker, не в static.

## 4.2. Weighting — теоретическая валидация

### Против equal weights

Wang & Strong и последующие empirical studies показывают, что data consumers **ранжируют dimensions по-разному**. Equal weights игнорируют decision-relevance. AHP Caballero et al. дал примеры валидных весовых распределений для ISO 25012.

### Против pure weighted sum (arithmetic mean)

Fully compensatory — может дать высокий score датасету с нулевой лицензией. Это **decision-error** для практического использования.

### За geometric mean (Cobb-Douglas)

- Veto-like dimensions (license, accessibility) не компенсируются
- UNDP HDI precedent (2010 switch)
- Cobb-Douglas production function analogy (inputs essential)
- Соответствует TDR §4.3 pattern (static_score как multiplier)

### За learned blending (FineWeb-Edu / Nemotron-CC)

- Лучший accuracy vs downstream
- Но требует ground truth labels / flywheel
- Для MVP — overkill; для production после сбора click data — logical next step

### Итог

**Разумная стратегия:**
1. Compute 4 sub-scores (Documentation, Representational, Social, Content-proxy) как weighted sum внутри блока (AHP-calibrated weights)
2. Aggregate через **weighted geometric mean** на верхнем уровне → single `static_score` в [0, 1]
3. Retain individual sub-scores в БД → allow query-time re-weighting (RedPajama-v2 pattern)
4. Task-appropriateness блок → в search ranker, не в static

---

## 4.3. Валидация подходов

| Подход | Сильные стороны | Слабости | Когда выбрать |
|---|---|---|---|
| **Kaggle-style**: 10-15 binary фич, weighted sum | Простой, интерпретируемый, работает в продакшене 10 лет. Прямой precedent. | Fully compensatory, content-blind, не learned. | MVP, быстрый старт, нужна прозрачная формула |
| **Venus-style**: rubric 10 questions | Academic-backed, peer-reviewed, применимо к scientific датасетам | Биомед-specific, yes/no teruda всё, нет popularity | Article citation, defensible baseline |
| **OpenML-style**: statistical meta-features | Rich descriptive signals, auto-computable | Требует доступа к данным; не scorable без task context | Для платформы с hosting; не для нас (мы агрегатор) |
| **FineWeb-Edu learned classifier** | Best accuracy vs downstream | Нужны LLM labels + training; opaque | V2 после сбора usage data |
| **Cobb-Douglas composite** | Теоретически обосновано (HDI, production functions), veto-safe, consistent с TDR §4.3 multiplicative ranking | Hard to calibrate без empirical grounding | **Рекомендованный default** |
| **RedPajama signal store** | Maximal flexibility, precompute-once | Move complexity в search ranker; UX сложнее | Если предполагается серьёзный query-time customization |

### Рекомендация для статьи + реализации

**Гибрид:**
- Hood: **RedPajama-v2 signal store** (precompute many features, store in DB)
- Default: **Cobb-Douglas composite** (для ranking), weights через **AHP-style elicitation**
- V2 (после sufficient usage data): **Learned classifier** (FineWeb-Edu pattern) для re-calibration

Это защищаемо с трёх сторон:
1. Academically (Wang & Strong + ISO 25012 + Cobb-Douglas, AHP, HDI precedent)
2. Industrially (Kaggle-weights-style interpretability + FineWeb learned flywheel upgrade path)
3. Empirically (planning ground truth через SearchLog CTR / clicks)

---

# Часть V. Финальная рекомендация — что взять в реализацию

## 5.1. Конкретная формула (научно обоснованная)

```
static_score = docs^w_D × repr^w_R × social^(w_S × 0.5 + 0.5) × content_proxy^w_C
```

где каждая компонента ∈ [0, 1], веса `w_D + w_R + w_S + w_C = 1`, и `social^(w·0.5 + 0.5)` — мягкий floor, чтобы новый качественный датасет не получал ноль.

### Предлагаемые веса (AHP-calibrated):
- `w_D = 0.35` — documentation (самый actionable и самый подтверждённый empirically)
- `w_R = 0.20` — representational (format, size, schema)
- `w_S = 0.25` — social (downloads, citations)
- `w_C = 0.20` — content-proxy (peer-review claim, ethics disclosure, preprocessing disclosed)

### Компоненты подробно

**Documentation (блок A):**
```
docs = 0.20 * has_title_and_desc(>100 chars)
     + 0.15 * has_tags
     + 0.20 * has_license_SPDX_valid
     + 0.20 * has_column_descriptions (для tabular; иначе renormalize)
     + 0.10 * has_intended_uses_disclosed
     + 0.10 * has_maintenance_signal (recent update)
     + 0.05 * has_cover_image_or_examples
```

**Representational (блок B):**
```
repr = 0.40 * format_score  # Parquet=1, CSV=0.9, ..., PDF=0.1
     + 0.20 * size_sensibility  # log-bucketed row_count
     + 0.20 * accessibility  # link-checker result
     + 0.20 * has_standardized_identifier  # DOI / HF id / Kaggle id
```

**Social (блок C):**
```
social = percentile_rank(log1p(downloads), p5_p95) * 0.5
       + percentile_rank(log1p(citations), p5_p95) * 0.3
       + percentile_rank(log1p(likes), p5_p95) * 0.2
```

**Content-proxy (блок D):**
```
content_proxy = 0.30 * has_peer_review_claim
              + 0.25 * preprocessing_described
              + 0.20 * limitations_disclosed
              + 0.15 * collection_process_described
              + 0.10 * demographic_disclosure  # для NLP/CV датасетов
```

**Freshness decay в search ranker (не в static):**
```
freshness = 1 / (1 + 0.1 * age_years)
```

## 5.2. Научное обоснование (для статьи)

**Choice of dimensions** — из Wang & Strong 4-категорной таксономии + Datasheets documentation questions + DAMA operational dimensions, адаптировано под metadata-only context. Каждое измерение cited and defensible.

**Choice of aggregation (geometric / Cobb-Douglas)** —
1. Cobb & Douglas 1928 production function theory (essential inputs)
2. UNDP HDI 2010 switch from arithmetic to geometric (rationale published)
3. TDR §4.3 already multiplicative (consistency)
4. Avoids decision-error from compensatory aggregation

**Choice of weights (AHP-elicitation)** —
Caballero et al. precedent на ISO/IEC 25012 dimensions. Consistency Ratio < 0.1 — validity check.

**Social signal (popularity floor):**
`social^(0.25·0.5 + 0.5) = social^0.625` → даже новый датасет с social = 0 получает множитель 0, но с floor "0.5 + 0.5·social" этого не происходит. Это hybrid: popularity влияет, но не vetos.

**Empirical validation plan:**
- Собрать golden set 30-50 запросов с вручную отмеченным релевантным датасетом
- NDCG@5, MRR@10 для baseline vs proposed
- CTR из SearchLog → correlation с static_score
- **DataComp-style ablation**: что происходит, если убрать компоненту?

## 5.3. Path to v2 (follow-up research)

После сбора 1-3 months usage data:
1. LLM-annotate 500-1000 dataset cards на overall quality (FineWeb-Edu pattern)
2. Train embedding-based regressor на этих labels
3. A/B test против rule-based static_score
4. Если beats baseline → hybrid: rule-based floor + learned signal

Nemotron-CC flywheel завершает loop.

---

# References

## Academic frameworks

- Wang, R. Y., & Strong, D. M. (1996). Beyond Accuracy: What Data Quality Means to Data Consumers. *Journal of Management Information Systems*, 12(4), 5-33. https://web.mit.edu/tdqm/www/tdqmpub/beyondaccuracy_files/beyondaccuracy.html
- Strong, D. M., Lee, Y. W., & Wang, R. Y. (1997). Data Quality in Context. *CACM*, 40(5), 103-110.
- Redman, T. C. (1997/2001). *Data Quality for the Information Age / Data Quality: The Field Guide*. Artech House / Digital Press.
- DAMA International (2017). *DAMA-DMBOK: Data Management Body of Knowledge, 2nd ed.*
- DAMA-NL (2020). *Dimensions of Data Quality (DDQ) Research Paper, v1.2*. https://dama-nl.org/wp-content/uploads/2020/09/DDQ-Dimensions-of-Data-Quality-Research-Paper-version-1.2-d.d.-3-Sept-2020.pdf
- ISO/IEC 25012:2008. *Software engineering — SQuaRE — Data quality model*. https://www.iso.org/standard/35736.html
- Gualo, F., Rodriguez, M., Verdugo, J., Caballero, I., & Piattini, M. (2021). Data Quality Certification using ISO/IEC 25012. arXiv:2102.11527.
- Gebru, T., et al. (2018/2021). Datasheets for Datasets. *CACM*, 64(12). arXiv:1803.09010.
- Pushkarna, M., Zaldivar, A., & Kjartansson, O. (2022). Data Cards. *FAccT '22*. arXiv:2204.01075.
- Bender, E. M., & Friedman, B. (2018). Data Statements for NLP. *TACL*, 6, 587-604.
- Holland, S., et al. (2018). The Dataset Nutrition Label. arXiv:1805.03677. https://datanutrition.org/
- Lawrence, N. D. (2017). Data Readiness Levels. arXiv:1705.02245.
- Swayamdipta, S., et al. (2020). Dataset Cartography. *EMNLP 2020*. arXiv:2009.10795.
- Northcutt, C. G., et al. (2021). Confident Learning. *JAIR*, 70, 1373-1411. arXiv:1911.00068.
- Ghorbani, A., & Zou, J. (2019). Data Shapley. *ICML 2019*. arXiv:1904.02868.
- Li, P., et al. (2019/2021). CleanML. arXiv:1904.09483.
- Rekatsinas, T., et al. (2017). HoloClean. *PVLDB* 10(11).
- Saaty, T. L. (1980). *The Analytic Hierarchy Process*. McGraw-Hill.
- Caballero et al. *Prioritization of ISO/IEC 25012 Data Quality Dimensions Using AHP*. SBQS.
- UNDP (2010). *Human Development Report 2010 — Technical Note 1* (arithmetic → geometric mean for HDI).
- Cobb, C. W., & Douglas, P. H. (1928). A Theory of Production. *American Economic Review*, 18(1 Suppl.).

## Industry practice

- [Kaggle Usability Rating announcement](https://www.kaggle.com/product-feedback/93922)
- Cabitza et al. (2024). "Venus Score" paper, *BioData Mining*. https://biodatamining.biomedcentral.com/articles/10.1186/s13040-024-00412-x (contains reverse-engineered Kaggle weights)
- [HuggingFace Dataset Cards docs](https://huggingface.co/docs/hub/en/datasets-cards)
- Yang, X., et al. (2024). Navigating Dataset Documentations in AI. *ICLR 2024*. arXiv:2401.13822.
- Vanschoren, J., et al. (2014). OpenML: Networked Science in Machine Learning. arXiv:1402.6013.
- [OpenML concepts docs](https://docs.openml.org/concepts/data/)
- [Google Dataset Search schema.org docs](https://developers.google.com/search/docs/appearance/structured-data/dataset)
- [Great Expectations](https://docs.greatexpectations.io/), [Deequ](https://github.com/awslabs/deequ), [Soda Core](https://github.com/sodadata/soda-core)
- Gadre, S. Y., et al. (2023). DataComp. *NeurIPS 2023*. arXiv:2304.14108.
- Mazumder, M., et al. (2022/2023). DataPerf. *NeurIPS 2023*. arXiv:2207.10062.
- Eyuboglu, S., et al. (2022). DCBench. *SIGMOD DEEM 2022*.
- Kiela, D., et al. (2021). Dynabench. *NAACL 2021*.

## Modern (2023-2025)

- Rae, J. W., et al. (2021). Scaling Language Models: Gopher. arXiv:2112.11446.
- Raffel, C., et al. (2020). Exploring the Limits of Transfer Learning (T5/C4). arXiv:1910.10683.
- Dodge, J., et al. (2021). Documenting C4. arXiv:2104.08758.
- Wenzek, G., et al. (2019). CCNet. arXiv:1911.00359.
- Gao, L., et al. (2020). The Pile. arXiv:2101.00027.
- Together AI (2023). [RedPajama-Data-v2 blog](https://www.together.ai/blog/redpajama-data-v2); NeurIPS 2024 paper.
- Penedo, G., et al. (2024). The FineWeb Datasets. arXiv:2406.17557. https://huggingface.co/HuggingFaceFW/fineweb-edu-classifier
- Soldaini, L., et al. (2024). Dolma. *ACL 2024*.
- NVIDIA (2024-2025). Nemotron-CC. https://research.nvidia.com/labs/adlr/Nemotron-CC/
- Fang, A., et al. (2024). Data Filtering Networks. *ICLR 2024*.
- Li, J., et al. (2024). DataComp-LM (DCLM). arXiv:2406.11794.
- Longpre, S., et al. (2023). A Pretrainer's Guide to Training Data.
- Engstrom, L., et al. (2024). Improving Pretraining Data Using Perplexity Correlations. arXiv:2409.05816.
- Zha, D., et al. (2023). Data-Centric AI: Perspectives and Challenges. *SDM 2023*. arXiv:2301.04819.
- Zha, D., et al. (2023). Data-Centric AI Survey. *ACM Computing Surveys*. arXiv:2303.10158.
- Zhou, C., et al. (2023). LIMA. arXiv:2305.11206.
- Xia, M., et al. (2024). LESS. *ICML 2024*. arXiv:2402.04333.
- Lee, K., et al. (2022). Deduplicating Training Data Makes LMs Better. *ACL 2022*. arXiv:2107.06499.
- LLM-as-Judge biases: arXiv:2406.07791 (position), arXiv:2411.15594 (survey), arXiv:2506.22316v1 (scoring bias).
- MIT DCAI course: https://dcai.csail.mit.edu/

## Key takeaway quotes (for article)

> "Quality is the consumer's judgement of fitness for use." — Wang & Strong (1996)

> "Fix the data, not the model." — Andrew Ng, DCAI founding principle (2021)

> "Model-based filtering is the dominant lever for data quality." — DataComp-LM (2024)

> "Less is more for alignment" — LIMA (2023)

> "Better data improves models, which improve classifiers, which improve data." — Nemotron-CC flywheel framing (2024)
