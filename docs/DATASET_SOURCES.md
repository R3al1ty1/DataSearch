# Dataset Sources With Open APIs

Документ фиксирует потенциальные источники датасетов, которые можно добавить после Kaggle и Hugging Face. В список попали источники с понятным публичным API и возможностью получить из ответа API ссылку на запись, файл, bulk download или страницу датасета.

## Критерии отбора

- Есть публичная документация API.
- API позволяет искать или перечислять записи программно.
- Из ответа можно собрать `url` датасета/записи или получить прямые ссылки на файлы.
- Источник полезен для специализированного поиска, особенно health, biomedical, legal, regulatory, science, geospatial, economic.
- Нет зависимости от платного доступа. Источники с бесплатным API key допустимы, но помечены отдельно.

## Рекомендуемые источники

| Источник | Области | API / документация | Доступ к ссылкам на датасеты | Auth | Приоритет |
| --- | --- | --- | --- | --- | --- |
| Data.Healthcare.gov | Healthcare, insurance, hospitals, public health policy | https://data.healthcare.gov/about/api | API возвращает dataset metadata и URL ресурсов портала | No key | A |
| ClinicalTrials.gov | Clinical trials, drugs, devices, conditions, sponsors | https://clinicaltrials.gov/data-api/api | `GET /api/v2/studies` и `GET /api/v2/studies/{nctId}` дают стабильный NCT ID; URL записи собирается как `https://clinicaltrials.gov/study/{nctId}` | No key | A |
| openFDA | Drugs, devices, food, adverse events, enforcement, recalls | https://open.fda.gov/apis/ | API endpoints возвращают records; bulk JSON downloads доступны через downloads API/page | No key for basic use | A |
| CourtListener | Case law, opinions, dockets, judges, PACER metadata | https://www.courtlistener.com/help/api/ | REST API возвращает `absolute_url`, related entities и bulk data references | API token recommended | A |
| Federal Register | Regulations, proposed rules, notices, presidential documents | https://www.federalregister.gov/developers/documentation/api/v1 | Document API возвращает HTML/PDF links, document number, agencies, topics | No key | A |
| Zenodo | Scientific datasets, publications, software artifacts, DOI records | https://developers.zenodo.org/ | Records API возвращает record URL, DOI, license и `files[].links.self` для скачивания | No key for public records | A |
| NCBI GEO | Genomics, transcriptomics, expression profiling, biomedical experiments | https://www.ncbi.nlm.nih.gov/geo/info/geo_paccess.html | E-utilities дают metadata/accessions; файлы скачиваются по FTP/HTTPS структуре GEO | No key, optional NCBI key | B |
| NCBI Datasets | Genomes, genes, taxonomy, comparative genomics | https://www.ncbi.nlm.nih.gov/datasets/docs/v2/api/ | API и CLI дают metadata reports и download package endpoints | No key, optional NCBI key | B |
| PRIDE Archive | Proteomics, mass spectrometry, biomedical research | https://www.ebi.ac.uk/pride/ws/archive/v2/docs/api-guide.html | Project/file endpoints возвращают project metadata и file records для скачивания | No key | B |
| OpenAIRE | Research datasets, publications, software, grants, organizations | https://graph.openaire.eu/docs/next/apis/home/ | Search API поддерживает datasets; records содержат identifiers, landing pages и related links | No key | B |
| Data.gov Catalog API | US government open data across health, climate, education, transport, finance | https://resources.data.gov/catalog-api/ | CKAN-compatible catalog metadata содержит dataset pages, distributions and resource URLs | No key | B |
| World Bank Data Catalog | Development, economy, demographics, poverty, procurement, public finance | https://ddh-openapi.worldbank.org/docs/index.html | Catalog API возвращает dataset metadata и endpoint `dataset/download` | No key | B |
| EPA Data | Environment, facilities, compliance, permits, air, water, climate | https://data.epa.gov/ | OpenAPI/Swagger endpoints дают dataset catalog records and resource links | No key | B |
| NASA Open APIs / OSDR | Space biology, earth science, missions, experiments, environmental metadata | https://api.nasa.gov/ | OSDR Data File API возвращает metadata и download locations for files | API key for many NASA APIs | B |
| USPTO Open Data Portal | Patents, trademarks, office actions, patent file wrappers | https://data.uspto.gov/apis/bulk-data/search | Bulk data API возвращает `fileDownloadURI` для zip/json datasets | No key for public bulk data | B |
| SEC EDGAR APIs | Company filings, financial statements, XBRL facts, public company data | https://www.sec.gov/edgar/sec-api-documentation | APIs возвращают JSON submissions/facts; SEC также публикует bulk zip datasets | No key | B |
| Regulations.gov | Rulemaking dockets, regulatory documents, public comments | https://open.gsa.gov/api/regulationsgov/ | API возвращает dockets, documents, comments и attachment/document metadata | api.data.gov key | B |
| data.europa.eu | EU open data, government, environment, economy, transport, law-adjacent public data | https://data.europa.eu/data/sparql | SPARQL endpoint по DCAT metadata позволяет получать datasets and distributions | No key | B |

## Самые простые первые интеграции

1. Data.Healthcare.gov
2. ClinicalTrials.gov
3. openFDA
4. CourtListener
5. Federal Register
6. Zenodo

Эти источники дают хороший баланс: понятная документация, стабильные идентификаторы, простое построение `url`, минимум нестандартной логики загрузки и высокая тематическая ценность.

## Источники для второго этапа

1. NCBI GEO
2. NCBI Datasets
3. PRIDE Archive
4. OpenAIRE
5. Data.gov
6. EPA Data
7. NASA OSDR

Они полезнее для глубокого специализированного поиска, но требуют более аккуратного парсинга metadata, обработки FTP/bulk download или нормализации неоднородных `distribution` links.

## Предлагаемые домены для классификации

| Домен | Источники |
| --- | --- |
| Medical / Healthcare | Data.Healthcare.gov, ClinicalTrials.gov, openFDA |
| Biomedical Research | NCBI GEO, NCBI Datasets, PRIDE Archive, NASA OSDR |
| Legal / Case Law | CourtListener |
| Regulatory / Government Law | Federal Register, Regulations.gov, SEC EDGAR, USPTO |
| Scientific Research | Zenodo, OpenAIRE |
| Government Open Data | Data.gov, data.europa.eu |
| Environment / Climate / Geospatial | EPA Data, NASA Open APIs, Data.gov, data.europa.eu |
| Economy / Finance / Development | World Bank Data Catalog, SEC EDGAR, Data.gov |

## Интеграционные заметки

- Для `source_name` лучше использовать стабильные lowercase names: `data_healthcare_gov`, `clinicaltrials`, `openfda`, `courtlistener`, `federal_register`, `zenodo`, `ncbi_geo`.
- `external_id` должен быть нативным ID источника: NCT ID, CourtListener ID, Zenodo record ID, GEO accession, Federal Register document number.
- Основной `url` должен вести на human-readable landing page, если она есть.
- Прямые file/download links лучше хранить в `source_meta`, потому что у многих источников на один dataset приходится несколько файлов или distributions.
- Для источников с API key не нужно добавлять mock/fake режимы в dev/prod. Лучше явно пропускать ingest task, если ключ не задан.
- Для Data.gov и data.europa.eu нужно ожидать неодинаковое качество resource links: часть ссылок будет вести на landing pages, часть на CSV/JSON/ZIP/PDF.
