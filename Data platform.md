# University Data Platform

## Purpose

This document defines a structured data platform for a university environment. It combines data ingestion, lakehouse storage, analytics, full-text search, and semantic retrieval in one architecture.

The platform is designed to support:

- external and internal data collection
- traceable raw data storage
- curated analytical datasets
- dashboards and SQL access
- full-text search
- semantic search and retrieval-augmented generation (RAG)

## Target Architecture

```text
[Web scraping / APIs / Files / Logs]
                |
                v
         [MinIO S3 - Raw Zone]
                |
                v
   [Spark ETL / Quality / Enrichment]
                |
                v
      [Apache Hudi - Curated Zone]
                |
                v
      [Hive Metastore / SQL Catalog]
         /              |            \
        v               v             v
 [Metabase BI]   [Elasticsearch]   [Qdrant]
```

## Architecture Principles

- Separate raw, curated, and serving responsibilities.
- Preserve source data for traceability and reprocessing.
- Use incremental processing instead of repeated full reloads.
- Keep analytics, keyword search, and semantic retrieval as complementary layers.
- Prefer simple ingestion methods first, then add heavier tooling only when needed.

## Core Components

| Layer | Technology | Role |
| --- | --- | --- |
| Data ingestion | Requests, BeautifulSoup, Playwright, Selenium, APIs | Collect external and internal data |
| Raw storage | MinIO | S3-compatible object storage for unprocessed data |
| Processing | Apache Spark | ETL, normalization, deduplication, enrichment, chunking |
| Lakehouse | Apache Hudi | Versioned analytical tables with upsert support |
| Metadata and SQL | Apache Hive Metastore | Catalog and SQL access layer for Hudi tables |
| BI | Metabase | Dashboards and self-service analytics |
| Full-text search | Elasticsearch | Search, filtering, faceting, and log analysis |
| Vector search | Qdrant | Semantic search, hybrid retrieval, and RAG support |

## Data Ingestion Strategy

### Recommended priority

1. Use official APIs when available.
2. Use `requests` plus `BeautifulSoup` for static pages.
3. Use Playwright or Selenium only for JavaScript-heavy sites.

### Typical sources

- university websites
- faculty profile pages
- publication repositories
- course catalogs
- PDF and office documents
- CSV and JSON exports
- application and pipeline logs

### Metadata captured at ingestion

- source system
- source URL or file path
- extraction timestamp
- HTTP status when relevant
- content checksum
- crawler or connector version
- raw object location

## Storage Design

### Raw zone on MinIO

The raw zone stores source data exactly as collected so it can be audited, replayed, or reprocessed.

Suggested buckets:

- `raw-web-html`
- `raw-documents`
- `raw-images`
- `raw-json`
- `raw-logs`

Suggested path convention:

```text
s3://raw-web-html/source=faculty_site/year=2026/month=03/day=25/...
```

Store the raw payload together with metadata describing the source, extraction date, and checksum.

### Curated zone on Apache Hudi

The curated zone stores normalized business entities ready for analytics and downstream indexing.

Suggested Hudi tables:

- `students_external_profiles`
- `research_publications`
- `faculty_profiles`
- `course_catalog`
- `university_news`
- `documents_registry`

Useful common fields:

```json
{
  "record_id": "string",
  "source_system": "string",
  "source_url": "string",
  "content_hash": "string",
  "crawl_timestamp": "timestamp",
  "business_timestamp": "timestamp",
  "is_deleted": false,
  "language": "string",
  "normalized_text": "string"
}
```

Hudi is a good fit here because it supports:

- upserts for changing pages or records
- incremental processing
- historical tracking
- fewer full reloads across the platform

## Processing Layer

Apache Spark is responsible for the main transformation pipeline:

- HTML parsing and text extraction
- document parsing for PDFs and other files
- schema normalization
- deduplication
- enrichment and classification
- chunk generation for long documents
- embedding generation for semantic retrieval
- publishing outputs to Hudi, Elasticsearch, and Qdrant

## Metadata and Analytics Layer

### Hive Metastore

Hive acts as the shared catalog for curated tables. It makes the Hudi layer easier to expose through SQL tools and data workflows.

Typical consumers:

- Spark SQL
- Metabase
- other SQL-compatible engines

### Metabase

Metabase is the lightweight BI layer for dashboards and exploration.

Typical dashboard themes:

- admissions by program
- student success and dropout indicators
- research output by lab or faculty
- publication activity over time
- operational KPIs for the platform itself

## Search and Retrieval Layer

### Elasticsearch

Elasticsearch should be used for:

- keyword and full-text search
- faceted navigation
- structured filters
- document discovery
- log and observability use cases

Typical university search use cases:

- courses
- publications
- staff and faculty profiles
- institutional news
- system logs

### Qdrant

Qdrant should be used for:

- semantic document search
- document chunk retrieval
- hybrid retrieval with metadata filters
- RAG pipelines and AI assistants

Example payload:

```json
{
  "doc_id": "regulation_2026_001",
  "type": "regulation",
  "faculty": "science",
  "language": "en",
  "year": 2026,
  "source": "website"
}
```

This payload structure allows semantic similarity to be combined with business filters such as faculty, language, year, or document type.

## End-to-End Data Flows

### 1. External academic data aggregation

Flow:

```text
Scraper or API -> MinIO raw -> Spark normalization -> Hudi curated -> Elasticsearch or Qdrant -> Metabase
```

Example outputs:

- faculty publication indexes
- calls for papers or grants
- internship and conference listings

### 2. University RAG pipeline

Flow:

```text
Documents or web pages -> MinIO -> Spark parsing and chunking -> embeddings -> Qdrant
```

Typical sources:

- regulations
- syllabi
- administrative notes
- theses and research papers
- student services FAQs

### 3. Institutional analytics

Flow:

```text
Operational and curated data -> Hudi and Hive -> Metabase
```

Typical outputs:

- admissions dashboards
- retention and success metrics
- lab activity indicators
- publication tracking

### 4. Platform observability

Flow:

```text
Applications and pipelines -> Elasticsearch
```

Typical outputs:

- scraper logs
- ETL execution logs
- ingestion errors
- job monitoring dashboards

## Why This Stack Is Coherent

Each component has a clear responsibility:

- MinIO stores raw source data durably.
- Spark transforms and enriches data.
- Hudi manages versioned analytical tables.
- Hive exposes those tables through a shared catalog.
- Metabase provides fast BI delivery.
- Elasticsearch handles keyword search and observability.
- Qdrant handles semantic retrieval and RAG.

Together, the stack supports BI, search, and AI workloads without forcing a single tool to do every job.

## Key Design Considerations

### 1. Use browser automation only when necessary

Selenium and similar tools are resource-intensive and harder to maintain at scale. Prefer APIs first, then lightweight HTTP scraping, and only use browser automation for dynamic sites that require it.

### 2. Preserve governance from the start

For a university platform, plan for:

- lineage and source traceability
- data quality controls
- versioning
- privacy and sensitive-data handling
- clear separation between raw, curated, and serving layers

### 3. Keep Elasticsearch and Qdrant separate

They solve different problems:

- Elasticsearch is not a vector database for your full analytics layer.
- Qdrant is not a replacement for BI, warehousing, or log analysis.

They work best as complementary services.

### 4. Add orchestration as the platform grows

For production-scale pipelines, introduce orchestration such as:

- Apache Airflow for scheduled DAGs
- Apache NiFi for low-code ingestion flows
- both, if ingestion and transformation needs diverge

## Recommended Platform Roadmap

### Minimum viable platform

- Python ingestion services
- MinIO
- Spark
- Hudi
- Hive Metastore
- Metabase
- Elasticsearch
- Qdrant

This is already a credible data and AI platform.

### Extended enterprise platform

Add the following when scale, governance, and operational complexity increase:

- Airflow for orchestration
- Kafka for streaming ingestion
- Keycloak for identity and access control
- Prometheus and Grafana for monitoring
- a data catalog
- an API gateway
- domain microservices
- an LLM service for RAG applications

## Conclusion

This platform is a strong fit for a modern university environment because it combines:

- a traceable raw data lake
- a versioned analytical lakehouse
- lightweight BI
- full-text search
- semantic retrieval for AI assistants

It is especially well suited for organizations that want one architecture to support analytics, document discovery, and AI-powered knowledge access.

## Concrete University Data Lake Sources (Morocco) — MVP + Backlog

Use these as starter ingestion targets for the university data lake (web + APIs + open data).

### 1) University institutional websites
1. https://www.um5.ac.ma/
2. https://www.uca.ma/
3. https://www.usmba.ac.ma/
4. https://www.uh2c.ac.ma/
5. https://www.usms.ac.ma/
6. https://www.uiz.ac.ma/

### 2) Student services and governance
7. https://www.onousc.ma/
8. https://www.enssup.gov.ma/
9. https://www.men.gov.ma/

### 3) Research and publication repositories
10. https://www.imist.ma/
11. https://toubkal.imist.ma/

### 4) National open data and statistics
12. https://www.data.gov.ma/
13. https://www.hcp.ma/

### 5) International academic enrichment APIs
14. https://api.openalex.org/
15. https://api.crossref.org/
16. https://pub.orcid.org/

### 6) Training content for mathematics (open knowledge)
17. https://en.wikipedia.org/wiki/Mathematics
18. https://en.wikipedia.org/wiki/Category:Mathematics_education
19. https://ocw.mit.edu/
20. https://www.khanacademy.org/math


## University Data Lake Source Categories

### Core university sources

- Official university websites (main portal, faculties, departments)
- Admissions/program pages (requirements, calendars, tuition, deadlines)
- Course catalogs and syllabus pages
- Faculty/staff profile pages
- Research lab/center pages
- University news/events/announcements
- Student services pages (housing, scholarships, FAQs, support)

### Academic and document sources

- Institutional repositories (theses, dissertations, papers)
- Library digital collections
- Policy/regulation/governance documents (often PDF-heavy)
- Administrative circulars and internal notes (if accessible)
- Public datasets published by the university (CSV/JSON exports)

### Operational/internal sources

- Student Information System (SIS)
- Learning Management System (LMS)
- HR and finance systems
- CRM/helpdesk/ticketing systems
- Application and pipeline logs (for observability lake use cases)

### External enrichment sources

- OpenAlex, Crossref, ORCID (publication/author enrichment)
- Ministry of higher education/open-government portals
- Accreditation/quality assurance portals
- Labor market and national statistics sources (for outcome analysis)

### Ingestion priority (recommended)

1. Official APIs
2. Static web + files (`requests`/HTML/PDF parsing)
3. Dynamic pages (Playwright/Selenium only when needed)

### Metadata to always capture

- Source system and URL/path
- Ingestion timestamp
- HTTP status (if web)
- Content checksum/hash
- Connector/crawler version
- Raw object storage path