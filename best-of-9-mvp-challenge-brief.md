# University Data Platform Challenge Brief

## Best-of-9 MVP Competition (3 Weeks)

## 1\) Challenge Objective

Each team will build a complete MVP of the same university data platform.  
After 3 weeks, projects are evaluated

Goal: deliver a reliable end-to-end flow:

`Sources -> MinIO (raw) -> Spark (transform) -> Hudi/Hive (curated + SQL) -> Metabase dashboard + Elasticsearch search`

## 2\) Team Format

- 9 teams  
- 6 students per team  
- Each team works independently on the full project  
- Same scope, same deadlines, same evaluation criteria

## 3\) Mandatory MVP Scope

Each team must deliver all items below.

- Ingest data from 3 source types:  
  - 1 API source  
  - 1 static web source  
  - 1 file/document source (PDF/CSV/JSON)  
- Store raw data in MinIO with metadata  
- Transform and normalize with Spark  
- Produce 2 curated Hudi tables:  
  - `faculty_profiles`  
  - `course_catalog`  
- Register curated tables in Hive for SQL access  
- Create 1 Metabase dashboard (5-7 KPIs)  
- Create 1 Elasticsearch index \+ basic search endpoint  
- Support daily execution with logs and retry logic

## 4\) Out of Scope (Do Not Build)

- Qdrant / embeddings / RAG  
- Kafka, Airflow, Keycloak  
- Large-scale multi-source expansion  
- Advanced enterprise governance stack  
- Heavy browser automation unless strictly necessary

## 5\) Technical Constraints

- Use the required stack:  
  - MinIO, Spark, Hudi, Hive Metastore, Metabase, Elasticsearch  
- Keep architecture simple and reproducible  
- Ensure traceability from curated/search outputs back to raw objects  
- Keep clear naming and schema conventions

## 6\) Competition Timeline (3 Weeks / 15 Working Days)

- Week 1: environment, ingestion, raw storage, initial transforms  
- Week 2: curated tables, SQL exposure, dashboard/search integration  
- Week 3: hardening, tests, rerun safety, demo readiness, documentation

## 7\) Submission Package (Required)

Each team submits:

- Source code repository  
- Setup instructions (`README`)  
- Architecture diagram  
- Pipeline run instructions  
- Runbook (run, monitor, recover)  
- Test evidence (screenshots/logs/reports)  
- Demo script (what to show in 15 minutes)

## 8\) Final Demo Format (Per Team)

- 15 min live demo  
- 10 min stress/break test by jury (for example rerun or failure recovery)  
- 10 min technical Q\&A  
- 5 min scoring

Important: pre-recorded demos alone are not sufficient. A live run or live verification is required.

## 9\) Evaluation Rubric (100 Points)

- 30 Reliability and stability  
  - Repeatable runs, retries, rerun safety, error handling  
- 20 Data quality and traceability  
  - Schema correctness, deduplication, metadata completeness  
- 15 BI usefulness  
  - KPI relevance, clarity, filter usability  
- 15 Search quality  
  - Relevance, filtering, practical usefulness  
- 10 Engineering quality  
  - Code organization, maintainability, tests  
- 10 Ops readiness  
  - Runbook quality, observability basics, deployment simplicity

## 10\) Integrity and Fairness Rules

- Teams may use public documentation and open-source libraries  
- Teams must keep transparent commit history  
- Teams must explain architecture decisions during Q\&A  
- Copying another team's private implementation is prohibited  
- Jury decisions are based on rubric and live evidence

## 11\) Definition of Success

A team succeeds when the platform:

- runs end-to-end reliably,  
- produces meaningful dashboard insights,  
- supports useful keyword search,  
- and can be operated by another person using its runbook.

## 12\) Instructor Notes (Optional to Share)

- Prefer stable systems over flashy features  
- Penalize fragile demos that cannot rerun  
- Reward clarity, reproducibility, and robustness

