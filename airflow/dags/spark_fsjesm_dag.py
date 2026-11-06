"""
University Data Platform — Spark Processing DAG for FSJES Mohammedia.

Processes FSJESM data through all 3 ETL pipelines:
  - faculty_profiles (Corps professoral)
  - course_catalog (Licence, Master, Formation Continue)
  - pdf_documents (Documents administratifs)

Pipeline:
  Deep scrape (04:00 UTC) → Classification → Normalization → Enrichment → Chunking → Embedding → Hudi

Generated via :func:`~university_data_platform.airflow.dag_factory.create_spark_dag`.
"""

import sys
from pathlib import Path

WORKSPACE = Path("/opt/airflow/workspace")
sys.path.insert(0, str(WORKSPACE))

from university_data_platform.airflow.dag_factory import create_spark_dag

dag = create_spark_dag(
    source_system="fsjesm_website",
    dag_id="spark_fsjesm",
    schedule=None,  # Trigger-only: launched by fsjesm_daily after ingestion completes
    pipelines=["faculty_profiles", "course_catalog", "pdf_documents", "university_news"],
    description="Phase 2 — Spark ETL: FSJESM faculty profiles, course catalog, documents and news (triggered by fsjesm_daily)",
    tags=["phase-2", "spark", "hudi", "fsjesm"],
    trigger_dag_id="university_search_indexing",
    trigger_conf={"source_system": "fsjesm_website"},
)
