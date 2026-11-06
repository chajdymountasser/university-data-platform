"""
University Data Platform — Spark Processing DAG for HCP (statistical data).

Processes HCP data through the PDF documents pipeline since HCP publishes
statistical reports primarily as PDF files (economic indicators, population
statistics, CPI, etc.).

Pipeline:
  Deep scrape (04:00 UTC) → PDF extraction → Chunking → Embedding → Hudi

Generated via :func:`~university_data_platform.airflow.dag_factory.create_spark_dag`.
"""

import sys
from pathlib import Path

WORKSPACE = Path("/opt/airflow/workspace")
sys.path.insert(0, str(WORKSPACE))

from university_data_platform.airflow.dag_factory import create_spark_dag

dag = create_spark_dag(
    source_system="hcp_website",
    dag_id="spark_hcp",
    schedule="0 6 * * *",
    pipelines=["pdf_documents"],
    description="Phase 2 — Spark ETL: HCP statistical PDF extraction, chunking and embedding",
    tags=["phase-2", "spark", "hudi", "hcp"],
)
