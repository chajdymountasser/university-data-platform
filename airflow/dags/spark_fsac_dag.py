"""
University Data Platform — Spark Processing DAG for FSAC.

Processes FSAC data through all 3 ETL pipelines:
  - faculty_profiles
  - course_catalog
  - pdf_documents

Generated via :func:`~university_data_platform.airflow.dag_factory.create_spark_dag`.
"""

import sys
from pathlib import Path

WORKSPACE = Path("/opt/airflow/workspace")
sys.path.insert(0, str(WORKSPACE))

from university_data_platform.airflow.dag_factory import create_spark_dag

dag = create_spark_dag(
    source_system="fsac_website",
    dag_id="spark_fsac",
    schedule="0 6 * * *",
    pipelines=["faculty_profiles", "course_catalog", "pdf_documents"],
    description="Phase 2 — Spark ETL: FSAC faculty profiles, course catalog and document extraction",
    tags=["phase-2", "spark", "hudi", "fsac"],
)
