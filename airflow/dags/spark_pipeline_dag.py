"""
University Data Platform — Phase 2 Spark Processing DAG.

Runs the Spark ETL pipelines after raw data has been ingested:
  1. faculty_profiles — normalized faculty/staff profiles
  2. course_catalog  — structured course catalog

Both pipelines read from MinIO (s3a://), process with PySpark,
and write Hudi tables synced to Hive Metastore.

Generated via :func:`~university_data_platform.airflow.dag_factory.create_spark_dag`.
"""

import sys
from pathlib import Path

WORKSPACE = Path("/opt/airflow/workspace")
sys.path.insert(0, str(WORKSPACE))

from university_data_platform.airflow.dag_factory import create_spark_dag

dag = create_spark_dag(
    dag_id="university_spark_pipeline",
    schedule="0 4 * * *",
    description="Phase 2 — Spark ETL: Hudi tables, normalization, enrichment",
    tags=["phase-2", "spark", "hudi"],
)
