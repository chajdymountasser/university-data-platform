"""
University Data Platform — Single-Source Spark DAG (UM5).

Processes only um5_website data through the Spark pipeline.
Generated via :func:`~university_data_platform.airflow.dag_factory.create_spark_dag`.
"""

import sys

from pathlib import Path

WORKSPACE = Path("/opt/airflow/workspace")
sys.path.insert(0, str(WORKSPACE))

from university_data_platform.airflow.dag_factory import create_spark_dag

dag = create_spark_dag(
    source_system="um5_website",
    dag_id="spark_um5",
    schedule=None,  # Trigger-only: launched by um5_daily after ingestion completes
    description="Phase 2 — Spark ETL: UM5 source only (triggered by um5_daily)",
    tags=["phase-2", "spark", "hudi", "um5"],
    trigger_dag_id="university_search_indexing",
    trigger_conf={"source_system": "um5_website"},
)
