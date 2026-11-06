"""
University Data Platform — Single-Source Spark DAG (UIZ).

Processes only uiz_website data through the Spark pipeline.
Generated via :func:`~university_data_platform.airflow.dag_factory.create_spark_dag`.
"""

import sys
from pathlib import Path

WORKSPACE = Path("/opt/airflow/workspace")
sys.path.insert(0, str(WORKSPACE))

from university_data_platform.airflow.dag_factory import create_spark_dag

dag = create_spark_dag(
    source_system="uiz_website",
    dag_id="spark_uiz",
    schedule=None,  # Trigger-only: launched by uiz_daily after ingestion completes
    description="Phase 2 — Spark ETL: UIZ source only (triggered by uiz_daily)",
    tags=["phase-2", "spark", "hudi", "uiz"],
    trigger_dag_id="university_search_indexing",
    trigger_conf={"source_system": "uiz_website"},
)
