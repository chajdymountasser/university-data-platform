"""
University Data Platform — Main Ingestion DAG.

Reads sources.yml dynamically and creates one parallel task per source.
All sources run concurrently. Scheduled daily at 02:00 UTC.

Clients who need custom DAGs should see examples/ or use:
    from university_data_platform.airflow.dag_factory import create_single_source_dag

Example — single-source DAG (place in airflow/dags/):

    from university_data_platform.airflow.dag_factory import create_single_source_dag
    from university_data_platform.config.models import HtmlSourceConfig

    dag = create_single_source_dag(
        source=HtmlSourceConfig(url="https://www.um5.ac.ma/"),
        dag_id="um5_daily",
        schedule="0 6 * * *",
    )
"""

from __future__ import annotations

import sys
from pathlib import Path

# ── Workspace path (mounted by docker-compose) ───────────────
WORKSPACE = Path("/opt/airflow/workspace")
sys.path.insert(0, str(WORKSPACE))

from university_data_platform.config.models import SourcesConfig
from university_data_platform.airflow.dag_factory import create_ingestion_dag

# ── Load & validate all sources ──────────────────────────────
SOURCES_FILE = WORKSPACE / "sources.yml"
config = SourcesConfig.from_yaml(str(SOURCES_FILE))

# ── Create DAG — one parallel task per source ────────────────
dag = create_ingestion_dag(
    sources=config.sources,
    dag_id="university_ingestion_daily",
    schedule="0 2 * * *",
    description="Ingestion of all university data sources (from sources.yml)",
    tags=["phase-1", "ingestion"],
)
