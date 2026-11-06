import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))
from university_data_platform.config.models import HtmlSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=HtmlSourceConfig(
        url="https://www.uiz.ac.ma/",
        source_system="uiz_website",
        max_depth=2,
        verify_ssl=False,
    ),
    dag_id="uiz_daily",
    schedule="0 6 * * *",
    description="Phase 1 — Ingestion: UIZ website daily scrape — depth 2",
    tags=["university_data_platform", "single_source", "uiz"],
    trigger_dag_id="spark_uiz",
)
