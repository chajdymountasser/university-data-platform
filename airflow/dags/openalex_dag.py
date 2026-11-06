import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))
from university_data_platform.config.models import ApiSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=ApiSourceConfig(
        target="openalex",
        query="university Morocco",
        max_pages=3,
    ),
    dag_id="openalex_daily",
    schedule="0 7 * * *",
    description="OpenAlex API — Morocco university publications",
    tags=["university_data_platform", "single_source", "openalex"],
)
