import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))

from university_data_platform.config.models import HtmlSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=HtmlSourceConfig(
        url="https://fsjesm.ma/",
        source_system="fsjesm_website",
        max_depth=3,
    ),
    dag_id="fsjesm_daily",
    schedule="0 4 * * *",
    description="FSJES Mohammedia — daily deep scrape of faculty profiles, course catalog, and administrative documents",
    tags=["university_data_platform", "single_source", "fsjesm"],
    trigger_dag_id="spark_fsjesm",
)
