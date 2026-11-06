import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))

from university_data_platform.config.models import HtmlSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=HtmlSourceConfig(
        url="https://fsac.univh2c.ma/front/index.html",
        source_system="fsac_website",
        max_depth=3,
        auto_playwright=True,
    ),
    dag_id="fsac_daily",
    schedule="0 3 * * *",
    description="FSAC — daily deep scrape of faculty profiles, course catalog, and administrative documents",
    tags=["university_data_platform", "single_source", "fsac"],
    trigger_dag_id="spark_fsac",
)
