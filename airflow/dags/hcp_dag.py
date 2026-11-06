import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))

from university_data_platform.config.models import HtmlSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=HtmlSourceConfig(
        url="https://www.hcp.ma/",
        source_system="hcp_website",
        max_depth=3,
    ),
    dag_id="hcp_daily",
    schedule="0 4 * * *",
    description="HCP (Haut-Commissariat au Plan) — daily deep scrape of statistics, reports, and economic data",
    tags=["university_data_platform", "single_source", "hcp"],
    trigger_dag_id="spark_hcp",
)
