import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))

from university_data_platform.config.models import HtmlSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=HtmlSourceConfig(
        url="https://www.uca.ma/",
        source_system="uca_website",
        max_depth=3,
    ),
    dag_id="uca_daily",
    schedule="0 6 * * *",
    description="UCA (Université Cadi Ayyad) website daily scrape — depth 3",
    tags=["university_data_platform", "single_source", "uca"],
)
