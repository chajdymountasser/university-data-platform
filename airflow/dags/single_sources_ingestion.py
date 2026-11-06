import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))

from university_data_platform.config.models import HtmlSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=HtmlSourceConfig(
        url="https://www.um5.ac.ma/",
        source_system="um5_website",
        max_depth=3,
    ),
    dag_id="um5_daily",
    schedule="0 2 * * *",
    trigger_dag_id="spark_um5",
)