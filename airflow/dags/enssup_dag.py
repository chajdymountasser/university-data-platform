import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))

from university_data_platform.config.models import HtmlSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=HtmlSourceConfig(
        url="https://www.enssup.gov.ma/",
        source_system="enssup_website",
        max_depth=2,
        verify_ssl=False,
    ),
    dag_id="enssup_daily",
    schedule="0 * * * *",
    description="ENSSUP (Enseignement Supérieur) website daily scrape",
)
