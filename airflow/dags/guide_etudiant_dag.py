import sys
from pathlib import Path
sys.path.insert(0, str(Path("/opt/airflow/workspace")))
from university_data_platform.config.models import FileSourceConfig
from university_data_platform.airflow.dag_factory import create_single_source_dag

dag = create_single_source_dag(
    source=FileSourceConfig(
        path="/opt/airflow/workspace/data/guide_etudiant.pdf",
        source_system="guide_etudiant",
    ),
    dag_id="guide_etudiant_daily",
    schedule="0 8 * * *",
    description="Guide Etudiant PDF ingestion",
    tags=["university_data_platform", "single_source", "pdf"],
)
