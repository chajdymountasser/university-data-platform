"""
Airflow integration for the University Data Platform.

Clients can create their own DAGs using the factory:

    from university_data_platform.airflow.dag_factory import create_single_source_dag
    from university_data_platform.config.models import HtmlSourceConfig

    dag = create_single_source_dag(
        source=HtmlSourceConfig(url="https://www.um5.ac.ma/",
                                 source_system="um5_website"),
        dag_id="my_um5_dag",
        schedule="0 6 * * *",
    )
"""

from university_data_platform.airflow.hooks.minio_hook import MinIOHook
from university_data_platform.airflow.operators.ingestion_operator import (
    HtmlIngestionOperator,
    ApiIngestionOperator,
    FileIngestionOperator,
    dispatch_operator,
)
from university_data_platform.airflow.dag_factory import (
    create_ingestion_dag,
    create_single_source_dag,
)

__all__ = [
    "MinIOHook",
    "HtmlIngestionOperator",
    "ApiIngestionOperator",
    "FileIngestionOperator",
    "dispatch_operator",
    "create_ingestion_dag",
    "create_single_source_dag",
]
