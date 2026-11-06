from university_data_platform.airflow.operators.ingestion_operator import (
    HtmlIngestionOperator,
    ApiIngestionOperator,
    FileIngestionOperator,
    dispatch_operator,
)

__all__ = [
    "HtmlIngestionOperator",
    "ApiIngestionOperator",
    "FileIngestionOperator",
    "dispatch_operator",
]
