"""
University Data Platform — Observability DAG.

Periodically checks platform health and ships aggregated status
to the Elasticsearch logs index.

Also demonstrates how to use the ElasticsearchLogHandler for shipping
Airflow task logs to ES for centralized observability.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

WORKSPACE = Path("/opt/airflow/workspace")
if str(WORKSPACE) not in sys.path:
    sys.path.insert(0, str(WORKSPACE))

_DEFAULT_ARGS = {
    "owner": "university_data_platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _check_platform_health() -> str:
    """Check health of platform services and log to ES."""
    import logging

    from university_data_platform.search.observability import configure_platform_logging

    configure_platform_logging(service="observability-dag")
    logger = logging.getLogger("observability.health")

    results = {}

    # MinIO
    try:
        from university_data_platform.storage.minio_client import MinIOClient
        mc = MinIOClient()
        buckets = mc.create_buckets()  # creates if not exists
        results["minio"] = "healthy"
        logger.info("MinIO health check passed", extra={"service": "minio"})
    except Exception as e:
        results["minio"] = f"unhealthy: {e}"
        logger.error("MinIO health check failed: %s", e, extra={"service": "minio"})

    # Elasticsearch
    try:
        from university_data_platform.search.es_client import es_is_healthy
        health = es_is_healthy()
        results["elasticsearch"] = health.get("status", "unknown")
        logger.info("ES health: %s", results["elasticsearch"], extra={"service": "elasticsearch"})
    except Exception as e:
        results["elasticsearch"] = f"unhealthy: {e}"
        logger.error("ES health check failed: %s", e, extra={"service": "elasticsearch"})

    # Qdrant
    try:
        from university_data_platform.search.qdrant_client import qdrant_is_healthy
        health = qdrant_is_healthy()
        results["qdrant"] = health.get("status", "unknown")
        logger.info("Qdrant health: %s", results["qdrant"], extra={"service": "qdrant"})
    except Exception as e:
        results["qdrant"] = f"unhealthy: {e}"
        logger.error("Qdrant health check failed: %s", e, extra={"service": "qdrant"})

    status = "healthy" if all(v == "healthy" for v in results.values() if isinstance(v, str)) else "degraded"
    logger.info(
        "Platform health: %s | %s",
        status,
        results,
        extra={"dag_id": "observability_dag", "platform_status": status},
    )

    return json.dumps(results)


def _ship_recent_logs() -> str:
    """Collect recent Airflow log files and index to ES.

    This is a demonstration placeholder — in production, use Filebeat
    or a log shipper to tail Airflow logs into ES.
    """
    import json
    from pathlib import Path

    log_dir = Path("/opt/airflow/logs")
    if not log_dir.exists():
        return "No logs directory found"

    # Count log files
    log_files = list(log_dir.rglob("*.log"))
    return f"Found {len(log_files)} log files. Use Filebeat for production log shipping."


import json

with DAG(
    dag_id="university_observability",
    default_args=_DEFAULT_ARGS,
    description="Platform observability — health checks + log monitoring",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["phase-4", "observability"],
) as dag:

    start = DummyOperator(task_id="start")

    health_check = PythonOperator(
        task_id="platform_health_check",
        python_callable=_check_platform_health,
    )

    log_check = PythonOperator(
        task_id="log_shipping_check",
        python_callable=_ship_recent_logs,
    )

    end = DummyOperator(task_id="end")

    start >> [health_check, log_check] >> end
