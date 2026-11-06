"""
DAG factory — create Airflow DAGs from source configs.

This is the main entry point for clients who want to create their own DAGs:

    from university_data_platform.airflow.dag_factory import (
        create_single_source_dag,
        create_ingestion_dag,
    )

    # Option A: One source, one DAG
    dag = create_single_source_dag(
        source=HtmlSourceConfig(url="https://www.um5.ac.ma/",
                                 source_system="um5_website"),
        dag_id="um5_daily",
        schedule="0 6 * * *",
    )

    # Option B: Multiple sources, one DAG
    config = SourcesConfig.from_yaml("/path/to/sources.yml")
    dag = create_ingestion_dag(
        sources=config.sources,
        dag_id="my_multi_source_dag",
    )
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from university_data_platform.airflow.operators.ingestion_operator import (
    dispatch_operator,
)
from university_data_platform.config.models import SourceConfig


# ── Helpers ──────────────────────────────────────────────────

def _task_id(source: Union[SourceConfig, dict]) -> str:
    """Build a deterministic task ID from a source config."""
    if isinstance(source, dict):
        t = source.get("type", "html")
        if t == "api":
            return f"api_{source['target'].replace('.', '_')}"
        sid = source.get("source_system") or source.get("url", "unknown")
        return f"{t}_{sid.replace('.', '_').replace('://', '_')}"
    else:
        if source.type == "api":
            return f"api_{source.target.replace('.', '_')}"
        sid = getattr(source, "source_system", None) or getattr(source, "url", "unknown")
        return f"{source.type}_{sid.replace('.', '_').replace('://', '_')}"


def _source_to_dict(source: Union[SourceConfig, dict]) -> dict:
    """Normalize a SourceConfig or raw dict to a plain dict."""
    if isinstance(source, dict):
        return source
    return source.model_dump()


_DEFAULT_ARGS = {
    "owner": "university_data_platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=8),
}


def create_ingestion_dag(
    sources: List[Union[SourceConfig, dict]],
    dag_id: str,
    schedule: str = "0 2 * * *",
    default_args: Optional[Dict[str, Any]] = None,
    max_active_runs: int = 1,
    catchup: bool = False,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    trigger_dag_id: Optional[str] = None,
    trigger_conf: Optional[dict] = None,
) -> DAG:
    """Create a DAG with one parallel task per source.

    Args:
        sources: List of source configs (Pydantic models or raw dicts).
        dag_id: Unique DAG identifier.
        schedule: Cron expression (default daily at 02:00).
        default_args: Override default Airflow task args.
        max_active_runs: Max concurrent DAG runs.
        catchup: Whether to backfill missed runs.
        description: DAG description.
        tags: List of Airflow DAG tags.
        start_date: DAG start date (defaults to 2026-06-01).
        trigger_dag_id: If set, a final task triggers this downstream DAG.
        trigger_conf: Conf dict passed to the triggered DAG run.

    Returns:
        An Airflow DAG object ready for the DAG folder.
    """
    merged_args = {**_DEFAULT_ARGS, **(default_args or {})}

    with DAG(
        dag_id=dag_id,
        default_args=merged_args,
        description=description or f"Ingestion DAG ({len(sources)} sources)",
        schedule=schedule,
        start_date=start_date or datetime(2026, 6, 1),
        catchup=catchup,
        max_active_runs=max_active_runs,
        tags=tags or ["university_data_platform"],
    ) as dag:
        tasks = []
        for source in sources:
            raw = _source_to_dict(source)
            tid = _task_id(source)
            t = dispatch_operator(source=raw, task_id=tid)
            tasks.append(t)

        if trigger_dag_id:
            end = DummyOperator(task_id="end")
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_{trigger_dag_id}",
                trigger_dag_id=trigger_dag_id,
                conf=trigger_conf or {},
                wait_for_completion=False,
            )
            tasks >> end >> trigger

    return dag


# ── Spark DAG constants ─────────────────────────────────────────

_SPARK_APP = "/opt/spark/apps/university_data_platform/pipelines/spark_processor.py"
_SPARK_MASTER = "spark://spark-master:7077"
_SPARK_EXEC = "docker exec spark-master env PYTHONPATH=/opt/spark/apps"

_SPARK_PIPELINES = {
    "faculty_profiles": "Faculty profiles ETL",
    "course_catalog": "Course catalog ETL",
    "pdf_documents": "PDF document text extraction ETL",
    "university_news": "University news ETL",
}

_DEFAULT_SPARK_ARGS = {
    "owner": "university_data_platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}


def create_spark_dag(
    source_system: Union[str, List[str], None] = None,
    dag_id: Optional[str] = None,
    schedule: str = "0 4 * * *",
    pipelines: Union[str, List[str]] = "all",
    default_args: Optional[Dict[str, Any]] = None,
    tags: Optional[List[str]] = None,
    description: Optional[str] = None,
    start_date: Optional[datetime] = None,
    trigger_dag_id: Optional[str] = None,
    trigger_conf: Optional[dict] = None,
) -> DAG:
    """Create a DAG that runs Spark ETL pipelines.

    Args:
        source_system: Source system filter — a single system (``"um5_website"``),
            a list of systems, or ``None``/``"all"`` for all sources.
        dag_id: Unique DAG ID (auto-generated from *source_system* if None).
        schedule: Cron expression (default daily at 04:00).
        pipelines: Which pipelines to run — ``"all"`` (default), or a list like
            ``["faculty_profiles", "course_catalog"]``.
        default_args: Override default Airflow task args.
        tags: List of Airflow DAG tags.
        description: DAG description.
        start_date: DAG start date (defaults to 2026-06-01).
        trigger_dag_id: If set, a final task triggers this downstream DAG.
        trigger_conf: Conf dict passed to the triggered DAG run.

    Returns:
        An Airflow DAG object ready for the DAG folder.
    """
    # Normalise source_system to the CLI flag
    if source_system is None or (isinstance(source_system, str) and source_system.lower() in ("all", "none")):
        cli_source = "all"
    elif isinstance(source_system, list):
        cli_source = ",".join(source_system)
    else:
        cli_source = source_system

    # Auto-generate dag_id
    if dag_id is None:
        src_tag = cli_source.replace(",", "_").replace("-", "_") if cli_source != "all" else "all"
        dag_id = f"spark_{src_tag}"

    # Normalise pipelines to a list
    pipeline_list: list[str] = (
        list(_SPARK_PIPELINES) if isinstance(pipelines, str) and pipelines == "all"
        else pipelines if isinstance(pipelines, list)
        else [pipelines]
    )

    merged_args = {**_DEFAULT_SPARK_ARGS, **(default_args or {})}
    desc = description or f"Spark ETL — {cli_source} ({', '.join(pipeline_list)})"

    pipeline_labels = {name: label for name, label in _SPARK_PIPELINES.items() if name in pipeline_list}

    with DAG(
        dag_id=dag_id,
        default_args=merged_args,
        description=desc,
        schedule=schedule,
        start_date=start_date or datetime(2026, 6, 1),
        catchup=False,
        max_active_runs=1,
        tags=tags or ["phase-2", "spark", "hudi"],
    ) as dag:
        from airflow.operators.bash import BashOperator
        from airflow.operators.dummy import DummyOperator

        start = DummyOperator(task_id="start")

        tasks = []
        for name, label in pipeline_labels.items():
            task = BashOperator(
                task_id=name,
                bash_command=(
                    f"{_SPARK_EXEC} spark-submit --master {_SPARK_MASTER} "
                    f"{_SPARK_APP} {name} --source_system {cli_source}"
                ),
            )
            tasks.append(task)

        # Run sequentially to avoid resource contention (2 Spark workers, 3+ pipelines)
        prev = start
        for task in tasks:
            prev >> task
            prev = task

        if trigger_dag_id:
            end = DummyOperator(task_id="end")
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_{trigger_dag_id}",
                trigger_dag_id=trigger_dag_id,
                conf=trigger_conf or {},
                wait_for_completion=False,
            )
            prev >> end >> trigger
        else:
            end = DummyOperator(task_id="end")
            prev >> end

    return dag


def create_single_source_dag(
    source: Union[SourceConfig, dict],
    dag_id: Optional[str] = None,
    schedule: str = "0 2 * * *",
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    trigger_dag_id: Optional[str] = None,
    trigger_conf: Optional[dict] = None,
    **kwargs,
) -> DAG:
    """Create a DAG with a single ingestion task.

    This is the main function for clients who want to ingest just one source:

        from university_data_platform.airflow.dag_factory import (
            create_single_source_dag,
        )

        dag = create_single_source_dag(
            source=HtmlSourceConfig(
                url="https://www.um5.ac.ma/",
                source_system="um5_website",
            ),
            dag_id="um5_daily",
            schedule="0 6 * * *",
            description="UM5 website daily scrape",
        )

    Args:
        source: A single source config (Pydantic model or raw dict).
        dag_id: Unique DAG ID (auto-generated from source if not provided).
        schedule: Cron expression.
        description: DAG description.
        tags: DAG tags.
        trigger_dag_id: If set, a final task triggers this downstream DAG.
        trigger_conf: Conf dict passed to the triggered DAG run.
        **kwargs: Passed through to create_ingestion_dag.

    Returns:
        An Airflow DAG object.
    """
    if dag_id is None:
        raw = _source_to_dict(source)
        dag_id = f"ingest_{_task_id(raw)}"

    if description is None:
        raw = _source_to_dict(source)
        desc_parts = [raw.get("source_system") or raw.get("url", "unknown source")]
        desc_parts.append(f"({raw.get('type', 'html')})")
        description = f"Ingestion: {' — '.join(desc_parts)}"

    return create_ingestion_dag(
        sources=[source],
        dag_id=dag_id,
        schedule=schedule,
        description=description,
        tags=tags or ["university_data_platform", "single_source"],
        trigger_dag_id=trigger_dag_id,
        trigger_conf=trigger_conf,
        **kwargs,
    )
