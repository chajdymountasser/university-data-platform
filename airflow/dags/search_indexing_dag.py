"""
University Data Platform — Dynamic Search Indexing DAG.

Indexes curated data from **all** Hudi tables into Elasticsearch and Qdrant
via Trino SQL (REST API) — no Spark required for the indexing step.

Tables are discovered at **runtime** via ``discovery.discover_tables()``
so every Hudi table is indexed automatically with zero manual config.

Pipeline:
  Spark ETL (04:00 UTC) → Hudi curated tables → Search Indexing (05:00 UTC)
    ↓
  ├── Trino ──→ Elasticsearch: every curated table with text columns
  └── Trino ──→ Qdrant: every curated table with a ``chunks`` column

Dynamic task mapping (Airflow 2.3+ ``expand()``) creates one task per
table per engine, so every table gets its own log, retries, and observability.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator

# Ensure workspace and scripts are on path so the scripts are importable
WORKSPACE = Path("/opt/airflow/workspace")
if str(WORKSPACE) not in sys.path:
    sys.path.insert(0, str(WORKSPACE))

SCRIPTS_DIR = str(Path("/opt/airflow/workspace/airflow/scripts"))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

_DEFAULT_ARGS = {
    "owner": "university_data_platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def _get_source_system(context: dict[str, Any]) -> str | None:
    """Read optional source_system from DAG conf (set by TriggerDagRunOperator)."""
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf:
        raw = dag_run.conf.get("source_system")
        if raw and raw != "all":
            return raw
    return None


# ── Dynamic discovery task ─────────────────────────────────────────

@task(
    task_id="discover_tables",
    retries=3,
    retry_delay=timedelta(seconds=30),
)
def discover_tables(**context: Any) -> list[str]:
    """Discover all Hudi curated tables via Trino at runtime."""
    from university_data_platform.search.discovery import discover_tables as _discover

    tables = _discover()
    return tables


# ── Index management task ──────────────────────────────────────────

@task(task_id="ensure_search_indexes")
def ensure_search_indexes(table_name: str, **context: Any) -> str:
    """Ensure ES index and Qdrant collection exist for a single table.

    Uses dynamic mapping for unknown ES indexes.  Qdrant collections
    are created only for tables with a ``chunks`` column.
    """
    from university_data_platform.search.discovery import has_chunks_column, qdrant_collection_name
    from university_data_platform.search.es_indexer import ensure_index
    from university_data_platform.search.qdrant_client import ensure_collection, get_qdrant_client

    # ES index (dynamic mapping if first time)
    ensure_index(table_name)

    # Qdrant collection (only if table has chunks)
    if has_chunks_column(table_name):
        qdrant = get_qdrant_client()
        collection = qdrant_collection_name(table_name)
        ensure_collection(
            collection,
            vector_size=int(os.getenv("QDRANT_VECTOR_SIZE", "768")),
            client=qdrant,
        )
        return f"Ensured ES index '{table_name}' + Qdrant collection '{collection}'"

    return f"Ensured ES index '{table_name}' (no chunks → skipped Qdrant)"


# ── Indexing tasks ─────────────────────────────────────────────────

@task(task_id="index_to_es")
def index_table_to_es(table_name: str, **context: Any) -> str:
    """Index a single Hudi table to Elasticsearch via Trino."""
    from index_hudi_to_es import index_table_to_es as _index

    source_system = _get_source_system(context)
    result = _index(table_name, source_system=source_system)
    return f"{table_name} → ES: {result['indexed']}/{result['rows_read']} docs ({result['took_ms']}ms)"


@task(task_id="index_to_qdrant")
def index_table_to_qdrant(table_name: str, **context: Any) -> str:
    """Index a single Hudi table to Qdrant via Trino (only if it has chunks)."""
    from index_hudi_to_qdrant import index_table_to_qdrant as _index

    source_system = _get_source_system(context)
    result = _index(table_name, source_system=source_system)
    if result.get("skipped_reason"):
        return f"{table_name}: skipped ({result['skipped_reason']})"
    return f"{table_name} → Qdrant: {result['points_indexed']} points from {result['rows_read']} rows ({result['took_ms']}ms)"


# ── DAG definition ─────────────────────────────────────────────────

with DAG(
    dag_id="university_search_indexing",
    default_args=_DEFAULT_ARGS,
    description="Dynamic indexing: auto-discovers Hudi tables → ES + Qdrant via Trino",
    schedule="0 5 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["phase-4", "search", "elasticsearch", "qdrant", "dynamic"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # Discover tables at runtime (upstream of all indexing)
    tables = discover_tables()

    # Fan out: one ensure + one ES index + one Qdrant index per table
    ensure_tasks = ensure_search_indexes.expand(table_name=tables)
    es_tasks = index_table_to_es.expand(table_name=tables)
    qdrant_tasks = index_table_to_qdrant.expand(table_name=tables)

    # ── Flow ───────────────────────────
    start >> tables
    tables >> ensure_tasks
    ensure_tasks >> es_tasks
    ensure_tasks >> qdrant_tasks
    [es_tasks, qdrant_tasks] >> end
