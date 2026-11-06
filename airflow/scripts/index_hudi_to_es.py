#!/usr/bin/env python3
"""
Read Hudi curated tables via Trino → index to Elasticsearch.

Uses Trino (SQL engine with Hudi connector + Hive Metastore catalog) to
query Hudi tables directly via REST API, without requiring Spark.

The Trino Hudi connector reads the files from MinIO curated zone
via Hive Metastore catalog, so we get deduplicated, enriched data.

Tables are discovered dynamically at runtime via ``discovery.discover_tables()``
— no hardcoded configuration needed.

Usage as Airflow PythonOperator:
    from airflow.scripts.index_hudi_to_es import index_table_to_es
    index_table_to_es("faculty_profiles")

Usage as CLI:
    python index_hudi_to_es.py [table_name|all]
    python index_hudi_to_es.py --source_system um5_website
"""

import json
import logging
import os
import sys
import time
from typing import Any

from elasticsearch import Elasticsearch, helpers

from university_data_platform.search.discovery import (
    _HUDI_META,
    _SKIP_COLUMNS,
    discover_tables,
    es_index_name,
    query_trino,
)

logger = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────────

ELASTICSEARCH_HOSTS = os.getenv("ELASTICSEARCH_HOSTS", "http://elasticsearch:9200")


# ── ES document builder ─────────────────────────────────────────────

def _row_to_doc(columns: list[str], row: list[Any]) -> dict[str, Any]:
    """Convert a Trino result row into an ES document dict.

    Strips Hudi metadata columns and binary/processing-only columns.
    """
    doc: dict[str, Any] = {}
    for i, col in enumerate(columns):
        if col in _HUDI_META or col in _SKIP_COLUMNS:
            continue
        val = row[i]
        if val is not None:
            doc[col] = val
    return doc


# ── Table reader ────────────────────────────────────────────────────

def _read_table(table_name: str, source_system: str | None = None
                ) -> tuple[list[str], list[list[Any]]]:
    """Read a Hudi table via Trino, optionally filtered by source_system.

    Returns (columns, rows) — columns with Hudi meta and binary fields removed.
    """
    sql = f"SELECT * FROM hudi.curated.{table_name}"
    if source_system:
        sql += f" WHERE source_system = '{source_system}'"

    logger.info("Trino query: %s", sql)
    columns, rows = query_trino(sql)
    logger.info("Trino returned %d rows, %d columns", len(rows), len(columns))
    return columns, rows


# ── Indexing ────────────────────────────────────────────────────────

def index_table_to_es(
    table_name: str,
    source_system: str | None = None,
) -> dict[str, Any]:
    """Read a Hudi table via Trino and index it into Elasticsearch.

    Args:
        table_name: Any Hudi curated table name (e.g. ``faculty_profiles``,
                    ``course_catalog``, ``pdf_documents``).
        source_system: Optional source filter (e.g. ``um5_website``).

    Returns:
        Summary dict with counts and timing.
    """
    start = time.monotonic()

    es_index = es_index_name(table_name)

    # Read from Hudi via Trino
    columns, rows = _read_table(table_name, source_system)

    if not rows:
        return {
            "table": table_name,
            "es_index": es_index,
            "rows_read": 0,
            "indexed": 0,
            "took_ms": int((time.monotonic() - start) * 1000),
        }

    # Build ES actions
    record_id_idx = columns.index("record_id") if "record_id" in columns else -1
    actions = [
        {
            "_index": es_index,
            "_id": row[record_id_idx] if record_id_idx >= 0 else None,
            "_source": _row_to_doc(columns, row),
        }
        for row in rows
    ]

    # Bulk index
    es = Elasticsearch(ELASTICSEARCH_HOSTS)
    success, errors = helpers.bulk(
        es, actions,
        raise_on_error=False,
        chunk_size=100,
        request_timeout=60,
    )

    took_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "ES '%s': %d/%d indexed, %d errors in %dms",
        es_index, success, len(rows), len(errors), took_ms,
    )
    return {
        "table": table_name,
        "es_index": es_index,
        "rows_read": len(rows),
        "indexed": success,
        "errors": len(errors),
        "took_ms": took_ms,
    }


def index_all_to_es(source_system: str | None = None) -> dict[str, Any]:
    """Discover all Hudi tables and index each one to Elasticsearch.

    Tables are discovered at runtime via Trino ``INFORMATION_SCHEMA``.
    """
    tables = discover_tables()
    results: dict[str, Any] = {}
    for table_name in tables:
        results[table_name] = index_table_to_es(table_name, source_system)
    return {"indexes": results}


# ── CLI ─────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("INGESTION_LOG_LEVEL", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    args = sys.argv[1:] if len(sys.argv) > 1 else []
    table = "all"
    source_system = None
    i = 0
    while i < len(args):
        if args[i] == "--source_system" and i + 1 < len(args):
            raw = args[i + 1]
            source_system = None if raw == "all" else raw
            i += 2
        elif table == "all":
            table = args[i]
            i += 1
        else:
            i += 1

    if table == "all":
        result = index_all_to_es(source_system)
    else:
        # Table name validation is dynamic — any table that exists in
        # hudi.curated is valid. The caller passes whatever table name.
        result = index_table_to_es(table, source_system)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
