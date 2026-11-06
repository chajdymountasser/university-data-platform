"""
Trino-based discovery of Hudi tables and source_systems.

Provides runtime discovery so indexing scripts and the DAG can operate
on whatever tables exist in ``hudi.curated`` without hardcoded configuration.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────────

TRINO_URL = os.getenv("TRINO_URL", "http://trino:8080/v1/statement")
HUDI_CATALOG = "hudi"
CURATED_SCHEMA = "curated"

# Hudi meta columns to strip from documents
_HUDI_META = frozenset({
    "_hoodie_commit_time", "_hoodie_commit_seqno",
    "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name",
})

# Binary / processing-only columns to strip from documents
_SKIP_COLUMNS = frozenset({
    "raw_html", "chunks", "embeddings",
    "embedding_gemini", "embedding_tfidf",
    "pdf_bytes",
})


# ── Trino query helper ──────────────────────────────────────────────

def query_trino(sql: str, trino_url: str | None = None) -> tuple[list[str], list[list]]:
    """Execute SQL on Trino via REST API, following pagination.

    Returns (columns_names, rows) where each row is a list of values.
    """
    url = trino_url or os.getenv("TRINO_URL", "http://trino:8080/v1/statement")
    headers = {"X-Trino-User": "airflow", "X-Trino-Source": "hudi-discovery"}
    next_uri = url
    data_payload = sql
    columns: list[str] = []
    rows: list[list] = []

    while next_uri:
        if next_uri == url:
            resp = requests.post(next_uri, data=data_payload, headers=headers, timeout=120)
        else:
            resp = requests.get(next_uri, headers=headers, timeout=120)
        resp.raise_for_status()
        body = resp.json()

        if body.get("columns"):
            columns = [c["name"] for c in body["columns"]]
        if body.get("data"):
            rows.extend(body["data"])

        next_uri = body.get("nextUri")

    return columns, rows


# ── Discovery ───────────────────────────────────────────────────────

def discover_tables(trino_url: str | None = None) -> list[str]:
    """Query Trino ``INFORMATION_SCHEMA.TABLES`` for ``hudi.curated``.

    Returns sorted list of table names (e.g. ``faculty_profiles``,
    ``course_catalog``, ``pdf_documents``).
    """
    sql = (
        f"SELECT table_name FROM {HUDI_CATALOG}.information_schema.tables "
        f"WHERE table_schema = '{CURATED_SCHEMA}'"
    )
    try:
        _, rows = query_trino(sql, trino_url)
        tables = sorted(row[0] for row in rows if row[0])
        logger.debug("Discovered %d tables in %s.%s: %s", len(tables), HUDI_CATALOG, CURATED_SCHEMA, tables)
        return tables
    except Exception as e:
        logger.error("Failed to discover tables: %s", e)
        return []


def discover_source_systems(trino_url: str | None = None) -> list[str]:
    """Discover all distinct ``source_system`` values across all curated tables.

    Queries each table individually (they're partitioned by source_system)
    and unions the results.
    """
    tables = discover_tables(trino_url)
    sources: set[str] = set()
    for table in tables:
        try:
            sql = f"SELECT DISTINCT source_system FROM {HUDI_CATALOG}.{CURATED_SCHEMA}.{table}"
            _, rows = query_trino(sql, trino_url)
            sources.update(row[0] for row in rows if row[0])
        except Exception as e:
            logger.warning("Could not query source_systems for table '%s': %s", table, e)
    return sorted(sources)


def discover_source_systems_for_table(table_name: str, trino_url: str | None = None) -> list[str]:
    """Query distinct ``source_system`` values for a specific table."""
    try:
        sql = (
            f"SELECT DISTINCT source_system FROM "
            f"{HUDI_CATALOG}.{CURATED_SCHEMA}.{table_name}"
        )
        _, rows = query_trino(sql, trino_url)
        return sorted(row[0] for row in rows if row[0])
    except Exception as e:
        logger.warning("Could not query source_systems for '%s': %s", table_name, e)
        return []


def get_table_columns(table_name: str, trino_url: str | None = None) -> list[str]:
    """Get column names for a curated table via ``INFORMATION_SCHEMA.COLUMNS``."""
    sql = (
        f"SELECT column_name FROM {HUDI_CATALOG}.information_schema.columns "
        f"WHERE table_schema = '{CURATED_SCHEMA}' AND table_name = '{table_name}'"
    )
    try:
        _, rows = query_trino(sql, trino_url)
        return [row[0] for row in rows]
    except Exception as e:
        logger.warning("Could not get columns for '%s': %s", table_name, e)
        return []


def has_chunks_column(table_name: str, trino_url: str | None = None) -> bool:
    """Check if a table has a ``chunks`` column (for Qdrant indexing)."""
    columns = get_table_columns(table_name, trino_url)
    return "chunks" in columns


def filter_indexable_columns(columns: list[str]) -> list[str]:
    """Return columns suitable for indexing (excludes Hudi meta and binary fields)."""
    return [c for c in columns if c not in _HUDI_META and c not in _SKIP_COLUMNS]


def es_index_name(table_name: str) -> str:
    """Derive the Elasticsearch index name from a Hudi table name.

    By default the index name matches the table name.  Override via
    environment variable ``ES_INDEX_{UPPER_TABLE_NAME}`` if needed.
    """
    env_var = f"ES_INDEX_{table_name.upper()}"
    return os.getenv(env_var, table_name)


def qdrant_collection_name(table_name: str) -> str:
    """Derive the Qdrant collection name from a Hudi table name.

    Convention: ``{table_name}_chunks`` (e.g. ``faculty_profiles_chunks``).
    Override via environment variable ``QDRANT_COLLECTION_{UPPER_TABLE_NAME}``
    if needed.
    """
    env_var = f"QDRANT_COLLECTION_{table_name.upper()}"
    return os.getenv(env_var, f"{table_name}_chunks")
