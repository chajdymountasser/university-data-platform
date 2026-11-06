"""
Elasticsearch index management and data indexing from Hudi curated tables.

Index mappings are aligned with the ``FACULTY_PROFILES_SCHEMA`` and
``COURSE_CATALOG_SCHEMA`` in ``processing.pipeline`` to ensure field
compatibility between Hudi and ES.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from elasticsearch import Elasticsearch

from university_data_platform.search.es_client import get_es_client
from university_data_platform.search.models import IndexBatchResult, IndexOperation

logger = logging.getLogger(__name__)


# ── Default index names ───────────────────────────────────────────

FACULTY_INDEX = os.getenv("ELASTICSEARCH_INDEX_FACULTY", "faculty_profiles")
COURSE_INDEX = os.getenv("ELASTICSEARCH_INDEX_COURSE", "course_catalog")
NEWS_INDEX = os.getenv("ELASTICSEARCH_INDEX_NEWS", "university_news")
LOGS_INDEX = os.getenv("ELASTICSEARCH_INDEX_LOGS", "platform-logs")


# ── Index mappings ────────────────────────────────────────────────

FACULTY_MAPPINGS: dict[str, Any] = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "french_analyzer": {
                    "type": "french",
                },
                "arabic_analyzer": {
                    "type": "arabic",
                },
                "english_analyzer": {
                    "type": "english",
                },
                "default": {
                    "type": "standard",
                },
            },
        },
    },
    "mappings": {
        "dynamic": True,
        "properties": {
            "record_id": {"type": "keyword"},
            "record_type": {"type": "keyword"},
            "source_system": {"type": "keyword"},
            "source_url": {"type": "keyword", "index": False},
            "content_hash": {"type": "keyword"},
            "crawl_timestamp": {"type": "long"},
            "business_timestamp": {"type": "date"},
            "normalized_text": {
                "type": "text",
                "fields": {
                    "french": {"type": "text", "analyzer": "french_analyzer"},
                    "english": {"type": "text", "analyzer": "english_analyzer"},
                    "arabic": {"type": "text", "analyzer": "arabic_analyzer"},
                    "keyword": {"type": "keyword"},
                },
            },
            "title": {"type": "text"},
            "full_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "email": {"type": "keyword"},
            "department": {
                "type": "keyword",
                "fields": {"text": {"type": "text"}},
            },
            "faculty": {
                "type": "keyword",
                "fields": {"text": {"type": "text"}},
            },
            "research_interests": {"type": "keyword"},
            "publications": {"type": "keyword"},
            "orcid_id": {"type": "keyword"},
            "language": {"type": "keyword"},
            "is_deleted": {"type": "keyword"},
            "ingested_at": {"type": "date"},
        },
    },
}

COURSE_MAPPINGS: dict[str, Any] = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "french_analyzer": {"type": "french"},
                "arabic_analyzer": {"type": "arabic"},
                "english_analyzer": {"type": "english"},
            },
        },
    },
    "mappings": {
        "dynamic": True,
        "properties": {
            "record_id": {"type": "keyword"},
            "record_type": {"type": "keyword"},
            "source_system": {"type": "keyword"},
            "source_url": {"type": "keyword", "index": False},
            "content_hash": {"type": "keyword"},
            "crawl_timestamp": {"type": "long"},
            "business_timestamp": {"type": "date"},
            "course_code": {"type": "keyword"},
            "course_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "description": {
                "type": "text",
                "fields": {
                    "french": {"type": "text", "analyzer": "french_analyzer"},
                    "english": {"type": "text", "analyzer": "english_analyzer"},
                    "arabic": {"type": "text", "analyzer": "arabic_analyzer"},
                },
            },
            "department": {"type": "keyword", "fields": {"text": {"type": "text"}}},
            "faculty": {"type": "keyword", "fields": {"text": {"type": "text"}}},
            "instructor": {"type": "keyword", "fields": {"text": {"type": "text"}}},
            "full_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "level": {"type": "keyword"},
            "credits": {"type": "keyword"},
            "language": {"type": "keyword"},
            "normalized_text": {
                "type": "text",
                "fields": {
                    "french": {"type": "text", "analyzer": "french_analyzer"},
                    "english": {"type": "text", "analyzer": "english_analyzer"},
                    "arabic": {"type": "text", "analyzer": "arabic_analyzer"},
                },
            },
            "is_deleted": {"type": "keyword"},
            "ingested_at": {"type": "date"},
        },
    },
}

LOGS_MAPPINGS: dict[str, Any] = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "dynamic": True,
        "properties": {
            "@timestamp": {"type": "date"},
            "message": {"type": "text"},
            "log_level": {"type": "keyword"},
            "logger": {"type": "keyword"},
            "service": {"type": "keyword"},
            "dag_id": {"type": "keyword"},
            "task_id": {"type": "keyword"},
            "source": {"type": "keyword"},
        },
    },
}

UNIVERSITY_NEWS_MAPPINGS: dict[str, Any] = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "french_analyzer": {"type": "french"},
                "arabic_analyzer": {"type": "arabic"},
                "english_analyzer": {"type": "english"},
            },
        },
    },
    "mappings": {
        "dynamic": True,
        "properties": {
            "record_id": {"type": "keyword"},
            "record_type": {"type": "keyword"},
            "source_system": {"type": "keyword"},
            "source_url": {"type": "keyword", "index": False},
            "content_hash": {"type": "keyword"},
            "crawl_timestamp": {"type": "long"},
            "business_timestamp": {"type": "date"},
            "title": {
                "type": "text",
                "fields": {
                    "french": {"type": "text", "analyzer": "french_analyzer"},
                    "english": {"type": "text", "analyzer": "english_analyzer"},
                    "keyword": {"type": "keyword"},
                },
            },
            "description": {
                "type": "text",
                "fields": {
                    "french": {"type": "text", "analyzer": "french_analyzer"},
                    "english": {"type": "text", "analyzer": "english_analyzer"},
                    "arabic": {"type": "text", "analyzer": "arabic_analyzer"},
                },
            },
            "category": {"type": "keyword"},
            "normalized_text": {
                "type": "text",
                "fields": {
                    "french": {"type": "text", "analyzer": "french_analyzer"},
                    "english": {"type": "text", "analyzer": "english_analyzer"},
                    "arabic": {"type": "text", "analyzer": "arabic_analyzer"},
                    "keyword": {"type": "keyword"},
                },
            },
            "language": {"type": "keyword"},
            "is_deleted": {"type": "keyword"},
            "ingested_at": {"type": "date"},
        },
    },
}

# Map index names to their configs
INDEX_CONFIGS: dict[str, dict[str, Any]] = {
    FACULTY_INDEX: FACULTY_MAPPINGS,
    COURSE_INDEX: COURSE_MAPPINGS,
    NEWS_INDEX: UNIVERSITY_NEWS_MAPPINGS,
    LOGS_INDEX: LOGS_MAPPINGS,
}


# ── Index management ──────────────────────────────────────────────

def ensure_index(
    index_name: str,
    client: Elasticsearch | None = None,
    force_recreate: bool = False,
) -> bool:
    """Create an index with its mapping if it doesn't exist.

    Args:
        index_name: Name of the index.
        client: ES client (uses singleton if None).
        force_recreate: If True, delete and recreate.

    Returns:
        True if the index is ready.
    """
    client = client or get_es_client()
    config = INDEX_CONFIGS.get(index_name)

    if client.indices.exists(index=index_name):
        if force_recreate:
            client.indices.delete(index=index_name)
            logger.info("Deleted index: %s (force_recreate)", index_name)
        else:
            logger.debug("Index already exists: %s", index_name)
            return True

    if config is None:
        # Dynamic mapping for unknown indexes
        client.indices.create(index=index_name)
        logger.info("Created index with dynamic mapping: %s", index_name)
        return True

    client.indices.create(index=index_name, body=config)
    logger.info("Created index with mapping: %s", index_name)
    return True


def ensure_all_indexes(client: Elasticsearch | None = None) -> list[str]:
    """Ensure all registered indexes exist. Returns list of created/verified index names."""
    client = client or get_es_client()
    created = []
    for name in INDEX_CONFIGS:
        ensure_index(name, client)
        created.append(name)
    return created


def ensure_dynamic_indexes(client: Elasticsearch | None = None) -> list[str]:
    """Discover Hudi tables via Trino and ensure an ES index exists for each.

    Unknown tables get a dynamic mapping (no explicit config needed).
    Also ensures the known static indexes (faculty_profiles, course_catalog,
    platform-logs) still exist.

    Returns list of all ensured index names.
    """
    client = client or get_es_client()

    # Start with static indexes
    ensured = list(INDEX_CONFIGS.keys())
    for name in INDEX_CONFIGS:
        ensure_index(name, client)

    # Discover dynamic tables from Trino
    try:
        from university_data_platform.search.discovery import discover_tables as _discover

        tables = _discover()
        for table in tables:
            if table not in ensured:
                ensure_index(table, client)
                ensured.append(table)
    except Exception as e:
        logger.warning("Could not discover dynamic tables: %s", e)

    return ensured


def delete_index(index_name: str, client: Elasticsearch | None = None) -> bool:
    """Delete an index. Returns True if deleted."""
    client = client or get_es_client()
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        logger.info("Deleted index: %s", index_name)
        return True
    return False


# ── Data indexing ─────────────────────────────────────────────────

def index_document(
    index_name: str,
    document: dict[str, Any],
    doc_id: str | None = None,
    client: Elasticsearch | None = None,
) -> IndexOperation:
    """Index a single document into Elasticsearch.

    Args:
        index_name: Target index.
        document: Document body (flat dict).
        doc_id: Document ID (uses ``record_id`` from document if None).
        client: ES client (uses singleton if None).

    Returns:
        IndexOperation describing the action taken.
    """
    client = client or get_es_client()
    doc_id = doc_id or document.get("record_id")
    start = time.monotonic()

    # Add ingestion timestamp
    doc = dict(document)
    doc["ingested_at"] = datetime.now(timezone.utc).isoformat()

    result = client.index(index=index_name, id=doc_id, body=doc, refresh="wait_for")
    took = int((time.monotonic() - start) * 1000)

    action = "created" if result.get("result") == "created" else "updated"
    logger.debug("Indexed doc %s into %s (%s)", doc_id, index_name, action)
    return IndexOperation(
        index=index_name,
        document_id=doc_id or "",
        action=action,
        took_ms=took,
    )


def index_batch(
    index_name: str,
    documents: list[dict[str, Any]],
    client: Elasticsearch | None = None,
    id_field: str = "record_id",
    batch_size: int = 100,
) -> IndexBatchResult:
    """Bulk-index a list of documents.

    Args:
        index_name: Target ES index.
        documents: List of flat dicts to index.
        client: ES client (uses singleton if None).
        id_field: Field to use as document ``_id``.
        batch_size: Number of docs per bulk call.

    Returns:
        Aggregated IndexBatchResult.
    """
    client = client or get_es_client()
    ensure_index(index_name, client)

    start = time.monotonic()
    succeeded = 0
    failed = 0
    errors: list[str] = []
    operations: list[IndexOperation] = []

    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        bulk_body: list[dict[str, Any]] = []

        for doc in batch:
            doc_id = doc.get(id_field)
            doc["ingested_at"] = datetime.now(timezone.utc).isoformat()
            bulk_body.append({"index": {"_index": index_name, "_id": doc_id}})
            bulk_body.append(doc)

        if bulk_body:
            response = client.bulk(body=bulk_body, refresh=True)
            if response.get("errors"):
                for item in response.get("items", []):
                    op = item.get("index", {})
                    if op.get("error"):
                        failed += 1
                        errors.append(str(op["error"]))
                    else:
                        succeeded += 1
                        operations.append(IndexOperation(
                            index=index_name,
                            document_id=op.get("_id", ""),
                            action="created" if op.get("result") == "created" else "updated",
                            took_ms=0,
                        ))
            else:
                for item in response.get("items", []):
                    op = item.get("index", {})
                    succeeded += 1
                    operations.append(IndexOperation(
                        index=index_name,
                        document_id=op.get("_id", ""),
                        action="created" if op.get("result") == "created" else "updated",
                        took_ms=0,
                    ))

    took_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "Bulk indexed %d docs into %s: %d succeeded, %d failed in %dms",
        len(documents), index_name, succeeded, failed, took_ms,
    )
    return IndexBatchResult(
        total=len(documents),
        succeeded=succeeded,
        failed=failed,
        errors=errors,
        operations=operations,
        took_ms=took_ms,
        source_table=index_name,
    )


# ── CLI entry point ──────────────────────────────────────────────

def main() -> None:
    """CLI entry point: ensure indexes and optionally index sample data."""
    logging.basicConfig(
        level=getattr(logging, os.getenv("INGESTION_LOG_LEVEL", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import argparse

    parser = argparse.ArgumentParser(description="Elasticsearch index manager")
    parser.add_argument("action", choices=["create", "delete", "reindex"], default="create", nargs="?")
    parser.add_argument("--index", help="Target index (default: all)")
    parser.add_argument("--input", help="Path to JSON file with documents to index")
    args = parser.parse_args()

    client = get_es_client()

    if args.action == "create":
        if args.index:
            ensure_index(args.index, client)
        else:
            ensure_all_indexes(client)
        print("Index creation complete.")

    elif args.action == "delete":
        if args.index:
            delete_index(args.index, client)
        else:
            for name in INDEX_CONFIGS:
                delete_index(name, client)
        print("Index deletion complete.")

    elif args.action == "reindex":
        if not args.input:
            print("--input is required for reindex")
            return
        if args.index:
            indexes = [args.index]
        else:
            indexes = list(INDEX_CONFIGS)

        with open(args.input) as f:
            docs = json.load(f)

        for idx in indexes:
            result = index_batch(idx, docs)
            print(f"Reindexed {idx}: {result.succeeded}/{result.total} docs")


if __name__ == "__main__":
    main()
