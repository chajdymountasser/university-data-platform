#!/usr/bin/env python3
"""
[DEPRECATED] Index data into Elasticsearch — reads from MinIO RAW zone.

⚠️  This script reads from MinIO raw-web-html directly, bypassing the Hudi
curated zone. Kept as a fallback only.

Replaced by ``index_hudi_to_es.py`` which reads from Hudi curated tables
via Spark:
    docker exec spark-master spark-submit --master spark://spark-master:7077 \\
        /opt/spark/apps/airflow/scripts/index_hudi_to_es.py {table}
"""

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

# Ensure the workspace is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from university_data_platform.search.es_client import get_es_client
from university_data_platform.search.es_indexer import (
    COURSE_INDEX,
    FACULTY_INDEX,
    ensure_all_indexes,
    index_batch,
)
from university_data_platform.storage.minio_client import MinIOClient

logger = logging.getLogger(__name__)


# ── Text extraction ───────────────────────────────────────────────

def extract_text_from_html(html: str) -> str:
    """Basic HTML-to-text extraction (mirrors processing.parser)."""
    if not html:
        return ""
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ── Metadata reader ───────────────────────────────────────────────

def read_metadata_sidecars(
    bucket: str,
    source_system: str | None = None,
    minio_client: MinIOClient | None = None,
) -> list[dict[str, Any]]:
    """Read all .meta.json sidecars from a MinIO bucket.

    Uses the MinIO client to list and fetch metadata objects.
    """
    client = minio_client or MinIOClient()
    s3 = client._client

    prefix = ""
    if source_system:
        prefix = f"source={source_system}/"

    docs: list[dict[str, Any]] = []
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".meta.json"):
                continue
            try:
                resp = s3.get_object(Bucket=bucket, Key=key)
                meta = json.loads(resp["Body"].read())
                docs.append(meta)
            except Exception as e:
                logger.warning("Failed to read %s: %s", key, e)

    logger.info("Read %d metadata sidecars from %s/%s", len(docs), bucket, prefix or "")
    return docs


# ── Enrichment (lightweight, Spark-free) ──────────────────────────

def _build_faculty_doc(meta: dict[str, Any], raw_html: str | None) -> dict[str, Any]:
    """Build an ES document from a raw HTML metadata sidecar."""
    text = extract_text_from_html(raw_html) if raw_html else meta.get("normalized_text", "")
    return {
        "record_id": meta.get("record_id", ""),
        "source_system": meta.get("source_system", ""),
        "source_url": meta.get("source_url", ""),
        "content_hash": meta.get("content_checksum", ""),
        "crawl_timestamp": int(meta.get("crawl_timestamp", datetime.now(timezone.utc).timestamp() * 1000)),
        "normalized_text": text,
        "title": meta.get("title", ""),
        "full_name": meta.get("full_name", ""),
        "email": meta.get("email", ""),
        "department": meta.get("department", ""),
        "faculty": meta.get("faculty", ""),
        "research_interests": meta.get("research_interests", []),
        "publications": meta.get("publications", []),
        "orcid_id": meta.get("orcid_id", ""),
        "language": meta.get("language", "unknown"),
        "is_deleted": "false",
    }


def _build_course_doc(meta: dict[str, Any], raw_html: str | None) -> dict[str, Any]:
    """Build an ES document from a course-related metadata sidecar."""
    text = extract_text_from_html(raw_html) if raw_html else meta.get("normalized_text", "")
    return {
        "record_id": meta.get("record_id", ""),
        "source_system": meta.get("source_system", ""),
        "source_url": meta.get("source_url", ""),
        "content_hash": meta.get("content_checksum", ""),
        "crawl_timestamp": int(meta.get("crawl_timestamp", datetime.now(timezone.utc).timestamp() * 1000)),
        "course_code": meta.get("course_code", ""),
        "course_name": meta.get("course_name", ""),
        "description": text,
        "department": meta.get("department", ""),
        "faculty": meta.get("faculty", ""),
        "instructor": meta.get("instructor", ""),
        "level": meta.get("level", ""),
        "credits": meta.get("credits", ""),
        "language": meta.get("language", "unknown"),
        "normalized_text": text,
        "is_deleted": "false",
    }


# ── Main indexing functions ───────────────────────────────────────

def index_faculty_to_es(
    source_system: str | None = None,
    minio_client: MinIOClient | None = None,
    es_client: Any | None = None,
) -> dict[str, Any]:
    """Index faculty profile data from MinIO into Elasticsearch."""
    es = es_client or get_es_client()
    ensure_all_indexes(es)
    minio = minio_client or MinIOClient()

    # Read metadata sidecars from the raw-web-html bucket
    logger.info("Reading faculty profile metadata from MinIO...")
    docs = read_metadata_sidecars("raw-web-html", source_system, minio)

    if not docs:
        logger.warning("No faculty metadata found to index")
        return {"total": 0, "succeeded": 0, "failed": 0, "took_ms": 0}

    # Build ES documents
    es_docs = [_build_faculty_doc(d, None) for d in docs]

    # Bulk index
    result = index_batch(FACULTY_INDEX, es_docs, es)
    logger.info("Faculty indexing complete: %s", result)
    return result.model_dump() if hasattr(result, "model_dump") else result


def index_courses_to_es(
    source_system: str | None = None,
    minio_client: MinIOClient | None = None,
    es_client: Any | None = None,
) -> dict[str, Any]:
    """Index course catalog data from MinIO into Elasticsearch."""
    es = es_client or get_es_client()
    ensure_all_indexes(es)
    minio = minio_client or MinIOClient()

    # Read metadata sidecars from raw-web-html (courses are scraped HTML too)
    docs = read_metadata_sidecars("raw-web-html", source_system, minio)

    if not docs:
        logger.warning("No course metadata found to index")
        return {"total": 0, "succeeded": 0, "failed": 0, "took_ms": 0}

    es_docs = [_build_course_doc(d, None) for d in docs]
    result = index_batch(COURSE_INDEX, es_docs, es)
    return result


def index_all_to_es(
    source_system: str | None = None,
) -> dict[str, Any]:
    """Index all data sources into Elasticsearch."""
    faculty_result = index_faculty_to_es(source_system)
    courses_result = index_courses_to_es(source_system)
    return {
        "faculty_profiles": faculty_result,
        "course_catalog": courses_result,
    }


# ── CLI entry point ───────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("INGESTION_LOG_LEVEL", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    source = sys.argv[1] if len(sys.argv) > 1 else None
    if source and source != "all":
        logger.info("Indexing source_system=%s to Elasticsearch", source)
        result = index_all_to_es(source)
    else:
        logger.info("Indexing ALL sources to Elasticsearch")
        result = index_all_to_es(None)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
