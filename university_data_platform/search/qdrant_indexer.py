"""
Qdrant indexer — creates collections and indexes document chunks with embeddings.

Reads data from Hudi tables (processed by Spark) or directly from MinIO,
generates embeddings (via the embedder pipeline), and upserts points into Qdrant.

Key design decisions:
  - Each document chunk becomes a separate Qdrant point so that RAG retrieval
    works at chunk granularity.
  - Payload carries source metadata (source_system, faculty, language, year, url)
    so that hybrid search can combine vector similarity with business filters.
  - The ``doc_id`` (deterministic UUID from the source) is used as the Qdrant
    point ID for idempotent upserts.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from qdrant_client import QdrantClient as _QdrantClient
from qdrant_client.http import models

from university_data_platform.search.qdrant_client import (
    ensure_collection,
    get_qdrant_client,
)

logger = logging.getLogger(__name__)


# ── Defaults ──────────────────────────────────────────────────────

FACULTY_COLLECTION = os.getenv("QDRANT_COLLECTION_FACULTY", "faculty_profiles_chunks")
COURSE_COLLECTION = os.getenv("QDRANT_COLLECTION_COURSE", "course_catalog_chunks")
VECTOR_SIZE = int(os.getenv("QDRANT_VECTOR_SIZE", "768"))  # Gemini embedding-2


# ── Payload schema ────────────────────────────────────────────────

def _build_payload(
    doc: dict[str, Any],
    chunk: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build Qdrant payload from a document dict, optionally with chunk info.

    The payload mirrors the example in Data platform.md:
      doc_id, type, faculty, language, year, source

    plus whatever additional metadata is available.
    """
    payload: dict[str, Any] = {
        "doc_id": doc.get("record_id", ""),
        "type": doc.get("type", doc.get("source_system", "unknown")),
        "faculty": doc.get("faculty", ""),
        "language": doc.get("language", "unknown"),
        "source": doc.get("source_system", "unknown"),
    }

    # Add source URL for traceability
    if "source_url" in doc:
        payload["source_url"] = doc["source_url"]

    # Add timestamp for temporal filtering
    if "crawl_timestamp" in doc:
        payload["crawl_timestamp"] = doc["crawl_timestamp"]

    # Add full_name / course_name for display
    for field in ("full_name", "course_name", "title", "department", "email", "level", "credits"):
        if doc.get(field):
            payload[field] = doc[field]

    # Add chunk-level info when chunking
    if chunk:
        payload["chunk_id"] = chunk.get("chunk_id")
        payload["chunk_text"] = chunk.get("chunk_text", "")
        payload["chunk_hash"] = chunk.get("chunk_hash", "")
    else:
        # Fallback: use normalized_text directly
        payload["chunk_text"] = doc.get("normalized_text", doc.get("description", ""))

    return payload


# ── Point indexing ────────────────────────────────────────────────

def index_documents(
    collection_name: str,
    documents: list[dict[str, Any]],
    client: _QdrantClient | None = None,
    vector_field: str = "embeddings",
    recreate_collection: bool = False,
    batch_size: int = 64,
) -> dict[str, Any]:
    """Index documents with their embeddings into a Qdrant collection.

    Expects each document to have:
      - ``record_id`` (string) — used as the Qdrant point ID
      - ``normalized_text`` (string) — text content
      - ``embeddings`` (list[float]) — vector embedding
      - Various metadata fields for the payload

    If ``chunks`` is present on a document (array from the chunker stage),
    each chunk is indexed as a separate point.  Otherwise the full document
    is indexed as a single point.

    Args:
        collection_name: Qdrant collection name.
        documents: List of document dicts.
        client: Qdrant client (uses singleton if None).
        vector_field: Name of the embedding array field.
        recreate_collection: If True, drop and recreate before indexing.
        batch_size: Points per upsert batch.

    Returns:
        Summary dict with counts and timing.
    """
    client = client or get_qdrant_client()

    start = time.monotonic()

    # Ensure collection exists
    ensure_collection(collection_name, vector_size=VECTOR_SIZE, client=client, recreate=recreate_collection)

    points: list[models.PointStruct] = []
    docs_with_points = 0
    docs_skipped = 0
    points_total = 0
    errors = 0

    for doc in documents:
        chunks = doc.get("chunks")
        vector = doc.get(vector_field)

        if not vector and not chunks:
            # Try to get text for embedding later
            text = doc.get("normalized_text") or doc.get("description") or ""
            if not text:
                docs_skipped += 1
                continue

        doc_points: list[models.PointStruct] = []

        if chunks:
            # One point per chunk
            for chunk in chunks:
                chunk_vector = chunk.get("embedding") or vector
                if not chunk_vector:
                    continue

                chunk_id = f"{doc['record_id']}_chunk_{chunk['chunk_id']}"
                payload = _build_payload(doc, chunk)

                doc_points.append(models.PointStruct(
                    id=chunk_id,
                    vector=chunk_vector,
                    payload=payload,
                ))
        elif vector:
            # Single point per document
            payload = _build_payload(doc)
            doc_points.append(models.PointStruct(
                id=doc["record_id"],
                vector=vector,
                payload=payload,
            ))

        if not doc_points:
            docs_skipped += 1
            continue

        docs_with_points += 1
        points_total += len(doc_points)
        points.extend(doc_points)

        # Upsert in batches
        if len(points) >= batch_size:
            try:
                client.upsert(
                    collection_name=collection_name,
                    points=points,
                    wait=True,
                )
            except Exception as e:
                errors += len(points)
                logger.error("Batch upsert failed (%d points): %s", len(points), e)
            points = []

    # Final batch
    if points:
        try:
            client.upsert(
                collection_name=collection_name,
                points=points,
                wait=True,
            )
        except Exception as e:
            errors += len(points)
            logger.error("Final batch upsert failed: %s", e)

    took_ms = int((time.monotonic() - start) * 1000)

    logger.info(
        "Indexed %d docs into Qdrant '%s': %d points, %d skipped, %d errors in %dms",
        docs_with_points, collection_name, points_total, docs_skipped, errors, took_ms,
    )

    return {
        "collection": collection_name,
        "total_documents": docs_with_points,
        "points_indexed": points_total,
        "skipped": docs_skipped,
        "errors": errors,
        "took_ms": took_ms,
    }


def delete_documents(
    collection_name: str,
    doc_ids: list[str],
    client: _QdrantClient | None = None,
) -> int:
    """Delete points by their IDs from a Qdrant collection.

    Returns the count of deleted points.
    """
    client = client or get_qdrant_client()
    result = client.delete(
        collection_name=collection_name,
        points_selector=models.PointIdsList(
            points=doc_ids,
        ),
        wait=True,
    )
    logger.info("Deleted %d points from '%s'", len(doc_ids), collection_name)
    return len(doc_ids)


# ── CLI entry point ──────────────────────────────────────────────

def main() -> None:
    """CLI entry point: create collections and optionally index data."""
    logging.basicConfig(
        level=getattr(logging, os.getenv("INGESTION_LOG_LEVEL", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import argparse

    parser = argparse.ArgumentParser(description="Qdrant index manager")
    parser.add_argument("action", choices=["create", "index", "info", "delete"], default="create", nargs="?")
    parser.add_argument("--collection", help="Target collection name")
    parser.add_argument("--input", help="Path to JSON file with documents to index")
    parser.add_argument("--recreate", action="store_true", help="Recreate collection before indexing")
    args = parser.parse_args()

    client = get_qdrant_client()

    if args.action == "create":
        collections = [args.collection] if args.collection else [FACULTY_COLLECTION, COURSE_COLLECTION]
        for coll in collections:
            ensure_collection(coll, vector_size=VECTOR_SIZE, client=client)
        print(f"Collections ready: {collections}")

    elif args.action == "index":
        if not args.input:
            print("--input is required for index action")
            return
        collection = args.collection or FACULTY_COLLECTION
        with open(args.input) as f:
            docs = json.load(f)
        result = index_documents(collection, docs, client=client, recreate_collection=args.recreate)
        print(json.dumps(result, indent=2))

    elif args.action == "info":
        from university_data_platform.search.qdrant_client import collection_info
        collections = [args.collection] if args.collection else [FACULTY_COLLECTION, COURSE_COLLECTION]
        for coll in collections:
            info = collection_info(coll, client)
            if info:
                print(json.dumps(info, indent=2))

    elif args.action == "delete":
        if not args.collection:
            print("--collection is required for delete")
            return
        from university_data_platform.search.qdrant_client import delete_collection
        delete_collection(args.collection, client)
        print(f"Deleted collection: {args.collection}")


if __name__ == "__main__":
    main()
