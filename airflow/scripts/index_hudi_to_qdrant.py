#!/usr/bin/env python3
"""
Read Hudi curated tables via Trino → index chunks to Qdrant.

Uses Trino (SQL engine with Hudi connector) to query Hudi tables
via REST API. Each document chunk becomes a separate Qdrant point
with its own vector embedding.

Tables are discovered dynamically at runtime — no hardcoded configuration.

Usage as Airflow PythonOperator:
    from airflow.scripts.index_hudi_to_qdrant import index_table_to_qdrant
    index_table_to_qdrant("faculty_profiles")

Usage as CLI:
    python index_hudi_to_qdrant.py [table_name|all]
    python index_hudi_to_qdrant.py --source_system um5_website
"""

import hashlib
import json
import logging
import os
import sys
import time
from typing import Any

import requests
from qdrant_client import QdrantClient
from qdrant_client.http import models

from university_data_platform.search.discovery import (
    _HUDI_META,
    _SKIP_COLUMNS,
    discover_tables,
    has_chunks_column,
    qdrant_collection_name,
    query_trino,
)

# Additional columns to skip in Qdrant payload only (kept for ES indexing).
# These are full-document text fields that bloat every point's payload
# (a single PDF with 10 chunks would store extracted_text 10×).
_QDRANT_SKIP_COLUMNS = frozenset({
    "extracted_text", "normalized_text",
})

logger = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────────

QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
VECTOR_SIZE = int(os.getenv("QDRANT_VECTOR_SIZE", "768"))


# ── Embedding ───────────────────────────────────────────────────────

_local_embed_model = None  # sentence-transformers model, lazy-loaded

def _get_local_embed_model():
    """Lazy-load the sentence-transformers local model."""
    global _local_embed_model
    if _local_embed_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            model_name = os.getenv("LOCAL_EMBEDDING_MODEL", "paraphrase-multilingual-mpnet-base-v2")
            # Use /tmp as cache to avoid permission issues in containers
            cache_dir = os.getenv("HF_HOME", "/tmp/hf_cache")
            os.makedirs(cache_dir, exist_ok=True)
            _local_embed_model = SentenceTransformer(model_name, cache_folder=cache_dir)
            logger.info("Loaded local embedding model: %s (cache: %s)", model_name, cache_dir)
        except Exception as e:
            logger.warning("Could not load local embedding model: %s", e)
            _local_embed_model = False  # prevent retry
    return _local_embed_model if _local_embed_model else None


def _local_embed(text: str) -> list[float] | None:
    """Embed text using the local sentence-transformers model."""
    model = _get_local_embed_model()
    if model is None:
        return None
    try:
        embedding = model.encode(text[:512], normalize_embeddings=True)
        return embedding.tolist()
    except Exception as e:
        logger.warning("Local embedding failed: %s", e)
        return None


def _hash_embed(text: str, dim: int = VECTOR_SIZE) -> list[float]:
    """Deterministic hash-based embedding fallback."""
    vec = [0.0] * dim
    for word in text.lower().split():
        h = hashlib.md5(word.encode()).hexdigest()
        idx = int(h[:4], 16) % dim
        vec[idx] += 1.0
    mag = sum(v * v for v in vec) ** 0.5
    return [v / mag for v in vec] if mag > 0 else vec


def _compute_embedding(text: str) -> list[float]:
    """Compute a vector embedding.

    Uses local sentence-transformers model (multilingual, no API quota).
    Falls back to hash-based embedding if the model is unavailable.
    """
    # 1. Try local sentence-transformers
    local_vec = _local_embed(text)
    if local_vec:
        return local_vec

    # 2. Hash-based fallback
    return _hash_embed(text)


def _gemini_embed(text: str, api_key: str) -> list[float] | None:
    """Embed a single text via the Gemini embedding API (direct HTTP)."""
    model = "gemini-embedding-2"
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:embedContent?key={api_key}"
    )
    payload = json.dumps({
        "model": f"models/{model}",
        "content": {"parts": [{"text": text[:30720]}]},
        "outputDimensionality": VECTOR_SIZE,
    }).encode("utf-8")

    try:
        req = requests.post(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        body = req.json()
        return body.get("embedding", {}).get("values")
    except Exception as e:
        logger.warning("Gemini embedding failed (falling back to local model): %s", e)
        return None


# ── Generic Qdrant point builder ────────────────────────────────────

def _build_points(
    columns: list[str],
    row: list[Any],
) -> list[models.PointStruct]:
    """Convert a Trino result row into Qdrant points (one per chunk).

    Builds a generic payload from ALL available columns — any non-Hudi,
    non-binary column is included automatically.  No hardcoded field
    names beyond the essential metadata keys.

    Each row has a ``chunks`` column containing a JSON array of
    chunk structs.  Each chunk becomes a separate point with its
    own vector embedding.
    """
    data = dict(zip(columns, row))

    # Extract chunks (JSON array from Trino)
    chunks_raw = data.get("chunks")
    if not chunks_raw:
        return []
    if isinstance(chunks_raw, str):
        chunks = json.loads(chunks_raw)
    elif isinstance(chunks_raw, list):
        chunks = chunks_raw
    else:
        return []

    record_id = data.get("record_id", "")
    if not record_id:
        return []

    # ── Build generic payload ───────────────────────────────────
    # Start with essential metadata fields always useful for filtering
    base_payload: dict[str, Any] = {
        "doc_id": record_id,
        "source": data.get("source_system", "unknown"),
    }

    # Add every non-Hudi, non-binary, non-chunk column dynamically
    for i, col in enumerate(columns):
        if col in _HUDI_META or col in _SKIP_COLUMNS or col in _QDRANT_SKIP_COLUMNS:
            continue
        if col in ("record_id", "chunks", "source_system"):
            continue  # already handled above or skip
        val = row[i]
        if val is not None:
            # Convert non-serializable types (e.g. dates) to string
            if not isinstance(val, (str, int, float, bool, list, dict)):
                base_payload[col] = str(val)
            else:
                base_payload[col] = val

    points: list[models.PointStruct] = []
    for chunk in chunks:
        # Trino returns struct arrays as positional lists
        if isinstance(chunk, list):
            chunk = dict(zip(
                ["chunk_id", "chunk_text", "chunk_start", "chunk_size", "chunk_hash"],
                chunk[:5],
            ))

        chunk_text = chunk.get("chunk_text", "")
        if not chunk_text:
            continue

        embedding = _compute_embedding(chunk_text)
        # Deterministic integer ID (u64) from string hash
        point_id = int(hashlib.md5(f"{record_id}_chunk_{chunk['chunk_id']}".encode()).hexdigest()[:16], 16)

        payload = dict(base_payload)
        payload.update({
            "chunk_id": chunk["chunk_id"],
            "chunk_text": chunk_text,
            "chunk_hash": chunk.get("chunk_hash", ""),
        })

        points.append(models.PointStruct(
            id=point_id,
            vector=embedding,
            payload=payload,
        ))

    return points


# ── Collection management ──────────────────────────────────────────

def _ensure_collection(collection: str, qdrant: QdrantClient, recreate: bool = False) -> None:
    """Ensure a Qdrant collection exists with the correct vector config."""
    if recreate:
        qdrant.recreate_collection(
            collection_name=collection,
            vectors_config=models.VectorParams(
                size=VECTOR_SIZE,
                distance=models.Distance.COSINE,
            ),
        )
        logger.info("Recreated Qdrant collection '%s'", collection)
        return

    collections = [c.name for c in qdrant.get_collections().collections]
    if collection not in collections:
        qdrant.create_collection(
            collection_name=collection,
            vectors_config=models.VectorParams(
                size=VECTOR_SIZE,
                distance=models.Distance.COSINE,
            ),
        )
        logger.info("Created Qdrant collection '%s'", collection)


# ── Indexing ────────────────────────────────────────────────────────

def index_table_to_qdrant(
    table_name: str,
    source_system: str | None = None,
    recreate: bool = False,
) -> dict[str, Any]:
    """Read a Hudi table via Trino and index its chunks to Qdrant.

    Only tables with a ``chunks`` column are indexed (discovered via
    ``has_chunks_column()``).  Tables without chunks (e.g. plain text
    tables with no chunking) are silently skipped.

    Args:
        table_name: Any Hudi curated table name (e.g. ``faculty_profiles``,
                    ``course_catalog``, ``pdf_documents``).
        source_system: Optional source filter (e.g. ``um5_website``).
        recreate: If True, drop and recreate the Qdrant collection.

    Returns:
        Summary dict with counts and timing.
    """
    start = time.monotonic()

    # Skip tables without a chunks column
    if not has_chunks_column(table_name):
        return {
            "collection": None,
            "table": table_name,
            "rows_read": 0,
            "points_indexed": 0,
            "skipped_reason": "no chunks column",
            "took_ms": int((time.monotonic() - start) * 1000),
        }

    collection = qdrant_collection_name(table_name)

    qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    _ensure_collection(collection, qdrant, recreate)

    # Read from Hudi via Trino
    sql = f"SELECT * FROM hudi.curated.{table_name}"
    if source_system:
        sql += f" WHERE source_system = '{source_system}'"

    logger.info("Trino query: %s", sql)
    columns, rows = query_trino(sql)
    logger.info("Trino returned %d rows for '%s'", len(rows), table_name)

    if not rows:
        return {
            "collection": collection,
            "table": table_name,
            "rows_read": 0,
            "points_indexed": 0,
            "took_ms": int((time.monotonic() - start) * 1000),
        }

    # Build all points (generic — works for any table with chunks)
    all_points: list[models.PointStruct] = []
    for row in rows:
        points = _build_points(columns, row)
        all_points.extend(points)

    if not all_points:
        logger.warning("No Qdrant points generated from '%s'", table_name)
        return {
            "collection": collection,
            "table": table_name,
            "rows_read": len(rows),
            "points_indexed": 0,
            "took_ms": int((time.monotonic() - start) * 1000),
        }

    # Upsert to Qdrant in small batches to avoid exceeding the 32 MB payload limit
    # (Qdrant default: 33554432 bytes).  PDF chunk_text can be very large, so
    # keep batch sizes small — 100 points of faculty_profiles fit in ~3 MB,
    # but 100 points of pdf_documents can exceed 60 MB.
    BATCH_SIZE = 20
    total_upserted = 0
    for i in range(0, len(all_points), BATCH_SIZE):
        batch = all_points[i:i + BATCH_SIZE]
        qdrant.upsert(
            collection_name=collection,
            points=batch,
            wait=True,
        )
        total_upserted += len(batch)
        logger.debug("Upserted %d/%d points to Qdrant '%s'", total_upserted, len(all_points), collection)

    took_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "Qdrant '%s': %d points from %d rows in %dms",
        collection, len(all_points), len(rows), took_ms,
    )
    return {
        "collection": collection,
        "table": table_name,
        "rows_read": len(rows),
        "points_indexed": len(all_points),
        "took_ms": took_ms,
    }


def index_all_to_qdrant(
    source_system: str | None = None,
    recreate: bool = False,
) -> dict[str, Any]:
    """Discover all Hudi tables and index chunked ones to Qdrant.

    Only tables with a ``chunks`` column get indexed.  Tables without
    chunks (e.g. plain tables that weren't chunked) are skipped with
    a ``skipped_reason``.
    """
    tables = discover_tables()
    results: dict[str, Any] = {}
    for table_name in tables:
        results[table_name] = index_table_to_qdrant(table_name, source_system, recreate)
    return {"collections": results}


# ── CLI ─────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("INGESTION_LOG_LEVEL", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    args = sys.argv[1:] if len(sys.argv) > 1 else []
    table = "all"
    source_system = None
    recreate = False
    i = 0
    while i < len(args):
        if args[i] == "--source_system" and i + 1 < len(args):
            raw = args[i + 1]
            source_system = None if raw == "all" else raw
            i += 2
        elif args[i] == "--recreate":
            recreate = True
            i += 1
        elif table == "all":
            table = args[i]
            i += 1
        else:
            i += 1

    if table == "all":
        result = index_all_to_qdrant(source_system, recreate)
    else:
        result = index_table_to_qdrant(table, source_system, recreate)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
