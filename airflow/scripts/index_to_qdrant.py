#!/usr/bin/env python3
"""
[DEPRECATED] Index data into Qdrant — reads from MinIO RAW zone.

⚠️  This script reads from MinIO raw-web-html directly, bypassing the Hudi
curated zone. Kept as a fallback only.

Replaced by ``index_hudi_to_qdrant.py`` which reads from Hudi curated tables
via Spark:
    docker exec spark-master spark-submit --master spark://spark-master:7077 \\
        /opt/spark/apps/airflow/scripts/index_hudi_to_qdrant.py {table}
"""

import hashlib
import json
import logging
import os
import re
import sys
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from university_data_platform.search.qdrant_client import ensure_collection, get_qdrant_client
from university_data_platform.search.qdrant_indexer import (
    FACULTY_COLLECTION,
    COURSE_COLLECTION,
    VECTOR_SIZE,
    _build_payload,
    index_documents,
)
from university_data_platform.storage.minio_client import MinIOClient

logger = logging.getLogger(__name__)


# ── Text extraction ───────────────────────────────────────────────

def extract_text_from_html(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ── Chunking ──────────────────────────────────────────────────────

def chunk_text(text: str, chunk_size: int = 512, overlap: int = 64) -> list[dict[str, Any]]:
    """Split text into overlapping word-level chunks (mirrors processing.chunker)."""
    if not text:
        return []
    words = text.split()
    chunks: list[dict[str, Any]] = []
    start = 0
    chunk_id = 0
    while start < len(words):
        end = start + chunk_size
        chunk_words = words[start:end]
        chunk_str = " ".join(chunk_words)
        chunks.append({
            "chunk_id": chunk_id,
            "chunk_text": chunk_str,
            "chunk_start": start,
            "chunk_size": len(chunk_words),
            "chunk_hash": hashlib.md5(chunk_str.encode("utf-8")).hexdigest(),
        })
        start += chunk_size - overlap
        chunk_id += 1
    return chunks


# ── Embedding generation ─────────────────────────────────────────

def compute_embeddings(texts: list[str]) -> list[list[float] | None]:
    """Compute embeddings for a list of texts.

    Tries Gemini API first, falls back to TF-IDF-like hash embeddings.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        return _gemini_embed_batch(texts, api_key)
    return [_hash_embed(t) for t in texts]


def _gemini_embed_batch(texts: list[str], api_key: str, model: str = "gemini-embedding-2") -> list[list[float] | None]:
    """Batch embedding via Gemini API."""
    import urllib.request

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:batchEmbedContents?key={api_key}"
    headers = {"Content-Type": "application/json"}
    batch_size = 100
    results: list[list[float] | None] = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        payload = json.dumps({
            "requests": [
                {"model": f"models/{model}", "content": {"parts": [{"text": t}]}}
                for t in batch
            ],
        }).encode("utf-8")

        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read())
            for emb in body.get("embeddings", []):
                results.append(emb.get("values"))
        except Exception as e:
            logger.error("Gemini batch failed at offset %d: %s", i, e)
            results.extend([None] * len(batch))

    return results


def _hash_embed(text: str, dim: int = 768) -> list[float]:
    """Simple hash-based embedding fallback (consistent hashing → sparse vector)."""
    vec = [0.0] * dim
    for word in text.lower().split():
        h = hashlib.md5(word.encode()).hexdigest()
        idx = int(h[:4], 16) % dim
        vec[idx] += 1.0
    mag = sum(v * v for v in vec) ** 0.5
    return [v / mag for v in vec] if mag > 0 else vec


# ── Document builder ──────────────────────────────────────────────

def _build_qdrant_docs(
    metas: list[dict[str, Any]],
    doc_type: str = "faculty",
) -> list[dict[str, Any]]:
    """Build Qdrant-ready documents from metadata sidecars.

    Each document gets:
      - Full payload metadata
      - Chunks with independent embeddings
    """
    docs: list[dict[str, Any]] = []

    for meta in metas:
        html_content = meta.get("raw_html", "")
        text = extract_text_from_html(html_content) if html_content else meta.get("normalized_text", "")

        if not text:
            continue

        # Create chunks
        chunks = chunk_text(text)
        if not chunks:
            continue

        # Generate embedding for each chunk
        chunk_texts = [c["chunk_text"] for c in chunks]
        embeddings = compute_embeddings(chunk_texts)

        # Attach embeddings to chunks
        for j, emb in enumerate(embeddings):
            if emb:
                chunks[j]["embedding"] = emb

        # Build the document
        doc = {
            "record_id": meta.get("record_id", ""),
            "source_system": meta.get("source_system", ""),
            "source_url": meta.get("source_url", ""),
            "faculty": meta.get("faculty", ""),
            "language": meta.get("language", "unknown"),
            "normalized_text": text,
            "chunks": [c for c in chunks if "embedding" in c],
        }

        # Type-specific fields
        if doc_type == "faculty":
            doc["full_name"] = meta.get("full_name", "")
            doc["title"] = meta.get("title", "")
            doc["department"] = meta.get("department", "")
            doc["email"] = meta.get("email", "")
        else:
            doc["course_name"] = meta.get("course_name", "")
            doc["description"] = text
            doc["level"] = meta.get("level", "")
            doc["credits"] = meta.get("credits", "")

        if doc["chunks"]:
            docs.append(doc)

    return docs


# ── MinIO reader ──────────────────────────────────────────────────

def read_metadata_from_minio(
    source_system: str | None = None,
    minio_client: MinIOClient | None = None,
) -> list[dict[str, Any]]:
    """Read metadata sidecars from MinIO raw-web-html bucket."""
    client = minio_client or MinIOClient()
    s3 = client._client
    prefix = f"source={source_system}/" if source_system else ""
    docs: list[dict[str, Any]] = []

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket="raw-web-html", Prefix=prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".meta.json"):
                continue
            try:
                resp = s3.get_object(Bucket="raw-web-html", Key=key)
                meta = json.loads(resp["Body"].read())

                # Try to fetch raw HTML content for text extraction
                raw_path = meta.get("raw_object_path", "")
                if raw_path:
                    try:
                        html_resp = s3.get_object(
                            Bucket=raw_path.split("/")[2],
                            Key="/".join(raw_path.split("/")[3:]),
                        )
                        meta["raw_html"] = html_resp["Body"].read().decode("utf-8", errors="ignore")
                    except Exception:
                        pass

                docs.append(meta)
            except Exception as e:
                logger.warning("Failed to read %s: %s", key, e)

    return docs


# ── Main indexing functions ───────────────────────────────────────

def index_faculty_to_qdrant(
    source_system: str | None = None,
    recreate: bool = False,
) -> dict[str, Any]:
    """Index faculty profile data into Qdrant."""
    client = get_qdrant_client()
    ensure_collection(FACULTY_COLLECTION, vector_size=VECTOR_SIZE, client=client, recreate=recreate)

    logger.info("Reading faculty data from MinIO...")
    metas = read_metadata_from_minio(source_system)
    logger.info("Read %d metadata records", len(metas))

    docs = _build_qdrant_docs(metas, doc_type="faculty")
    logger.info("Built %d Qdrant-ready faculty documents", len(docs))

    if not docs:
        return {"collection": FACULTY_COLLECTION, "points_indexed": 0}

    result = index_documents(FACULTY_COLLECTION, docs, client)
    return result


def index_courses_to_qdrant(
    source_system: str | None = None,
    recreate: bool = False,
) -> dict[str, Any]:
    """Index course catalog data into Qdrant."""
    client = get_qdrant_client()
    ensure_collection(COURSE_COLLECTION, vector_size=VECTOR_SIZE, client=client, recreate=recreate)

    metas = read_metadata_from_minio(source_system)
    docs = _build_qdrant_docs(metas, doc_type="course")

    if not docs:
        return {"collection": COURSE_COLLECTION, "points_indexed": 0}

    result = index_documents(COURSE_COLLECTION, docs, client)
    return result


def index_all_to_qdrant(
    source_system: str | None = None,
    recreate: bool = False,
) -> dict[str, Any]:
    """Index all data into Qdrant."""
    return {
        "faculty_profiles_chunks": index_faculty_to_qdrant(source_system, recreate),
        "course_catalog_chunks": index_courses_to_qdrant(source_system, recreate),
    }


# ── CLI entry point ───────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("INGESTION_LOG_LEVEL", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import argparse
    parser = argparse.ArgumentParser(description="Index data to Qdrant")
    parser.add_argument("--source", default=None, help="Source system filter")
    parser.add_argument("--recreate", action="store_true", help="Recreate collections")
    args = parser.parse_args()

    result = index_all_to_qdrant(args.source, args.recreate)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
