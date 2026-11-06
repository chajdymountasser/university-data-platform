"""
processing.chunker — Split long text into overlapping chunks for RAG.

Each chunk carries metadata (position, size, hash) so that downstream
embedders and vector stores can reference the original source span.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

# ── Tunable constants ───────────────────────────────────────────

CHUNK_SIZE = 512          # words per chunk
CHUNK_OVERLAP = 64        # overlapping words between consecutive chunks
MAX_CHARS_PER_CHUNK = 0   # 0 = no character limit; > 0 overrides word-based

CHUNK_SCHEMA = ArrayType(StructType([
    StructField("chunk_id", LongType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("chunk_start", LongType(), False),
    StructField("chunk_size", LongType(), False),
    StructField("chunk_hash", StringType(), False),
]))


# ── Pure chunking logic (UDF-ready) ─────────────────────────────

def chunk_text(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> list[dict[str, Any]]:
    """Split *text* into overlapping word-level chunks.

    Returns a list of dicts — each entry is one chunk with position,
    word-count, content, and an MD5 hash for dedup / lookup.
    """
    if not text:
        return []

    words = text.split()
    # Use character-based chunking when *max_chars_per_chunk* is set
    if MAX_CHARS_PER_CHUNK > 0:
        return _chunk_by_chars(text)

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
            "chunk_hash": hashlib.md5(
                chunk_str.encode("utf-8")
            ).hexdigest(),
        })
        start += chunk_size - overlap
        chunk_id += 1
    return chunks


def _chunk_by_chars(text: str) -> list[dict[str, Any]]:
    """Alternate chunking strategy: fixed-size character windows."""
    chunks: list[dict[str, Any]] = []
    start = 0
    chunk_id = 0
    stride = MAX_CHARS_PER_CHUNK - CHUNK_OVERLAP
    while start < len(text):
        chunk_str = text[start:start + MAX_CHARS_PER_CHUNK]
        if not chunk_str:
            break
        chunks.append({
            "chunk_id": chunk_id,
            "chunk_text": chunk_str,
            "chunk_start": start,
            "chunk_size": len(chunk_str),
            "chunk_hash": hashlib.md5(
                chunk_str.encode("utf-8")
            ).hexdigest(),
        })
        start += stride
        chunk_id += 1
    return chunks


# ── DataFrame-level chunking step ───────────────────────────────

def generate_chunks(
    df: DataFrame,
    text_column: str = "normalized_text",
) -> DataFrame:
    """Generate chunk arrays from a text column and store in *chunks*.

    The resulting ``chunks`` column is an array of :attr:`CHUNK_SCHEMA`
    structs that the embedder stage can flatten and vectorise.
    """
    chunk_udf = F.udf(
        lambda text: chunk_text(text) if text else [],
        CHUNK_SCHEMA,
    )
    return df.withColumn("chunks", chunk_udf(F.col(text_column)))
