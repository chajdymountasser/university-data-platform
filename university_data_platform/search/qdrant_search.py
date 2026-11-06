"""
Qdrant search service — semantic and hybrid (keyword + vector) retrieval.

Semantic search:
  Embeds the query into a vector and searches Qdrant for nearest-neighbour
  document chunks.

Hybrid search:
  Runs the same query against Elasticsearch (keyword leg) and Qdrant
  (semantic leg), then fuses results using reciprocal rank fusion (RRF)
  or a weighted score.

All search functions support payload-level filters (faculty, language,
source_system, etc.) so results can be scoped to a specific business context.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from qdrant_client import QdrantClient as _QdrantClient
from qdrant_client.http import models

from university_data_platform.search.es_search import keyword_search
from university_data_platform.search.models import (
    HybridSearchRequest,
    HybridSearchResponse,
    QdrantHit,
    SemanticSearchRequest,
    SemanticSearchResponse,
)
from university_data_platform.search.qdrant_client import get_qdrant_client

logger = logging.getLogger(__name__)

# ── Default collection names ──────────────────────────────────────

FACULTY_COLLECTION = os.getenv("QDRANT_COLLECTION_FACULTY", "faculty_profiles_chunks")
COURSE_COLLECTION = os.getenv("QDRANT_COLLECTION_COURSE", "course_catalog_chunks")


# ── Local embedding model (lazy-loaded) ─────────────────────────

_local_embed_model = None  # sentence-transformers model, loaded on first use

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


def preload_embedding_model() -> bool:
    """Pre-load the sentence-transformers model at startup.

    Returns True if the model loaded successfully.
    Call this from the FastAPI startup event to avoid first-query latency.
    """
    model = _get_local_embed_model()
    return model is not None


def _local_embed_query(query: str) -> list[float] | None:
    """Embed a query using the local sentence-transformers model."""
    model = _get_local_embed_model()
    if model is None:
        return None
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(model.encode, query, normalize_embeddings=True)
            embedding = future.result(timeout=10)
            return embedding.tolist()
    except concurrent.futures.TimeoutError:
        logger.warning("Local embedding timed out for query: %s", query[:50])
        return None
    except Exception as e:
        logger.warning("Local embedding failed: %s", e)
        return None


# ── Embedding helper (query → vector) ────────────────────────────

def _embed_query(query: str) -> list[float]:
    """Embed a search query into a vector.

    Uses local sentence-transformers model (multilingual, no API quota).
    Falls back to hash-based embedding if the model is unavailable.
    """
    # 1. Try local sentence-transformers
    local_vec = _local_embed_query(query)
    if local_vec:
        return local_vec

    # 2. Hash-based fallback (last resort)
    return _simple_embed_query(query)


def _gemini_embed_query(query: str, api_key: str) -> list[float]:
    """Embed a query via the Gemini embedding API."""
    import json
    import urllib.request

    model = os.getenv("GEMINI_EMBEDDING_MODEL", "gemini-embedding-2")
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:embedContent?key={api_key}"
    )
    payload = json.dumps({
        "model": f"models/{model}",
        "content": {"parts": [{"text": query}]},
        "outputDimensionality": 768,
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
        return body.get("embedding", {}).get("values", [])
    except Exception as e:
        logger.warning("Gemini query embedding failed: %s — falling back to local model", e)
        local_vec = _local_embed_query(query)
        if local_vec:
            return local_vec
        return _simple_embed_query(query)


def _simple_embed_query(query: str) -> list[float]:
    """Simple bag-of-words embedding for fallback.

    Creates a 768-dim vector where each dimension corresponds to a char-based
    hash bucket.  This is NOT a real embedding — it's a deterministic sparse
    vector that provides basic term overlap for the fallback path.
    """
    import hashlib

    vec = [0.0] * 768
    words = query.lower().split()
    for word in words:
        h = hashlib.md5(word.encode()).hexdigest()
        idx = int(h[:4], 16) % 768
        vec[idx] += 1.0

    # Normalize
    magnitude = sum(v * v for v in vec) ** 0.5
    if magnitude > 0:
        vec = [v / magnitude for v in vec]
    return vec


# ── Payload filter builder ────────────────────────────────────────

def _build_filter_condition(
    filters: dict[str, Any],
) -> models.Filter | None:
    """Convert a flat filter dict into a Qdrant Filter object.

    Example:
      {"faculty": "science", "language": ["fr", "en"]}

    becomes:
      Filter(must=[
        FieldCondition(key="faculty", match=MatchValue(value="science")),
        FieldCondition(key="language", match=MatchAny(any=["fr", "en"])),
      ])
    """
    if not filters:
        return None

    conditions: list[models.Condition] = []
    for field, value in filters.items():
        if isinstance(value, list):
            conditions.append(
                models.FieldCondition(
                    key=field,
                    match=models.MatchAny(any=value),
                )
            )
        else:
            conditions.append(
                models.FieldCondition(
                    key=field,
                    match=models.MatchValue(value=value),
                )
            )

    return models.Filter(must=conditions)


# ── Semantic search ────────────────────────────────────────────────

def semantic_search(
    collection_name: str,
    request: SemanticSearchRequest,
    client: _QdrantClient | None = None,
) -> SemanticSearchResponse:
    """Run a pure semantic (vector) search against a Qdrant collection.

    Args:
        collection_name: Qdrant collection to search.
        request: Semantic search parameters.
        client: Qdrant client (uses singleton if None).

    Returns:
        SemanticSearchResponse with ranked hits.
    """
    client = client or get_qdrant_client()
    start = time.monotonic()

    # Embed the query
    query_vector = _embed_query(request.query)

    # Build filter if provided
    query_filter = _build_filter_condition(request.filters)

    # Query Qdrant (v1.18+ API)
    query_results = client.query_points(
        collection_name=collection_name,
        query=query_vector,
        query_filter=query_filter,
        limit=request.limit,
        score_threshold=request.score_threshold,
        timeout=10,
    )
    results = query_results.points if query_results else []

    took_ms = int((time.monotonic() - start) * 1000)

    hits = [
        QdrantHit(
            id=str(hit.id),
            score=hit.score,
            payload={**(dict(hit.payload or {})), "collection": collection_name},
            chunk_text=(hit.payload or {}).get("chunk_text", ""),
        )
        for hit in results
    ]

    return SemanticSearchResponse(
        query=request.query,
        hits=hits,
        total=len(hits),
        took_ms=took_ms,
    )


# ── Hybrid search (keyword + semantic via RRF) ────────────────────

def _derive_qdrant_collection(es_index: str) -> str:
    """Derive Qdrant collection name from an ES index name.

    Convention: ``{index}_chunks`` (e.g. ``faculty_profiles`` → ``faculty_profiles_chunks``).
    Override via environment variable ``QDRANT_COLLECTION_{UPPER_INDEX}``.
    """
    import os
    env_var = f"QDRANT_COLLECTION_{es_index.upper()}"
    override = os.getenv(env_var)
    if override:
        return override
    return f"{es_index}_chunks"


def hybrid_search(
    request: HybridSearchRequest,
    es_client: Any | None = None,
    qdrant_client: _QdrantClient | None = None,
) -> HybridSearchResponse:
    """Run hybrid search: combine ES keyword scores + Qdrant semantic scores.

    Uses Reciprocal Rank Fusion (RRF) by default when keyword_weight and
    semantic_weight are equal.  When weights differ, a weighted score fusion
    is applied instead.

    The Qdrant collection is derived dynamically from ``request.es_index``
    using the convention ``{index}_chunks``.

    Args:
        request: Hybrid search parameters (includes ES index, weights).
        es_client: Elasticsearch client.
        qdrant_client: Qdrant client.

    Returns:
        HybridSearchResponse with fused results.
    """
    start = time.monotonic()

    # 1. Elasticsearch keyword leg
    es_response = keyword_search(
        index_name=request.es_index,
        query=request.query,
        filters=request.filters,
        size=request.limit,
        client=es_client,
    )

    # 2. Qdrant semantic leg (derive collection dynamically)
    qdrant_collection = _derive_qdrant_collection(request.es_index)
    semantic_request = SemanticSearchRequest(
        query=request.query,
        filters=request.filters,
        limit=request.limit,
    )
    qdrant_response = semantic_search(
        collection_name=qdrant_collection,
        request=semantic_request,
        client=qdrant_client,
    )

    # 3. Fuse results using weighted scores
    fused = _fuse_results(
        es_hits=es_response.hits,
        qdrant_hits=qdrant_response.hits,
        keyword_weight=request.keyword_weight,
        semantic_weight=request.semantic_weight,
        top_k=request.limit,
    )

    took_ms = int((time.monotonic() - start) * 1000)

    return HybridSearchResponse(
        query=request.query,
        hits=fused,
        total=len(fused),
        es_total=es_response.total,
        qdrant_total=qdrant_response.total,
        took_ms=took_ms,
    )


def _fuse_results(
    es_hits: list[Any],
    qdrant_hits: list[QdrantHit],
    keyword_weight: float = 0.3,
    semantic_weight: float = 0.7,
    top_k: int = 20,
) -> list[QdrantHit]:
    """Fuse Elasticsearch + Qdrant results using weighted scores.

    Each hit from either leg gets a normalised score (0-1), then the
    weighted sum is computed.  Results are sorted descending by the
    fused score and de-duplicated by document ID.
    """
    from collections import OrderedDict

    # Normalise scores (min-max within each leg)
    fused: dict[str, tuple[float, QdrantHit]] = {}

    if es_hits:
        es_scores = [h.score for h in es_hits]
        es_max = max(es_scores) if es_scores else 1.0
        for h in es_hits:
            doc_id = h.source.get("record_id", h.id)
            norm_score = (h.score / es_max) * keyword_weight
            chunk_text = h.source.get("normalized_text", "")
            qdrant_hit = QdrantHit(
                id=doc_id,
                score=norm_score,
                payload=h.source,
                chunk_text=chunk_text,
            )
            if doc_id not in fused or norm_score > fused[doc_id][0]:
                fused[doc_id] = (norm_score, qdrant_hit)

    if qdrant_hits:
        qdrant_scores = [h.score for h in qdrant_hits]
        qdrant_max = max(qdrant_scores) if qdrant_scores else 1.0
        for h in qdrant_hits:
            doc_id = h.id
            norm_score = (h.score / qdrant_max) * semantic_weight
            if doc_id in fused:
                existing_score = fused[doc_id][0]
                existing_hit = fused[doc_id][1]
                # For docs in both legs, the existing score is the keyword weight.
                # Add the semantic weight.
                fused[doc_id] = (existing_score + norm_score, QdrantHit(
                    id=doc_id,
                    score=existing_score + norm_score,
                    payload=existing_hit.payload,
                    chunk_text=existing_hit.chunk_text or h.chunk_text,
                ))
            else:
                fused[doc_id] = (norm_score, h)

    # Sort by fused score descending
    sorted_hits = sorted(fused.values(), key=lambda x: x[0], reverse=True)
    return [hit for _, hit in sorted_hits[:top_k]]


# ── Convenience shortcut ──────────────────────────────────────────

def search_semantic(
    collection: str = "faculty_profiles_chunks",
    query: str = "",
    filters: dict[str, Any] | None = None,
    limit: int = 20,
) -> SemanticSearchResponse:
    """Quick semantic search."""
    return semantic_search(
        collection,
        SemanticSearchRequest(
            query=query,
            filters=filters or {},
            limit=limit,
        ),
    )
