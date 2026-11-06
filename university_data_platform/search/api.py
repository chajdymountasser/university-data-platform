"""
FastAPI application — search and retrieval API.

Exposes Elasticsearch (keyword, filters, facets, observability logs)
and Qdrant (semantic search, hybrid retrieval, RAG) as REST endpoints.

Endpoints
---------
  GET    /health                    — Service health overview
  POST   /es/{index}/search         — Elasticsearch keyword search
  POST   /es/{index}/faceted-search — Elasticsearch with facet counts
  POST   /es/logs/search            — Observability log search
  POST   /qdrant/semantic-search    — Qdrant pure semantic search
  POST   /qdrant/hybrid-search      — Combined ES + Qdrant hybrid search
  POST   /rag/query                 — RAG pipeline (retrieve → generate)
  GET    /indexes                   — List available indexes/collections
  GET    /ui                        — Web chat UI for testing
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from university_data_platform.search.es_client import es_is_healthy, get_es_client
from university_data_platform.search.es_indexer import (
    FACULTY_INDEX,
    COURSE_INDEX,
    LOGS_INDEX,
    INDEX_CONFIGS,
    ensure_all_indexes,
    ensure_dynamic_indexes,
)
from university_data_platform.search.es_search import faceted_search, search
from university_data_platform.search.models import (
    HybridSearchRequest,
    HybridSearchResponse,
    RAGRequest,
    RAGResponse,
    SearchRequest,
    SearchResponse,
    SemanticSearchRequest,
    SemanticSearchResponse,
)
from university_data_platform.search.qdrant_client import get_qdrant_client, list_collections, qdrant_is_healthy
from university_data_platform.search.qdrant_search import hybrid_search, semantic_search, FACULTY_COLLECTION
from university_data_platform.search.rag import rag_query

logger = logging.getLogger(__name__)

# ── FastAPI app ───────────────────────────────────────────────────

app = FastAPI(
    title="University Data Platform — Search API",
    description=(
        "Elasticsearch keyword search + Qdrant semantic search + RAG pipeline. "
        "Provides faceted navigation, structured filters, hybrid retrieval, "
        "and LLM-powered answers grounded in university data."
    ),
    version="4.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TimeoutMiddleware(BaseHTTPMiddleware):
    """Kill requests that exceed a timeout threshold."""

    def __init__(self, app, max_timeout: int = 30):
        super().__init__(app)
        self.max_timeout = max_timeout

    async def dispatch(self, request, call_next):
        try:
            response = await asyncio.wait_for(
                call_next(request),
                timeout=self.max_timeout,
            )
            return response
        except asyncio.TimeoutError:
            return JSONResponse(
                status_code=504,
                content={"detail": "Request timed out. Try a simpler query or reduce result size."},
            )


app.add_middleware(TimeoutMiddleware, max_timeout=30)


# ── Health ────────────────────────────────────────────────────────

@app.get("/health")
def health() -> dict[str, Any]:
    """Health check for all search-layer services."""
    # Check embedding model status
    model_loaded = False
    try:
        from university_data_platform.search.qdrant_search import _local_embed_model
        model_loaded = _local_embed_model is not None and _local_embed_model is not False
    except Exception:
        pass

    # Try to discover dynamic tables
    try:
        from university_data_platform.search.discovery import discover_tables
        dynamic = discover_tables()
    except Exception:
        dynamic = []

    return {
        "status": "ok",
        "elasticsearch": es_is_healthy(),
        "qdrant": qdrant_is_healthy(),
        "embedding_model": "loaded" if model_loaded else "not_loaded",
        "indexes": list(INDEX_CONFIGS.keys()) + dynamic,
        "collections": list_collections(),
    }


# ── Index management ──────────────────────────────────────────────

@app.get("/indexes")
def list_known_indexes() -> dict[str, Any]:
    """List available Elasticsearch indexes and Qdrant collections."""
    # Try to discover dynamic tables
    try:
        from university_data_platform.search.discovery import discover_tables
        dynamic = discover_tables()
    except Exception:
        dynamic = []

    return {
        "elasticsearch_indexes": list(INDEX_CONFIGS.keys()) + dynamic,
        "qdrant_collections": list_collections(),
    }


@app.post("/indexes/ensure")
def ensure_search_indexes() -> dict[str, Any]:
    """Create ES indexes and Qdrant collections for ALL known and discovered tables.

    Dynamic discovery via Trino ensures any new Hudi table gets indexed
    without manual configuration.
    """
    from university_data_platform.search.discovery import discover_tables, has_chunks_column, qdrant_collection_name
    from university_data_platform.search.qdrant_client import ensure_collection, get_qdrant_client
    from university_data_platform.search.qdrant_indexer import VECTOR_SIZE

    # ES: known static + dynamically discovered tables
    es_indexes = ensure_dynamic_indexes()

    # Qdrant: collections for every table with a chunks column
    qdrant = get_qdrant_client()
    qdrant_collections = []
    for table in discover_tables():
        if has_chunks_column(table):
            coll = qdrant_collection_name(table)
            ensure_collection(coll, vector_size=VECTOR_SIZE, client=qdrant)
            qdrant_collections.append(coll)

    return {
        "elasticsearch_indexes": es_indexes,
        "qdrant_collections": qdrant_collections,
    }


# ── Elasticsearch search endpoints ────────────────────────────────

@app.post("/es/{index_name}/search", response_model=SearchResponse)
def es_search(index_name: str, request: SearchRequest) -> SearchResponse:
    """Full-text keyword search against an Elasticsearch index.

    Supports:
      - Multi-field keyword search with fuzziness
      - Structured filters (faculty, language, department, ...)
      - Pagination and sorting
    """
    try:
        return search(index_name, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/es/{index_name}/faceted-search", response_model=SearchResponse)
def es_faceted_search(index_name: str, request: SearchRequest) -> SearchResponse:
    """Search with faceted aggregation counts returned alongside results.

    Pass facet field names in ``request.facets``:
      {"query": "professor", "facets": ["faculty", "language", "department"]}

    Returns results plus bucket counts for each requested facet field.
    """
    if not request.facets:
        raise HTTPException(status_code=400, detail="'facets' list is required for faceted search")
    try:
        return search(index_name, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/es/logs/search", response_model=SearchResponse)
def es_log_search(request: SearchRequest) -> SearchResponse:
    """Search platform logs stored in Elasticsearch (observability).

    Typical use: filter by ``dag_id``, ``log_level``, or ``service``
    to troubleshoot pipeline runs.
    """
    try:
        return search(LOGS_INDEX, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Qdrant semantic search endpoints ──────────────────────────────

@app.post("/qdrant/semantic-search", response_model=SemanticSearchResponse)
def qdrant_semantic_search(request: SemanticSearchRequest) -> SemanticSearchResponse:
    """Pure semantic (vector) search against Qdrant.

    The query is embedded in real-time and nearest-neighbour chunks are
    returned.  Supports payload filters and score threshold.
    """
    try:
        # Auto-select collection based on typical use
        collection = FACULTY_COLLECTION
        return semantic_search(collection, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/qdrant/semantic-search/{collection}", response_model=SemanticSearchResponse)
def qdrant_semantic_search_collection(
    collection: str,
    request: SemanticSearchRequest,
) -> SemanticSearchResponse:
    """Semantic search scoped to a specific Qdrant collection."""
    try:
        return semantic_search(collection, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/qdrant/hybrid-search", response_model=HybridSearchResponse)
def qdrant_hybrid_search(request: HybridSearchRequest) -> HybridSearchResponse:
    """Hybrid search: combines ES keyword relevance with Qdrant semantic scores.

    Uses weighted score fusion (default: 30% keyword + 70% semantic).
    Results from both engines are fused, deduplicated, and ranked.
    """
    try:
        return hybrid_search(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── RAG endpoint ──────────────────────────────────────────────────

@app.post("/rag/query", response_model=RAGResponse)
def rag_query_endpoint(request: RAGRequest) -> RAGResponse:
    """Retrieval-Augmented Generation: retrieve chunks → generate answer.

    Steps:
      1. Embed the query and search Qdrant for relevant chunks.
      2. Build a context-grounded prompt with the retrieved chunks.
      3. Call Gemma (via Gemini API) to generate an answer citing the sources.

    Models (set via ``RAG_MODEL`` env var):
      - ``gemma-4-31b-it`` (default) — best quality
      - ``gemma-4-26b-a4b-it`` — faster
      - ``gemini-2.0-flash`` — fallback

    Requires ``GEMINI_API_KEY`` to be set.
    """
    try:
        return rag_query(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Web UI ───────────────────────────────────────────────────────────

@app.get("/ui", response_class=HTMLResponse)
def web_ui() -> str:
    """Minimal chat interface for testing search and RAG endpoints."""
    ui_path = os.path.join(os.path.dirname(__file__), "ui.html")
    with open(ui_path, encoding="utf-8") as f:
        return f.read()


# ── Startup event ─────────────────────────────────────────────────

@app.on_event("startup")
def startup() -> None:
    """Verify connectivity to ES and Qdrant on startup."""
    logging.basicConfig(
        level=getattr(logging, "INGESTION_LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("Search API starting — verifying backend connections...")

    # ES
    try:
        es = get_es_client()
        if es.ping():
            ensure_all_indexes(es)
            logger.info("Elasticsearch ready")
    except Exception as e:
        logger.warning("Elasticsearch not available: %s", e)

    # Qdrant
    try:
        qdrant = get_qdrant_client()
        logger.info("Qdrant ready — collections: %s", list_collections(qdrant))
    except Exception as e:
        logger.warning("Qdrant not available: %s", e)

    # Pre-load embedding model (avoids 30-60s delay on first semantic query)
    try:
        from university_data_platform.search.qdrant_search import preload_embedding_model
        if preload_embedding_model():
            logger.info("Embedding model loaded successfully")
        else:
            logger.warning("Embedding model not loaded — semantic search will use fallback")
    except Exception as e:
        logger.warning("Could not pre-load embedding model: %s", e)
