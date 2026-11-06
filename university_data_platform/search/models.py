"""
Pydantic models for search requests, responses, and index operations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ── Search request models ──────────────────────────────────────────

class SearchRequest(BaseModel):
    """Incoming search query with optional filters and facets."""

    query: str = Field(default="", description="Full-text search query")
    filters: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured field filters, e.g. {'faculty': 'science', 'language': 'fr'}",
    )
    facets: list[str] = Field(
        default_factory=list,
        description="List of fields to compute facet counts for, e.g. ['faculty', 'language', 'department']",
    )
    highlight: bool = Field(
        default=False,
        description="Enable ES native highlighting on text fields",
    )
    page: int = Field(default=1, ge=1, description="Page number (1-based)")
    size: int = Field(default=20, ge=1, le=100, description="Results per page")
    sort_by: str | None = Field(default=None, description="Sort field")
    sort_order: str = Field(default="desc", description="Sort direction: 'asc' or 'desc'")


class SemanticSearchRequest(BaseModel):
    """Incoming semantic / hybrid search request."""

    query: str = Field(..., min_length=1, description="Natural-language query")
    filters: dict[str, Any] = Field(
        default_factory=dict,
        description="Qdrant payload filters, e.g. {'faculty': 'science', 'language': 'fr'}",
    )
    limit: int = Field(default=20, ge=1, le=100, description="Number of results")
    score_threshold: float | None = Field(default=None, ge=0.0, le=1.0)


class HybridSearchRequest(SemanticSearchRequest):
    """Request that combines keyword (ES) and semantic (Qdrant) search."""

    es_index: str = Field(default="faculty_profiles", description="ES index for keyword leg")
    keyword_weight: float = Field(default=0.3, ge=0.0, le=1.0, description="Weight for keyword score")
    semantic_weight: float = Field(default=0.7, ge=0.0, le=1.0, description="Weight for semantic score")


class RAGRequest(BaseModel):
    """Request for a Retrieval-Augmented Generation query."""

    query: str = Field(..., min_length=1, description="User question")
    filters: dict[str, Any] = Field(default_factory=dict)
    max_chunks: int = Field(default=10, ge=1, le=30, description="Number of chunks to retrieve")
    collection: str = Field(default="all", description="Qdrant collection to search, or 'all' for cross-collection")


# ── Search response models ────────────────────────────────────────

class SearchHit(BaseModel):
    """A single Elasticsearch hit."""

    id: str = Field(alias="_id")
    score: float = Field(alias="_score")
    index: str = Field(alias="_index")
    source: dict[str, Any] = Field(alias="_source")
    highlight: dict[str, list[str]] = Field(
        default_factory=dict,
        description="ES highlight fragments per field",
    )

    class Config:
        populate_by_name = True
        allow_population_by_field_name = True


class FacetResult(BaseModel):
    """Facet counts for a single field."""

    field: str
    buckets: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of {'key': str, 'doc_count': int}",
    )


class SearchResponse(BaseModel):
    """Structured Elasticsearch search response."""

    total: int
    hits: list[SearchHit]
    facets: list[FacetResult] = Field(default_factory=list)
    page: int
    size: int
    took_ms: int


class QdrantHit(BaseModel):
    """A single Qdrant search result."""

    id: str
    score: float
    payload: dict[str, Any]
    chunk_text: str | None = None


class SemanticSearchResponse(BaseModel):
    """Semantic / hybrid search response."""

    query: str
    hits: list[QdrantHit]
    total: int
    took_ms: int


class HybridSearchResponse(BaseModel):
    """Combined keyword + semantic search response."""

    query: str
    hits: list[QdrantHit]
    total: int
    es_total: int
    qdrant_total: int
    took_ms: int


class RAGResponse(BaseModel):
    """Response from a RAG query."""

    query: str
    answer: str
    sources: list[QdrantHit] = Field(default_factory=list)
    model: str = "gemini-2.0-flash"
    took_ms: int
    collection_label: str = Field(default="", description="Collection(s) searched")


# ── Indexing models ───────────────────────────────────────────────

class IndexOperation(BaseModel):
    """Result of a single document index operation."""

    index: str
    document_id: str
    action: str  # 'created', 'updated', 'skipped'
    took_ms: int


class IndexBatchResult(BaseModel):
    """Aggregated result of a batch index operation."""

    total: int
    succeeded: int
    failed: int
    errors: list[str] = Field(default_factory=list)
    operations: list[IndexOperation] = Field(default_factory=list)
    took_ms: int
    source_table: str
