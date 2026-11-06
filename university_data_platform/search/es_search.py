"""
Elasticsearch search service — keyword query, structured filters, faceted navigation.

Designed to complement semantic search (Qdrant) so both can be used together
for hybrid retrieval in the RAG pipeline.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from elasticsearch import Elasticsearch

from university_data_platform.search.es_client import get_es_client
from university_data_platform.search.models import (
    FacetResult,
    SearchHit,
    SearchRequest,
    SearchResponse,
)

logger = logging.getLogger(__name__)


# ── Core search ───────────────────────────────────────────────────

def search(
    index_name: str,
    request: SearchRequest,
    client: Elasticsearch | None = None,
) -> SearchResponse:
    """Execute a keyword search with optional filters and facets.

    Args:
        index_name: Target ES index (e.g. ``faculty_profiles``).
        request: Structured search request with query, filters, facets, paging.
        client: ES client (uses singleton if None).

    Returns:
        SearchResponse with hits, facets, and metadata.
    """
    client = client or get_es_client()
    start = time.monotonic()

    body = _build_query_body(request)
    es_response = client.search(index=index_name, body=body, timeout="10s")

    took_ms = int((time.monotonic() - start) * 1000)

    # Parse hits
    hits_raw = es_response.get("hits", {})
    total = hits_raw.get("total", {}).get("value", 0)
    hits = [
        SearchHit(
            _id=h["_id"],
            _score=h.get("_score", 0.0),
            _index=h.get("_index", index_name),
            _source=h.get("_source", {}),
            highlight=h.get("highlight", {}),
        )
        for h in hits_raw.get("hits", [])
    ]

    # Parse facets
    facets = _parse_facets(es_response, request.facets)

    return SearchResponse(
        total=total,
        hits=hits,
        facets=facets,
        page=request.page,
        size=request.size,
        took_ms=took_ms,
    )


def search_all(
    request: SearchRequest,
    indexes: list[str] | None = None,
    client: Elasticsearch | None = None,
) -> dict[str, SearchResponse]:
    """Search across multiple indexes and return per-index results.

    Args:
        request: Search request (same query applied to each index).
        indexes: List of index names (defaults to all known indexes).
        client: ES client.

    Returns:
        Dict mapping index name -> SearchResponse.
    """
    if indexes is None:
        from university_data_platform.search.es_indexer import INDEX_CONFIGS
        indexes = list(INDEX_CONFIGS.keys())

    results: dict[str, SearchResponse] = {}
    for idx in indexes:
        try:
            results[idx] = search(idx, request, client)
        except Exception as e:
            logger.warning("Search failed on index %s: %s", idx, e)
            results[idx] = SearchResponse(total=0, hits=[], page=request.page, size=request.size, took_ms=0)
    return results


# ── Query builder ─────────────────────────────────────────────────

def _build_query_body(request: SearchRequest) -> dict[str, Any]:
    """Build the ES query DSL from a SearchRequest.

    Combines:
      - ``query_string`` for the keyword query (or ``match_all`` if empty).
      - ``filter`` for structured field filters.
      - ``aggs`` for requested facet fields.
    """
    body: dict[str, Any] = {
        "from": (request.page - 1) * request.size,
        "size": request.size,
    }

    # ── Query ──────────────────────────────────────────────────
    if request.query.strip():
        body["query"] = {
            "bool": {
                "must": {
                    "multi_match": {
                        "query": request.query.strip(),
                        "fields": [
                            # ── Tier 1: French-analyzed text (stemming + stopwords) ──
                            "normalized_text.french^4",
                            "description.french^3",

                            # ── Tier 2: High-value structured fields ──
                            "full_name^3",
                            "title^3",
                            "course_name^3",

                            # ── Tier 3: Standard analyzer (fallback) ──
                            "normalized_text^2",
                            "description^2",

                            # ── Tier 4: Keyword-adjacent fields ──
                            "department.text",
                            "faculty.text",
                            "instructor.text",
                        ],
                        "type": "best_fields",
                        "fuzziness": "1",
                        "prefix_length": 2,
                    }
                },
            }
        }
    else:
        body["query"] = {"match_all": {}}

    # ── Filters ────────────────────────────────────────────────
    if request.filters:
        filters = []
        for field, value in request.filters.items():
            if isinstance(value, list):
                filters.append({"terms": {field: value}})
            else:
                filters.append({"term": {field: value}})

        if filters:
            # Wrap into a filtered bool query
            bool_q = body["query"] if "query" in body else {"match_all": {}}
            body["query"] = {
                "bool": {
                    "must": bool_q,
                    "filter": filters,
                }
            }

    # ── Sorting ────────────────────────────────────────────────
    if request.sort_by:
        order = "asc" if request.sort_order == "asc" else "desc"
        body["sort"] = [{request.sort_by: {"order": order}}]

    # ── Aggregations (facets) ──────────────────────────────────
    if request.facets:
        aggs = {}
        for field in request.facets:
            aggs[field] = {
                "terms": {
                    "field": field,
                    "size": 50,
                    "order": {"_count": "desc"},
                }
            }
        body["aggs"] = aggs

    # ── Highlighting ────────────────────────────────────────────
    if request.highlight and request.query.strip():
        body["highlight"] = {
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
            "fields": {
                "normalized_text": {
                    "number_of_fragments": 3,
                    "fragment_size": 300,
                },
                "normalized_text.french": {
                    "number_of_fragments": 3,
                    "fragment_size": 300,
                },
                "title": {},
                "full_name": {},
                "course_name": {},
                "description": {
                    "number_of_fragments": 3,
                    "fragment_size": 300,
                },
                "description.french": {
                    "number_of_fragments": 3,
                    "fragment_size": 300,
                },
            },
        }

    return body


def _parse_facets(
    es_response: dict[str, Any],
    requested_facets: list[str],
) -> list[FacetResult]:
    """Parse ES aggregation results into FacetResult objects."""
    aggs = es_response.get("aggregations", {})
    results: list[FacetResult] = []

    for field in requested_facets:
        bucket_data = aggs.get(field, {})
        buckets = [
            {"key": b.get("key"), "doc_count": b.get("doc_count", 0)}
            for b in bucket_data.get("buckets", [])
        ]
        results.append(FacetResult(field=field, buckets=buckets))

    return results


# ── Convenience shortcuts ─────────────────────────────────────────

def keyword_search(
    index_name: str,
    query: str,
    filters: dict[str, Any] | None = None,
    page: int = 1,
    size: int = 20,
    client: Elasticsearch | None = None,
) -> SearchResponse:
    """Quick keyword search shortcut."""
    return search(
        index_name,
        SearchRequest(query=query, filters=filters or {}, page=page, size=size),
        client,
    )


def faceted_search(
    index_name: str,
    query: str,
    facets: list[str],
    filters: dict[str, Any] | None = None,
    page: int = 1,
    size: int = 20,
    client: Elasticsearch | None = None,
) -> SearchResponse:
    """Search with faceted counts returned alongside results."""
    return search(
        index_name,
        SearchRequest(query=query, filters=filters or {}, facets=facets, page=page, size=size),
        client,
    )
