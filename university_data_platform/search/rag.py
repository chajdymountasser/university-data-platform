"""
RAG (Retrieval-Augmented Generation) pipeline.

Retrieves relevant document chunks from Qdrant, builds a context-grounded
prompt, and generates a response via the Gemini API.

Architecture
------------
  User query
      │
      ▼
  Qdrant semantic search ─── retrieve top-K chunks
      │
      ▼
  Build prompt: context + query
      │
      ▼
  Gemini API (or fallback LLM) ─── generate answer
      │
      ▼
  Structured response with answer + source citations
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from typing import Any

from university_data_platform.search.models import QdrantHit, RAGRequest, RAGResponse
from university_data_platform.search.qdrant_client import get_qdrant_client
from university_data_platform.search.qdrant_search import (
    _embed_query,
    _build_filter_condition,
    semantic_search,
    SemanticSearchRequest,
)

logger = logging.getLogger(__name__)


# ── All known Qdrant collections ─────────────────────────────────

ALL_COLLECTIONS = [
    "faculty_profiles_chunks",
    "course_catalog_chunks",
    "university_news_chunks",
    "pdf_documents_chunks",
]


# ── Defaults ──────────────────────────────────────────────────────

# Available models (Google AI Studio / Gemini API):
#   - gemma-4-31b-it    (best quality, slower)
#   - gemma-4-26b-a4b-it  (good quality, faster)
#   - gemini-2.0-flash  (fallback)
RAG_MODEL = os.getenv("RAG_MODEL", "gemma-4-31b-it")
RAG_TEMPERATURE = float(os.getenv("RAG_TEMPERATURE", "0.3"))


# ── Prompt templates ──────────────────────────────────────────────

RAG_SYSTEM_PROMPT = """You are a helpful university data assistant. Answer the user's question based ONLY on the provided context. If the context does not contain enough information to answer, say so clearly.

Cite the source documents when possible by referencing the document title or URL.

Keep your answer concise, factual, and well-structured."""

RAG_USER_TEMPLATE = """Context:
{context}

---

Question: {query}

---

Answer (based only on the context above):"""


def _format_context(hits: list[QdrantHit]) -> str:
    """Format retrieved chunks into a context block for the LLM prompt.

    Each chunk is prefixed with its source so the LLM can cite it.
    """
    parts = []
    for i, hit in enumerate(hits, 1):
        source = hit.payload.get("source_url") or hit.payload.get("doc_id", "unknown")
        faculty = hit.payload.get("faculty", "")
        name = hit.payload.get("full_name") or hit.payload.get("course_name", "")
        collection = hit.payload.get("collection", "")
        meta = f"[Source: {source}"
        if faculty:
            meta += f", Faculty: {faculty}"
        if name:
            meta += f", {name}"
        if collection:
            meta += f", Collection: {collection}"
        meta += "]"

        text = hit.chunk_text or ""
        parts.append(f"--- Chunk {i} {meta}\n{text}")

    return "\n\n".join(parts)


# ── Gemini API call ───────────────────────────────────────────────

def _call_gemini(
    prompt: str,
    model: str = RAG_MODEL,
    temperature: float = RAG_TEMPERATURE,
) -> str:
    """Call the Gemini generation API via direct HTTP.

    Returns the generated text, or an error message if the call fails.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return "RAG unavailable: GEMINI_API_KEY not set."

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={api_key}"
    )

    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": 1024,
        },
    }).encode("utf-8")

    try:
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())

        candidates = body.get("candidates", [])
        if not candidates:
            return "RAG: No response generated."

        text_parts = []
        for part in candidates[0].get("content", {}).get("parts", []):
            text_parts.append(part.get("text", ""))
        return "".join(text_parts)

    except Exception as e:
        logger.error("Gemini generation failed: %s", e)
        return f"RAG: Generation error — {e}"


# ── RAG pipeline ──────────────────────────────────────────────────

def _search_all_collections(
    query: str,
    max_chunks: int,
    filters: dict[str, Any] | None = None,
    qdrant_client: Any | None = None,
) -> list[QdrantHit]:
    """Search all known Qdrant collections and return merged, deduplicated hits.

    Requests ``per_collection_limit`` chunks from each collection, merges them,
    deduplicates by chunk ID, and returns the top ``max_chunks`` by score.
    """
    per_collection_limit = 3
    all_hits: list[QdrantHit] = []
    seen_ids: set[str] = set()

    search_request = SemanticSearchRequest(
        query=query,
        filters=filters or {},
        limit=per_collection_limit,
    )

    for collection in ALL_COLLECTIONS:
        try:
            response = semantic_search(
                collection_name=collection,
                request=search_request,
                client=qdrant_client,
            )
            for hit in response.hits:
                if hit.id not in seen_ids:
                    seen_ids.add(hit.id)
                    all_hits.append(hit)
        except Exception as e:
            logger.warning("RAG cross-collection search failed for %s: %s", collection, e)

    all_hits.sort(key=lambda h: h.score, reverse=True)
    return all_hits[:max_chunks]


def rag_query(
    request: RAGRequest,
    qdrant_client: Any | None = None,
) -> RAGResponse:
    """Run the full RAG pipeline: retrieve → prompt → generate → respond.

    Args:
        request: RAG query with filters and chunk count.
        qdrant_client: Qdrant client (uses singleton if None).

    Returns:
        RAGResponse with answer and cited sources.
    """
    start = time.monotonic()

    # 1. Retrieve relevant chunks — single collection or all
    if request.collection == "all" or not request.collection:
        hits = _search_all_collections(
            query=request.query,
            max_chunks=request.max_chunks,
            filters=request.filters,
            qdrant_client=qdrant_client,
        )
        collection_label = "all collections"
    else:
        search_request = SemanticSearchRequest(
            query=request.query,
            filters=request.filters,
            limit=request.max_chunks,
        )
        search_response = semantic_search(
            collection_name=request.collection,
            request=search_request,
            client=qdrant_client,
        )
        hits = search_response.hits
        collection_label = request.collection

    if not hits:
        return RAGResponse(
            query=request.query,
            answer="No relevant documents found to answer your question.",
            sources=[],
            model=RAG_MODEL,
            took_ms=int((time.monotonic() - start) * 1000),
            collection_label=collection_label,
        )

    # 2. Build the context-grounded prompt
    context = _format_context(hits)
    user_prompt = RAG_USER_TEMPLATE.format(context=context, query=request.query)
    full_prompt = f"{RAG_SYSTEM_PROMPT}\n\n{user_prompt}"

    # 3. Generate answer
    answer = _call_gemini(full_prompt)

    took_ms = int((time.monotonic() - start) * 1000)

    return RAGResponse(
        query=request.query,
        answer=answer,
        sources=hits,
        model=RAG_MODEL,
        took_ms=took_ms,
        collection_label=collection_label,
    )


def rag_query_simple(
    query: str,
    collection: str = "all",
    filters: dict[str, Any] | None = None,
    max_chunks: int = 10,
) -> RAGResponse:
    """Convenience shortcut for a single RAG query."""
    return rag_query(
        RAGRequest(
            query=query,
            collection=collection,
            filters=filters or {},
            max_chunks=max_chunks,
        ),
    )
