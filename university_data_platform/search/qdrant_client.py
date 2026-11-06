"""
Qdrant client singleton — wraps the official Python client.

Handles connection management, health checks, and collection lifecycle.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from qdrant_client import QdrantClient as _QdrantClient
from qdrant_client.http import models

logger = logging.getLogger(__name__)


# ── Client factory ────────────────────────────────────────────────

_qdrant_client: _QdrantClient | None = None


def get_qdrant_client(
    host: str | None = None,
    port: int | None = None,
    grpc_port: int = 6334,
    prefer_grpc: bool = False,
    timeout: int = 10,
    **kwargs: Any,
) -> _QdrantClient:
    """Return a singleton Qdrant client.

    Args:
        host: Qdrant host (default: from ``QDRANT_HOST`` env var or ``localhost``).
        port: REST port (default: from ``QDRANT_PORT`` env var or ``6333``).
        grpc_port: gRPC port (default: ``6334``).
        prefer_grpc: Use gRPC for faster operations.
        timeout: Connection/request timeout in seconds (default 10).
    """
    global _qdrant_client
    if _qdrant_client is not None:
        return _qdrant_client

    host = host or os.getenv("QDRANT_HOST", "localhost")
    port = port or int(os.getenv("QDRANT_PORT", "6333"))

    _qdrant_client = _QdrantClient(
        host=host,
        port=port,
        grpc_port=grpc_port,
        prefer_grpc=prefer_grpc,
        timeout=timeout,
        **kwargs,
    )

    # Verify connectivity
    try:
        health = _qdrant_client.get_collections()
        logger.info("Connected to Qdrant at %s:%d — %d collections", host, port, len(health.collections))
    except Exception as e:
        logger.warning("Qdrant not reachable at %s:%d: %s", host, port, e)

    return _qdrant_client


def reset_qdrant_client() -> None:
    """Reset the singleton (useful for testing / reconnection)."""
    global _qdrant_client
    _qdrant_client = None


# ── Health check ──────────────────────────────────────────────────

def qdrant_is_healthy() -> dict[str, Any]:
    """Return Qdrant health info."""
    try:
        client = get_qdrant_client()
        collections = client.get_collections()
        return {
            "status": "healthy",
            "collections_count": len(collections.collections),
            "collections": [c.name for c in collections.collections],
        }
    except Exception as e:
        return {"status": "unavailable", "error": str(e)}


# ── Collection helpers ────────────────────────────────────────────

def ensure_collection(
    collection_name: str,
    vector_size: int = 768,
    distance: models.Distance = models.Distance.COSINE,
    client: _QdrantClient | None = None,
    recreate: bool = False,
) -> bool:
    """Create a Qdrant collection if it doesn't exist.

    Args:
        collection_name: Name of the collection.
        vector_size: Embedding dimension (default 768 for Gemini embedding-2).
        distance: Distance metric (default COSINE).
        client: Qdrant client (uses singleton if None).
        recreate: If True, delete and recreate.

    Returns:
        True if the collection is ready.
    """
    client = client or get_qdrant_client()

    collections = [c.name for c in client.get_collections().collections]
    if collection_name in collections:
        if recreate:
            client.delete_collection(collection_name)
            logger.info("Deleted collection: %s (recreate)", collection_name)
        else:
            logger.debug("Collection already exists: %s", collection_name)
            return True

    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=vector_size,
            distance=distance,
        ),
    )
    logger.info(
        "Created collection: %s (vector_size=%d, distance=%s)",
        collection_name, vector_size, distance.name,
    )
    return True


def delete_collection(
    collection_name: str,
    client: _QdrantClient | None = None,
) -> bool:
    """Delete a Qdrant collection."""
    client = client or get_qdrant_client()
    collections = [c.name for c in client.get_collections().collections]
    if collection_name in collections:
        client.delete_collection(collection_name)
        logger.info("Deleted collection: %s", collection_name)
        return True
    return False


def list_collections(client: _QdrantClient | None = None) -> list[str]:
    """List all Qdrant collections."""
    client = client or get_qdrant_client()
    return [c.name for c in client.get_collections().collections]


def collection_info(
    collection_name: str,
    client: _QdrantClient | None = None,
) -> dict[str, Any] | None:
    """Get info about a collection (vector config, point count)."""
    client = client or get_qdrant_client()
    try:
        info = client.get_collection(collection_name)
        return {
            "name": collection_name,
            "status": str(info.status),
            "vectors_count": info.vectors_count,
            "points_count": info.points_count,
            "config": {
                "vector_size": info.config.params.vectors.size,
                "distance": str(info.config.params.vectors.distance),
            },
        }
    except Exception as e:
        logger.warning("Could not get collection info: %s", e)
        return None
