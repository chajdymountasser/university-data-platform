"""
Elasticsearch client singleton — wraps the official Python client.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


# ── Client factory ────────────────────────────────────────────────

_es_client: Elasticsearch | None = None


def get_es_client(
    hosts: str | list[str] | None = None,
    request_timeout: int = 10,
    **kwargs: Any,
) -> Elasticsearch:
    """Return a singleton Elasticsearch client.

    Args:
        hosts: One or more ES node URLs.  Falls back to
               ``ELASTICSEARCH_HOSTS`` env var (default ``http://localhost:9200``).
        request_timeout: HTTP request timeout in seconds (default 10).
    """
    global _es_client
    if _es_client is not None:
        return _es_client

    if hosts is None:
        raw = os.getenv("ELASTICSEARCH_HOSTS", "http://localhost:9200")
        hosts = [h.strip() for h in raw.split(",") if h.strip()]

    _es_client = Elasticsearch(
        hosts,
        headers={"accept": "application/vnd.elasticsearch+json; compatible-with=8"},
        request_timeout=request_timeout,
        **kwargs,
    )

    # Verify connectivity
    if not _es_client.ping(request_timeout=5):
        logger.warning("Elasticsearch not reachable at %s", hosts)
    else:
        logger.info("Connected to Elasticsearch at %s", hosts)

    return _es_client


def reset_es_client() -> None:
    """Reset the singleton (useful for testing / reconnection)."""
    global _es_client
    _es_client = None


# ── Health check ──────────────────────────────────────────────────

def es_is_healthy() -> dict[str, Any]:
    """Return cluster health info, or an error dict."""
    try:
        client = get_es_client()
        health = client.cluster.health()
        return dict(health)
    except Exception as e:
        return {"status": "unavailable", "error": str(e)}
