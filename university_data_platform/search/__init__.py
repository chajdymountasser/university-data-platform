"""
University Data Platform — Search & Retrieval Layer (Phase 4).

Elasticsearch
  Keyword search, faceted navigation, structured filters, observability.

Qdrant
  Semantic search, hybrid retrieval (keyword + vector), RAG pipelines.

Usage
-----
  # Search API (FastAPI)
  uvicorn university_data_platform.search.api:app --host 0.0.0.0 --port 8000

  # Indexing (standalone / Airflow)
  python -m university_data_platform.search.es_indexer
  python -m university_data_platform.search.qdrant_indexer
"""

from university_data_platform.search import es_client
from university_data_platform.search import es_indexer
from university_data_platform.search import es_search
from university_data_platform.search import models

# Qdrant and RAG modules are loaded lazily — they require optional
# dependencies (qdrant-client, google-genai) that may not be installed
# in all environments (e.g. Airflow containers).
def _lazy_import(name: str):
    import importlib
    return importlib.import_module(f"university_data_platform.search.{name}")

def __getattr__(name: str):
    lazy = {"qdrant_client", "qdrant_indexer", "qdrant_search", "rag", "observability"}
    if name in lazy:
        return _lazy_import(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "es_client",
    "es_indexer",
    "es_search",
    "qdrant_client",
    "qdrant_indexer",
    "qdrant_search",
    "rag",
    "observability",
    "models",
]
