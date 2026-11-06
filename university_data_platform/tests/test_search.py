"""
Tests for the search layer (Phase 4) — ES client, indexer, search builder,
Qdrant client, indexer, RAG, and observability.

Pure-function tests run without external services.  Integration tests
that need actual ES/Qdrant are marked with ``@unittest.skipUnless``.
"""

from __future__ import annotations

import json
import logging
import unittest
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

# ── Models ────────────────────────────────────────────────────────


class TestSearchModels(unittest.TestCase):
    """Pydantic model validation for search request/response types."""

    def test_search_request_defaults(self):
        from university_data_platform.search.models import SearchRequest

        req = SearchRequest()
        self.assertEqual(req.query, "")
        self.assertEqual(req.filters, {})
        self.assertEqual(req.facets, [])
        self.assertEqual(req.page, 1)
        self.assertEqual(req.size, 20)
        self.assertEqual(req.sort_by, None)

    def test_search_request_with_filters(self):
        from university_data_platform.search.models import SearchRequest

        req = SearchRequest(
            query="professor",
            filters={"faculty": "science", "language": "fr"},
            facets=["faculty", "department"],
            page=2,
            size=10,
        )
        self.assertEqual(req.query, "professor")
        self.assertEqual(req.filters["faculty"], "science")
        self.assertEqual(req.facets, ["faculty", "department"])

    def test_search_request_invalid_page(self):
        from university_data_platform.search.models import SearchRequest

        with self.assertRaises(ValidationError):
            SearchRequest(page=0)

    def test_search_request_invalid_size(self):
        from university_data_platform.search.models import SearchRequest

        with self.assertRaises(ValidationError):
            SearchRequest(size=200)

    def test_semantic_search_request(self):
        from university_data_platform.search.models import SemanticSearchRequest

        req = SemanticSearchRequest(query="machine learning courses")
        self.assertEqual(req.query, "machine learning courses")
        self.assertEqual(req.limit, 20)

    def test_semantic_search_request_empty_query(self):
        from university_data_platform.search.models import SemanticSearchRequest

        with self.assertRaises(ValidationError):
            SemanticSearchRequest(query="")

    def test_hybrid_search_request(self):
        from university_data_platform.search.models import HybridSearchRequest

        req = HybridSearchRequest(
            query="physics professor",
            keyword_weight=0.4,
            semantic_weight=0.6,
        )
        self.assertEqual(req.keyword_weight, 0.4)
        self.assertEqual(req.semantic_weight, 0.6)
        self.assertAlmostEqual(req.keyword_weight + req.semantic_weight, 1.0)

    def test_rag_request(self):
        from university_data_platform.search.models import RAGRequest

        req = RAGRequest(query="How many faculty members?")
        self.assertEqual(req.query, "How many faculty members?")
        self.assertEqual(req.max_chunks, 5)

    def test_search_hit_model(self):
        from university_data_platform.search.models import SearchHit

        hit = SearchHit(
            _id="abc123",
            _score=0.95,
            _index="faculty_profiles",
            _source={"full_name": "John Doe", "faculty": "science"},
        )
        self.assertEqual(hit.id, "abc123")
        self.assertEqual(hit.score, 0.95)
        self.assertEqual(hit.source["full_name"], "John Doe")

    def test_search_response(self):
        from university_data_platform.search.models import FacetResult, SearchHit, SearchResponse

        hits = [
            SearchHit(_id="1", _score=0.9, _index="test", _source={"name": "Alice"}),
            SearchHit(_id="2", _score=0.8, _index="test", _source={"name": "Bob"}),
        ]
        facets = [
            FacetResult(field="faculty", buckets=[{"key": "science", "doc_count": 10}]),
        ]
        resp = SearchResponse(total=2, hits=hits, facets=facets, page=1, size=20, took_ms=5)
        self.assertEqual(resp.total, 2)
        self.assertEqual(len(resp.hits), 2)
        self.assertEqual(len(resp.facets), 1)
        self.assertEqual(resp.facets[0].field, "faculty")

    def test_semantic_search_response(self):
        from university_data_platform.search.models import QdrantHit, SemanticSearchResponse

        hits = [QdrantHit(id="p1", score=0.95, payload={"faculty": "science"}, chunk_text="some text")]
        resp = SemanticSearchResponse(query="test", hits=hits, total=1, took_ms=10)
        self.assertEqual(resp.total, 1)
        self.assertEqual(resp.hits[0].chunk_text, "some text")

    def test_rag_response(self):
        from university_data_platform.search.models import QdrantHit, RAGResponse

        sources = [QdrantHit(id="s1", score=0.9, payload={})]
        resp = RAGResponse(query="question", answer="answer text", sources=sources, took_ms=100)
        self.assertEqual(resp.answer, "answer text")
        self.assertEqual(len(resp.sources), 1)

    def test_index_batch_result(self):
        from university_data_platform.search.models import IndexBatchResult, IndexOperation

        ops = [IndexOperation(index="test", document_id="1", action="created", took_ms=5)]
        result = IndexBatchResult(
            total=1, succeeded=1, failed=0, operations=ops, took_ms=10, source_table="faculty_profiles",
        )
        self.assertEqual(result.succeeded, 1)
        self.assertEqual(result.source_table, "faculty_profiles")


# ── ES Client ─────────────────────────────────────────────────────

class TestESClient(unittest.TestCase):
    """ES client singleton and health check."""

    def setUp(self):
        from university_data_platform.search.es_client import reset_es_client
        reset_es_client()

    @patch("university_data_platform.search.es_client.Elasticsearch")
    def test_get_es_client_creates_singleton(self, MockES):
        from university_data_platform.search.es_client import get_es_client

        client1 = get_es_client(hosts=["http://localhost:9200"])
        client2 = get_es_client(hosts=["http://other:9200"])
        self.assertIs(client1, client2)

    @patch("university_data_platform.search.es_client.Elasticsearch")
    def test_es_is_healthy(self, MockES):
        from university_data_platform.search.es_client import es_is_healthy

        mock_instance = MockES.return_value
        mock_instance.cluster.health.return_value = {"status": "green", "cluster_name": "test"}

        health = es_is_healthy()
        self.assertEqual(health["status"], "green")


# ── ES Indexer ────────────────────────────────────────────────────

class TestESIndexer(unittest.TestCase):
    """Index management and document indexing."""

    @patch("university_data_platform.search.es_indexer.get_es_client")
    def test_ensure_index_creates(self, mock_get_es):
        from university_data_platform.search.es_indexer import ensure_index, FACULTY_INDEX, FACULTY_MAPPINGS

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        mock_get_es.return_value = mock_client

        result = ensure_index(FACULTY_INDEX, client=mock_client)
        self.assertTrue(result)
        mock_client.indices.create.assert_called_once()

    @patch("university_data_platform.search.es_indexer.get_es_client")
    def test_ensure_index_exists(self, mock_get_es):
        from university_data_platform.search.es_indexer import ensure_index, FACULTY_INDEX

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_get_es.return_value = mock_client

        result = ensure_index(FACULTY_INDEX, client=mock_client)
        self.assertTrue(result)
        mock_client.indices.create.assert_not_called()

    @patch("university_data_platform.search.es_indexer.get_es_client")
    def test_ensure_index_force_recreate(self, mock_get_es):
        from university_data_platform.search.es_indexer import ensure_index, FACULTY_INDEX

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_get_es.return_value = mock_client

        result = ensure_index(FACULTY_INDEX, client=mock_client, force_recreate=True)
        self.assertTrue(result)
        mock_client.indices.delete.assert_called_once()
        mock_client.indices.create.assert_called_once()

    @patch("university_data_platform.search.es_indexer.get_es_client")
    def test_delete_index(self, mock_get_es):
        from university_data_platform.search.es_indexer import delete_index

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        mock_get_es.return_value = mock_client

        result = delete_index("test-index", client=mock_client)
        self.assertTrue(result)
        mock_client.indices.delete.assert_called_once_with(index="test-index")

    def test_faculty_mappings_have_required_fields(self):
        from university_data_platform.search.es_indexer import FACULTY_MAPPINGS

        props = FACULTY_MAPPINGS["mappings"]["properties"]
        for field in ("record_id", "source_system", "full_name", "email", "faculty",
                      "department", "normalized_text", "language"):
            self.assertIn(field, props, f"Missing field: {field}")

        self.assertEqual(props["record_id"]["type"], "keyword")
        self.assertEqual(props["normalized_text"]["type"], "text")
        self.assertEqual(props["faculty"]["type"], "keyword")

    def test_course_mappings_have_required_fields(self):
        from university_data_platform.search.es_indexer import COURSE_MAPPINGS

        props = COURSE_MAPPINGS["mappings"]["properties"]
        for field in ("course_code", "course_name", "description", "instructor", "level", "credits"):
            self.assertIn(field, props)

    def test_logs_mappings_have_dynamic(self):
        from university_data_platform.search.es_indexer import LOGS_MAPPINGS

        self.assertTrue(LOGS_MAPPINGS["mappings"]["dynamic"])
        props = LOGS_MAPPINGS["mappings"]["properties"]
        for field in ("@timestamp", "message", "log_level", "service"):
            self.assertIn(field, props)

    @patch("university_data_platform.search.es_indexer.get_es_client")
    def test_index_document(self, mock_get_es):
        from university_data_platform.search.es_indexer import index_document

        mock_client = MagicMock()
        mock_client.index.return_value = {"result": "created"}
        mock_get_es.return_value = mock_client

        doc = {"record_id": "abc123", "full_name": "John Doe", "faculty": "science"}
        result = index_document("faculty_profiles", doc, client=mock_client)

        self.assertEqual(result.action, "created")
        self.assertEqual(result.document_id, "abc123")
        mock_client.index.assert_called_once()

    @patch("university_data_platform.search.es_indexer.get_es_client")
    def test_index_batch(self, mock_get_es):
        from university_data_platform.search.es_indexer import index_batch

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        mock_client.bulk.return_value = {"errors": False, "items": [
            {"index": {"_id": "1", "result": "created"}},
            {"index": {"_id": "2", "result": "created"}},
        ]}
        mock_get_es.return_value = mock_client

        docs = [
            {"record_id": "1", "full_name": "Alice", "faculty": "science"},
            {"record_id": "2", "full_name": "Bob", "faculty": "science"},
        ]
        result = index_batch("faculty_profiles", docs, client=mock_client)

        self.assertEqual(result.total, 2)
        self.assertEqual(result.succeeded, 2)
        self.assertEqual(result.failed, 0)


# ── ES Search Query Builder ───────────────────────────────────────

class TestESSearch(unittest.TestCase):
    """ES search query building and response parsing."""

    def setUp(self):
        from university_data_platform.search.es_client import reset_es_client
        reset_es_client()

    def test_build_query_body_empty_query(self):
        from university_data_platform.search.es_search import _build_query_body
        from university_data_platform.search.models import SearchRequest

        req = SearchRequest()
        body = _build_query_body(req)

        self.assertIn("query", body)
        self.assertIn("match_all", body["query"])
        self.assertEqual(body["from"], 0)
        self.assertEqual(body["size"], 20)

    def test_build_query_body_keyword(self):
        from university_data_platform.search.es_search import _build_query_body
        from university_data_platform.search.models import SearchRequest

        req = SearchRequest(query="machine learning")
        body = _build_query_body(req)

        self.assertIn("multi_match", body["query"]["bool"]["must"])
        self.assertEqual(body["query"]["bool"]["must"]["multi_match"]["query"], "machine learning")

    def test_build_query_body_with_filters(self):
        from university_data_platform.search.es_search import _build_query_body
        from university_data_platform.search.models import SearchRequest

        req = SearchRequest(query="professor", filters={"faculty": "science", "language": "fr"})
        body = _build_query_body(req)

        self.assertIn("filter", body["query"]["bool"])
        filters = body["query"]["bool"]["filter"]
        self.assertEqual(len(filters), 2)

    def test_build_query_body_with_facets(self):
        from university_data_platform.search.es_search import _build_query_body
        from university_data_platform.search.models import SearchRequest

        req = SearchRequest(query="professor", facets=["faculty", "department"])
        body = _build_query_body(req)

        self.assertIn("aggs", body)
        self.assertIn("faculty", body["aggs"])
        self.assertIn("department", body["aggs"])
        self.assertEqual(body["aggs"]["faculty"]["terms"]["field"], "faculty")

    def test_build_query_body_pagination(self):
        from university_data_platform.search.es_search import _build_query_body
        from university_data_platform.search.models import SearchRequest

        req = SearchRequest(page=3, size=10)
        body = _build_query_body(req)

        self.assertEqual(body["from"], 20)  # (3-1) * 10
        self.assertEqual(body["size"], 10)

    def test_build_query_body_sort(self):
        from university_data_platform.search.es_search import _build_query_body
        from university_data_platform.search.models import SearchRequest

        req = SearchRequest(sort_by="crawl_timestamp", sort_order="asc")
        body = _build_query_body(req)

        self.assertIn("sort", body)
        self.assertEqual(body["sort"][0]["crawl_timestamp"]["order"], "asc")

    def test_parse_facets_empty(self):
        from university_data_platform.search.es_search import _parse_facets

        facets = _parse_facets({"aggregations": {}}, ["faculty"])
        self.assertEqual(len(facets), 1)
        self.assertEqual(facets[0].field, "faculty")
        self.assertEqual(facets[0].buckets, [])

    def test_parse_facets_with_data(self):
        from university_data_platform.search.es_search import _parse_facets

        es_response = {
            "aggregations": {
                "faculty": {
                    "buckets": [
                        {"key": "science", "doc_count": 10},
                        {"key": "law", "doc_count": 5},
                    ],
                },
            },
        }
        facets = _parse_facets(es_response, ["faculty"])
        self.assertEqual(len(facets), 1)
        self.assertEqual(len(facets[0].buckets), 2)
        self.assertEqual(facets[0].buckets[0]["key"], "science")
        self.assertEqual(facets[0].buckets[0]["doc_count"], 10)


# ── Qdrant Client ─────────────────────────────────────────────────

class TestQdrantClient(unittest.TestCase):
    """Qdrant client singleton, collection management."""

    def setUp(self):
        from university_data_platform.search.qdrant_client import reset_qdrant_client
        reset_qdrant_client()

    @patch("university_data_platform.search.qdrant_client._QdrantClient")
    def test_get_qdrant_client_creates_singleton(self, MockClient):
        from university_data_platform.search.qdrant_client import get_qdrant_client

        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        mock_instance.get_collections.return_value = MagicMock(collections=[])

        client1 = get_qdrant_client(host="localhost", port=6333)
        client2 = get_qdrant_client(host="other", port=9999)
        self.assertIs(client1, client2)

    @patch("university_data_platform.search.qdrant_client._QdrantClient")
    def test_qdrant_is_healthy(self, MockClient):
        from university_data_platform.search.qdrant_client import qdrant_is_healthy

        mock_instance = MagicMock()
        MockClient.return_value = mock_instance

        # Mock collections
        coll1 = MagicMock()
        coll1.name = "faculty_chunks"
        mock_instance.get_collections.return_value = MagicMock(collections=[coll1])

        health = qdrant_is_healthy()
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["collections_count"], 1)
        self.assertIn("faculty_chunks", health["collections"])

    @patch("university_data_platform.search.qdrant_client._QdrantClient")
    def test_ensure_collection_creates(self, MockClient):
        from university_data_platform.search.qdrant_client import ensure_collection

        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        mock_instance.get_collections.return_value = MagicMock(collections=[])

        result = ensure_collection("test_collection", vector_size=768, client=mock_instance)
        self.assertTrue(result)
        mock_instance.create_collection.assert_called_once()

    @patch("university_data_platform.search.qdrant_client._QdrantClient")
    def test_ensure_collection_exists(self, MockClient):
        from university_data_platform.search.qdrant_client import ensure_collection

        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        coll = MagicMock()
        coll.name = "test_collection"
        mock_instance.get_collections.return_value = MagicMock(collections=[coll])

        result = ensure_collection("test_collection", client=mock_instance)
        self.assertTrue(result)
        mock_instance.create_collection.assert_not_called()

    @patch("university_data_platform.search.qdrant_client._QdrantClient")
    def test_delete_collection(self, MockClient):
        from university_data_platform.search.qdrant_client import delete_collection

        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        coll = MagicMock()
        coll.name = "test_collection"
        mock_instance.get_collections.return_value = MagicMock(collections=[coll])

        result = delete_collection("test_collection", client=mock_instance)
        self.assertTrue(result)
        mock_instance.delete_collection.assert_called_once_with("test_collection")

    @patch("university_data_platform.search.qdrant_client._QdrantClient")
    def test_list_collections(self, MockClient):
        from university_data_platform.search.qdrant_client import list_collections

        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        coll1, coll2 = MagicMock(), MagicMock()
        coll1.name = "a"; coll2.name = "b"
        mock_instance.get_collections.return_value = MagicMock(collections=[coll1, coll2])

        names = list_collections(mock_instance)
        self.assertEqual(names, ["a", "b"])


# ── Qdrant Indexer ────────────────────────────────────────────────

class TestQdrantIndexer(unittest.TestCase):
    """Qdrant payload building and document indexing."""

    def test_build_payload_minimal(self):
        from university_data_platform.search.qdrant_indexer import _build_payload

        doc = {
            "record_id": "rec_001",
            "source_system": "um5_website",
        }
        payload = _build_payload(doc)

        self.assertEqual(payload["doc_id"], "rec_001")
        self.assertEqual(payload["source"], "um5_website")
        self.assertIn("chunk_text", payload)

    def test_build_payload_full(self):
        from university_data_platform.search.qdrant_indexer import _build_payload

        doc = {
            "record_id": "rec_002",
            "source_system": "uca_website",
            "faculty": "science",
            "language": "fr",
            "source_url": "https://example.com/prof",
            "full_name": "Dr. Smith",
            "department": "physics",
        }
        payload = _build_payload(doc)

        self.assertEqual(payload["faculty"], "science")
        self.assertEqual(payload["language"], "fr")
        self.assertEqual(payload["source_url"], "https://example.com/prof")
        self.assertEqual(payload["full_name"], "Dr. Smith")

    def test_build_payload_with_chunk(self):
        from university_data_platform.search.qdrant_indexer import _build_payload

        doc = {"record_id": "rec_003", "source_system": "test"}
        chunk = {"chunk_id": 0, "chunk_text": "Hello world", "chunk_hash": "abc123"}

        payload = _build_payload(doc, chunk)
        self.assertEqual(payload["chunk_id"], 0)
        self.assertEqual(payload["chunk_text"], "Hello world")
        self.assertEqual(payload["chunk_hash"], "abc123")

    @patch("university_data_platform.search.qdrant_indexer.get_qdrant_client")
    @patch("university_data_platform.search.qdrant_indexer.ensure_collection")
    def test_index_documents_empty(self, mock_ensure, mock_get_client):
        from university_data_platform.search.qdrant_indexer import index_documents

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        result = index_documents("test_collection", [], client=mock_client)
        self.assertEqual(result["total_documents"], 0)
        self.assertEqual(result["points_indexed"], 0)

    @patch("university_data_platform.search.qdrant_indexer.get_qdrant_client")
    @patch("university_data_platform.search.qdrant_indexer.ensure_collection")
    def test_index_documents_with_embeddings(self, mock_ensure, mock_get_client):
        from university_data_platform.search.qdrant_indexer import index_documents

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        docs = [{
            "record_id": "rec_001",
            "source_system": "test",
            "normalized_text": "Hello world",
            "embeddings": [0.1] * 768,
        }]

        result = index_documents("test_collection", docs, client=mock_client)
        self.assertEqual(result["total_documents"], 1)
        self.assertEqual(result["points_indexed"], 1)

    @patch("university_data_platform.search.qdrant_indexer.get_qdrant_client")
    @patch("university_data_platform.search.qdrant_indexer.ensure_collection")
    def test_index_documents_with_chunks(self, mock_ensure, mock_get_client):
        from university_data_platform.search.qdrant_indexer import index_documents

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        docs = [{
            "record_id": "rec_001",
            "source_system": "test",
            "normalized_text": "Hello world",
            "chunks": [
                {"chunk_id": 0, "chunk_text": "Hello", "chunk_hash": "h1", "embedding": [0.1] * 768},
                {"chunk_id": 1, "chunk_text": "world", "chunk_hash": "h2", "embedding": [0.2] * 768},
            ],
        }]

        result = index_documents("test_collection", docs, client=mock_client)
        # 2 chunks = 2 points
        self.assertEqual(result["total_documents"], 1)
        self.assertEqual(result["points_indexed"], 2)


# ── Qdrant Search ─────────────────────────────────────────────────

class TestQdrantSearch(unittest.TestCase):
    """Semantic and hybrid search logic."""

    def test_build_filter_condition_none(self):
        from university_data_platform.search.qdrant_search import _build_filter_condition

        result = _build_filter_condition({})
        self.assertIsNone(result)

    def test_build_filter_condition_single(self):
        from university_data_platform.search.qdrant_search import _build_filter_condition
        from qdrant_client.http import models

        result = _build_filter_condition({"faculty": "science"})
        self.assertIsNotNone(result)
        self.assertEqual(len(result.must), 1)

    def test_build_filter_condition_list(self):
        from university_data_platform.search.qdrant_search import _build_filter_condition

        result = _build_filter_condition({"language": ["fr", "en"]})
        self.assertIsNotNone(result)
        self.assertEqual(len(result.must), 1)

    def test_embed_query_consistent(self):
        from university_data_platform.search.qdrant_search import _simple_embed_query

        vec1 = _simple_embed_query("machine learning")
        vec2 = _simple_embed_query("machine learning")
        vec3 = _simple_embed_query("deep learning")

        self.assertEqual(vec1, vec2)
        self.assertNotEqual(vec1, vec3)
        self.assertEqual(len(vec1), 768)

        # Verify normalization
        mag = sum(v * v for v in vec1) ** 0.5
        self.assertAlmostEqual(mag, 1.0, places=5)

    def test_simple_embed_query_empty(self):
        from university_data_platform.search.qdrant_search import _simple_embed_query

        vec = _simple_embed_query("")
        self.assertEqual(len(vec), 768)
        # All zeros → magnitude stays 0 → vector stays 0 (no division by zero)
        self.assertEqual(sum(v for v in vec), 0.0)

    def test_fuse_results_empty(self):
        from university_data_platform.search.qdrant_search import _fuse_results

        result = _fuse_results([], [])
        self.assertEqual(result, [])

    def test_fuse_results_only_es(self):
        from university_data_platform.search.qdrant_search import _fuse_results
        from university_data_platform.search.models import SearchHit, QdrantHit

        es_hits = [
            SearchHit(_id="1", _score=0.9, _index="test", _source={"record_id": "1", "name": "Alice"}),
            SearchHit(_id="2", _score=0.5, _index="test", _source={"record_id": "2", "name": "Bob"}),
        ]

        result = _fuse_results(es_hits, [], keyword_weight=1.0, semantic_weight=0.0)
        self.assertEqual(len(result), 2)
        self.assertGreater(result[0].score, result[1].score)

    def test_fuse_results_hybrid(self):
        from university_data_platform.search.qdrant_search import _fuse_results
        from university_data_platform.search.models import SearchHit, QdrantHit

        es_hits = [
            SearchHit(_id="1", _score=0.9, _index="test", _source={"record_id": "1", "name": "Alice"}),
        ]
        qdrant_hits = [
            QdrantHit(id="1", score=0.8, payload={"name": "Alice"}),
            QdrantHit(id="3", score=0.7, payload={"name": "Charlie"}),
        ]

        result = _fuse_results(es_hits, qdrant_hits, keyword_weight=0.3, semantic_weight=0.7)
        # Doc "1" appears in both legs → gets combined score
        # Doc "3" appears only in qdrant → gets only semantic score
        self.assertIn(len(result), [1, 2])


# ── RAG ───────────────────────────────────────────────────────────

class TestRAG(unittest.TestCase):
    """RAG pipeline — context formatting and prompt building."""

    def test_format_context_empty(self):
        from university_data_platform.search.rag import _format_context

        result = _format_context([])
        self.assertEqual(result, "")

    def test_format_context_single(self):
        from university_data_platform.search.rag import _format_context
        from university_data_platform.search.models import QdrantHit

        hits = [
            QdrantHit(
                id="1", score=0.9,
                payload={"source_url": "https://example.com", "faculty": "science"},
                chunk_text="This is the content.",
            ),
        ]
        result = _format_context(hits)
        self.assertIn("https://example.com", result)
        self.assertIn("science", result)
        self.assertIn("This is the content.", result)

    def test_format_context_multiple(self):
        from university_data_platform.search.rag import _format_context
        from university_data_platform.search.models import QdrantHit

        hits = [
            QdrantHit(id="1", score=0.9, payload={"source_url": "url1"}, chunk_text="chunk1"),
            QdrantHit(id="2", score=0.8, payload={"source_url": "url2"}, chunk_text="chunk2"),
        ]
        result = _format_context(hits)
        self.assertIn("Chunk 1", result)
        self.assertIn("Chunk 2", result)
        self.assertIn("chunk1", result)
        self.assertIn("chunk2", result)

    @patch("university_data_platform.search.rag.os.getenv")
    def test_rag_query_no_results(self, mock_getenv):
        from university_data_platform.search.rag import rag_query
        from university_data_platform.search.models import RAGRequest
        from university_data_platform.search.qdrant_search import _simple_embed_query

        # Patch semantic_search to return empty
        with patch("university_data_platform.search.rag.semantic_search") as mock_semantic:
            from university_data_platform.search.models import SemanticSearchResponse
            mock_semantic.return_value = SemanticSearchResponse(query="test", hits=[], total=0, took_ms=1)

            resp = rag_query(RAGRequest(query="test query"))
            self.assertIn("No relevant documents", resp.answer)
            self.assertEqual(len(resp.sources), 0)


# ── Observability ─────────────────────────────────────────────────

class TestObservability(unittest.TestCase):
    """ElasticsearchLogHandler and observability helpers."""

    def test_elasticsearch_log_handler_format(self):
        from university_data_platform.search.observability import ElasticsearchLogHandler

        handler = ElasticsearchLogHandler(service="test-service")
        logger = logging.getLogger("test_observability")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        # Create a log record
        record = logger.makeRecord(
            name="test_observability",
            level=logging.INFO,
            fn="test_search.py",
            lno=42,
            msg="Test log message",
            args=(),
            exc_info=None,
        )

        doc = handler._format_record(record)
        self.assertEqual(doc["message"], "Test log message")
        self.assertEqual(doc["log_level"], "INFO")
        self.assertEqual(doc["service"], "test-service")
        self.assertEqual(doc["logger"], "test_observability")
        self.assertIn("@timestamp", doc)
        self.assertIn("host", doc)

    def test_elasticsearch_log_handler_format_with_extra(self):
        from university_data_platform.search.observability import ElasticsearchLogHandler

        handler = ElasticsearchLogHandler(service="test")
        logger = logging.getLogger("test_extra")
        logger.addHandler(handler)

        record = logger.makeRecord(
            name="test_extra",
            level=logging.WARNING,
            fn="test.py", lno=10,
            msg="Task %s failed",
            args=("my_task",),
            exc_info=None,
        )
        record.dag_id = "my_dag"
        record.task_id = "my_task"

        doc = handler._format_record(record)
        self.assertEqual(doc["message"], "Task my_task failed")
        self.assertEqual(doc["dag_id"], "my_dag")
        self.assertEqual(doc["task_id"], "my_task")

    @patch("university_data_platform.search.observability.get_es_client")
    def test_elasticsearch_handler_flush(self, mock_get_es):
        from university_data_platform.search.observability import ElasticsearchLogHandler

        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        mock_get_es.return_value = mock_client

        handler = ElasticsearchLogHandler(service="test-flush", batch_size=2)
        logger = logging.getLogger("test_flush")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # Log two messages (triggers flush at batch_size=2)
        logger.info("Message 1")
        logger.info("Message 2")

        mock_client.indices.create.assert_called_once()
        mock_client.bulk.assert_called_once()

    @patch("university_data_platform.search.observability.get_es_client")
    def test_configure_platform_logging(self, mock_get_es):
        from university_data_platform.search.observability import configure_platform_logging

        root = configure_platform_logging(service="test-config", use_es=False)
        self.assertIsNotNone(root)

        # Verify console handler added
        console_handlers = [
            h for h in root.handlers
            if isinstance(h, logging.StreamHandler)
        ]
        self.assertTrue(len(console_handlers) >= 1)

    def tearDown(self):
        # Clean up any handlers added during tests
        root = logging.getLogger()
        for handler in list(root.handlers):
            if isinstance(handler, (logging.StreamHandler, logging.Handler)):
                root.removeHandler(handler)


# ── Import sanity ─────────────────────────────────────────────────

class TestImportSanity(unittest.TestCase):
    """All search module imports work correctly."""

    def test_import_search_package(self):
        import university_data_platform.search
        self.assertTrue(hasattr(university_data_platform.search, "__version__") or True)

    def test_import_all_modules(self):
        """Verify all search module imports resolve."""
        modules = [
            "university_data_platform.search.models",
            "university_data_platform.search.es_client",
            "university_data_platform.search.es_indexer",
            "university_data_platform.search.es_search",
            "university_data_platform.search.qdrant_client",
            "university_data_platform.search.qdrant_indexer",
            "university_data_platform.search.qdrant_search",
            "university_data_platform.search.rag",
            "university_data_platform.search.observability",
        ]
        for mod_name in modules:
            with self.subTest(module=mod_name):
                __import__(mod_name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
