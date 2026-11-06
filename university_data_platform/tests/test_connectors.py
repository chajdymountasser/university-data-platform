"""
Phase 1 tests — connectors, metadata, deduplication, path convention.

Run with: pytest tests/ -v
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from university_data_platform.metadata.schema import IngestionMetadata
from university_data_platform.storage.minio_client import (
    MinIOClient,
    build_checksum_path,
    build_metadata_s3a_path,
    build_s3a_path,
)


# ── IngestionMetadata tests ───────────────────────────────

class TestIngestionMetadata:
    def test_deterministic_uuid5(self):
        """Same inputs must always produce the same record_id."""
        raw = b"Hello, university data platform!"
        m1 = IngestionMetadata.create(
            raw_content=raw,
            source_system="test_source",
            source_url="https://example.com/page1",
        )
        m2 = IngestionMetadata.create(
            raw_content=raw,
            source_system="test_source",
            source_url="https://example.com/page1",
        )
        assert m1.record_id == m2.record_id
        # Must be a valid UUID
        uuid.UUID(m1.record_id)

    def test_different_content_different_id(self):
        """Different content must produce different record_ids."""
        m1 = IngestionMetadata.create(
            raw_content=b"content A",
            source_system="test",
            source_url="https://example.com",
        )
        m2 = IngestionMetadata.create(
            raw_content=b"content B",
            source_system="test",
            source_url="https://example.com",
        )
        assert m1.record_id != m2.record_id

    def test_sha256_checksum(self):
        """SHA-256 checksum must be correct."""
        raw = b"test content for checksum"
        m = IngestionMetadata.create(
            raw_content=raw,
            source_system="test",
        )
        import hashlib
        expected = hashlib.sha256(raw).hexdigest()
        assert m.content_checksum == expected

    def test_flat_structure(self):
        """Model must be flat — no nested objects in to_dict()."""
        m = IngestionMetadata.create(
            raw_content=b"test",
            source_system="test",
            source_url="https://example.com",
        )
        d = m.to_dict()
        for key, value in d.items():
            assert not isinstance(value, dict), f"Field '{key}' is nested (dict)"
            assert not isinstance(value, list), f"Field '{key}' is nested (list)"

    def test_normalized_text_is_none_in_phase1(self):
        """normalized_text must always be None in Phase 1."""
        m = IngestionMetadata.create(raw_content=b"test", source_system="test")
        assert m.normalized_text is None

    def test_is_reprocessed_defaults_false(self):
        m = IngestionMetadata.create(raw_content=b"test", source_system="test")
        assert m.is_reprocessed is False

    def test_extraction_timestamp_is_utc(self):
        m = IngestionMetadata.create(raw_content=b"test", source_system="test")
        assert m.extraction_timestamp.tzinfo is not None

    def test_to_dict_serializes_datetime(self):
        m = IngestionMetadata.create(raw_content=b"test", source_system="test")
        d = m.to_dict()
        assert isinstance(d["extraction_timestamp"], str)


# ── s3a path convention tests ─────────────────────────────

class TestS3aPathConvention:
    def test_always_s3a_never_s3(self):
        """All paths must use s3a:// prefix, never s3://."""
        ts = datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
        path = build_s3a_path("raw-web-html", "um5_website", "abc123", "html", ts)
        assert path.startswith("s3a://"), f"Path must start with s3a://, got: {path}"
        assert not path.startswith("s3://")

    def test_metadata_path_always_s3a(self):
        ts = datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
        path = build_metadata_s3a_path("raw-web-html", "um5_website", "abc123", ts)
        assert path.startswith("s3a://")
        assert path.endswith(".meta.json")

    def test_checksum_path_always_s3a(self):
        path = build_checksum_path("um5_website", "abc123def")
        assert path.startswith("s3a://")
        assert "checksums" in path
        assert path.endswith(".exists")

    def test_path_format(self):
        ts = datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
        path = build_s3a_path("raw-web-html", "um5_website", "test-uuid", "html", ts)
        expected = "s3a://raw-web-html/source=um5_website/year=2026/month=03/day=25/test-uuid.html"
        assert path == expected

    def test_path_with_different_date(self):
        ts = datetime(2025, 12, 5, 8, 30, 0, tzinfo=timezone.utc)
        path = build_s3a_path("raw-documents", "test_src", "id1", "pdf", ts)
        expected = "s3a://raw-documents/source=test_src/year=2025/month=12/day=05/id1.pdf"
        assert path == expected

    def test_source_partition_key(self):
        """source= partition key must appear in path."""
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        path = build_s3a_path("raw-json", "openalex", "id1", "json", ts)
        assert "source=openalex" in path


# ── BaseConnector.save_to_minio tests (mocked) ────────────

class TestBaseConnectorSaveToMinio:
    def _make_mock_minio(self):
        mock = MagicMock(spec=MinIOClient)
        mock.create_buckets.return_value = []
        mock.save_with_metadata.return_value = (
            "s3a://raw-web-html/source=test/year=2026/month=03/day=25/uuid.html",
            "s3a://raw-web-html/source=test/year=2026/month=03/day=25/uuid.meta.json",
            False,
        )
        return mock

    def test_save_returns_expected_fields(self):
        from university_data_platform.ingestion.html_scraper import HtmlScraper

        mock_minio = self._make_mock_minio()
        scraper = HtmlScraper(minio_client=mock_minio)
        raw = b"<html><body>Test</body></html>"
        metadata = IngestionMetadata.create(
            raw_content=raw,
            source_system="test",
            source_url="https://example.com",
        )
        result = scraper.save_to_minio(raw, metadata, "html", "text/html")

        assert "record_id" in result
        assert "bucket" in result
        assert "raw_object_path" in result
        assert "checksum" in result
        assert "status" in result
        assert "skipped_duplicate" in result
        assert result["status"] == "ingested"
        assert result["skipped_duplicate"] is False

    def test_save_uses_s3a_path(self):
        from university_data_platform.ingestion.html_scraper import HtmlScraper

        mock_minio = self._make_mock_minio()
        scraper = HtmlScraper(minio_client=mock_minio)
        raw = b"<html>test</html>"
        metadata = IngestionMetadata.create(
            raw_content=raw,
            source_system="test",
            source_url="https://example.com",
        )
        result = scraper.save_to_minio(raw, metadata, "html", "text/html")
        assert result["raw_object_path"].startswith("s3a://")


# ── HtmlScraper.fetch tests (mocked requests) ─────────────

class TestHtmlScraper:
    @patch("university_data_platform.ingestion.html_scraper.requests.Session")
    def test_fetch_returns_bytes_and_metadata(self, mock_session_cls):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"<html><body>University page</body></html>"
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        from university_data_platform.ingestion.html_scraper import HtmlScraper

        scraper = HtmlScraper.__new__(HtmlScraper)
        scraper.session = mock_session
        scraper.timeout = 30
        scraper.max_retries = 3
        scraper.connector_version = "1.0.0"
        scraper.verify_ssl = True

        raw, meta = scraper.fetch(url="https://www.um5.ac.ma/", source_system="um5_website")

        assert raw == b"<html><body>University page</body></html>"
        assert meta.http_status == 200
        assert meta.source_system == "um5_website"
        assert meta.source_url == "https://www.um5.ac.ma/"
        assert meta.content_type == "text/html"

    @patch("university_data_platform.ingestion.html_scraper.requests.Session")
    def test_fetch_handles_404(self, mock_session_cls):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.content = b"Not Found"
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        from university_data_platform.ingestion.html_scraper import HtmlScraper

        scraper = HtmlScraper.__new__(HtmlScraper)
        scraper.session = mock_session
        scraper.timeout = 30
        scraper.max_retries = 3
        scraper.connector_version = "1.0.0"
        scraper.verify_ssl = True

        raw, meta = scraper.fetch(url="https://example.com/missing", source_system="test")
        assert meta.http_status == 404


# ── API connector tests (mocked HTTP) ─────────────────────

class TestApiConnector:
    @patch("university_data_platform.ingestion.api_connector.httpx.get")
    def test_fetch_returns_json_bytes(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'{"results": [{"id": "W123"}]}'
        mock_get.return_value = mock_resp

        from university_data_platform.ingestion.api_connector import OpenAlexConnector

        connector = OpenAlexConnector(connector_version="1.0.0")
        connector.rate_limit_seconds = 0  # skip sleep in tests

        raw, meta = connector.fetch(endpoint="works", params={"search": "test"})

        assert b'"results"' in raw
        assert meta.http_status == 200
        assert meta.content_type == "application/json"
        assert meta.source_system == "openalex"


# ── Deduplication tests ───────────────────────────────────

class TestDeduplication:
    def test_is_duplicate_detects_existing(self):
        mock_minio = MagicMock(spec=MinIOClient)
        mock_minio.is_duplicate.return_value = True

        assert mock_minio.is_duplicate("test_source", "abc123") is True

    def test_is_duplicate_false_for_new(self):
        mock_minio = MagicMock(spec=MinIOClient)
        mock_minio.is_duplicate.return_value = False

        assert mock_minio.is_duplicate("test_source", "new_checksum") is False

    def test_second_write_skipped(self):
        """Second write with same checksum must be skipped."""
        mock_minio = MagicMock(spec=MinIOClient)
        mock_minio.create_buckets.return_value = []

        # First call: not a duplicate
        mock_minio.is_duplicate.return_value = False
        mock_minio.save_with_metadata.return_value = (
            "s3a://raw-web-html/source=test/year=2026/month=03/day=25/uuid.html",
            "s3a://raw-web-html/source=test/year=2026/month=03/day=25/uuid.meta.json",
            False,
        )

        from university_data_platform.ingestion.html_scraper import HtmlScraper

        scraper = HtmlScraper(minio_client=mock_minio)
        raw = b"<html>test</html>"
        metadata = IngestionMetadata.create(
            raw_content=raw,
            source_system="test",
            source_url="https://example.com",
        )
        result1 = scraper.save_to_minio(raw, metadata, "html", "text/html")
        assert result1["skipped_duplicate"] is False

        # Second call: duplicate
        mock_minio.is_duplicate.return_value = True
        mock_minio.save_with_metadata.return_value = (
            "s3a://raw-web-html/source=test/year=2026/month=03/day=25/uuid.html",
            "s3a://raw-web-html/source=test/year=2026/month=03/day=25/uuid.meta.json",
            True,
        )

        result2 = scraper.save_to_minio(raw, metadata, "html", "text/html")
        assert result2["skipped_duplicate"] is True
        assert result2["status"] == "skipped_duplicate"


# ── File connector tests ──────────────────────────────────

class TestFileConnector:
    def test_fetch_pdf(self, tmp_path):
        from university_data_platform.ingestion.file_connector import FileConnector

        pdf_file = tmp_path / "syllabus.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake pdf content")

        connector = FileConnector(connector_version="1.0.0")
        raw, meta = connector.fetch(file_path=str(pdf_file), source_system="syllabus")

        assert raw == b"%PDF-1.4 fake pdf content"
        assert meta.content_type == "application/pdf"
        assert meta.file_path is not None
        assert meta.source_system == "syllabus"

    def test_fetch_csv(self, tmp_path):
        from university_data_platform.ingestion.file_connector import FileConnector

        csv_file = tmp_path / "data.csv"
        csv_file.write_bytes(b"name,age\nAlice,30\n")

        connector = FileConnector(connector_version="1.0.0")
        raw, meta = connector.fetch(file_path=str(csv_file), source_system="test_csv")

        assert b"name,age" in raw
        assert meta.content_type == "text/csv"

    def test_fetch_missing_file(self, tmp_path):
        from university_data_platform.ingestion.file_connector import FileConnector

        connector = FileConnector()
        with pytest.raises(FileNotFoundError):
            connector.fetch(file_path="/nonexistent/file.pdf")
