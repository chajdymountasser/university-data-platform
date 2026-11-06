"""
Tests for the processing package — each layer independently.
Uses a local Spark session (no cluster needed).
"""

from __future__ import annotations

import os
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# ── Layer imports ───────────────────────────────────────────────
from university_data_platform.processing.parser import (
    extract_text_from_html,
    extract_name_from_title,
    parse_faculty_from_raw,
    parse_course_from_raw,
)
from university_data_platform.processing.normalizer import (
    normalize_text,
    normalize_email,
    normalize_department,
    normalize_faculty_name,
)
from university_data_platform.processing.deduplicator import (
    deduplicate_by_hash,
    deduplicate_by_entity,
)
from university_data_platform.processing.classifier import (
    classify_page,
)
from university_data_platform.processing.chunker import (
    chunk_text,
    generate_chunks,
)
from university_data_platform.processing.enricher import (
    detect_language,
    enrich_with_language,
)
from university_data_platform.processing.pdf_extractor import (
    extract_pdf_text,
    pdf_to_clean_text,
    extract_pdf_metadata,
)


class TestPureFunctions(unittest.TestCase):
    """Functions that do NOT need a SparkSession."""

    # ── Parser ─────────────────────────────────────────────────

    def test_extract_text_from_html_strips_tags(self):
        html = "<html><body><p>Hello <b>World</b></p></html>"
        self.assertEqual(extract_text_from_html(html), "Hello World")

    def test_extract_text_from_html_empty(self):
        self.assertEqual(extract_text_from_html(""), "")
        self.assertEqual(extract_text_from_html(None), "")

    def test_extract_text_from_html_removes_scripts(self):
        html = "<script>var x=1;</script><p>Content</p>"
        self.assertEqual(extract_text_from_html(html), "Content")

    def test_extract_text_from_html_entities(self):
        html = "<p>Foo&nbsp;Bar &amp; Baz</p>"
        self.assertEqual(extract_text_from_html(html), "Foo Bar & Baz")

    def test_extract_name_from_title(self):
        self.assertEqual(extract_name_from_title("Dr. John Smith"), "Dr. John Smith")
        self.assertEqual(extract_name_from_title("John Smith - UM5"), "John Smith")
        self.assertEqual(extract_name_from_title("Prof. Jane | Université"), "Prof. Jane")

    # ── Normalizer ─────────────────────────────────────────────

    def test_normalize_text(self):
        self.assertEqual(normalize_text("  Hello   World  "), "Hello World")
        self.assertEqual(normalize_text(""), "")
        self.assertEqual(normalize_text(None), "")

    def test_normalize_email_valid(self):
        self.assertEqual(normalize_email("John@Example.COM"), "john@example.com")

    def test_normalize_email_invalid(self):
        self.assertIsNone(normalize_email("not-an-email"))
        self.assertIsNone(normalize_email(""))

    def test_normalize_department(self):
        self.assertEqual(normalize_department("info"), "informatique")
        self.assertEqual(normalize_department("Physics"), "physique")

    def test_normalize_faculty_name(self):
        self.assertEqual(normalize_faculty_name("FSR - Université"), "FSR")
        self.assertEqual(normalize_faculty_name("FST"), "FST")

    # ── Chunker ────────────────────────────────────────────────

    def test_chunk_text_empty(self):
        self.assertEqual(chunk_text(""), [])
        self.assertEqual(chunk_text(None), [])

    def test_chunk_text_short(self):
        chunks = chunk_text("Hello world", chunk_size=10, overlap=2)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["chunk_text"], "Hello world")
        self.assertEqual(chunks[0]["chunk_id"], 0)
        self.assertIn("chunk_hash", chunks[0])

    def test_chunk_text_overlap(self):
        text = "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty"
        chunks = chunk_text(text, chunk_size=10, overlap=2)
        self.assertGreater(len(chunks), 1)
        # Check overlap: consecutive chunks share some words
        c1 = set(chunks[0]["chunk_text"].split())
        c2 = set(chunks[1]["chunk_text"].split())
        self.assertGreaterEqual(len(c1 & c2), 2)

    # ── Enricher ───────────────────────────────────────────────

    def test_detect_language_empty(self):
        self.assertEqual(detect_language(""), "unknown")
        self.assertEqual(detect_language(None), "unknown")

    def test_detect_language_english(self):
        # No Arabic chars, few function words → English
        text = "This is a simple test for language detection."
        self.assertEqual(detect_language(text), "en")

    def test_detect_language_french(self):
        text = "Bonjour le monde, nous sommes dans la classe."
        self.assertEqual(detect_language(text), "fr")

    def test_detect_language_arabic(self):
        text = "مرحبا بالعالم هذا اختبار"
        self.assertEqual(detect_language(text), "ar")

    # ── PDF extractor ────────────────────────────────────────────

    def test_extract_pdf_text(self):
        """Requires PyMuPDF (fitz)."""
        try:
            import fitz
        except ImportError:
            self.skipTest("PyMuPDF not installed")

        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 720), "Hello PDF World", fontsize=12)
        pdf_bytes = doc.write()
        doc.close()

        text = extract_pdf_text(pdf_bytes)
        self.assertIn("Hello PDF World", text)

    def test_extract_pdf_text_empty(self):
        self.assertEqual(extract_pdf_text(b""), "")
        self.assertEqual(extract_pdf_text(None), "")

    def test_extract_pdf_metadata(self):
        try:
            import fitz
        except ImportError:
            self.skipTest("PyMuPDF not installed")

        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 720), "Content", fontsize=12)
        doc.set_metadata({"title": "Test Doc", "author": "Test Author"})
        pdf_bytes = doc.write()
        doc.close()

        meta = extract_pdf_metadata(pdf_bytes)
        self.assertEqual(meta["title"], "Test Doc")
        self.assertEqual(meta["author"], "Test Author")
        self.assertEqual(meta["page_count"], 1)
        self.assertTrue(meta["has_text"])

    def test_pdf_to_clean_text(self):
        try:
            import fitz
        except ImportError:
            self.skipTest("PyMuPDF not installed")

        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 720), "Hello    World", fontsize=12)
        pdf_bytes = doc.write()
        doc.close()

        clean = pdf_to_clean_text(pdf_bytes)
        self.assertEqual(clean, "Hello World")

    # ── Classifier ─────────────────────────────────────────────

    def test_classify_faculty_by_url(self):
        url = "https://www.um5.ac.ma/prof/john-doe"
        result = classify_page(url, "some academic content")
        self.assertEqual(result, "faculty_profile")

    def test_classify_faculty_by_title(self):
        url = "https://www.um5.ac.ma/page1"
        result = classify_page(url, "Page personnelle du Professeur Mohamed", page_title="Profil du Professeur")
        self.assertEqual(result, "faculty_profile")

    def test_classify_faculty_by_body(self):
        url = "https://www.um5.ac.ma/staff"
        result = classify_page(url, "email: john@um5.ac.ma departement informatique")
        self.assertEqual(result, "faculty_profile")

    def test_classify_course_by_url(self):
        url = "https://www.uca.ma/course/CS101"
        result = classify_page(url, "some course content")
        self.assertEqual(result, "course_catalog")

    def test_classify_course_by_title(self):
        url = "https://www.uca.ma/page1"
        result = classify_page(url, "Cours de mathématiques", page_title="Licence en Informatique")
        self.assertEqual(result, "course_catalog")

    def test_classify_course_by_body(self):
        url = "https://www.uca.ma/programme"
        result = classify_page(url,
            "Ce module fait 6 crédits. Volume horaire: 60h. Evaluation: examen final.")
        self.assertEqual(result, "course_catalog")

    def test_classify_other(self):
        url = "https://www.uca.ma/contact"
        result = classify_page(url, "Contactez-nous au 0522-123456")
        self.assertEqual(result, "other")

    def test_classify_empty(self):
        self.assertEqual(classify_page("", ""), "other")
        self.assertEqual(classify_page(None, None), "other")

class TestSparkProcessing(unittest.TestCase):
    """Tests that require a local SparkSession."""

    spark: SparkSession = None

    @classmethod
    def setUpClass(cls):
        # Windows PySpark requires env vars with full path
        venv_python = os.path.abspath(".venv/Scripts/python.exe")
        os.environ["PYSPARK_PYTHON"] = venv_python
        os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python
        cls.spark = (
            SparkSession.builder
            .appName("test-processing")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.python.worker.reuse", "false")
            .config("spark.python.use.daemon", "false")
            .config("spark.python.connect.timeout", "120")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()

    # ── Parser (DataFrame level) ───────────────────────────────

    def test_parse_faculty_from_raw(self):
        data = [{
            "source_system": "um5_website",
            "source_url": "http://example.com/prof/john-doe",
            "content_checksum": "abc123",
            "extraction_timestamp": "2026-01-01T00:00:00Z",
            "raw_object_path": "/data/profiles/john_doe.html",
        }]
        schema = StructType([
            StructField("source_system", StringType(), True),
            StructField("source_url", StringType(), True),
            StructField("content_checksum", StringType(), True),
            StructField("extraction_timestamp", StringType(), True),
            StructField("raw_object_path", StringType(), True),
        ])
        df = self.spark.createDataFrame(data, schema)
        result = parse_faculty_from_raw(df)

        self.assertIn("record_id", result.columns)
        self.assertIn("full_name", result.columns)
        self.assertIn("crawl_timestamp", result.columns)
        self.assertIn("is_deleted", result.columns)

        row = result.collect()[0]
        self.assertEqual(row["source_system"], "um5_website")
        self.assertEqual(row["is_deleted"], "false")
        self.assertEqual(row["title"], "john_doe")
        self.assertIsNotNone(row["crawl_timestamp"])

    def test_parse_course_from_raw(self):
        data = [{
            "source_system": "um5_website",
            "source_url": "http://example.com/course/CS101",
            "content_checksum": "def456",
            "extraction_timestamp": "2026-01-01T00:00:00Z",
        }]
        schema = StructType([
            StructField("source_system", StringType(), True),
            StructField("source_url", StringType(), True),
            StructField("content_checksum", StringType(), True),
            StructField("extraction_timestamp", StringType(), True),
        ])
        df = self.spark.createDataFrame(data, schema)
        result = parse_course_from_raw(df)

        self.assertIn("course_code", result.columns)
        self.assertIn("instructor", result.columns)
        row = result.collect()[0]
        self.assertEqual(row["is_deleted"], "false")

    # ── Deduplicator (Spark level) ─────────────────────────────

    def test_deduplicate_by_hash_spark(self):
        data = [
            ("id1", "hash_a", 100),
            ("id2", "hash_a", 200),   # duplicate hash, higher ts
            ("id3", "hash_b", 150),
        ]
        df = self.spark.createDataFrame(data, ["record_id", "content_hash", "crawl_timestamp"])
        result = deduplicate_by_hash(df, hash_col="content_hash")
        rows = result.collect()
        self.assertEqual(len(rows), 2)
        ids = {r["record_id"] for r in rows}
        self.assertIn("id2", ids)  # highest ts for hash_a
        self.assertIn("id3", ids)

    def test_deduplicate_by_entity(self):
        data = [
            ("id1", "john@mail.com", None, 100),
            ("id2", None, "0000-0002-1234", 200),
            ("id3", "john@mail.com", None, 300),  # dup email, higher ts
            ("id4", "jane@mail.com", None, 250),
        ]
        df = self.spark.createDataFrame(data, ["record_id", "email", "orcid_id", "crawl_timestamp"])
        result = deduplicate_by_entity(df, entity_cols=["email", "orcid_id"])
        rows = result.collect()
        self.assertEqual(len(rows), 3)
        ids = {r["record_id"] for r in rows}
        self.assertIn("id3", ids)  # id1 dropped: same email, lower ts
        self.assertIn("id2", ids)  # only orcid_id
        self.assertIn("id4", ids)  # jane

    # ── Enricher (Spark level) ─────────────────────────────────

    def test_enrich_with_language(self):
        data = [
            ("1", "Bonjour le monde"),
            ("2", "This is English"),
        ]
        df = self.spark.createDataFrame(data, ["record_id", "normalized_text"])
        result = enrich_with_language(df)
        rows = {r["record_id"]: r["language"] for r in result.collect()}
        self.assertEqual(rows["1"], "fr")
        self.assertEqual(rows["2"], "en")

    # ── Chunker (Spark level) ──────────────────────────────────

    def test_generate_chunks(self):
        data = [
            ("1", "word " * 600),   # 600 words → multiple chunks
            ("2", "short text"),     # 2 words → 1 chunk
        ]
        df = self.spark.createDataFrame(data, ["record_id", "normalized_text"])
        result = generate_chunks(df)

        self.assertIn("chunks", result.columns)
        rows = {r["record_id"]: r["chunks"] for r in result.collect()}
        self.assertGreater(len(rows["1"]), 1)  # multiple chunks
        self.assertEqual(len(rows["2"]), 1)    # single chunk

        # Verify chunk structure
        chunk0 = rows["2"][0]
        self.assertIn("chunk_id", chunk0)
        self.assertIn("chunk_hash", chunk0)
        self.assertIn("chunk_text", chunk0)

    # ── Normalizer (Spark level) ───────────────────────────────

    def test_normalize_faculty_profiles(self):
        """Test that the normalizer import works and runs."""
        from university_data_platform.processing.normalizer import normalize_faculty_profiles
        data = [("1", "  John   Smith  ", "John@Example.COM", "  INFO  ")]
        df = self.spark.createDataFrame(data, ["record_id", "full_name", "email", "department"])
        result = normalize_faculty_profiles(df)
        row = result.collect()[0]
        self.assertEqual(row["full_name"], "John Smith")
        self.assertEqual(row["email"], "john@example.com")

    def test_normalize_course_catalog(self):
        from university_data_platform.processing.normalizer import normalize_course_catalog
        data = [("1", "  Intro   to CS  ", "  info  ")]
        df = self.spark.createDataFrame(data, ["record_id", "course_name", "department"])
        result = normalize_course_catalog(df)
        row = result.collect()[0]
        self.assertEqual(row["course_name"], "Intro to CS")


if __name__ == "__main__":
    unittest.main(verbosity=2)
