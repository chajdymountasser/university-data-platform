"""
university_data_platform.processing — Modular ETL processing layers.

Each module implements a single processing stage that can be chained
in a pipeline or called independently:

    parser       — Extract structured content from raw HTML/documents
    normalizer   — Clean, standardize, and validate fields
    deduplicator — Remove duplicate records
    enricher     — Augment data with language detection, external APIs
    chunker      — Split long text into overlapping chunks (RAG)
    embedder     — Generate vector embeddings from text
    pipeline     — Orchestrate stages into a complete processing flow
"""

from university_data_platform.processing.parser import (
    extract_text_from_html,
    extract_name_from_title,
    parse_html_content,
    parse_faculty_from_raw,
    parse_course_from_raw,
)
from university_data_platform.processing.normalizer import (
    normalize_text,
    normalize_email,
    normalize_department,
    normalize_faculty_name,
    normalize_field,
)
from university_data_platform.processing.deduplicator import (
    deduplicate_by_hash,
    deduplicate_by_content,
    deduplicate_by_entity,
)
from university_data_platform.processing.pdf_extractor import (
    extract_pdf_text,
    pdf_to_clean_text,
    extract_pdf_metadata,
)
from university_data_platform.processing.enricher import (
    detect_language,
    enrich_with_language,
    enrich_from_openalex,
)
from university_data_platform.processing.classifier import (
    classify_page,
    classify_dataframe,
)
from university_data_platform.processing.chunker import (
    chunk_text,
    generate_chunks,
)

# Embedder — lazy import, skip if deps missing (install google-genai to activate)
try:
    from university_data_platform.processing.embedder import (
        compute_tfidf_embeddings,
        compute_gemini_embeddings,
        add_embeddings_to_df,
    )
except ImportError:
    compute_tfidf_embeddings = None
    compute_gemini_embeddings = None
    add_embeddings_to_df = None
from university_data_platform.processing.pipeline import (
    FACULTY_PROFILES_SCHEMA,
    COURSE_CATALOG_SCHEMA,
    PDF_DOCUMENTS_SCHEMA,
    UNIVERSITY_NEWS_SCHEMA,
    build_faculty_profiles,
    build_course_catalog,
    build_pdf_documents,
    build_university_news,
    process_faculty_profiles,
    process_course_catalog,
    process_pdf_documents,
    process_university_news,
    run_all_pipelines,
)

# ── Also re-export I/O helpers so callers need only ``from processing import *`` ─
from university_data_platform.processing.io import (
    create_spark_session,
    ensure_curated_database,
    fetch_raw_pdf_content,
    read_raw_documents,
    read_raw_html,
    read_raw_json,
    write_to_hudi,
)

__all__ = [
    # Parser
    "extract_text_from_html",
    "extract_name_from_title",
    "parse_html_content",
    "parse_faculty_from_raw",
    "parse_course_from_raw",
    # PDF extractor
    "extract_pdf_text",
    "pdf_to_clean_text",
    "extract_pdf_metadata",
    # Normalizer
    "normalize_text",
    "normalize_email",
    "normalize_department",
    "normalize_faculty_name",
    "normalize_field",
    # Deduplicator
    "deduplicate_by_hash",
    "deduplicate_by_content",
    "deduplicate_by_entity",
    # Enricher
    "detect_language",
    "enrich_with_language",
    "enrich_from_openalex",
    # Classifier
    "classify_page",
    "classify_dataframe",
    # Chunker
    "chunk_text",
    "generate_chunks",
    # Embedder
    "compute_tfidf_embeddings",
    "compute_gemini_embeddings",
    "add_embeddings_to_df",
    # Pipeline
    "FACULTY_PROFILES_SCHEMA",
    "COURSE_CATALOG_SCHEMA",
    "PDF_DOCUMENTS_SCHEMA",
    "build_faculty_profiles",
    "build_course_catalog",
    "build_pdf_documents",
    "process_faculty_profiles",
    "process_course_catalog",
    "process_pdf_documents",
    "run_all_pipelines",
    # I/O
    "create_spark_session",
    "ensure_curated_database",
    "fetch_raw_pdf_content",
    "read_raw_documents",
    "read_raw_html",
    "read_raw_json",
    "write_to_hudi",
]
