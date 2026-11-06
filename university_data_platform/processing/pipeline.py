"""
processing.pipeline — Orchestrate processing stages into ETL pipelines.

Each public ``build_*`` function runs the full chain:

    parse → normalize → deduplicate → enrich → chunk → embed

while providing hooks to skip or parametrise individual stages.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from university_data_platform.processing.chunker import generate_chunks
from university_data_platform.processing.classifier import (
    classify_page_scores,
    classify_page_semantic_scores,
    combine_semantic_and_regex,
)
from university_data_platform.processing.pdf_extractor import pdf_to_clean_text
from university_data_platform.processing.deduplicator import (
    deduplicate_by_hash,
)
from university_data_platform.processing.enricher import (
    enrich_from_openalex,
    enrich_with_language,
)
from university_data_platform.processing.io import (
    ensure_curated_database,
    fetch_raw_html_content,
    fetch_raw_pdf_content,
    read_raw_documents,
    read_raw_html,
    read_raw_json,
    write_to_hudi,
)
from university_data_platform.processing.normalizer import (
    normalize_course_catalog,
    normalize_faculty_profiles,
    normalize_university_news,
)
from university_data_platform.processing.parser import (
    extract_emails_from_text,
    parse_course_from_raw,
    parse_faculty_from_raw,
    parse_html_content,
)

logger = logging.getLogger(__name__)


# ── Multi-signal classifier (semantic + regex) ───────────────────

def _combined_classify(
    df: DataFrame,
    target_type: str,
    title_col: str = "title",
) -> DataFrame:
    """Multi-signal classifier: semantic anchors (primary) + regex (correction).

    Strategy:
      1. Semantic embeds text → label + confidence scores
      2. Regex checks URL patterns + keywords → label + confidence scores
      3. combine_semantic_and_regex() merges both:
         - Both agree → high confidence
         - Semantic uncertain → regex helps
         - Regex very confident for different class → regex corrects
         - Semantic wins in ambiguous cases (content > URL)
    """
    # 1. Semantic scores (adds columns: sem_label, sem_faculty, sem_course, sem_news)
    sem_udf = F.udf(
        lambda url, text, title: classify_page_semantic_scores(url or "", text or "", title or ""),
        StructType([
            StructField("label", StringType()),
            StructField("faculty_score", StringType()),
            StructField("course_score", StringType()),
            StructField("news_score", StringType()),
            StructField("other_score", StringType()),
        ]),
    )
    title_expr = F.col(title_col) if title_col in df.columns else F.lit("")
    df = df.withColumn("_sem_result", sem_udf(F.col("source_url"), F.col("normalized_text"), title_expr))
    df = df.withColumn("sem_label", F.col("_sem_result.label"))
    df = df.withColumn("sem_faculty", F.col("_sem_result.faculty_score").cast("double"))
    df = df.withColumn("sem_course", F.col("_sem_result.course_score").cast("double"))
    df = df.withColumn("sem_news", F.col("_sem_result.news_score").cast("double"))

    # 2. Regex scores (adds columns: regex_label, regex_faculty, regex_course, regex_news)
    regex_udf = F.udf(
        lambda url, text, title: classify_page_scores(url or "", text or "", title or ""),
        StructType([
            StructField("label", StringType()),
            StructField("faculty_score", StringType()),
            StructField("course_score", StringType()),
            StructField("news_score", StringType()),
            StructField("other_score", StringType()),
        ]),
    )
    df = df.withColumn("_regex_result", regex_udf(F.col("source_url"), F.col("normalized_text"), title_expr))
    df = df.withColumn("regex_label", F.col("_regex_result.label"))
    df = df.withColumn("regex_faculty", F.col("_regex_result.faculty_score").cast("double"))
    df = df.withColumn("regex_course", F.col("_regex_result.course_score").cast("double"))
    df = df.withColumn("regex_news", F.col("_regex_result.news_score").cast("double"))

    # 3. Combine both signals
    combine_udf = F.udf(
        lambda sl, sf, sc, sn, rl, rf, rc, rn: combine_semantic_and_regex(
            sl, sf, sc, sn, rl, rf, rc, rn, target_type
        ),
        StringType(),
    )
    df = df.withColumn(
        "record_type",
        combine_udf(
            F.col("sem_label"), F.col("sem_faculty"), F.col("sem_course"), F.col("sem_news"),
            F.col("regex_label"), F.col("regex_faculty"), F.col("regex_course"), F.col("regex_news"),
        ),
    )

    # Drop intermediate columns
    df = df.drop("_sem_result", "_regex_result", "sem_label", "sem_faculty", "sem_course", "sem_news",
                  "regex_label", "regex_faculty", "regex_course", "regex_news")

    return df


# ── Schema definitions (shared) ─────────────────────────────────

FACULTY_PROFILES_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", LongType(), False),
    StructField("business_timestamp", TimestampType(), True),
    StructField("raw_html", StringType(), True),
    StructField("normalized_text", StringType(), True),
    StructField("title", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("department", StringType(), True),
    StructField("faculty", StringType(), True),
    StructField("research_interests", ArrayType(StringType()), True),
    StructField("publications", ArrayType(StringType()), True),
    StructField("orcid_id", StringType(), True),
    StructField("language", StringType(), True),
    StructField("is_deleted", StringType(), False),
])

COURSE_CATALOG_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", LongType(), False),
    StructField("business_timestamp", TimestampType(), True),
    StructField("raw_html", StringType(), True),
    StructField("course_code", StringType(), True),
    StructField("course_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("department", StringType(), True),
    StructField("faculty", StringType(), True),
    StructField("instructor", StringType(), True),
    StructField("level", StringType(), True),
    StructField("credits", StringType(), True),
    StructField("language", StringType(), True),
    StructField("normalized_text", StringType(), True),
    StructField("is_deleted", StringType(), False),
])

PDF_DOCUMENTS_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", LongType(), False),
    StructField("business_timestamp", TimestampType(), True),
    StructField("file_name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("page_count", LongType(), True),
    StructField("extracted_text", StringType(), True),
    StructField("normalized_text", StringType(), True),
    StructField("language", StringType(), True),
    StructField("is_deleted", StringType(), False),
])

UNIVERSITY_NEWS_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("record_type", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("source_url", StringType(), True),
    StructField("content_hash", StringType(), True),
    StructField("crawl_timestamp", LongType(), False),
    StructField("business_timestamp", TimestampType(), True),
    StructField("raw_html", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("normalized_text", StringType(), True),
    StructField("language", StringType(), True),
    StructField("is_deleted", StringType(), False),
])


# ── Faculty profiles pipeline ───────────────────────────────────

def build_faculty_profiles(
    spark: SparkSession,
    source_system: str | None = None,
    *,
    skip_embedding: bool = False,
    embedding_method: str = "auto",
    skip_openalex: bool = False,
) -> DataFrame:
    """Run the full faculty-profiles ETL chain.

    Steps
    -----
    1. Read raw HTML metadata from MinIO
    2. Parse into the faculty schema
    3. Normalise fields (names, email, department…)
    4. Deduplicate by content hash
    5. Enrich with language detection
    6. (optional) Enrich with OpenAlex publications
    7. Generate text chunks (for RAG)
    8. (optional) Compute embeddings (Gemini by default, falls back to TF-IDF)
    """
    ensure_curated_database(spark)

    html_meta = read_raw_html(spark, source_system)
    if html_meta.isEmpty():
        logger.warning("No raw HTML data to process for faculty profiles")
        return spark.createDataFrame([], FACULTY_PROFILES_SCHEMA)

    # Fetch actual raw HTML content from MinIO (populates raw_html)
    html_meta = fetch_raw_html_content(html_meta, spark)

    # Parse into the faculty schema (keeps raw_html if present)
    df = parse_faculty_from_raw(html_meta)

    # Extract normalized text from raw HTML (populates normalized_text)
    df = parse_html_content(df)

    # Filter out non-HTML rows before classification
    df = df.filter(F.col("raw_html").startswith("<"))

    # Classify page type — multi-signal (semantic + regex)
    df = _combined_classify(df, "faculty_profile")

    before = df.count()
    df = df.filter(F.col("record_type") == "faculty_profile")
    logger.info(
        "Faculty classifier (combined): %d / %d pages classified as faculty_profile",
        df.count(), before,
    )

    # If still 0 results, return empty schema early (skip downstream stages)
    if df.isEmpty():
        logger.warning("No faculty profiles found after classification — returning empty result")
        return spark.createDataFrame([], FACULTY_PROFILES_SCHEMA)

    # Extract email from normalized_text when no dedicated field exists
    df = extract_emails_from_text(df)

    # Normalise
    df = normalize_faculty_profiles(df)

    # Deduplicate
    df = deduplicate_by_hash(df, hash_col="content_hash")
    df = deduplicate_by_hash(df, hash_col="source_url")

    # Enrich
    df = enrich_with_language(df)

    if not skip_openalex:
        api_df = read_raw_json(spark, source_system)
        if not api_df.isEmpty():
            try:
                df = enrich_from_openalex(spark, df, source_system)
            except Exception as e:
                logger.warning("OpenAlex enrichment failed: %s", e)

    # Chunk
    df = generate_chunks(df)

    # Embed (Gemini by default, falls back to TF-IDF if no API key)
    if not skip_embedding:
        from university_data_platform.processing.embedder import add_embeddings_to_df
        df = add_embeddings_to_df(spark, df, method=embedding_method)

    return df


# ── Course catalog pipeline ─────────────────────────────────────

def build_course_catalog(
    spark: SparkSession,
    source_system: str | None = None,
    *,
    skip_embedding: bool = False,
    embedding_method: str = "auto",
) -> DataFrame:
    """Run the full course-catalog ETL chain.

    Steps
    -----
    1. Read raw HTML metadata from MinIO
    2. Parse into the course schema
    3. Normalise fields
    4. Deduplicate by content hash
    5. Enrich with language detection
    6. Generate text chunks (for RAG)
    7. (optional) Compute embeddings (Gemini by default)
    """
    ensure_curated_database(spark)

    html_meta = read_raw_html(spark, source_system)
    if html_meta.isEmpty():
        logger.warning("No raw data to process for course catalog")
        return spark.createDataFrame([], COURSE_CATALOG_SCHEMA)

    # Fetch actual raw HTML content from MinIO (populates raw_html)
    html_meta = fetch_raw_html_content(html_meta, spark)

    # Parse into the course schema
    df = parse_course_from_raw(html_meta)

    # Extract normalized text from raw HTML
    df = parse_html_content(df)

    # Filter out non-HTML rows before classification
    df = df.filter(F.col("raw_html").startswith("<"))

    # Classify page type — multi-signal (semantic + regex)
    df = _combined_classify(df, "course_catalog", title_col="course_name")

    before = df.count()
    df = df.filter(F.col("record_type") == "course_catalog")
    logger.info(
        "Course classifier (combined): %d / %d pages classified as course_catalog",
        df.count(), before,
    )

    if df.isEmpty():
        logger.warning("No course catalog pages found after classification — returning empty result")
        return spark.createDataFrame([], COURSE_CATALOG_SCHEMA)

    # Normalise
    df = normalize_course_catalog(df)

    # Deduplicate
    df = deduplicate_by_hash(df, hash_col="content_hash")
    df = deduplicate_by_hash(df, hash_col="source_url")

    # Enrich
    df = enrich_with_language(df)

    # Chunk
    df = generate_chunks(df)

    # Embed (Gemini by default)
    if not skip_embedding:
        from university_data_platform.processing.embedder import add_embeddings_to_df
        df = add_embeddings_to_df(spark, df, method=embedding_method)

    return df


# ── PDF documents pipeline ────────────────────────────────────────

def build_pdf_documents(
    spark: SparkSession,
    source_system: str | None = None,
    *,
    skip_embedding: bool = False,
    embedding_method: str = "auto",
) -> DataFrame:
    """Run the PDF documents ETL chain.

    Steps
    -----
    1. Read PDF metadata sidecars from MinIO
    2. Fetch actual PDF binary content
    3. Extract text from PDFs
    4. Normalise extracted text
    5. Deduplicate by content hash
    6. Enrich with language detection
    7. Generate text chunks (for RAG)
    8. (optional) Compute embeddings
    """
    ensure_curated_database(spark)

    doc_meta = read_raw_documents(spark, source_system)
    if doc_meta.isEmpty():
        logger.warning("No PDF documents to process")
        return spark.createDataFrame([], PDF_DOCUMENTS_SCHEMA)

    # Filter to PDFs only
    doc_meta = doc_meta.filter(
        F.lower(F.col("content_type")).contains("pdf")
        | F.lower(F.col("file_name")).endswith(".pdf")
        | F.lower(F.col("raw_object_path")).endswith(".pdf")
    )
    if doc_meta.isEmpty():
        logger.warning("No PDF documents found after content_type filter")
        return spark.createDataFrame([], PDF_DOCUMENTS_SCHEMA)

    # Fetch actual PDF binary content
    doc_meta = fetch_raw_pdf_content(doc_meta, spark)

    # Extract text from PDF bytes using PyMuPDF
    extract_udf = F.udf(lambda b: pdf_to_clean_text(b) if b else None, StringType())
    df = doc_meta.withColumn("extracted_text", extract_udf(F.col("pdf_bytes")))

    # Build structured columns
    df = df.withColumn(
        "record_id",
        F.when(F.col("source_url").isNotNull(), F.md5(F.col("source_url")))
         .otherwise(F.md5(F.col("raw_object_path")))
    )
    df = df.withColumn("crawl_timestamp", F.unix_timestamp() * 1000)
    df = df.withColumn("is_deleted", F.lit("false"))
    df = df.withColumn("page_count", F.lit(None).cast(LongType()))
    df = df.withColumn("record_type", F.lit("pdf_document"))

    df = df.select(
        "record_id",
        "record_type",
        "source_system",
        "source_url",
        F.col("content_checksum").alias("content_hash"),
        "crawl_timestamp",
        F.col("extraction_timestamp").cast(TimestampType()).alias("business_timestamp"),
        F.col("file_name").cast(StringType()).alias("file_name"),
        F.lit(None).cast(StringType()).alias("title"),
        F.lit(None).cast(StringType()).alias("author"),
        "page_count",
        "extracted_text",
        F.col("extracted_text").alias("normalized_text"),
        F.lit("fr").alias("language"),
        "is_deleted",
    )

    # Deduplicate
    df = deduplicate_by_hash(df, hash_col="content_hash")
    df = deduplicate_by_hash(df, hash_col="source_url")

    # Enrich with language detection
    df = enrich_with_language(df)

    # Chunk for RAG
    df = generate_chunks(df)

    # Embed
    if not skip_embedding:
        from university_data_platform.processing.embedder import add_embeddings_to_df
        df = add_embeddings_to_df(spark, df, method=embedding_method)

    return df


# ── University news pipeline ─────────────────────────────────────

def build_university_news(
    spark: SparkSession,
    source_system: str | None = None,
    *,
    skip_embedding: bool = False,
    embedding_method: str = "auto",
) -> DataFrame:
    """Run the full university-news ETL chain.

    Processes news, announcements, schedules, and archive pages.

    Steps
    -----
    1. Read raw HTML metadata from MinIO
    2. Extract normalized text from raw HTML
    3. Classify page type (semantic + regex)
    4. Filter to ``university_news`` records
    5. Normalise fields
    6. Deduplicate by content hash and URL
    7. Enrich with language detection
    8. Generate text chunks (for RAG)
    9. (optional) Compute embeddings
    """
    ensure_curated_database(spark)

    html_meta = read_raw_html(spark, source_system)
    if html_meta.isEmpty():
        logger.warning("No raw data to process for university news")
        return spark.createDataFrame([], UNIVERSITY_NEWS_SCHEMA)

    # Fetch actual raw HTML content from MinIO (populates raw_html)
    html_meta = fetch_raw_html_content(html_meta, spark)

    # Build a minimal schema for classification — title comes from HTML extraction
    if "raw_html" not in html_meta.columns:
        html_meta = html_meta.withColumn("raw_html",
                                          F.lit(None).cast(StringType()))

    df = (
        html_meta
        .withColumn("record_id", F.md5(F.col("source_url")))
        .withColumn("crawl_timestamp", F.unix_timestamp() * 1000)
        .withColumn("is_deleted", F.lit("false"))
        .withColumn("record_type", F.lit("university_news"))
        .select(
            "record_id",
            "record_type",
            "source_system",
            "source_url",
            F.col("content_checksum").alias("content_hash"),
            "crawl_timestamp",
            F.col("extraction_timestamp").cast(TimestampType())
             .alias("business_timestamp"),
            "raw_html",
            F.lit(None).cast(StringType()).alias("title"),
            F.lit(None).cast(StringType()).alias("description"),
            F.lit(None).cast(StringType()).alias("category"),
            F.lit(None).cast(StringType()).alias("normalized_text"),
            F.lit("fr").alias("language"),
            "is_deleted",
        )
    )

    # Extract normalized text from raw HTML
    df = parse_html_content(df)

    # Filter out non-HTML rows before classification
    df = df.filter(F.col("raw_html").startswith("<"))

    # Classify page type — multi-signal (semantic + regex)
    df = _combined_classify(df, "university_news")

    before = df.count()
    df = df.filter(F.col("record_type") == "university_news")
    logger.info(
        "News classifier (combined): %d / %d pages classified as university_news",
        df.count(), before,
    )

    if df.isEmpty():
        logger.warning("No university news pages found after classification — returning empty result")
        return spark.createDataFrame([], UNIVERSITY_NEWS_SCHEMA)

    # Normalise
    df = normalize_university_news(df)

    # Deduplicate
    df = deduplicate_by_hash(df, hash_col="content_hash")
    df = deduplicate_by_hash(df, hash_col="source_url")

    # Enrich
    df = enrich_with_language(df)

    # Chunk
    df = generate_chunks(df)

    # Embed (Gemini by default)
    if not skip_embedding:
        from university_data_platform.processing.embedder import add_embeddings_to_df
        df = add_embeddings_to_df(spark, df, method=embedding_method)

    return df


# ── End-to-end runners (parse → Hudi) ──────────────────────────

def process_faculty_profiles(
    spark: SparkSession | None = None,
    source_system: str | None = None,
    **kwargs: Any,
) -> None:
    """Build and persist faculty_profiles to Hudi.

    Also applies BS4 extraction to any existing Hudi records that were
    not re-classified by the current run (legacy data).  This ensures
    title/heading/normalized_text are populated even when the classifier
    no longer identifies a page as ``faculty_profile``.
    """
    from university_data_platform.processing.io import create_spark_session

    own_session = spark is None
    if own_session:
        spark = create_spark_session("hudi-faculty-profiles")

    try:
        df = build_faculty_profiles(spark, source_system, **kwargs)

        # ── Re-process legacy Hudi rows with BS4 ──────────────────
        try:
            hudi_path = f"s3a://curated-zone/faculty_profiles"
            existing = spark.read.format("hudi").load(hudi_path)
            existing = existing.filter(F.col("is_deleted") == "false")

            if not existing.isEmpty():
                if df.isEmpty():
                    to_update = existing
                else:
                    new_urls = df.select("source_url").distinct()
                    to_update = existing.join(new_urls, "source_url", "left_anti")

                if not to_update.isEmpty():
                    n = to_update.count()
                    logger.info(
                        "Re-processing %d existing faculty_profiles with BS4",
                        n,
                    )
                    to_update = parse_html_content(to_update)
                    # Strip Hudi metadata columns and align schemas.
                    # df may lack columns that Hudi has (e.g. embeddings
                    # when skip_embedding=True).  Add null columns so
                    # unionByName doesn't fail.
                    hudi_cols = [
                        c for c in to_update.columns
                        if not c.startswith("_hoodie_")
                    ]
                    for c in hudi_cols:
                        if c not in df.columns:
                            df = df.withColumn(c, F.lit(None))
                    df = df.select(*hudi_cols).unionByName(
                        to_update.select(*hudi_cols)
                    )
                    df = deduplicate_by_hash(df, hash_col="source_url")
                    logger.info(
                        "Merged legacy rows — %d total records to write",
                        df.count(),
                    )
        except Exception as e:
            logger.warning("Could not merge existing Hudi data: %s", e)

        if not df.isEmpty():
            write_to_hudi(df, "faculty_profiles")
        else:
            logger.info("No faculty profiles to write")
    finally:
        if own_session:
            spark.stop()


def process_course_catalog(
    spark: SparkSession | None = None,
    source_system: str | None = None,
    **kwargs: Any,
) -> None:
    """Build and persist course_catalog to Hudi."""
    from university_data_platform.processing.io import create_spark_session

    own_session = spark is None
    if own_session:
        spark = create_spark_session("hudi-course-catalog")

    try:
        df = build_course_catalog(spark, source_system, **kwargs)
        if not df.isEmpty():
            write_to_hudi(df, "course_catalog")
        else:
            logger.info("No course catalog data to write")
    finally:
        if own_session:
            spark.stop()


def process_pdf_documents(
    spark: SparkSession | None = None,
    source_system: str | None = None,
    **kwargs: Any,
) -> None:
    """Build and persist pdf_documents to Hudi."""
    from university_data_platform.processing.io import create_spark_session

    own_session = spark is None
    if own_session:
        spark = create_spark_session("hudi-pdf-documents")

    try:
        df = build_pdf_documents(spark, source_system, **kwargs)
        if not df.isEmpty():
            write_to_hudi(df, "pdf_documents")
        else:
            logger.info("No PDF documents to write")
    finally:
        if own_session:
            spark.stop()


def process_university_news(
    spark: SparkSession | None = None,
    source_system: str | None = None,
    **kwargs: Any,
) -> None:
    """Build and persist university_news to Hudi."""
    from university_data_platform.processing.io import create_spark_session

    own_session = spark is None
    if own_session:
        spark = create_spark_session("hudi-university-news")

    try:
        df = build_university_news(spark, source_system, **kwargs)
        if not df.isEmpty():
            write_to_hudi(df, "university_news")
        else:
            logger.info("No university news to write")
    finally:
        if own_session:
            spark.stop()


def run_all_pipelines(
    source_system: str | None = None,
    **kwargs: Any,
) -> None:
    """Run all ETL pipelines sequentially in a single Spark session."""
    from university_data_platform.processing.io import create_spark_session

    spark = create_spark_session("hudi-all-pipelines")
    try:
        process_faculty_profiles(spark, source_system, **kwargs)
        process_course_catalog(spark, source_system, **kwargs)
        process_pdf_documents(spark, source_system, **kwargs)
        process_university_news(spark, source_system, **kwargs)
    finally:
        spark.stop()
