"""
processing.enricher — Augment data with additional metadata.

Built-in enrichments:

    language     — Heuristic language detection (fr/ar/en) from text.
    openalex     — Publication metadata from the OpenAlex API (via raw JSON).
    orcid        — ORCID profile data (stub).
"""

from __future__ import annotations

import logging
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


# ── Language detection ───────────────────────────────────────────

def detect_language(text: str) -> str:
    """Simple character-range heuristic for *fr*, *ar*, *en*, or *unknown*."""
    if not text:
        return "unknown"
    arabic_chars = len(re.findall(r"[؀-ۿ]", text))
    french_words = len(re.findall(
        r"\b(le|la|les|des|dans|pour|avec|sur|est|sont|nous|vous|"
        r"ils|elles|qui|que|dont|ou|et|mais|donc|car|en|un|une)\b",
        text, re.IGNORECASE,
    ))
    total_words = len(text.split())
    if total_words == 0:
        return "unknown"
    if arabic_chars / max(len(text), 1) > 0.1:
        return "ar"
    if french_words > 0 and french_words / max(total_words, 1) > 0.02:
        return "fr"
    return "en"


def enrich_with_language(df: DataFrame) -> DataFrame:
    """Add a *language* column derived from *normalized_text*."""
    lang_udf = F.udf(detect_language, StringType())
    if "language" in df.columns:
        df = df.drop("language")
    return df.withColumn("language", lang_udf(F.col("normalized_text")))


# ── OpenAlex enrichment ──────────────────────────────────────────

def _read_openalex_data(spark, source_system: str | None) -> DataFrame:
    """Read raw OpenAlex JSON from the ingestion layer."""
    from university_data_platform.processing.io import read_raw_json
    return read_raw_json(spark, source_system=source_system)


def enrich_from_openalex(
    spark,
    faculty_df: DataFrame,
    source_system: str | None = None,
) -> DataFrame:
    """Join faculty profiles with publication data from OpenAlex.

    Matches on a fuzzy title-contains-name heuristic.  Aggregates
    matched publication titles into an *publications* array column.
    """
    openalex_df = _read_openalex_data(spark, source_system)
    if openalex_df.isEmpty():
        logger.warning("No OpenAlex data found — skipping enrichment")
        return faculty_df

    enriched = (
        openalex_df
        .select(
            F.col("title").alias("pub_title"),
            F.col("doi"),
            F.col("publication_year"),
            F.col("authorships").alias("authors"),
        )
        .withColumn("pub_title_lower", F.lower(F.col("pub_title")))
        .cache()
    )

    faculty_with = faculty_df.withColumn("name_lower", F.lower(F.col("full_name")))
    result = faculty_with.join(
        enriched,
        faculty_with.name_lower.contains(F.col("pub_title_lower")),
        "left_outer",
    )
    pub_agg = result.groupBy("record_id").agg(
        F.collect_list(F.col("pub_title")).alias("publications")
    )
    return faculty_df.join(pub_agg, on="record_id", how="left_outer")


# ── ORCID enrichment (stub) ─────────────────────────────────────

def enrich_from_orcid(df: DataFrame) -> DataFrame:
    """Enrich with ORCID profile data.

    .. note::
       Placeholder — implement when ORCID API ingestion is active.
    """
    logger.info("ORCID enrichment called — no-op (stub)")
    return df
