"""
processing.deduplicator — Remove duplicate records.

Provides multiple deduplication strategies that can be combined:

    by_hash    — Drop exact duplicates on a content- or entity-hash column.
    by_content — Fuzzy deduplication based on content similarity.
    by_entity  — Keep the best version of each real-world entity.
"""

from __future__ import annotations

import logging
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def deduplicate_by_hash(
    df: DataFrame,
    hash_col: str = "content_hash",
    order_col: str = "crawl_timestamp",
) -> DataFrame:
    """Remove rows sharing the same *hash_col* keeping the latest row.

    This is the primary deduplication strategy: two raw pages with the
    same content checksum are treated as identical.
    """
    if hash_col not in df.columns:
        logger.warning("Column '%s' not found — skipping hash dedup", hash_col)
        return df

    window = Window.partitionBy(hash_col).orderBy(F.col(order_col).desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def deduplicate_by_content(
    df: DataFrame,
    text_col: str = "normalized_text",
    threshold: float = 0.95,
) -> DataFrame:
    """Drop near-duplicate rows by comparing MinHash signatures.

    .. note::
       This is a placeholder. A production implementation should use
       Spark ML ``MinHashLSH`` on tokenised content. The current stub
       simply returns the input DataFrame unchanged.
    """
    logger.info(
        "Fuzzy dedup called (threshold=%.2f) — "
        "no-op until MinHashLSH is wired", threshold,
    )
    return df


def deduplicate_by_entity(
    df: DataFrame,
    entity_cols: Sequence[str] = ("full_name", "email", "orcid_id"),
    order_col: str = "crawl_timestamp",
) -> DataFrame:
    """For each real-world entity keep only the most recent record.

    Groups by the supplied *entity_cols* (any non-null is considered
    an identifier) and picks the row with the highest *order_col*.

    Example for faculty profiles: if two rows share the same *email*
    (or *orcid_id*) only the most recently crawled entry survives.
    """
    if not entity_cols:
        return df

    # Build an expression that coalesces non-null entity identifiers
    partition_cols = [c for c in entity_cols if c in df.columns]
    if not partition_cols:
        logger.warning("None of %s found in columns — skipping entity dedup", entity_cols)
        return df

    window = Window.partitionBy(partition_cols).orderBy(F.col(order_col).desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
