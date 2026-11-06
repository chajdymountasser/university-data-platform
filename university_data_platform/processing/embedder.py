"""
processing.embedder — Generate vector embeddings from text chunks.

Provides multiple strategies that can be mixed:

    tfidf   — Lightweight Spark ML-based TF-IDF embeddings.
              No external API calls; works offline.
    gemini  — Remote embedding via the Google Gemini API (requires key).
    local   — Local sentence-transformers model (free, no API key).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StructField, StructType

logger = logging.getLogger(__name__)

# ── Local sentence-transformers embeddings ─────────────────────

_local_embed_model = None  # lazy-loaded

def _get_local_embed_model():
    """Lazy-load the sentence-transformers model."""
    global _local_embed_model
    if _local_embed_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            model_name = os.getenv("LOCAL_EMBEDDING_MODEL", "paraphrase-multilingual-mpnet-base-v2")
            # Use /tmp as cache to avoid permission issues in containers
            cache_dir = os.getenv("HF_HOME", "/tmp/hf_cache")
            os.makedirs(cache_dir, exist_ok=True)
            _local_embed_model = SentenceTransformer(model_name, cache_folder=cache_dir)
            logger.info("Loaded local embedding model: %s (cache: %s)", model_name, cache_dir)
        except Exception as e:
            logger.warning("Could not load local embedding model: %s", e)
            _local_embed_model = False  # prevent retry
    return _local_embed_model if _local_embed_model else None


def compute_local_embeddings(
    df: DataFrame,
    text_col: str = "normalized_text",
    output_col: str = "embedding_local",
    batch_size: int = 64,
) -> DataFrame:
    """Generate embeddings using a local sentence-transformers model.

    No API key needed — runs entirely on the driver node.
    Falls back gracefully if sentence-transformers is not installed.
    """
    model = _get_local_embed_model()
    if model is None:
        logger.warning("Local embedding model not available — skipping")
        return df

    spark = df.sparkSession

    rows = df.collect()
    text_idx_pairs = [(i, row[text_col]) for i, row in enumerate(rows) if row[text_col]]
    if not text_idx_pairs:
        logger.warning("No text to embed — returning df unchanged")
        return df

    indices = [p[0] for p in text_idx_pairs]
    texts = [p[1] for p in text_idx_pairs]

    logger.info("Computing local embeddings (model=%s, num_texts=%d)",
                os.getenv("LOCAL_EMBEDDING_MODEL", "all-mpnet-base-v2"), len(texts))

    # Batch encode
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        embeddings = model.encode(batch, normalize_embeddings=True, show_progress_bar=False)
        all_embeddings.extend(embeddings.tolist())

    emb_map: dict[int, list[float]] = dict(zip(indices, all_embeddings))

    new_rows: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        d = row.asDict()
        d[output_col] = emb_map.get(i)
        new_rows.append(d)

    new_schema = df.schema.add(
        StructField(output_col, ArrayType(DoubleType()), True),
    )
    return spark.createDataFrame(new_rows, new_schema)

# ── TF-IDF embeddings (Spark ML, offline) ──────────────────────

def compute_tfidf_embeddings(
    spark: SparkSession,
    df: DataFrame,
    text_col: str = "normalized_text",
    output_col: str = "embedding_tfidf",
    num_features: int = 256,
) -> DataFrame:
    """Compute TF-IDF feature vectors for each row.

    Requires ``spark.ml`` — uses
    ``Tokenizer → HashingTF → IDF`` pipeline.

    Returns the input DataFrame with a new float-array *output_col*.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import HashingTF, IDF, Tokenizer

    if df.isEmpty():
        logger.warning("Empty DataFrame — skipping TF-IDF embeddings")
        return df

    # Separate rows with null text — Tokenizer crashes on null input
    null_text_df = df.filter(F.col(text_col).isNull())
    non_null_df = df.filter(F.col(text_col).isNotNull())

    if non_null_df.isEmpty():
        logger.warning("All rows have null text — skipping TF-IDF embeddings")
        return df

    tokenizer = Tokenizer(inputCol=text_col, outputCol="__tokens__")
    hashing_tf = HashingTF(
        inputCol="__tokens__", outputCol="__raw_features__",
        numFeatures=num_features,
    )
    idf = IDF(inputCol="__raw_features__", outputCol=output_col)

    pipeline = Pipeline(stages=[tokenizer, hashing_tf, idf])
    model = pipeline.fit(non_null_df)

    result = model.transform(non_null_df)

    # Add null-embedding rows back (null_text_df won't have the embedding col)
    if not null_text_df.isEmpty():
        for c in ("__tokens__", "__raw_features__", output_col):
            if c in result.columns and c not in null_text_df.columns:
                null_text_df = null_text_df.withColumn(c, F.lit(None))
        result = result.unionByName(null_text_df, allowMissingColumns=True)
    # Convert DenseVector → array<double> for Parquet/Hudi compatibility
    to_array = F.udf(lambda v: v.toArray().tolist() if v is not None else None,
                      ArrayType(DoubleType()))
    result = result.withColumn(output_col, to_array(F.col(output_col)))

    # Drop intermediate columns
    for c in ("__tokens__", "__raw_features__"):
        if c in result.columns:
            result = result.drop(c)

    logger.info("TF-IDF embeddings written to column '%s' (%d features)",
                output_col, num_features)
    return result


# ── Google Gemini embeddings ─────────────────────────────────

def compute_gemini_embeddings(
    df: DataFrame,
    text_col: str = "normalized_text",
    output_col: str = "embedding_gemini",
    model: str = "gemini-embedding-2",
    api_key: str | None = None,
    batch_size: int = 100,
) -> DataFrame:
    """Generate embeddings via the Google Gemini API.

    Falls back gracefully if the API key is not set (logs a warning
    and returns the DataFrame unchanged).

    Uses direct HTTP calls (``urllib.request``) — no external SDK needed.
    """
    key = api_key or os.getenv("GEMINI_API_KEY")
    if not key:
        logger.warning(
            "GEMINI_API_KEY not set — skipping Gemini embeddings. "
            "Set the env var or use compute_tfidf_embeddings() instead."
        )
        return df

    spark = df.sparkSession

    # Collect all rows as a Python list so we can enumerate reliably.
    # This is fine for typical RAG-scale data (hundreds to low thousands of rows).
    rows = df.collect()
    text_idx_pairs = [(i, row[text_col]) for i, row in enumerate(rows) if row[text_col]]
    if not text_idx_pairs:
        logger.warning("No text to embed — returning df unchanged")
        return df

    indices = [p[0] for p in text_idx_pairs]
    texts = [p[1] for p in text_idx_pairs]

    logger.info("Computing Gemini embeddings (model=%s, batch_size=%d, num_texts=%d)",
                model, batch_size, len(texts))

    embeddings = _gemini_batch(texts, key, model, batch_size)

    # Map original index -> embedding (None if API failed)
    emb_map: dict[int, list[float] | None] = dict(zip(indices, embeddings))

    # Build new rows with the embedding column appended
    new_rows: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        d = row.asDict()
        d[output_col] = emb_map.get(i)
        new_rows.append(d)

    new_schema = df.schema.add(
        StructField(output_col, ArrayType(DoubleType()), True),
    )
    return spark.createDataFrame(new_rows, new_schema)


def _gemini_batch(
    texts: list[str],
    api_key: str,
    model: str,
    batch_size: int = 100,
) -> list[list[float] | None]:
    """Call the Gemini embedding API in batches via direct HTTP.

    Uses ``urllib.request`` (stdlib) so no external SDK is needed.
    This is important because spark-master runs Python 3.8 which
    cannot install ``google-genai`` (requires 3.9+).
    """
    import json
    import urllib.request

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:batchEmbedContents?key={api_key}"
    )
    headers = {"Content-Type": "application/json"}

    results: list[list[float] | None] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        payload = json.dumps({
            "requests": [
                {
                    "model": f"models/{model}",
                    "content": {"parts": [{"text": t}]},
                }
                for t in batch
            ],
        }).encode("utf-8")

        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read())
            for emb in body.get("embeddings", []):
                results.append(emb.get("values"))
        except Exception as e:
            logger.error("Gemini embedding batch failed at offset %d: %s", i, e)
            results.extend([None] * len(batch))
    return results


# ── Generic embedder (auto-select) ──────────────────────────────

def add_embeddings_to_df(
    spark: SparkSession,
    df: DataFrame,
    text_col: str = "normalized_text",
    output_col: str = "embeddings",
    method: str = "auto",
) -> DataFrame:
    """Add an embeddings column using the best available method.

    *method* can be:
      - ``"auto"``   — tries Gemini → local (sentence-transformers) → TF-IDF
      - ``"tfidf"``  — Spark ML TF-IDF (256-dim, offline)
      - ``"gemini"`` — Gemini API (768-dim)
      - ``"local"``  — sentence-transformers (768-dim, free)
    """
    if method == "auto":
        # 1. Try Gemini API
        if os.getenv("GEMINI_API_KEY"):
            result = compute_gemini_embeddings(df, text_col, output_col)
            if output_col in result.columns:
                return result
        # 2. Try local sentence-transformers
        result = compute_local_embeddings(df, text_col, output_col)
        if output_col in result.columns:
            return result
        # 3. Fall back to TF-IDF
        return compute_tfidf_embeddings(spark, df, text_col, output_col)
    if method == "tfidf":
        return compute_tfidf_embeddings(spark, df, text_col, output_col)
    if method == "gemini":
        return compute_gemini_embeddings(df, text_col, output_col)
    if method == "local":
        return compute_local_embeddings(df, text_col, output_col)
    logger.warning("Unknown embedding method '%s' — returning df unchanged", method)
    return df
