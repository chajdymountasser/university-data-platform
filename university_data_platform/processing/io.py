"""
processing.io — Spark I/O helpers for reading raw data and writing Hudi.

Pulls the read/write plumbing out of the main processor so that
individual processing modules can access data without circular imports.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

logger = logging.getLogger(__name__)

# ── Bucket constants (mirrors storage/minio_client.py) ───────────

RAW_BUCKETS = {
    "html": "raw-web-html",
    "json": "raw-json",
    "documents": "raw-documents",
    "images": "raw-images",
}
CURATED_DB = "curated"

# ── Hadoop / Hive config keys ───────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
HIVE_METASTORE_URL = os.getenv("HIVE_METASTORE_URL", "thrift://hive-metastore:9083")


# ── Spark session factory ────────────────────────────────────────

def create_spark_session(app_name: str = "university-spark-pipeline") -> SparkSession:
    """Create a SparkSession configured for Hudi, Hive, and MinIO/s3a."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.hive.metastore.version", "2.3.9")
        .config("spark.sql.hive.metastore.jars", "builtin")
        .config("spark.sql.hive.metastore.uris", HIVE_METASTORE_URL)
        .config("spark.sql.hive.metastore.schema.verification", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.warehouse.dir", "s3a://curated-zone/")
        .config("spark.executorEnv.PYTHONPATH", "/opt/spark/apps")
        .enableHiveSupport()
        .getOrCreate()
    )


# ── Hudi write options ──────────────────────────────────────────

def hudi_options(
    table_name: str,
    record_key: str = "record_id",
    precombine: str = "crawl_timestamp",
    partition_fields: str = "source_system",
) -> dict[str, str]:
    """Return Hudi write-option dict with Hive sync enabled."""
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.partitionpath.field": partition_fields,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.upsert.shuffle.parallelism": "4",
        "hoodie.insert.shuffle.parallelism": "4",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": CURATED_DB,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,
        "hoodie.datasource.hive_sync.partition_extractor_class": (
            "org.apache.hudi.hive.MultiPartKeysValueExtractor"
        ),
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.metastore.uris": HIVE_METASTORE_URL,
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }


# ── Hudi writer ─────────────────────────────────────────────────

def write_to_hudi(
    df: DataFrame,
    table_name: str,
    record_key: str = "record_id",
    partition_fields: str = "source_system",
    precombine: str = "crawl_timestamp",
    base_path: str | None = None,
) -> None:
    """Write a DataFrame to a Hudi table (append mode) and sync to HMS."""
    if base_path is None:
        base_path = f"s3a://curated-zone/{table_name}"

    if precombine not in df.columns:
        df = df.withColumn(precombine, F.unix_timestamp() * 1000)

    logger.info("Writing to Hudi table: %s at %s", table_name, base_path)
    (
        df.write
        .format("hudi")
        .mode("append")
        .options(**hudi_options(table_name, record_key, precombine, partition_fields))
        .save(base_path)
    )
    logger.info("Hudi write complete: %s", table_name)


# ── Database helpers ────────────────────────────────────────────

def ensure_curated_database(spark: SparkSession) -> None:
    """Create the *curated* Hive database if it does not exist."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CURATED_DB}")
    logger.info("Ensured curated database exists: %s", CURATED_DB)


# ── Raw data readers ────────────────────────────────────────────

def read_raw_html(
    spark: SparkSession,
    source_system: str | None = None,
) -> DataFrame:
    """Read HTML metadata sidecars from MinIO."""
    path = f"s3a://{RAW_BUCKETS['html']}/"
    if source_system:
        path += f"source={source_system}/"

    schema = StructType([
        StructField("source_system", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("extraction_timestamp", StringType(), True),
        StructField("content_checksum", StringType(), True),
        StructField("connector_version", StringType(), True),
        StructField("raw_object_path", StringType(), True),
    ])
    return (
        spark.read.schema(schema)
        .json(path, recursiveFileLookup=True, multiLine=True)
        .withColumn("_meta_file_path", F.input_file_name())
        .filter(F.col("source_system").isNotNull())
        .dropDuplicates(["content_checksum"])
    )


def read_raw_json(
    spark: SparkSession,
    source_system: str | None = None,
) -> DataFrame:
    """Read raw JSON payloads from MinIO (API responses, etc.)."""
    path = f"s3a://{RAW_BUCKETS['json']}/"
    if source_system:
        path += f"source={source_system}/"
    try:
        return (
            spark.read.format("json")
            .option("multiLine", "true")
            .option("recursiveFileLookup", "true")
            .load(path)
        )
    except Exception as e:
        logger.warning("No JSON data found at %s: %s", path, e)
        from pyspark.sql.types import StructType as _EmptySchema
        return spark.createDataFrame([], _EmptySchema([]))


def fetch_raw_html_content(
    html_meta: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    """Read actual raw HTML content from MinIO and join onto metadata sidecars.

    The metadata sidecars contain *raw_object_path* pointing to the real HTML
    file in MinIO.  This function loads all HTML files in the bucket and
    left-joins the content so downstream stages can extract text and fields.
    """
    bucket_path = f"s3a://{RAW_BUCKETS['html']}/"

    try:
        content_df = (
            spark.read.format("binaryFile")
            .load(bucket_path)
            .filter(F.col("path").endswith(".html"))
            .select(
                F.col("path").alias("file_path"),
                F.col("content").cast("string").alias("raw_html"),
            )
        )

        # raw_object_path is a full s3a:// path (build_s3a_path in minio_client).
        # For old sidecars that were saved before the fix (empty raw_object_path),
        # derive the HTML path from the .meta.json file path.
        result = (
            html_meta
            .withColumn("_join_path",
                        F.when(F.col("raw_object_path") != "",
                               F.col("raw_object_path"))
                         .otherwise(F.regexp_replace(
                             F.col("_meta_file_path"), r"\.meta\.json$", ".html")))
            .join(content_df,
                  F.col("_join_path") == F.col("file_path"),
                  "left")
            .drop("_join_path", "file_path", "_meta_file_path")
        )
        logger.info(
            "Fetched raw HTML content — %d rows with content",
            result.filter(F.col("raw_html").isNotNull()).count(),
        )
        return result
    except Exception as e:
        logger.warning("Could not fetch raw HTML content: %s", e)
        return html_meta.withColumn("raw_html",
                                     F.lit(None).cast(StringType()))


def fetch_raw_pdf_content(
    doc_meta: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    """Read actual PDF binaries from MinIO and join onto metadata sidecars.

    Covers both standard PDFs (``.pdf`` extension) and extensionless paths
    such as HCP's ``/file/NNNN/`` URLs. The key is that ``pipeline.py``
    already filters the metadata sidecars to PDF rows (by content_type),
    so the only thing needed here is to load the matching binary files
    and join by their exact ``raw_object_path``.

    Strategy
    --------
    1. Collect unique directory prefixes from ``doc_meta.raw_object_path``.
    2. Load binary files under each prefix (avoids full bucket scan).
    3. Left-join onto ``doc_meta`` by matching ``raw_object_path``.
    """
    bucket_path = f"s3a://{RAW_BUCKETS['documents']}/"

    try:
        # Extract unique directory prefixes from metadata paths so we
        # only scan relevant partitions — handles both .pdf and extensionless.
        path_prefixes = (
            doc_meta
            .filter(F.col("raw_object_path").isNotNull() & (F.col("raw_object_path") != ""))
            .select(F.regexp_extract("raw_object_path", "(.*)/[^/]+$", 1).alias("prefix"))
            .distinct()
            .rdd.flatMap(lambda r: [r.prefix])
            .collect()
        )

        content_dfs = []
        bucket_name = RAW_BUCKETS["documents"]
        for prefix in path_prefixes:
            s3_prefix = prefix.replace(f"s3a://{bucket_name}/", "")
            try:
                pdf_df = (
                    spark.read.format("binaryFile")
                    .load(f"s3a://{bucket_name}/{s3_prefix}")
                    .select(
                        F.col("path").alias("file_path"),
                        F.col("content").alias("pdf_bytes"),
                    )
                )
                content_dfs.append(pdf_df)
            except Exception:
                continue

        if content_dfs:
            content_df = content_dfs[0]
            for df in content_dfs[1:]:
                content_df = content_df.union(df)
        else:
            logger.warning("No path prefixes from metadata — loading full bucket")
            content_df = (
                spark.read.format("binaryFile")
                .load(bucket_path)
                .select(
                    F.col("path").alias("file_path"),
                    F.col("content").alias("pdf_bytes"),
                )
            )

        result = (
            doc_meta
            .withColumn("_join_path",
                        F.when(F.col("raw_object_path") != "",
                               F.col("raw_object_path"))
                         .otherwise(F.regexp_replace(
                             F.col("_meta_file_path"), r"\.meta\.json$", ".pdf")))
            .join(content_df,
                  F.col("_join_path") == F.col("file_path"),
                  "left")
            .drop("_join_path", "file_path", "_meta_file_path")
        )
        logger.info(
            "Fetched raw PDF content — %d rows with content",
            result.filter(F.col("pdf_bytes").isNotNull()).count(),
        )
        return result
    except Exception as e:
        logger.warning("Could not fetch raw PDF content: %s", e)
        return doc_meta.withColumn("pdf_bytes",
                                    F.lit(None).cast("binary"))


def read_raw_images(
    spark: SparkSession,
    source_system: str | None = None,
) -> DataFrame:
    """Read image metadata sidecars from MinIO (raw-images bucket)."""
    path = f"s3a://{RAW_BUCKETS['images']}/"
    if source_system:
        path += f"source={source_system}/"

    schema = StructType([
        StructField("source_system", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("content_checksum", StringType(), True),
        StructField("extraction_timestamp", StringType(), True),
        StructField("connector_version", StringType(), True),
        StructField("raw_object_path", StringType(), True),
    ])
    return (
        spark.read.schema(schema)
        .json(path, recursiveFileLookup=True, multiLine=True)
        .filter(F.col("source_system").isNotNull())
        .dropDuplicates(["content_checksum"])
    )


def read_raw_documents(
    spark: SparkSession,
    source_system: str | None = None,
) -> DataFrame:
    """Read document metadata from MinIO."""
    path = f"s3a://{RAW_BUCKETS['documents']}/"
    if source_system:
        path += f"source={source_system}/"

    schema = StructType([
        StructField("source_system", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("content_checksum", StringType(), True),
        StructField("extraction_timestamp", StringType(), True),
        StructField("raw_object_path", StringType(), True),
    ])
    return (
        spark.read.schema(schema)
        .json(path, recursiveFileLookup=True, multiLine=True)
        .filter(F.col("source_system").isNotNull())
        .withColumn("_meta_file_path", F.input_file_name())
    )
