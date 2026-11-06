"""
MinIO storage client — all MinIO operations.

Path convention (permanent — used by all phases):
  s3a://{bucket}/source={source_system}/year={YYYY}/month={MM}/day={DD}/{record_id}.{ext}

Metadata sidecar:
  s3a://{bucket}/source={source_system}/year={YYYY}/month={MM}/day={DD}/{record_id}.meta.json

Rules:
- s3a:// prefix always — never s3:// (Spark requires s3a in Phase 2)
- source= partition key maps directly to Hive partition column in Phase 2
- This path convention is permanent and must never be changed
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Phase 1 buckets — lowercase, no uppercase, no spaces
# These names become Hive table names in Phase 2 — do not change.
BUCKETS = [
    "raw-web-html",
    "raw-documents",
    "raw-json",
    "raw-images",
    "raw-logs",
    "curated-zone",          # Phase 2: Hudi curated tables
]

# Checksum dedup index prefix
CHECKSUM_PREFIX = "checksums"


def build_s3a_path(
    bucket: str,
    source_system: str,
    record_id: str,
    extension: str,
    ts: Optional[datetime] = None,
) -> str:
    """Build the permanent s3a:// storage path.

    Format:
      s3a://{bucket}/source={source_system}/year={YYYY}/month={MM}/day={DD}/{record_id}.{ext}

    Always returns s3a:// — never s3:// (Spark requires s3a in Phase 2).
    """
    ts = ts or datetime.now(timezone.utc)
    date_parts = f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}"
    return f"s3a://{bucket}/source={source_system}/{date_parts}/{record_id}.{extension}"


def build_metadata_s3a_path(
    bucket: str,
    source_system: str,
    record_id: str,
    ts: Optional[datetime] = None,
) -> str:
    """Build the .meta.json sidecar path."""
    ts = ts or datetime.now(timezone.utc)
    date_parts = f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}"
    return f"s3a://{bucket}/source={source_system}/{date_parts}/{record_id}.meta.json"


def build_checksum_path(source_system: str, checksum: str) -> str:
    """Build the dedup checksum index path inside raw-logs."""
    return f"s3a://raw-logs/{CHECKSUM_PREFIX}/{source_system}/{checksum}.exists"


def _s3a_to_bucket_and_key(s3a_path: str) -> tuple[str, str]:
    """Convert s3a://bucket/prefix/key → (bucket, prefix/key)."""
    if not s3a_path.startswith("s3a://"):
        raise ValueError(f"Path must start with s3a://, got: {s3a_path}")
    without_scheme = s3a_path[len("s3a://"):]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


class MinIOClient:
    """Wraps boto3 S3 operations for the university data platform."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        use_ssl: Optional[bool] = None,
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.use_ssl = use_ssl if use_ssl is not None else (
            os.getenv("MINIO_USE_SSL", "false").lower() == "true"
        )

        protocol = "https" if self.use_ssl else "http"
        self._client = boto3.client(
            "s3",
            endpoint_url=f"{protocol}://{self.endpoint}",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

    def create_buckets(self) -> list[str]:
        """Create all Phase 1 buckets if they don't exist. Returns list of bucket names."""
        created = []
        for bucket in BUCKETS:
            try:
                self._client.head_bucket(Bucket=bucket)
                logger.debug("Bucket already exists: %s", bucket)
            except ClientError:
                self._client.create_bucket(Bucket=bucket)
                logger.info("Created bucket: %s", bucket)
                created.append(bucket)
        return created

    def bucket_exists(self, bucket: str) -> bool:
        try:
            self._client.head_bucket(Bucket=bucket)
            return True
        except ClientError:
            return False

    def object_exists(self, s3a_path: str) -> bool:
        """Check if an object exists at the given s3a:// path."""
        bucket, key = _s3a_to_bucket_and_key(s3a_path)
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def put_object(self, s3a_path: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        """Upload bytes to MinIO at the given s3a:// path."""
        bucket, key = _s3a_to_bucket_and_key(s3a_path)
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        logger.debug("Uploaded %d bytes to %s", len(data), s3a_path)

    def get_object(self, s3a_path: str) -> bytes:
        """Download bytes from MinIO at the given s3a:// path."""
        bucket, key = _s3a_to_bucket_and_key(s3a_path)
        resp = self._client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()

    def is_duplicate(self, source_system: str, checksum: str) -> bool:
        """Check if this checksum has already been ingested for this source."""
        path = build_checksum_path(source_system, checksum)
        return self.object_exists(path)

    def mark_checksum(self, source_system: str, checksum: str) -> None:
        """Write a dedup marker for this checksum."""
        path = build_checksum_path(source_system, checksum)
        marker = json.dumps({
            "source_system": source_system,
            "checksum": checksum,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }).encode("utf-8")
        self.put_object(path, marker, content_type="application/json")

    def save_with_metadata(
        self,
        raw_content: bytes,
        metadata_dict: dict,
        bucket: str,
        source_system: str,
        record_id: str,
        extension: str,
        content_type: str,
        ts: Optional[datetime] = None,
    ) -> tuple[str, str, bool]:
        """Save raw bytes + .meta.json sidecar to MinIO.

        Returns: (raw_object_path, metadata_path, skipped_duplicate)
        """
        checksum = metadata_dict.get("content_checksum", "")

        # Deduplication check
        if self.is_duplicate(source_system, checksum):
            logger.info("Duplicate skipped: checksum=%s source=%s", checksum[:12], source_system)
            raw_path = build_s3a_path(bucket, source_system, record_id, extension, ts)
            meta_path = build_metadata_s3a_path(bucket, source_system, record_id, ts)
            return raw_path, meta_path, True

        # Build paths
        raw_path = build_s3a_path(bucket, source_system, record_id, extension, ts)
        meta_path = build_metadata_s3a_path(bucket, source_system, record_id, ts)

        # Upload raw content
        self.put_object(raw_path, raw_content, content_type=content_type)

        # Inject the actual path into the metadata before saving the sidecar
        metadata_dict["raw_object_path"] = raw_path

        # Upload metadata sidecar
        meta_bytes = json.dumps(metadata_dict, ensure_ascii=False, indent=2).encode("utf-8")
        self.put_object(meta_path, meta_bytes, content_type="application/json")

        # Mark checksum for dedup
        self.mark_checksum(source_system, checksum)

        logger.info("Saved: %s (%d bytes)", raw_path, len(raw_content))
        return raw_path, meta_path, False
