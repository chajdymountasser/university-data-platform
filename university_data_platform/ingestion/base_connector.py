"""
Base connector — abstract base all connectors inherit.

No connector talks to MinIO directly — only through save_to_minio().
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from university_data_platform.metadata.schema import IngestionMetadata
from university_data_platform.storage.minio_client import MinIOClient

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Abstract base for all ingestion connectors.

    Subclasses must implement:
      - source_type: str
      - bucket_name: str
      - fetch() → Tuple[bytes, IngestionMetadata]

    The save_to_minio() method is shared — no connector talks to MinIO directly.
    """

    def __init__(
        self,
        minio_client: Optional[MinIOClient] = None,
        connector_version: str = "1.0.0",
    ):
        self.minio = minio_client or MinIOClient()
        self.connector_version = connector_version

    # ── Abstract interface ─────────────────────────────────

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Type of source: 'api', 'html', 'file', 'playwright'."""
        ...

    @property
    @abstractmethod
    def bucket_name(self) -> str:
        """MinIO bucket name for this connector type."""
        ...

    @abstractmethod
    def fetch(self, **kwargs: Any) -> Tuple[bytes, IngestionMetadata]:
        """Fetch raw content and build metadata.

        Returns:
            (raw_bytes, IngestionMetadata)
        """
        ...

    # ── Shared save ────────────────────────────────────────

    def save_to_minio(
        self,
        raw_content: bytes,
        metadata: IngestionMetadata,
        extension: str,
        content_type: str,
    ) -> dict:
        """Save raw bytes + .meta.json sidecar to MinIO.

        Handles deduplication via checksum index.
        No connector talks to MinIO directly — only through this method.

        Returns:
            { record_id, bucket, raw_object_path, metadata_path,
              checksum, status, skipped_duplicate }
        """
        # Ensure buckets exist
        self.minio.create_buckets()

        ts = metadata.extraction_timestamp or datetime.now(timezone.utc)

        raw_path, meta_path, skipped = self.minio.save_with_metadata(
            raw_content=raw_content,
            metadata_dict=metadata.to_dict(),
            bucket=self.bucket_name,
            source_system=metadata.source_system,
            record_id=metadata.record_id,
            extension=extension,
            content_type=content_type,
            ts=ts,
        )

        status = "skipped_duplicate" if skipped else "ingested"

        return {
            "record_id": metadata.record_id,
            "bucket": self.bucket_name,
            "raw_object_path": raw_path,
            "metadata_path": meta_path,
            "checksum": metadata.content_checksum,
            "status": status,
            "skipped_duplicate": skipped,
        }
