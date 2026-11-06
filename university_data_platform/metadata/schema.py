"""
IngestionMetadata schema — Pydantic v2 flat model.

Flat structure is required for direct Elasticsearch indexing in Phase 2.
All field names map to Hudi table columns and Elasticsearch index fields.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


def _deterministic_uuid(source_url_or_path: str, content_checksum: str) -> str:
    """UUID5(namespace=DNS, name=f'{source_url_or_path}::{content_checksum}').

    Same content always produces the same ID.
    This becomes the stable Qdrant point ID and Hudi record key in Phase 2.
    """
    name = f"{source_url_or_path}::{content_checksum}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name))


def _sha256(data: bytes) -> str:
    """SHA-256 hex digest — becomes Hudi upsert key in Phase 2."""
    return hashlib.sha256(data).hexdigest()


class IngestionMetadata(BaseModel):
    """Flat Pydantic v2 model — no nested objects.

    Every field maps directly to:
      - A Hive partition column or table column (Phase 2)
      - An Elasticsearch index field (Phase 2)
      - A Qdrant payload field (Phase 2)
    """

    # ── Identity ──────────────────────────────────────────
    record_id: str = Field(
        description="Deterministic UUID5 from source_url_or_path + content_checksum"
    )
    content_checksum: str = Field(
        description="SHA-256 of raw content — Hudi precombine / upsert key in Phase 2"
    )

    # ── Source ────────────────────────────────────────────
    source_system: str = Field(
        description="Identifier for the source (e.g. 'um5_website', 'openalex')"
    )
    source_url: Optional[str] = Field(
        default=None,
        description="HTTP URL scraped or called"
    )
    file_path: Optional[str] = Field(
        default=None,
        description="Local file path for file-based ingestion"
    )

    # ── Ingestion context ─────────────────────────────────
    extraction_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when data was collected"
    )
    http_status: Optional[int] = Field(
        default=None,
        description="HTTP status code, None for file-based sources"
    )
    connector_version: str = Field(
        default="1.0.0",
        description="Version of the connector that collected this record"
    )

    # ── Storage ───────────────────────────────────────────
    raw_object_path: str = Field(
        description="s3a:// path where raw bytes are stored in MinIO"
    )
    content_type: str = Field(
        description="MIME type or format (e.g. 'text/html', 'application/pdf', 'application/json')"
    )

    # ── Phase 2 reserved fields ───────────────────────────
    is_reprocessed: bool = Field(
        default=False,
        description="Reserved for Hudi incremental reprocessing in Phase 2"
    )
    language: Optional[str] = Field(
        default=None,
        description="Reserved for Spark language detection in Phase 2"
    )
    normalized_text: Optional[str] = Field(
        default=None,
        description="Always None in Phase 1. Spark writes this in Phase 2."
    )

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

    def to_dict(self) -> dict:
        """Serialize to a flat dict for .meta.json sidecar and Elasticsearch."""
        d = self.model_dump(mode="json")
        d["extraction_timestamp"] = self.extraction_timestamp.isoformat()
        return d

    # ── Factory ───────────────────────────────────────────

    @classmethod
    def create(
        cls,
        *,
        raw_content: bytes,
        source_system: str,
        source_url: Optional[str] = None,
        file_path: Optional[str] = None,
        http_status: Optional[int] = None,
        raw_object_path: str = "",
        content_type: str = "",
        connector_version: str = "1.0.0",
    ) -> "IngestionMetadata":
        """Create metadata with deterministic record_id and checksum.

        The record_id is UUID5(DNS, url_or_path::checksum).
        Same inputs always yield the same ID.
        """
        content_checksum = _sha256(raw_content)
        url_or_path = source_url or file_path or raw_object_path
        record_id = _deterministic_uuid(url_or_path, content_checksum)

        return cls(
            record_id=record_id,
            content_checksum=content_checksum,
            source_system=source_system,
            source_url=source_url,
            file_path=file_path,
            http_status=http_status,
            raw_object_path=raw_object_path,
            content_type=content_type,
            connector_version=connector_version,
        )
