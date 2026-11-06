"""
File connector — local files: PDF, CSV, JSON, Office docs.

Raw bytes only — no parsing.
Auto-routes by extension to the correct bucket.
"""

from __future__ import annotations

import logging
import mimetypes
import os
from pathlib import Path
from typing import Any, Optional, Tuple

from university_data_platform.ingestion.base_connector import BaseConnector
from university_data_platform.metadata.schema import IngestionMetadata

logger = logging.getLogger(__name__)

# Extension → (bucket, content_type_hint)
EXTENSION_MAP = {
    ".pdf": ("raw-documents", "application/pdf"),
    ".docx": ("raw-documents", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ".doc": ("raw-documents", "application/msword"),
    ".xlsx": ("raw-documents", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ".xls": ("raw-documents", "application/vnd.ms-excel"),
    ".pptx": ("raw-documents", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
    ".csv": ("raw-json", "text/csv"),
    ".json": ("raw-json", "application/json"),
    ".png": ("raw-images", "image/png"),
    ".jpg": ("raw-images", "image/jpeg"),
    ".jpeg": ("raw-images", "image/jpeg"),
    ".gif": ("raw-images", "image/gif"),
    ".svg": ("raw-images", "image/svg+xml"),
    ".webp": ("raw-images", "image/webp"),
}


def _infer_bucket_and_content_type(file_path: str) -> Tuple[str, str]:
    """Auto-route file to the correct bucket based on extension."""
    ext = Path(file_path).suffix.lower()
    if ext in EXTENSION_MAP:
        return EXTENSION_MAP[ext]
    # Fallback: guess from mimetypes
    guessed, _ = mimetypes.guess_type(file_path)
    if guessed and guessed.startswith("image/"):
        return "raw-images", guessed or "application/octet-stream"
    return "raw-documents", guessed or "application/octet-stream"


def _infer_source_system(file_path: str) -> str:
    """Infer source_system from the filename (without extension)."""
    stem = Path(file_path).stem
    return stem.replace(" ", "_").replace("-", "_").lower() or "local_file"


class FileConnector(BaseConnector):
    """Reads local files as raw bytes and stores them in MinIO.

    No parsing — raw bytes only.
    Auto-routes by file extension to the correct bucket.
    """

    def __init__(
        self,
        *,
        minio_client=None,
        connector_version: str = "1.0.0",
    ):
        # bucket_name is determined per-file, so we set a default here
        self._default_bucket = "raw-documents"
        super().__init__(minio_client=minio_client, connector_version=connector_version)

    @property
    def source_type(self) -> str:
        return "file"

    @property
    def bucket_name(self) -> str:
        return self._default_bucket

    def fetch(
        self,
        *,
        file_path: str,
        source_system: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bytes, IngestionMetadata]:
        """Read a local file and build metadata.

        Returns:
            (raw_file_bytes, IngestionMetadata)
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        raw_bytes = path.read_bytes()
        bucket, content_type = _infer_bucket_and_content_type(file_path)

        if not source_system:
            source_system = _infer_source_system(file_path)

        # Temporarily override bucket for this file
        self._default_bucket = bucket

        metadata = IngestionMetadata.create(
            raw_content=raw_bytes,
            source_system=source_system,
            file_path=str(path.resolve()),
            http_status=None,
            raw_object_path="",
            content_type=content_type,
            connector_version=self.connector_version,
        )

        return raw_bytes, metadata
