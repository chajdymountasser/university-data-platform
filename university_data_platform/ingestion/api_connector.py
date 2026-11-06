"""
API connector — REST APIs with pagination, rate limiting, HTTP error handling.

Stores raw JSON bytes — no parsing.
Pre-configured factories for OpenAlex, Crossref, ORCID.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from university_data_platform.ingestion.base_connector import BaseConnector
from university_data_platform.metadata.schema import IngestionMetadata

logger = logging.getLogger(__name__)


class ApiConnector(BaseConnector):
    """Generic REST API connector with pagination and rate limiting."""

    def __init__(
        self,
        *,
        base_url: str,
        source_system: str,
        minio_client=None,
        connector_version: str = "1.0.0",
        rate_limit_seconds: float = 1.0,
        max_retries: int = 3,
        page_size: int = 200,
        page_param: str = "page",
        offset_param: str = "offset",
        cursor_param: Optional[str] = None,
        results_key: Optional[str] = None,
    ):
        super().__init__(minio_client=minio_client, connector_version=connector_version)
        self.base_url = base_url.rstrip("/")
        self._source_system = source_system
        self.rate_limit_seconds = rate_limit_seconds
        self.max_retries = max_retries
        self.page_size = page_size
        self.page_param = page_param
        self.offset_param = offset_param
        self.cursor_param = cursor_param
        self.results_key = results_key

    @property
    def source_type(self) -> str:
        return "api"

    @property
    def bucket_name(self) -> str:
        return "raw-json"

    def _request(self, url: str, params: Optional[Dict] = None) -> Tuple[bytes, int]:
        """Make an HTTP GET with retries and rate limiting. Returns (body, status_code)."""
        for attempt in range(1, self.max_retries + 1):
            try:
                time.sleep(self.rate_limit_seconds)
                resp = httpx.get(url, params=params, timeout=30, follow_redirects=True)
                return resp.content, resp.status_code
            except httpx.HTTPError as e:
                logger.warning("HTTP error (attempt %d/%d): %s", attempt, self.max_retries, e)
                if attempt == self.max_retries:
                    raise
                time.sleep(2 * attempt)
        return b"", 0

    def fetch(self, *, endpoint: str = "", params: Optional[Dict] = None, **kwargs) -> Tuple[bytes, IngestionMetadata]:
        """Fetch a single API endpoint. Returns (raw_json_bytes, metadata)."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        merged_params = params or {}

        body, status = self._request(url, merged_params)

        if status >= 400:
            logger.error("API error %d from %s", status, url)

        metadata = IngestionMetadata.create(
            raw_content=body,
            source_system=self._source_system,
            source_url=url,
            http_status=status,
            raw_object_path="",  # filled by save_to_minio
            content_type="application/json",
            connector_version=self.connector_version,
        )

        return body, metadata

    def fetch_paginated(
        self,
        *,
        endpoint: str = "",
        params: Optional[Dict] = None,
        max_pages: int = 10,
        **kwargs,
    ) -> List[dict]:
        """Fetch all pages and save each page as a separate record.

        Returns a list of result dicts from save_to_minio().
        """
        results = []
        url = f"{self.base_url}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        merged_params = params or {}

        page = 1
        offset = 0
        cursor = None

        while page <= max_pages:
            page_params = dict(merged_params)

            if self.cursor_param and cursor:
                page_params[self.cursor_param] = cursor
            elif self.offset_param:
                page_params[self.offset_param] = offset
            else:
                page_params[self.page_param] = page

            body, status = self._request(url, page_params)
            page_url = url  # In production, reconstruct with params

            metadata = IngestionMetadata.create(
                raw_content=body,
                source_system=self._source_system,
                source_url=page_url,
                http_status=status,
                raw_object_path="",
                content_type="application/json",
                connector_version=self.connector_version,
            )

            result = self.save_to_minio(body, metadata, extension="json", content_type="application/json")
            results.append(result)

            # Check if there are more pages
            try:
                data = json.loads(body)
            except Exception:
                break

            if self.cursor_param:
                cursor = data.get("next_cursor") or data.get("meta", {}).get("next_cursor")
                if not cursor:
                    break
            elif self.results_key:
                items = data.get(self.results_key, [])
                if not items:
                    break
                offset += len(items)
            else:
                # Try common patterns
                items = (
                    data.get("results")
                    or data.get("data")
                    or data.get("items")
                    or []
                )
                if not items:
                    break
                offset += len(items)

            page += 1

        return results


# ── Pre-configured factories ──────────────────────────────

def OpenAlexConnector(**kwargs) -> ApiConnector:
    """Factory for https://api.openalex.org/"""
    defaults = dict(
        base_url="https://api.openalex.org",
        source_system="openalex",
        rate_limit_seconds=1.0,
        page_size=200,
        page_param="page",
        results_key="results",
    )
    defaults.update(kwargs)
    return ApiConnector(**defaults)


def CrossrefConnector(**kwargs) -> ApiConnector:
    """Factory for https://api.crossref.org/"""
    defaults = dict(
        base_url="https://api.crossref.org",
        source_system="crossref",
        rate_limit_seconds=1.0,
        page_size=200,
        offset_param="offset",
        results_key="items",
    )
    defaults.update(kwargs)
    return ApiConnector(**defaults)


def OrcidConnector(**kwargs) -> ApiConnector:
    """Factory for https://pub.orcid.org/"""
    defaults = dict(
        base_url="https://pub.orcid.org",
        source_system="orcid",
        rate_limit_seconds=1.0,
        page_size=200,
    )
    defaults.update(kwargs)
    return ApiConnector(**defaults)


import json  # noqa: E402 — needed for fetch_paginated
