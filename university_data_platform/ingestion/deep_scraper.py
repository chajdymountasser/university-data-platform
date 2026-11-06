"""
Deep scraper — full page harvest with images, documents, and internal links.

Orchestrates per starting URL:
  1. Fetch HTML page → store in raw-web-html
  2. Extract ALL internal links → recurse up to max_depth
  3. Extract ALL images → download + store in raw-images
  4. Extract ALL documents → download + store in raw-documents
  5. Every asset stored with its own .meta.json sidecar
  6. Everything linked from the raw HTML record so nothing is orphaned
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from university_data_platform.ingestion.html_scraper import (
    HtmlScraper,
    _get_extension,
    _infer_source_system,
    _is_html_url,
    _normalize_url,
    extract_documents,
    extract_images,
    extract_internal_links,
)
from university_data_platform.metadata.schema import IngestionMetadata
from university_data_platform.storage.minio_client import MinIOClient

logger = logging.getLogger(__name__)


class DeepScraper:
    """Full-page harvest: HTML, images, documents, and internal links.

    Usage:
        scraper = DeepScraper()
        results = scraper.scrape("https://www.um5.ac.ma/", max_depth=2)
    """

    def __init__(
        self,
        minio_client: Optional[MinIOClient] = None,
        connector_version: str = "1.0.0",
        request_timeout: int = 30,
        user_agent: str = "UniversityDataPlatform/2.0",
        verify_ssl: bool = True,
        request_delay: float = 0.0,
        auto_playwright: bool = False,
    ):
        self.minio = minio_client or MinIOClient()
        self.connector_version = connector_version
        self.request_timeout = request_timeout
        self.verify_ssl = verify_ssl
        self.request_delay = request_delay
        self.auto_playwright = auto_playwright

        self.html_scraper = HtmlScraper(
            minio_client=self.minio,
            connector_version=connector_version,
            timeout=request_timeout,
            user_agent=user_agent,
            verify_ssl=verify_ssl,
            auto_playwright=auto_playwright,
            request_delay=request_delay,
            request_jitter=1.0,
        )
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept-Language": "fr,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self.visited_urls: set[str] = set()
        self.results: list[dict] = []
        self._robots_cache: dict[str, RobotFileParser] = {}

    # ── Public API ───────────────────────────────────────────

    def scrape(
        self,
        url: str,
        source_system: Optional[str] = None,
        max_depth: int = 2,
    ) -> list[dict]:
        """Perform a full deep scrape starting from *url*.

        BFS through internal pages up to *max_depth*.
        At each page: store HTML, download images, download documents,
        queue internal links.

        Returns a list of result dicts (one per stored asset).
        """
        self.visited_urls.clear()
        self.results.clear()

        self.minio.create_buckets()

        domain = urlparse(url).hostname or ""
        if not source_system:
            source_system = _infer_source_system(url)

        logger.info(
            "Deep scrape start: %s  depth=%d  source=%s",
            url, max_depth, source_system,
        )

        queue: list[tuple[str, int]] = [(url, 0)]

        while queue:
            current_url, depth = queue.pop(0)
            normalized = _normalize_url(current_url)

            if normalized in self.visited_urls:
                continue
            if depth > max_depth:
                continue
            if not _is_html_url(current_url):
                continue

            self.visited_urls.add(normalized)

            if not self._can_fetch(current_url):
                logger.info("robots.txt disallows: %s", current_url)
                continue

            # HtmlScraper handles its own delay and UA rotation internally

            # ── Fetch HTML page ──────────────────────────────
            logger.info("[depth=%d] %s", depth, current_url)
            try:
                raw_html, metadata = self.html_scraper.fetch(
                    url=current_url,
                    source_system=source_system,
                )
            except Exception as e:
                logger.error("Fetch failed: %s — %s", current_url, e)
                continue

            # Non-HTML content (e.g., PDFs served without extension via /file/ URLs)
            # → store in appropriate bucket, skip media and unknown types
            if metadata.content_type and "html" not in metadata.content_type:
                from university_data_platform.ingestion.html_scraper import (
                    _get_extension,
                    DOCUMENT_EXTENSIONS,
                    IMAGE_EXTENSIONS,
                    MEDIA_EXTENSIONS,
                    SKIPPED_EXTENSIONS,
                )
                ct = metadata.content_type
                ext = _get_extension(current_url) or ct.split("/")[-1] or "bin"

                # Skip explicitly excluded file types (e.g. .dta Stata, .sav SPSS data files)
                if ext in SKIPPED_EXTENSIONS:
                    logger.info("Skipping excluded file type: %s — %s", current_url, ext)
                    continue
                # Also skip statistical data formats by content-type (e.g., /file/NNNN/ with no extension)
                if ct.startswith("application/x-spss") or ct.startswith("application/x-stata") or "spss" in ct or "stata" in ct:
                    logger.info("Skipping statistical data by content-type: %s — %s", current_url, ct)
                    continue

                if ct.startswith("application/") or ext in DOCUMENT_EXTENSIONS:
                    result = self._store_asset(
                        raw_content=raw_html, metadata=metadata,
                        asset_url=current_url, source_system=source_system,
                        asset_type="document", bucket="raw-documents",
                        extension=ext,
                    )
                elif ct.startswith("image/") or ext in IMAGE_EXTENSIONS:
                    result = self._store_asset(
                        raw_content=raw_html, metadata=metadata,
                        asset_url=current_url, source_system=source_system,
                        asset_type="image", bucket="raw-images",
                        extension=ext,
                    )
                elif ct.startswith(("video/", "audio/")) or ext in MEDIA_EXTENSIONS:
                    logger.info("Skipping media: %s — %s", current_url, ct)
                    result = None
                else:
                    logger.info("Skipping unknown content: %s — %s (%d bytes)",
                                current_url, ct, len(raw_html))
                    result = None

                if result:
                    self.results.append(result)
                continue

            # Skip CAPTCHA (418) and non-success pages
            if metadata.http_status is not None and metadata.http_status not in (200, 304, 418):
                continue
            if metadata.http_status == 418:
                logger.warning("CAPTCHA blocked, skipping: %s", current_url)
                continue

            # Use final URL (JS redirects may have been followed internally)
            final_url = metadata.source_url
            final_normalized = _normalize_url(final_url)
            self.visited_urls.add(final_normalized)
            domain = urlparse(final_url).hostname or domain

            # Store page with enriched metadata
            page_result = self._store_page(raw_html, metadata, final_url, source_system, depth)
            self.results.append(page_result)

            # ── Parse HTML for assets & links ────────────────
            try:
                soup = BeautifulSoup(raw_html, "html.parser")
            except Exception:
                continue

            # Images
            for img in extract_images(soup, final_url):
                result = self._download_asset(
                    url=img["url"],
                    parent_url=final_url,
                    source_system=source_system,
                    asset_type="image",
                    bucket="raw-images",
                )
                if result:
                    self.results.append(result)

            # Documents
            for doc in extract_documents(soup, final_url):
                result = self._download_asset(
                    url=doc["url"],
                    parent_url=final_url,
                    source_system=source_system,
                    asset_type="document",
                    bucket="raw-documents",
                )
                if result:
                    self.results.append(result)

            # Internal links for next depth
            if depth < max_depth:
                for link in extract_internal_links(soup, final_url, domain):
                    link_norm = _normalize_url(link)
                    if link_norm not in self.visited_urls:
                        queue.append((link, depth + 1))

        pages = sum(1 for r in self.results if r.get("asset_type") == "page")
        images = sum(1 for r in self.results if r.get("asset_type") == "image")
        documents = sum(1 for r in self.results if r.get("asset_type") == "document")
        logger.info(
            "Deep scrape done: %d pages, %d images, %d documents — %d total assets",
            pages, images, documents, len(self.results),
        )
        return self.results

    # ── Internal helpers ─────────────────────────────────────

    def _can_fetch(self, url: str) -> bool:
        """Check robots.txt for *url*. Assumes allowed if unreadable."""
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        if robots_url not in self._robots_cache:
            rp = RobotFileParser()
            try:
                # Use the scraper's own session (rotated UA) instead of
                # urllib's generic Python-urllib/x.x which WordPress blocks.
                resp = self.html_scraper.session.get(robots_url, timeout=10)
                if resp.status_code == 200:
                    rp.parse(resp.text.splitlines())
                    self._robots_cache[robots_url] = rp
                else:
                    logger.debug("robots.txt HTTP %d at %s — allowing", resp.status_code, robots_url)
                    return True
            except Exception as e:
                logger.debug("robots.txt unreadable at %s: %s", robots_url, e)
                return True

        return self._robots_cache[robots_url].can_fetch(
            self.html_scraper.session.headers["User-Agent"], url,
        )

    def _store_page(
        self,
        raw_html: bytes,
        metadata: IngestionMetadata,
        page_url: str,
        source_system: str,
        depth: int,
    ) -> dict:
        """Save an HTML page with enriched metadata to MinIO."""
        meta_dict = metadata.to_dict()
        meta_dict["parent_url"] = None
        meta_dict["asset_type"] = "page"
        meta_dict["file_size_bytes"] = len(raw_html)
        meta_dict["depth"] = depth

        ts = metadata.extraction_timestamp or datetime.now(timezone.utc)
        raw_path, meta_path, skipped = self.minio.save_with_metadata(
            raw_content=raw_html,
            metadata_dict=meta_dict,
            bucket="raw-web-html",
            source_system=source_system,
            record_id=metadata.record_id,
            extension="html",
            content_type="text/html",
            ts=ts,
        )

        status = "skipped_duplicate" if skipped else "ingested"
        result = {
            "record_id": metadata.record_id,
            "bucket": "raw-web-html",
            "raw_object_path": raw_path,
            "metadata_path": meta_path,
            "checksum": metadata.content_checksum,
            "status": status,
            "skipped_duplicate": skipped,
            "asset_type": "page",
            "parent_url": None,
            "source_url": page_url,
            "depth": depth,
        }
        return result

    def _store_asset(
        self,
        raw_content: bytes,
        metadata: IngestionMetadata,
        asset_url: str,
        source_system: str,
        asset_type: str,
        bucket: str,
        extension: str,
    ) -> Optional[dict]:
        """Store pre-fetched content as an asset (document, image, etc.) in MinIO.

        Used for non-HTML content discovered via BFS (e.g., PDFs served
        without a file extension). The caller is responsible for dedup.
        """
        meta_dict = metadata.to_dict()
        meta_dict["parent_url"] = None
        meta_dict["asset_type"] = asset_type
        meta_dict["file_size_bytes"] = len(raw_content)

        ct = metadata.content_type or "application/octet-stream"
        ts = metadata.extraction_timestamp or datetime.now(timezone.utc)
        raw_path, meta_path, skipped = self.minio.save_with_metadata(
            raw_content=raw_content,
            metadata_dict=meta_dict,
            bucket=bucket,
            source_system=source_system,
            record_id=metadata.record_id,
            extension=extension,
            content_type=ct,
            ts=ts,
        )

        status = "skipped_duplicate" if skipped else "ingested"
        logger.info(
            "Stored %s %s → %s (%d B) [%s]",
            asset_type, asset_url, raw_path, len(raw_content), status,
        )
        return {
            "record_id": metadata.record_id,
            "bucket": bucket,
            "raw_object_path": raw_path,
            "metadata_path": meta_path,
            "checksum": metadata.content_checksum,
            "status": status,
            "skipped_duplicate": skipped,
            "asset_type": asset_type,
            "parent_url": None,
            "source_url": asset_url,
            "file_size_bytes": len(raw_content),
        }

    def _download_asset(
        self,
        url: str,
        parent_url: str,
        source_system: str,
        asset_type: str,
        bucket: str,
    ) -> Optional[dict]:
        """Download an image or document and store in MinIO.

        Returns result dict or None on failure / duplicate.
        """
        norm = _normalize_url(url)
        if norm in self.visited_urls:
            return None
        self.visited_urls.add(norm)

        try:
            resp = self.session.get(url, timeout=self.request_timeout, verify=self.verify_ssl)
            if resp.status_code != 200:
                logger.debug("HTTP %d fetching %s (%s)", resp.status_code, url, asset_type)
                return None

            raw_bytes = resp.content
            if not raw_bytes:
                return None

            content_type = resp.headers.get("Content-Type", "application/octet-stream")
            ext = _get_extension(url).lstrip(".") or "bin"

            metadata = IngestionMetadata.create(
                raw_content=raw_bytes,
                source_system=source_system,
                source_url=url,
                http_status=200,
                raw_object_path="",
                content_type=content_type,
                connector_version=self.connector_version,
            )

            meta_dict = metadata.to_dict()
            meta_dict["parent_url"] = parent_url
            meta_dict["asset_type"] = asset_type
            meta_dict["file_size_bytes"] = len(raw_bytes)

            ts = metadata.extraction_timestamp or datetime.now(timezone.utc)
            raw_path, meta_path, skipped = self.minio.save_with_metadata(
                raw_content=raw_bytes,
                metadata_dict=meta_dict,
                bucket=bucket,
                source_system=source_system,
                record_id=metadata.record_id,
                extension=ext,
                content_type=content_type,
                ts=ts,
            )

            status = "skipped_duplicate" if skipped else "ingested"
            logger.debug(
                "%s %s → %s (%d B) [%s]",
                asset_type, url, raw_path, len(raw_bytes), status,
            )
            return {
                "record_id": metadata.record_id,
                "bucket": bucket,
                "raw_object_path": raw_path,
                "metadata_path": meta_path,
                "checksum": metadata.content_checksum,
                "status": status,
                "skipped_duplicate": skipped,
                "asset_type": asset_type,
                "parent_url": parent_url,
                "source_url": url,
                "file_size_bytes": len(raw_bytes),
            }

        except Exception as e:
            logger.warning("Failed to download %s (%s): %s", url, asset_type, e)
            return None
