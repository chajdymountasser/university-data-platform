"""
Playwright scraper — full browser rendering for JS-heavy pages.

Same interface as HtmlScraper.
Graceful fallback if playwright not installed.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Tuple
from urllib.parse import urlparse

from university_data_platform.ingestion.base_connector import BaseConnector
from university_data_platform.metadata.schema import IngestionMetadata

logger = logging.getLogger(__name__)

# Source system map (same as html_scraper)
SOURCE_SYSTEM_MAP = {
    "khanacademy.org": "khanacademy_website",
    "ocw.mit.edu": "mit_ocw_website",
    "um5.ac.ma": "um5_website",
    "uca.ma": "uca_website",
    "usmba.ac.ma": "usmba_website",
    "uh2c.ac.ma": "uh2c_website",
    "usms.ac.ma": "usms_website",
    "uiz.ac.ma": "uiz_website",
    "onousc.ma": "onousc_website",
    "enssup.gov.ma": "enssup_website",
    "men.gov.ma": "men_website",
    "imist.ma": "imist_website",
    "toubkal.imist.ma": "toubkal_website",
    "data.gov.ma": "datagovma_website",
    "hcp.ma": "hcp_website",
    "wikipedia.org": "wikipedia_website",
}


def _infer_source_system(url: str) -> str:
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    for domain, system in SOURCE_SYSTEM_MAP.items():
        if domain in hostname:
            return system
    return hostname.replace(".", "_").replace("-", "_") or "unknown"


class PlaywrightScraper(BaseConnector):
    """Dynamic page scraper using Playwright for JS-heavy sites.

    Same interface as HtmlScraper.
    Graceful fallback if playwright is not installed.
    """

    def __init__(
        self,
        *,
        minio_client=None,
        connector_version: str = "1.0.0",
        timeout: int = 30000,
        wait_for_selector: Optional[str] = None,
        wait_for_load_state: str = "networkidle",
        headless: bool = True,
    ):
        super().__init__(minio_client=minio_client, connector_version=connector_version)
        self.timeout = timeout
        self.wait_for_selector = wait_for_selector
        self.wait_for_load_state = wait_for_load_state
        self.headless = headless
        self._playwright_available = self._check_playwright()

    @staticmethod
    def _check_playwright() -> bool:
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
            return True
        except ImportError:
            logger.warning("Playwright not installed. Install with: pip install playwright && playwright install chromium")
            return False

    @property
    def source_type(self) -> str:
        return "playwright"

    @property
    def bucket_name(self) -> str:
        return "raw-web-html"

    def fetch(
        self,
        *,
        url: str,
        source_system: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bytes, IngestionMetadata]:
        """Fetch fully rendered HTML from URL using Playwright.

        Returns:
            (raw_html_bytes, IngestionMetadata)
        """
        if not self._playwright_available:
            raise RuntimeError(
                "Playwright is not installed. "
                "Install with: pip install playwright && playwright install chromium"
            )

        if not source_system:
            source_system = _infer_source_system(url)

        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--single-process",
                    "--disable-software-rasterizer",
                    "--js-flags=--max-old-space-size=256",
                ],
            )
            page = browser.new_page()

            try:
                response = page.goto(url, timeout=self.timeout, wait_until=self.wait_for_load_state)

                if self.wait_for_selector:
                    page.wait_for_selector(self.wait_for_selector, timeout=self.timeout)

                raw_bytes = page.content().encode("utf-8")
                status = response.status if response else 200

                metadata = IngestionMetadata.create(
                    raw_content=raw_bytes,
                    source_system=source_system,
                    source_url=url,
                    http_status=status,
                    raw_object_path="",
                    content_type="text/html",
                    connector_version=self.connector_version,
                )

                return raw_bytes, metadata

            except Exception as e:
                logger.error("Playwright error for %s: %s", url, e)
                error_body = f"Playwright error: {e}".encode("utf-8")
                metadata = IngestionMetadata.create(
                    raw_content=error_body,
                    source_system=source_system,
                    source_url=url,
                    http_status=0,
                    raw_object_path="",
                    content_type="text/html",
                    connector_version=self.connector_version,
                )
                return error_body, metadata

            finally:
                browser.close()
