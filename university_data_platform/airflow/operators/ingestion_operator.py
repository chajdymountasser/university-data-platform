"""
Airflow operators for ingestion tasks.

Each operator calls the corresponding pipeline function with
proper logging and error handling.
"""

from __future__ import annotations

from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context

from university_data_platform.pipelines.run_ingestion import (
    ingest_url_deep,
    ingest_api,
    ingest_file,
)


class HtmlIngestionOperator(BaseOperator):
    """Ingest a web page (deep scrape: HTML + images + docs + links).

    Usage in a DAG:

        scrape_um5 = HtmlIngestionOperator(
            task_id="scrape_um5",
            url="https://www.um5.ac.ma/",
            source_system="um5_website",
            max_depth=2,
        )
    """

    def __init__(
        self,
        url: str,
        source_system: Optional[str] = None,
        max_depth: int = 2,
        verify_ssl: bool = True,
        auto_playwright: Optional[bool] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.url = url
        self.source_system = source_system
        self.max_depth = max_depth
        self.verify_ssl = verify_ssl
        self.auto_playwright = auto_playwright

    def execute(self, context: Context) -> list[dict]:
        self.log.info(
            "Scraping %s (depth=%d, verify_ssl=%s, auto_playwright=%s)",
            self.url, self.max_depth, self.verify_ssl, self.auto_playwright,
        )
        result = ingest_url_deep(
            url=self.url,
            source_system=self.source_system,
            max_depth=self.max_depth,
            verify_ssl=self.verify_ssl,
            auto_playwright=self.auto_playwright,
        )
        pages = sum(1 for r in result if r.get("asset_type") == "page")
        images = sum(1 for r in result if r.get("asset_type") == "image")
        docs = sum(1 for r in result if r.get("asset_type") == "document")
        self.log.info(
            "Done: %d pages, %d images, %d documents — %d total assets",
            pages, images, docs, len(result),
        )
        return result


class ApiIngestionOperator(BaseOperator):
    """Ingest from an API source."""

    def __init__(
        self,
        target: str,
        query: str = "",
        endpoint: str = "",
        max_pages: int = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target = target
        self.query = query
        self.endpoint = endpoint
        self.max_pages = max_pages

    def execute(self, context: Context) -> list[dict]:
        self.log.info("Querying %s: %s", self.target, self.query or "(no query)")
        return ingest_api(
            target=self.target,
            query=self.query,
            endpoint=self.endpoint,
            max_pages=self.max_pages,
        )


class FileIngestionOperator(BaseOperator):
    """Ingest a local file."""

    def __init__(
        self,
        file_path: str,
        source_system: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.source_system = source_system

    def execute(self, context: Context) -> dict:
        self.log.info("Ingesting file: %s", self.file_path)
        return ingest_file(
            file_path=self.file_path,
            source_system=self.source_system,
        )


def dispatch_operator(source: dict, task_id: str, **kwargs) -> BaseOperator:
    """Create the right operator for a source config dict.

    Used internally by the DAG factory.
    """
    t = source["type"]
    if t == "html":
        return HtmlIngestionOperator(
            task_id=task_id,
            url=source["url"],
            source_system=source.get("source_system"),
            max_depth=source.get("max_depth", 2),
            verify_ssl=source.get("verify_ssl", True),
            auto_playwright=source.get("auto_playwright"),
            **kwargs,
        )
    elif t == "api":
        return ApiIngestionOperator(
            task_id=task_id,
            target=source["target"],
            query=source.get("query", ""),
            endpoint=source.get("endpoint", ""),
            max_pages=source.get("max_pages", 1),
            **kwargs,
        )
    elif t == "file":
        return FileIngestionOperator(
            task_id=task_id,
            file_path=source["path"],
            source_system=source.get("source_system"),
            **kwargs,
        )
    else:
        raise ValueError(f"Unknown source type: {t}")
