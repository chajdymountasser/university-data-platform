"""
Pipeline runner — CLI + programmatic API.

CLI (typer):
  python -m pipelines.run_ingestion --source html --url https://www.um5.ac.ma/
  python -m pipelines.run_ingestion --source api --target openalex --query "university Morocco"
  python -m pipelines.run_ingestion --source file --path ./data/syllabus.pdf
  python -m pipelines.run_ingestion --source batch --config sources.yml

Programmatic API (Airflow calls these in Phase 3):
  ingest_url(url, source_system)          → dict
  ingest_api(target, query, **kwargs)     → dict
  ingest_file(file_path, source_system)   → dict
  ingest_batch(config_path)               → list[dict]

Each call returns:
  { record_id, bucket, raw_object_path, checksum, status, skipped_duplicate }
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from dotenv import load_dotenv

from university_data_platform.ingestion.api_connector import ApiConnector, OpenAlexConnector, CrossrefConnector, OrcidConnector
from university_data_platform.ingestion.file_connector import FileConnector
from university_data_platform.ingestion.html_scraper import HtmlScraper
from university_data_platform.storage.minio_client import MinIOClient

# Load .env if present
load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("INGESTION_LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="University Data Platform — Ingestion Pipeline (Phase 1)")


def _get_version() -> str:
    return os.getenv("CONNECTOR_VERSION", "1.0.0")


# ── Programmatic API ──────────────────────────────────────

def ingest_url(
    url: str,
    source_system: Optional[str] = None,
    bucket: str = "raw-web-html",
    connector_version: str = "1.0.0",
) -> dict:
    """Ingest a single URL using the HTML scraper (page only).

    Args:
        url: The URL to scrape.
        source_system: Override the auto-detected source system name.
        bucket: MinIO bucket (default: raw-web-html).
        connector_version: Connector version string.

    Returns:
        { record_id, bucket, raw_object_path, metadata_path,
          checksum, status, skipped_duplicate }
    """
    minio = MinIOClient()
    minio.create_buckets()
    scraper = HtmlScraper(minio_client=minio, connector_version=connector_version)
    raw_bytes, metadata = scraper.fetch(url=url, source_system=source_system)
    result = scraper.save_to_minio(raw_bytes, metadata, extension="html", content_type="text/html")
    return result


def ingest_url_deep(
    url: str,
    source_system: Optional[str] = None,
    max_depth: int = 2,
    connector_version: str = "1.0.0",
    verify_ssl: bool = True,
    request_delay: float = 0.0,
    auto_playwright: bool | None = None,
) -> list[dict]:
    """Deep scrape: page HTML + images + documents + internal links.

    Follows internal links up to *max_depth*, extracts every image and
    document from every page, and stores each asset with its own
    .meta.json sidecar in the appropriate MinIO bucket.

    Args:
        url: Starting URL.
        source_system: Source system identifier (auto-detected if None).
        max_depth: How many levels of internal links to follow (default 2).
        connector_version: Connector version string.
        verify_ssl: Whether to verify SSL certificates.
        request_delay: Seconds to wait between requests (from env REQUEST_DELAY if not set).
        auto_playwright: If True, fallback to Playwright when JS-heavy detected.
                         Falls back to env AUTO_PLAYWRIGHT if None.

    Returns:
        List of result dicts (one per stored asset).
    """
    from university_data_platform.ingestion.deep_scraper import DeepScraper

    if request_delay <= 0:
        request_delay = float(os.getenv("REQUEST_DELAY", "0"))
    if auto_playwright is None:
        auto_playwright = os.getenv("AUTO_PLAYWRIGHT", "").lower() in ("true", "1", "yes")

    scraper = DeepScraper(
        connector_version=connector_version,
        verify_ssl=verify_ssl,
        request_delay=request_delay,
        auto_playwright=auto_playwright,
    )
    return scraper.scrape(url=url, source_system=source_system, max_depth=max_depth)


def ingest_api(
    target: str,
    query: str = "",
    endpoint: str = "",
    max_pages: int = 1,
    connector_version: str = "1.0.0",
) -> List[dict]:
    """Ingest from an API source.

    Args:
        target: One of 'openalex', 'crossref', 'orcid'.
        query: Search query string.
        endpoint: API endpoint path (appended to base URL).
        max_pages: Maximum number of pages to fetch.
        connector_version: Connector version string.

    Returns:
        List of result dicts from save_to_minio().
    """
    minio = MinIOClient()
    minio.create_buckets()

    version = connector_version

    if target == "openalex":
        connector = OpenAlexConnector(minio_client=minio, connector_version=version)
        params = {"search": query, "per-page": 200} if query else {"per-page": 200}
        return connector.fetch_paginated(
            endpoint=endpoint or "works",
            params=params,
            max_pages=max_pages,
        )
    elif target == "crossref":
        connector = CrossrefConnector(minio_client=minio, connector_version=version)
        params = {"query": query, "rows": 200} if query else {"rows": 200}
        return connector.fetch_paginated(
            endpoint=endpoint or "works",
            params=params,
            max_pages=max_pages,
        )
    elif target == "orcid":
        connector = OrcidConnector(minio_client=minio, connector_version=version)
        params = {"q": query} if query else {}
        body, metadata = connector.fetch(endpoint=endpoint or "search", params=params)
        result = connector.save_to_minio(body, metadata, extension="json", content_type="application/json")
        return [result]
    else:
        raise ValueError(f"Unknown API target: {target}. Choose from: openalex, crossref, orcid")


def ingest_file(
    file_path: str,
    source_system: Optional[str] = None,
    connector_version: str = "1.0.0",
) -> dict:
    """Ingest a local file.

    Args:
        file_path: Path to the local file.
        source_system: Override the auto-detected source system name.
        connector_version: Connector version string.

    Returns:
        { record_id, bucket, raw_object_path, metadata_path,
          checksum, status, skipped_duplicate }
    """
    minio = MinIOClient()
    minio.create_buckets()
    connector = FileConnector(minio_client=minio, connector_version=connector_version)
    raw_bytes, metadata = connector.fetch(file_path=file_path, source_system=source_system)

    ext = Path(file_path).suffix.lstrip(".") or "bin"
    result = connector.save_to_minio(raw_bytes, metadata, extension=ext, content_type=metadata.content_type)
    return result


def ingest_batch(
    config_path: str,
    connector_version: str = "1.0.0",
) -> List[dict]:
    """Ingest from a batch config file (YAML).

    Config format:
        sources:
          - type: html
            url: https://www.um5.ac.ma/
            source_system: um5_website
          - type: api
            target: openalex
            query: "university Morocco"
            max_pages: 3
          - type: file
            path: ./data/syllabus.pdf

    Returns:
        List of result dicts.
    """
    import yaml

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config = yaml.safe_load(config_file.read_text())
    sources = config.get("sources", [])
    results: List[dict] = []

    for source in sources:
        source_type = source.get("type", "html")
        try:
            if source_type == "html":
                if source.get("deep", True):
                    results.extend(ingest_url_deep(
                        url=source["url"],
                        source_system=source.get("source_system"),
                        max_depth=source.get("max_depth", 2),
                        connector_version=connector_version,
                    ))
                else:
                    result = ingest_url(
                        url=source["url"],
                        source_system=source.get("source_system"),
                        connector_version=connector_version,
                    )
                    results.append(result)
            elif source_type == "api":
                api_results = ingest_api(
                    target=source["target"],
                    query=source.get("query", ""),
                    endpoint=source.get("endpoint", ""),
                    max_pages=source.get("max_pages", 1),
                    connector_version=connector_version,
                )
                results.extend(api_results)
            elif source_type == "file":
                result = ingest_file(
                    file_path=source["path"],
                    source_system=source.get("source_system"),
                    connector_version=connector_version,
                )
                results.append(result)
            else:
                logger.warning("Unknown source type: %s", source_type)
        except Exception as e:
            logger.error("Failed to ingest source %s: %s", source, e)
            results.append({"status": "error", "error": str(e), "source": source})

    return results


# ── CLI ───────────────────────────────────────────────────

@app.command("ingest")
def cli_ingest(
    source: str = typer.Option(..., "--source", "-s", help="Source type: api, html, file, batch, playwright"),
    url: Optional[str] = typer.Option(None, "--url", "-u", help="URL to scrape (html/playwright)"),
    file_path: Optional[str] = typer.Option(None, "--path", "-p", help="Local file path (file)"),
    target: Optional[str] = typer.Option(None, "--target", "-t", help="API target: openalex, crossref, orcid"),
    query: Optional[str] = typer.Option(None, "--query", "-q", help="Search query (api)"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Batch config YAML path (batch)"),
    source_system: Optional[str] = typer.Option(None, "--source-system", help="Override source system name"),
    max_pages: int = typer.Option(1, "--max-pages", "-n", help="Max API pages to fetch"),
    max_depth: int = typer.Option(2, "--max-depth", "-d", help="Max link depth for deep scrape"),
    deep: bool = typer.Option(False, "--deep", help="Deep scrape (images + docs + internal links)"),
    endpoint: Optional[str] = typer.Option("", "--endpoint", "-e", help="API endpoint path"),
):
    """Run an ingestion pipeline."""
    version = _get_version()

    if source == "html":
        if not url:
            typer.echo("Error: --url is required for html source", err=True)
            raise typer.Exit(1)
        if deep:
            results = ingest_url_deep(url=url, source_system=source_system, max_depth=max_depth, connector_version=version)
            for r in results:
                _print_result(r)
        else:
            result = ingest_url(url=url, source_system=source_system, connector_version=version)
            _print_result(result)

    elif source == "playwright":
        if not url:
            typer.echo("Error: --url is required for playwright source", err=True)
            raise typer.Exit(1)
        from university_data_platform.ingestion.playwright_scraper import PlaywrightScraper
        minio = MinIOClient()
        minio.create_buckets()
        scraper = PlaywrightScraper(minio_client=minio, connector_version=version)
        raw_bytes, metadata = scraper.fetch(url=url, source_system=source_system)
        result = scraper.save_to_minio(raw_bytes, metadata, extension="html", content_type="text/html")
        _print_result(result)

    elif source == "api":
        if not target:
            typer.echo("Error: --target is required for api source", err=True)
            raise typer.Exit(1)
        results = ingest_api(
            target=target,
            query=query or "",
            endpoint=endpoint or "",
            max_pages=max_pages,
            connector_version=version,
        )
        for r in results:
            _print_result(r)

    elif source == "file":
        if not file_path:
            typer.echo("Error: --path is required for file source", err=True)
            raise typer.Exit(1)
        result = ingest_file(file_path=file_path, source_system=source_system, connector_version=version)
        _print_result(result)

    elif source == "batch":
        if not config:
            typer.echo("Error: --config is required for batch source", err=True)
            raise typer.Exit(1)
        results = ingest_batch(config_path=config, connector_version=version)
        for r in results:
            _print_result(r)

    else:
        typer.echo(f"Error: unknown source type '{source}'. Choose from: api, html, file, batch, playwright", err=True)
        raise typer.Exit(1)


def _print_result(result: dict) -> None:
    """Pretty-print an ingestion result."""
    typer.echo("─" * 50)
    for key, value in result.items():
        typer.echo(f"  {key}: {value}")
    typer.echo("─" * 50)


# ── Entry point ───────────────────────────────────────────

if __name__ == "__main__":
    app()
