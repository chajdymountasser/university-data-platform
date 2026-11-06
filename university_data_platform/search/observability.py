"""
Observability — ship application and pipeline logs to Elasticsearch.

Provides:
  - ``ElasticsearchLogHandler``: a Python ``logging.Handler`` that sends
    each log record to an Elasticsearch ``platform-logs`` index.
  - ``configure_platform_logging``: one-call setup for Airflow DAGs and
    Python scripts to ship logs to ES alongside standard output.

Index mapping for logs is defined in ``es_indexer.LOGS_MAPPINGS`` with
dynamic mapping enabled so new fields from any service are accepted.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from datetime import datetime, timezone
from typing import Any

from elasticsearch import Elasticsearch

from university_data_platform.search.es_client import get_es_client
from university_data_platform.search.es_indexer import LOGS_INDEX, ensure_index

logger = logging.getLogger(__name__)


# ── Elasticsearch log handler ─────────────────────────────────────

class ElasticsearchLogHandler(logging.Handler):
    """Python logging handler that sends records to Elasticsearch.

    Each log record is indexed as a JSON document with fields:
      @timestamp, message, log_level, logger, service, host,
      and any extra custom fields passed via the ``extra`` parameter.

    Buffers records and ships in batches to reduce HTTP overhead.

    Example::

        handler = ElasticsearchLogHandler(service="airflow-scheduler")
        logger.addHandler(handler)
        logger.warning("Task failed", extra={"dag_id": "my_dag", "task_id": "my_task"})
    """

    def __init__(
        self,
        service: str = "university-platform",
        level: int = logging.INFO,
        batch_size: int = 50,
        flush_interval: float = 5.0,
        es_client: Elasticsearch | None = None,
    ):
        super().__init__(level)
        self.service = service
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer: list[dict[str, Any]] = []
        self._last_flush = time.monotonic()
        self._es_client = es_client

    @property
    def es_client(self) -> Elasticsearch:
        if self._es_client is None:
            self._es_client = get_es_client()
        return self._es_client

    def emit(self, record: logging.LogRecord) -> None:
        """Queue a log record for batch shipping."""
        try:
            doc = self._format_record(record)
            self._buffer.append(doc)

            # Flush if buffer is full or enough time has passed
            if len(self._buffer) >= self.batch_size or (
                time.monotonic() - self._last_flush > self.flush_interval
            ):
                self.flush()
        except Exception:
            self.handleError(record)

    def _format_record(self, record: logging.LogRecord) -> dict[str, Any]:
        """Format a LogRecord into an ES document."""
        message = self.format(record)

        doc: dict[str, Any] = {
            "@timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "message": message,
            "log_level": record.levelname,
            "logger": record.name,
            "service": self.service,
            "host": socket.gethostname(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Include any extra custom fields from the record
        for key, value in record.__dict__.items():
            if key not in (
                "args", "asctime", "created", "exc_info", "exc_text",
                "filename", "funcName", "levelname", "levelno",
                "lineno", "module", "msecs", "message", "msg",
                "name", "pathname", "process", "processName",
                "relativeCreated", "stack_info", "thread", "threadName",
            ):
                try:
                    json.dumps(value)
                    doc[key] = value
                except (TypeError, ValueError):
                    doc[key] = str(value)

        return doc

    def flush(self) -> None:
        """Send buffered log records to Elasticsearch."""
        if not self._buffer:
            return

        batch = self._buffer
        self._buffer = []
        self._last_flush = time.monotonic()

        try:
            # Ensure the logs index exists
            ensure_index(LOGS_INDEX, self.es_client)

            # Bulk index
            bulk_body: list[dict[str, Any]] = []
            for doc in batch:
                bulk_body.append({"index": {"_index": LOGS_INDEX}})
                bulk_body.append(doc)

            if bulk_body:
                self.es_client.bulk(body=bulk_body, refresh=False)
        except Exception as e:
            # Don't let logging failures cause more logging failures
            self._buffer.extend(batch)
            print(f"ES log shipping failed: {e}", file=__import__("sys").stderr)

    def close(self) -> None:
        """Flush remaining logs and close."""
        self.flush()
        super().close()


# ── Convenience configuration ─────────────────────────────────────

def configure_platform_logging(
    service: str = "university-platform",
    log_level: str | int = logging.INFO,
    use_es: bool = True,
    verbose: bool = False,
) -> logging.Logger:
    """Configure Python logging with console output and optional ES shipping.

    Args:
        service: Service name for ES log documents.
        log_level: Logging level (default: INFO).
        use_es: If True, add the ElasticsearchLogHandler.
        verbose: If True, set log_level to DEBUG.

    Returns:
        The root logger.
    """
    level = logging.DEBUG if verbose else (
        log_level if isinstance(log_level, int) else getattr(logging, log_level.upper(), logging.INFO)
    )

    root = logging.getLogger()
    root.setLevel(level)

    # Console handler (always)
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        console = logging.StreamHandler()
        console.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        console.setFormatter(formatter)
        root.addHandler(console)

    # ES handler
    if use_es:
        es_handler = ElasticsearchLogHandler(service=service)
        es_handler.setLevel(level)
        # Don't add if already present with the same service name
        existing_es = [
            h for h in root.handlers
            if isinstance(h, ElasticsearchLogHandler) and h.service == service
        ]
        if not existing_es:
            root.addHandler(es_handler)
            logger.info("Elasticsearch log shipping enabled (service=%s)", service)

    return root


# ── CLI test ──────────────────────────────────────────────────────

def main() -> None:
    """Quick test: ship a few log lines to ES."""
    configure_platform_logging(service="observability-test", verbose=True)
    test_logger = logging.getLogger("observability.test")

    test_logger.info("Platform observability test — info message")
    test_logger.warning("Platform observability test — warning message")
    test_logger.error("Platform observability test — error message")

    # Flush ES handler
    for handler in logging.getLogger().handlers:
        if isinstance(handler, ElasticsearchLogHandler):
            handler.flush()
            handler.close()

    print("Test log entries sent. Check Kibana at http://localhost:5601")


if __name__ == "__main__":
    main()
