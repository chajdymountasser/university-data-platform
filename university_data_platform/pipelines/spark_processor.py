"""
Spark ETL processor — Phase 2 (CLI entry point).

Delegates all logic to the :mod:`~university_data_platform.processing` package.
Kept as a thin shim so that Airflow DAG invocations via

    spark-submit spark_processor.py {faculty_profiles|course_catalog|all}

continue to work without changes.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Ensure the workspace root is on sys.path (mounted at /opt/spark/apps)
_workspace = Path(__file__).resolve().parent.parent.parent
if str(_workspace) not in sys.path:
    sys.path.insert(0, str(_workspace))

# Load .env so GEMINI_API_KEY is available for the embedder.
# When running inside a container the env vars are already set by
# Docker compose, so a missing dotenv module is non-fatal.
try:
    from dotenv import load_dotenv
    load_dotenv(_workspace / ".env")
except ImportError:
    pass

# Re-export symbols so existing code that imports from here still works
from university_data_platform.processing.io import (            # noqa: F401
    CURATED_DB,
    HIVE_METASTORE_URL,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    RAW_BUCKETS,
    create_spark_session,
    fetch_raw_pdf_content,
    hudi_options,
    read_raw_documents,
    read_raw_html,
    read_raw_images,
    read_raw_json,
    write_to_hudi,
)
from university_data_platform.processing.pipeline import (      # noqa: F401
    FACULTY_PROFILES_SCHEMA,
    COURSE_CATALOG_SCHEMA,
    PDF_DOCUMENTS_SCHEMA,
    UNIVERSITY_NEWS_SCHEMA,
    build_faculty_profiles,
    build_course_catalog,
    build_pdf_documents,
    build_university_news,
)

logger = logging.getLogger(__name__)


# ── CLI dispatch ────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("SPARK_LOG_LEVEL", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    args = sys.argv[1:] if len(sys.argv) > 1 else []
    pipeline = "all"
    source_system = None

    i = 0
    while i < len(args):
        if args[i] == "--source_system" and i + 1 < len(args):
            raw = args[i + 1]
            source_system = None if raw == "all" else raw
            i += 2
        elif pipeline == "all":
            pipeline = args[i]
            i += 1
        else:
            i += 1

    from university_data_platform.processing.pipeline import (
        process_course_catalog,
        process_faculty_profiles,
        process_pdf_documents,
        process_university_news,
        run_all_pipelines,
    )

    pipelines = {
        "faculty_profiles": lambda: process_faculty_profiles(
            source_system=source_system,
        ),
        "course_catalog": lambda: process_course_catalog(
            source_system=source_system,
        ),
        "pdf_documents": lambda: process_pdf_documents(
            source_system=source_system,
        ),
        "university_news": lambda: process_university_news(
            source_system=source_system,
        ),
        "all": lambda: run_all_pipelines(source_system=source_system),
    }

    runner = pipelines.get(pipeline)
    if runner is None:
        print(f"Unknown pipeline: {pipeline!r}")
        print(
            f"Usage: {sys.argv[0]} "
            "{faculty_profiles|course_catalog|pdf_documents|all} "
            "[--source_system <source>]"
        )
        sys.exit(1)

    logger.info("Starting pipeline: %s | source_system: %s",
                pipeline, source_system or "all")
    runner()
    logger.info("Pipeline complete: %s", pipeline)


if __name__ == "__main__":
    main()
