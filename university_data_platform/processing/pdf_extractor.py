"""
processing.pdf_extractor — Extract text and metadata from PDF bytes.

Uses PyMuPDF (fitz) for fast, reliable text extraction.
Designed to run inside a Spark UDF or batch Python job.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract all text from a PDF byte stream.

    Args:
        pdf_bytes: Raw PDF file bytes.

    Returns:
        Concatenated text from all pages, or empty string on failure.
    """
    if not pdf_bytes:
        return ""

    try:
        import fitz
    except ImportError:
        logger.error("PyMuPDF (fitz) not installed. Install with: pip install PyMuPDF")
        return ""

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages_text = []
        for page in doc:
            text = page.get_text()
            if text:
                pages_text.append(text)
        doc.close()
        result = "\n\n".join(pages_text)
        return result.strip()
    except Exception as e:
        logger.warning("PDF text extraction failed: %s", e)
        return ""


def extract_pdf_metadata(pdf_bytes: bytes) -> dict[str, Any]:
    """Extract PDF metadata (title, author, subject, page count).

    Args:
        pdf_bytes: Raw PDF file bytes.

    Returns:
        Dict with keys: title, author, subject, page_count, has_text.
        All string values default to empty string if missing.
    """
    try:
        import fitz
    except ImportError:
        return {"title": "", "author": "", "subject": "", "page_count": 0, "has_text": False}

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        meta = doc.metadata or {}
        page_count = len(doc)
        has_text = any(page.get_text().strip() for page in doc)
        doc.close()
        return {
            "title": meta.get("title", "") or "",
            "author": meta.get("author", "") or "",
            "subject": meta.get("subject", "") or "",
            "page_count": page_count,
            "has_text": has_text,
        }
    except Exception as e:
        logger.warning("PDF metadata extraction failed: %s", e)
        return {"title": "", "author": "", "subject": "", "page_count": 0, "has_text": False}


def pdf_to_clean_text(pdf_bytes: bytes) -> str:
    """Extract and clean PDF text: collapse whitespace, strip empty lines."""
    text = extract_pdf_text(pdf_bytes)
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text
