"""
processing.parser — Extract structured content from raw HTML/documents.

Transforms raw crawled HTML into structured fields (title, name, text)
and maps raw data into the target schema for each entity type.

Content extraction uses **trafilatura** which automatically:
  - Detects the main content area (removes nav, header, footer, sidebar)
  - Strips breadcrumbs, skip-links, UI chrome
  - Handles multilingual content (French, Arabic, English)
  - Normalizes whitespace and encoding

BS4 is only used as a lightweight fallback for <title> and <h1> extraction
when trafilatura cannot parse the document.
"""

from __future__ import annotations

import logging
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# ── Boilerplate removal ──────────────────────────────────────────

# Common boilerplate patterns found on Moroccan university pages
_BOILERPLATE_PATTERNS = [
    # Copyright / legal
    r"copyright\s*\d{4}[\.\s,]*tous\s+les\s+droits?\s+r[ée]serv[ée]s?.*",
    r"©\s*\d{4}.*",
    r"all\s+rights?\s+reserved.*",
    # University footer boilerplate
    r"universit[ée]\s+[\w\s]+(rabat|casablanca|mohammed\s+v|ibn\s+tofail|sidi\s+mohammed\s+ben\s+abdellah|hassan\s+ii|chorok\s+mohammed).*",
    r"(facult[ée]|fsjes|fsac|fst|flsh|fmp)\s+[\w\s]*(mohamm[ée]dia|rabat|casablanca|f[èe]s|marrakech|tanger|agadir|oujda).*",
    # Navigation / UI text
    r"(aller\s+au\s+contenu|menu|navigation|accueil|page\s+pr[ée]c[ée]dente|page\s+suivante)\s*",
    r"(partager|imprimer|envoyer\s+par\s+email|t[ée]l[ée]charger)\s*$",
    # Social / sharing
    r"(suivez[\s-]nous|facebook|twitter|linkedin|instagram)\s*[:\-]?\s*\S*\s*$",
    # Cookie / legal notices
    r"(ce\s+site\s+utilise|en\s+continuant|politique\s+de\s+confidentialit[ée]|mentions?\s+l[ée]gales?).*",
    # Date-only lines
    r"^\s*\d{1,2}\s*(janvier|f[ée]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[ée]cembre)\s+\d{4}\s*$",
    # Empty / whitespace-only lines
    r"^\s*$",
]

_BOILERPLATE_RE = re.compile("|".join(_BOILERPLATE_PATTERNS), re.IGNORECASE)


def remove_boilerplate(text: str) -> str:
    """Remove common boilerplate text (copyright, footer, navigation, etc.).

    Processes text line-by-line and removes lines matching known patterns.
    This runs AFTER trafilatura extraction to clean residual boilerplate
    that wasn't detected by the main content extractor.
    """
    if not text:
        return text
    lines = text.split("\n")
    cleaned = [line for line in lines if not _BOILERPLATE_RE.match(line.strip())]
    result = "\n".join(cleaned)
    # Collapse multiple whitespace/newlines
    result = re.sub(r"\n\s*\n", "\n", result)
    result = re.sub(r"[ \t]+", " ", result)
    return result.strip()


# ── Content extraction library ────────────────────────────────────

try:
    import trafilatura
    HAS_TRAFILATURA = True
except ImportError:
    trafilatura = None
    HAS_TRAFILATURA = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    BeautifulSoup = None
    HAS_BS4 = False


# ── Core extraction functions ─────────────────────────────────────

def extract_clean_text_from_html(html: str) -> str:
    """Extract the main content text from raw HTML.

    Uses trafilatura to intelligently detect and extract the primary
    content area, stripping all boilerplate (nav, footer, ads, etc.).

    Returns empty string when extraction fails or HTML is None.
    """
    if not html:
        return ""

    # 1. Primary: trafilatura (best content detection)
    if HAS_TRAFILATURA:
        try:
            text = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=True,
                favor_recall=True,  # prefer more content over less
                deduplicate=True,
            )
            if text:
                return remove_boilerplate(text)
        except Exception as e:
            logger.warning("trafilatura extraction failed: %s — falling back to BS4", e)

    # 2. Fallback: BS4 basic text extraction
    if HAS_BS4:
        try:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup.find_all(["script", "style", "nav", "header",
                                       "footer", "aside", "noscript"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
            text = re.sub(r"\s+", " ", text).strip()
            return remove_boilerplate(text)
        except Exception:
            pass

    # 3. Last resort: regex strip
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_title_from_html(html: str) -> str | None:
    """Extract a clean page title from the HTML document.

    Strategy:
      1. Use trafilatura metadata (title field) — already cleaned
      2. Fallback to BS4 <title> tag with university suffix stripped

    Strips trailing site-name separators (|, –, -, —) and everything after,
    including common university suffixes like "– FSJES MOHAMMEDIA".

    Returns None when the title is just a university/institution name.
    """
    if not html:
        return None

    # 1. trafilatura metadata
    if HAS_TRAFILATURA:
        try:
            meta = trafilatura.extract_metadata(html)
            if meta and meta.title:
                title = meta.title.strip()
                title = _strip_site_suffix(title)
                if _is_university_name(title):
                    return None
                return title or None
        except Exception:
            pass

    # 2. BS4 fallback
    if HAS_BS4:
        try:
            soup = BeautifulSoup(html, "html.parser")
            tag = soup.find("title")
            if tag:
                title = tag.get_text(strip=True)
                title = _strip_site_suffix(title)
                if _is_university_name(title):
                    return None
                return title or None
        except Exception:
            pass

    return None


def extract_heading_from_html(html: str) -> str | None:
    """Extract the main page heading (<h1>) from HTML.

    Uses BS4 directly — trafilatura doesn't return headings separately.
    Tries <h1> first, then <h3 class="title"> (Drupal), then any h2/h3/h4.

    Returns None when nothing meaningful is found.
    """
    if not html or not HAS_BS4:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    # Try <h1> first — skip if it's just a university/site name
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text and len(text) > 2 and not _is_university_name(text):
            return text

    # Drupal often uses <h3 class="title"> for the page heading
    h3 = soup.find("h3", class_=lambda c: c and "title" in c.split())
    if h3:
        text = h3.get_text(strip=True)
        if text:
            return text

    # Fallback: any visible heading not in nav
    _NAV = {"menu", "nav", "navigation", "header", "footer", "sidebar"}
    for tag in soup.find_all(["h2", "h3", "h4"]):
        cls = tag.attrs.get("class") if tag.attrs else None
        if cls and any(any(kw in c.lower() for kw in _NAV) for c in cls):
            continue
        text = tag.get_text(strip=True)
        if text and text not in ("Main navigation", "Secondary navigation"):
            return text

    return None


# ── Aliases for backward compatibility ───────────────────────────

def extract_text_from_html(html_content: str) -> str:
    """Strip HTML tags and return clean readable text.

    Delegates to :func:`extract_clean_text_from_html` (trafilatura).
    Kept as public alias for backward compatibility.
    """
    return extract_clean_text_from_html(html_content)


def extract_main_text_from_html(html: str) -> str:
    """Extract clean text from the main content area.

    Delegates to :func:`extract_clean_text_from_html` (trafilatura).
    Kept as public alias for backward compatibility.
    """
    return extract_clean_text_from_html(html)


def extract_name_from_title(title: str) -> str:
    """Attempt to extract a person name from an HTML page title."""
    if not title:
        return ""
    # Strip everything after "-" or "|"
    title = re.sub(r"\s*[-|]\s*.*$", "", title).strip()
    return title


# ── Private helpers ──────────────────────────────────────────────

def _strip_site_suffix(title: str) -> str:
    """Strip trailing site-name separator and everything after.

    Handles common web title conventions:
      "Page Title | Site Name"
      "Page Title – Site Name"
      "Page Title — Site Name"
      "Page Title - Site Name"

    Also strips trailing university/institution names like "– FSJES MOHAMMEDIA".
    """
    if not title:
        return title
    # Strip after |, –, —, or - followed by site name
    title = re.sub(r"\s*[|–—-]\s*\S.*$", "", title).strip()
    return title


def _is_university_name(title: str) -> bool:
    """Check if a title is just a university/institution name (not a page title)."""
    return bool(re.match(
        r"^(?:Universit[ée]\s+.+|University\s+.+|UM5|UCA|UIZ|USMBA|UH2C|ENS.*)$",
        title, re.IGNORECASE,
    ))


# ── Spark UDF wrappers ───────────────────────────────────────────

def parse_html_content(df: DataFrame) -> DataFrame:
    """Apply HTML→text extraction as UDFs over the *raw_html* column.

    Uses trafilatura (when available) to:
    - Extract clean title from metadata / <title> tag
    - Extract page heading as full_name
    - Extract main content text (stripping all boilerplate)

    When trafilatura is not available on a Spark worker the UDFs return
    *None*, so original column values are preserved via ``coalesce``.
    """
    title_udf = F.udf(extract_title_from_html, StringType())
    heading_udf = F.udf(extract_heading_from_html, StringType())
    text_udf = F.udf(extract_clean_text_from_html, StringType())

    result = df.withColumn("normalized_text", text_udf(F.col("raw_html")))

    # Overwrite title with real extraction when available, fall back to
    # the regex-from-filename value that parse_faculty_from_raw set.
    if "title" in result.columns:
        result = result.withColumn(
            "title",
            F.coalesce(title_udf(F.col("raw_html")), F.col("title")),
        )

    # Extract heading as full_name — use title (cleaner) over heading (often site name)
    if "full_name" in result.columns:
        result = result.withColumn(
            "full_name",
            F.coalesce(title_udf(F.col("raw_html")), F.col("full_name")),
        )
    else:
        result = result.withColumn(
            "full_name", title_udf(F.col("raw_html")),
        )

    # For course dataframes: populate course_name from the title
    if "course_name" in result.columns:
        result = result.withColumn(
            "course_name",
            F.coalesce(title_udf(F.col("raw_html")), F.col("course_name")),
        )

    return result


# ── Raw-to-schema mappers ─────────────────────────────────────────

def parse_faculty_from_raw(html_meta: DataFrame) -> DataFrame:
    """Map raw HTML metadata into the faculty_profiles schema columns.

    Expects columns from a metadata sidecar reader:
        source_system, source_url, content_checksum,
        extraction_timestamp, raw_object_path

    If a *raw_html* column is already present (e.g. from
    :func:`~io.fetch_raw_html_content`) it is kept; otherwise it is
    created as NULL.
    """
    if "raw_html" not in html_meta.columns:
        html_meta = html_meta.withColumn("raw_html",
                                          F.lit(None).cast(StringType()))

    return (
        html_meta
        .withColumn("record_id",
                     F.md5(F.col("source_url")))
        .withColumn("crawl_timestamp",
                     F.unix_timestamp() * 1000)
        .withColumn("title",
                     F.regexp_extract(F.col("raw_object_path"),
                                      r"/([^/]+)\.\w+$", 1))
        .withColumn("full_name",
                     F.lit(None).cast(StringType()))
        .withColumn("is_deleted", F.lit("false"))
        .withColumn("record_type", F.lit("faculty_profile"))
        .select(
            "record_id",
            "record_type",
            "source_system",
            "source_url",
            F.col("content_checksum").alias("content_hash"),
            "crawl_timestamp",
            F.col("extraction_timestamp").cast(TimestampType())
             .alias("business_timestamp"),
            "raw_html",
            F.lit(None).cast(StringType()).alias("normalized_text"),
            "title",
            "full_name",
            F.lit(None).cast(StringType()).alias("email"),
            F.lit(None).cast(StringType()).alias("department"),
            F.lit(None).cast(StringType()).alias("faculty"),
            F.lit(None).cast(ArrayType(StringType()))
             .alias("research_interests"),
            F.lit(None).cast(ArrayType(StringType())).alias("publications"),
            F.lit(None).cast(StringType()).alias("orcid_id"),
            F.lit("fr").alias("language"),
            "is_deleted",
        )
    )


# ── Field extraction helpers ─────────────────────────────────────────

_EMAIL_RE = r"[a-zA-Z0-9][a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"


def _first_match(text: str | None, pattern: str) -> str | None:
    """Return the first regex match in *text*, or None."""
    if not text:
        return None
    m = re.search(pattern, text)
    return m.group(0) if m else None


def extract_emails_from_text(df: DataFrame) -> DataFrame:
    """Extract the first email address found in *normalized_text*.

    Only fills the *email* column when it is currently NULL so that
    any existing value (e.g. from a dedicated metadata field) is kept.
    """
    extract_udf = F.udf(lambda t: _first_match(t, _EMAIL_RE), StringType())
    return df.withColumn(
        "email",
        F.when(F.col("email").isNull(),
               extract_udf(F.col("normalized_text")))
         .otherwise(F.col("email")),
    )


def parse_course_from_raw(html_meta: DataFrame) -> DataFrame:
    """Map raw HTML metadata into the course_catalog schema columns.

    If a *raw_html* column is already present it is kept, otherwise it
    is created as NULL so that :func:`parse_html_content` can extract
    normalized text downstream.
    """
    if "raw_html" not in html_meta.columns:
        html_meta = html_meta.withColumn("raw_html",
                                          F.lit(None).cast(StringType()))

    return (
        html_meta
        .withColumn("record_id",
                     F.md5(F.col("source_url")))
        .withColumn("crawl_timestamp",
                     F.unix_timestamp() * 1000)
        .withColumn("is_deleted", F.lit("false"))
        .withColumn("record_type", F.lit("course_catalog"))
        .select(
            "record_id",
            "record_type",
            "source_system",
            "source_url",
            F.col("content_checksum").alias("content_hash"),
            "crawl_timestamp",
            F.col("extraction_timestamp").cast(TimestampType())
             .alias("business_timestamp"),
            "raw_html",
            F.lit(None).cast(StringType()).alias("course_code"),
            F.lit(None).cast(StringType()).alias("course_name"),
            F.lit(None).cast(StringType()).alias("description"),
            F.lit(None).cast(StringType()).alias("department"),
            F.lit(None).cast(StringType()).alias("faculty"),
            F.lit(None).cast(StringType()).alias("instructor"),
            F.lit(None).cast(StringType()).alias("level"),
            F.lit(None).cast(StringType()).alias("credits"),
            F.lit("fr").alias("language"),
            F.lit(None).cast(StringType()).alias("normalized_text"),
            "is_deleted",
        )
    )
