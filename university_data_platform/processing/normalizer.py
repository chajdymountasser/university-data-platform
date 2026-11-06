"""
processing.normalizer — Clean, standardise, and validate fields.

Operates at both the string level (UDF-ready pure functions) and
the DataFrame level (batch normalisation steps for a pipeline).
"""

from __future__ import annotations

import logging
import re
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

# ── Regex patterns ────────────────────────────────────────────────
_EMAIL_RE = r"[a-zA-Z0-9][a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"


def _first_match(text: str | None, pattern: str) -> str | None:
    """Return the first regex match in *text*, or None."""
    if not text:
        return None
    m = re.search(pattern, text)
    return m.group(0) if m else None


# ── Field-level normalisers (pure functions, UDF-ready) ───────────

def normalize_text(text: str | None) -> str:
    """Collapse whitespace, strip leading/trailing space."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def normalize_email(email: str | None) -> str | None:
    """Lower-case and strip an email address; return None if invalid."""
    if not email:
        return None
    email = email.strip().lower()
    if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        return email
    return None


def normalize_department(dept: str | None) -> str | None:
    """Standardise common department name variations."""
    if not dept:
        return None
    d = dept.strip().lower()
    mapping = {
        "info": "informatique",
        "computer science": "informatique",
        "maths": "mathématiques",
        "physics": "physique",
        "biology": "biologie",
        "chemistry": "chimie",
    }
    return mapping.get(d, dept.strip())


def normalize_faculty_name(name: str | None) -> str | None:
    """Clean up faculty/school names."""
    if not name:
        return None
    n = name.strip()
    n = re.sub(r"\s*[-|]\s*Université.*$", "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"\s*[-|]\s*University.*$", "", n, flags=re.IGNORECASE).strip()
    return n if n else None


def normalize_field(value: Any) -> Any:
    """Generic passthrough normalizer; override for specific fields."""
    return value


# ── Field extraction from text (for filling NULL fields) ─────────

def extract_department_from_text(text: str | None) -> str | None:
    """Extract department name from text content.

    Matches patterns like:
      "Département Droit Privé"
      "Département des langues et Soft Skills"
      "Department of Computer Science"
    """
    if not text:
        return None
    # Broader terminators: Pr., Prof., Enseignant, Facult, Ecole, commas, newlines, end
    _TERM = r"(?:\s+(?:Pr\.|Prof\.|Enseignant|[Ff]acult|[EÉ]cole|[Dd]octorat|[Mm]aster|[Ll]icence|[,\n]|$))"
    patterns = [
        # "Département de/in/du/des X" — capture after preposition
        # NOTE: "des" MUST come before "de" in alternation to avoid partial match
        r"[Dd][ée]partement\s+(?:des\s+|du\s+|de\s+|d[‘’]\s*)([\w\s&éèêëàâäùûüôöîïç-]{3,}?)" + _TERM,
        # "Département X" — no preposition, direct name (uppercase start)
        r"[Dd][ée]partement\s+([A-ZÀ-Ÿ][\w\s&éèêëàâäùûüôöîïç-]{2,}?)" + _TERM,
        # English: "Department of X"
        r"[Dd]epartment\s+of\s+([\w\s&-]{3,}?)" + _TERM,
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            dept = m.group(1).strip().rstrip(".,;:")
            if len(dept) > 2:
                return dept
    return None


def extract_faculty_from_text(text: str | None) -> str | None:
    """Extract faculty/school name from text content.

    Matches: "Faculté des Sciences Juridiques...", "Faculty of Law"
    """
    if not text:
        return None
    # Terminate faculty name at department, Pr., Prof., newlines, etc.
    _FAC_TERM = r"(?:\s*[-–—,.\n]|\s+(?:[Dd][ée]partement|Department|Pr\.|Prof\.|[Dd]octorat|[Mm]aster|[Ll]icence|$))"
    patterns = [
        r"Facult[ée]\s+des?\s+([\w\séèêëàâäùûüôöîïç&-]{2,}?)" + _FAC_TERM,
        r"Faculty\s+of\s+([\w\s&-]{2,}?)" + _FAC_TERM,
        r"[EÉ]cole\s+([\w\séèêëàâäùûüôöîïç&-]{2,}?)" + _FAC_TERM,
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            fac = m.group(1).strip().rstrip(".,;:")
            if len(fac) > 2:
                return fac
    return None


def extract_instructor_from_text(text: str | None) -> str | None:
    """Extract instructor/professor name from text content.

    Matches: "Pr. ABOULHOUCINE MOHAMED", "Prof. Name", "Enseignant: Name"
    """
    if not text:
        return None
    patterns = [
        r"Pr\.?\s+([A-ZÀ-Ÿ][\w\s-]{2,}?)(?:\s+(?:Département|Department|[,\n]|$))",
        r"Prof\.?\s+([A-ZÀ-Ÿ][\w\s-]{2,}?)(?:\s+(?:Département|Department|[,\n]|$))",
        r"Enseignant(?:e)?\.?\s*:?\s+([A-ZÀ-Ÿ][\w\s-]{2,}?)(?:\s+(?:Département|Department|[,\n]|$))",
        # Fallback: "Pr." or "Prof." followed by ALL-CAPS name (common in Morocco)
        r"(?:Pr|Prof)\.?\s+([A-ZÀ-Ÿ]{2,}(?:\s+[A-ZÀ-Ÿ]{2,})+)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            name = m.group(1).strip().rstrip(".,;:")
            if len(name) > 2:
                return name
    return None


def extract_level_from_text(text: str | None) -> str | None:
    """Extract academic level from text content.

    Matches: "Licence", "Master", "Doctorat", "L1", "M1", etc.
    """
    if not text:
        return None
    patterns = [
        r"\b(Doctorat(?:e)?)\b",
        r"\b(Master[2]?)\b",
        r"\b(Licence[3]?)\b",
        r"\b([LM][123])\b",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


# ── DataFrame-level normalisation steps ───────────────────────────

def normalize_columns(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Apply :func:`normalize_text` to every string column (or a subset)."""
    if columns is None:
        columns = [
            f.name for f in df.schema.fields
            if isinstance(f.dataType, StringType)
        ]
    for col_name in columns:
        if col_name in df.columns:
            udf_norm = F.udf(lambda v: normalize_text(v), StringType())
            df = df.withColumn(col_name, udf_norm(F.col(col_name)))
    return df


def normalize_faculty_profiles(df: DataFrame) -> DataFrame:
    """Run all normalisation steps specific to faculty profiles."""
    df = normalize_columns(df, [
        "full_name", "title", "department", "faculty",
        "research_interests", "normalized_text",
    ])
    if "email" in df.columns:
        email_udf = F.udf(lambda v: normalize_email(v), StringType())
        df = df.withColumn("email", email_udf(F.col("email")))

    # ── Fill NULL fields by extracting from normalized_text ──
    if "normalized_text" in df.columns:
        text_col = F.col("normalized_text")

        # department: fill when NULL
        if "department" in df.columns:
            dept_udf = F.udf(extract_department_from_text, StringType())
            df = df.withColumn(
                "department",
                F.when(F.col("department").isNull(),
                       dept_udf(text_col))
                 .otherwise(F.col("department")),
            )

        # faculty: fill when NULL
        if "faculty" in df.columns:
            fac_udf = F.udf(extract_faculty_from_text, StringType())
            df = df.withColumn(
                "faculty",
                F.when(F.col("faculty").isNull(),
                       fac_udf(text_col))
                 .otherwise(F.col("faculty")),
            )

        # email: fill when NULL (extract from text)
        if "email" in df.columns:
            email_extract_udf = F.udf(
                lambda t: _first_match(t, _EMAIL_RE) if t else None,
                StringType(),
            )
            df = df.withColumn(
                "email",
                F.when(F.col("email").isNull(),
                       email_extract_udf(text_col))
                 .otherwise(F.col("email")),
            )

    return df


def normalize_course_catalog(df: DataFrame) -> DataFrame:
    """Run all normalisation steps specific to course catalog."""
    df = normalize_columns(df, [
        "course_name", "description", "department",
        "faculty", "instructor", "normalized_text",
    ])

    # ── Fill NULL fields by extracting from normalized_text ──
    if "normalized_text" in df.columns:
        text_col = F.col("normalized_text")

        # department: fill when NULL
        if "department" in df.columns:
            dept_udf = F.udf(extract_department_from_text, StringType())
            df = df.withColumn(
                "department",
                F.when(F.col("department").isNull(),
                       dept_udf(text_col))
                 .otherwise(F.col("department")),
            )

        # faculty: fill when NULL
        if "faculty" in df.columns:
            fac_udf = F.udf(extract_faculty_from_text, StringType())
            df = df.withColumn(
                "faculty",
                F.when(F.col("faculty").isNull(),
                       fac_udf(text_col))
                 .otherwise(F.col("faculty")),
            )

        # instructor: fill when NULL
        if "instructor" in df.columns:
            instr_udf = F.udf(extract_instructor_from_text, StringType())
            df = df.withColumn(
                "instructor",
                F.when(F.col("instructor").isNull(),
                       instr_udf(text_col))
                 .otherwise(F.col("instructor")),
            )

        # level: fill when NULL
        if "level" in df.columns:
            level_udf = F.udf(extract_level_from_text, StringType())
            df = df.withColumn(
                "level",
                F.when(F.col("level").isNull(),
                       level_udf(text_col))
                 .otherwise(F.col("level")),
            )

    return df


def normalize_university_news(df: DataFrame) -> DataFrame:
    """Run all normalisation steps specific to university news."""
    df = normalize_columns(df, ["title", "description", "normalized_text"])

    # Extract category from URL path when NULL
    if "category" in df.columns and "source_url" in df.columns:
        category_udf = F.udf(
            lambda url: _extract_news_category(url) if url else None,
            StringType(),
        )
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(),
                   category_udf(F.col("source_url")))
             .otherwise(F.col("category")),
        )

    # Fill description from first 300 chars of normalized_text when NULL
    if "description" in df.columns and "normalized_text" in df.columns:
        df = df.withColumn(
            "description",
            F.when(
                F.col("description").isNull() & F.col("normalized_text").isNotNull(),
                F.substring(F.col("normalized_text"), 1, 300),
            ).otherwise(F.col("description")),
        )

    return df


def _extract_news_category(url: str | None) -> str | None:
    """Extract a category from a news article URL path."""
    if not url:
        return None
    from urllib.parse import urlparse
    try:
        path = urlparse(url).path
    except Exception:
        return None
    # Look for common category segments in the URL path
    categories = {
        "actualites": "actualites",
        "actualites": "actualites",
        "news": "actualites",
        "annonces": "annonces",
        "annonces": "annonces",
        "evenements": "evenements",
        "events": "evenements",
        "calendrier": "calendrier",
        "emploi-du-temps": "emploi_du_temps",
        "recrutement": "recrutement",
        "inscriptions": "inscriptions",
    }
    path_lower = path.lower()
    for keyword, cat in categories.items():
        if f"/{keyword}/" in path_lower or path_lower.endswith(f"/{keyword}"):
            return cat
    return None
