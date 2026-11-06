"""
processing.classifier — Rule-based page classifier for HTML content.

Heuristics use URL path patterns, page title, and body text to decide whether
a page is a *faculty profile*, a *course catalog entry*, or *other*.

Scoring
-------
Each indicator contributes a positive or negative score.  Whichever type
reaches the higher total wins.  This avoids brittle binary checks and is
easy to extend when new patterns appear.

Scoring threshold:
  12+ → confident classification  (strong match)
   6+ → weak match (at least some signals)
  <6  → "other" / unclassified
"""

from __future__ import annotations

import logging
import os
import re
from urllib.parse import urlparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

# ── Score threshold ──────────────────────────────────────────────
SCORE_THRESHOLD = 8


# ── URL path patterns ───────────────────────────────────────────

_FACULTY_PATH_PATTERNS = [
    r"/prof/", r"/professeur", r"/enseignant", r"/staff/", r"/faculty/",
    r"/cv/", r"/membre", r"/personnel", r"/equipe", r"/team/",
    r"/chercheur", r"/doctorant", r"/page-personnelle",
    r"/mon_profil", r"/profil/", r"/fichier_enseignement",
]

_COURSE_PATH_PATTERNS = [
    r"/course/", r"/cours/", r"/matiere", r"/module", r"/formation",
    r"/licence", r"/master", r"/semestre", r"/filiere", r"/programme",
    r"/td/", r"/tp/", r"/ressource",
    r"/syllabus", r"/curriculum", r"/maquette",
]


# ── Title / <h1> indicators ──────────────────────────────────────

_FACULTY_TITLE_WORDS = [
    r"professeur", r"enseignant", r"enseignante", r"doctorant",
    r"pro(e?sseur)?", r"dr\.?\b", r"pr\.?\b",
    r"profil", r"curriculum vitae", r"cv ",
    r"research", r"recherche", r"publications",
    r"membre.*equipe", r"encadrement",
]

_COURSE_TITLE_WORDS = [
    r"cours", r"formation", r"module", r"matière", r"matières",
    r"programme", r"licence", r"master", r"semestre",
    r"objectifs?", r"prérequis", r"prerequis",
    r"descriptif", r"contenu", r"plan.*cours", r"syllabus",
    r"emploi.*temps", r"volume.*horaire",
    r"fiche.*matière", r"fiche.*module",
]


# ── Body text indicators (scored lower — less reliable) ──────────

_FACULTY_BODY_WORDS = [
    r"\bemail\b", r"@.*\.[a-z]{2,}",       # email pattern
    r"\bdépartement\b", r"\bdepartement\b", r"\bdepartment\b",
    r"\blaboratoire\b", r"\béquipe\b", r"\bequipe\b",
    r"\brecherche\b", r"\bresearch\b",
    r"\bpublications\b", r"\bpublis\b",
    r"\bencadrement\b", r"\bthèse\b", r"\bthese\b",
    r"\borcid\b", r"\bhal\b",
    r"\bgrade\b", r"\btitres?\b",
    r"\bbureau\b", r"\btéléphone\b", r"\btelephone\b",
]

_COURSE_BODY_WORDS = [
    r"\bcrédits?\b", r"\bcredits?\b", r"\bects\b",
    r"\bsemestre\b", r"\bmodule\b",
    r"\bvolume horaire\b", r"\bvolume horaire\b",
    r"\brégime\b", r"\bregime\b",
    r"\bévaluation\b", r"\bevaluation\b", r"\bexamen\b",
    r"\btd\b", r"\btp\b", r"\bcours\b",
    r"\bprérequis?\b", r"\bprerequis?\b",
    r"\bobjectifs?\b", r"\bcompétences?\b", r"\bcompetences?\b",
    r"\bcoefficient\b", r"\bcoeff\b",
]


# ── News / announcement indicators (negative signal) ──────────────
# Pages matching these patterns are likely news, announcements, or
# general university info — NOT faculty profiles or course catalogs.

_NEWS_BODY_WORDS = [
    r"\bavis\s+etudiant", r"\bavis\s+aux\s+étudiant",
    r"\btélécharg", r"\bdownload", r"\btéléchargez",
    r"\bdate\s+limite", r"\bdate\s+butoir",
    r"\blundi\b", r"\bmardi\b", r"\bmercredi\b",
    r"\bjeudi\b", r"\bvendredi\b", r"\bsamedi\b", r"\bdimanche\b",
    r"\bactualités?\b", r"\bactualite", r"\bnews\b",
    r"\bévènement", r"\bevenement", r"\bevents?\b",
    r"\binscription", r"\bconvocation", r"\brecours",
    r"\bconcours", r"\bacadémie", r"\bacademie",
    r"\bcalendrier", r"\brendez[- ]?vous",
    r"\bjob\b", r"\boffre\s+d", r"\brecrutement",
    r"\bprocédure", r"\bprocedure",
]

_NEWS_PATH_PATTERNS = [
    r"/actualites?/", r"/actualites?/", r"/news/",
    r"/evenement", r"/events?/",
    r"/annonces?/", r"/annonces?/",
    r"/inscription/", r"/calendrier",
    r"/recrutement/", r"/offres?/",
    r"/emploi-du-temps", r"/emploi.du.temps",
]

_NEWS_TITLE_WORDS = [
    r"actualit", r"actualites",
    r"annonce", r"annonces",
    r"événement", r"evenement", r"events?",
    r"nouvelle", r"nouvelles",
    r"avis\s+etudiant", r"avis\s+aux\s+étudiant",
    r"calendrier", r"emploi.*temps",
    r"recrutement", r"offre\s+d",
    r"inscription", r"convocation",
    r"concours", r"procédure",
]


# ── Scoring helpers ──────────────────────────────────────────────

def _check_patterns(text: str, patterns: list[str], score: int = 4) -> int:
    """Return *score* for each pattern matched in *text*."""
    if not text:
        return 0
    total = 0
    for pat in patterns:
        if re.search(pat, text, re.IGNORECASE):
            total += score
    return total


def _check_wordlist(text: str, words: list[str], score: int = 2) -> int:
    """Return *score* for each word matched in *text*."""
    return _check_patterns(text, words, score)


# ── Public classification functions ──────────────────────────────

def classify_page(
    url: str,
    normalized_text: str,
    page_title: str = "",
) -> str:
    """Classify a single HTML page as faculty, course, news, or other.

    Args:
        url: The page URL (used for path pattern matching).
        normalized_text: Clean body text (HTML stripped).
        page_title: The page title (usually extracted from <title> or <h1>).

    Returns:
        ``"faculty_profile"``, ``"course_catalog"``, ``"university_news"``,
        or ``"other"``.
    """
    faculty_score = 0
    course_score = 0

    # 1. URL path patterns (strong signal)
    try:
        path = urlparse(url).path
    except Exception:
        path = url

    faculty_score += _check_patterns(path, _FACULTY_PATH_PATTERNS, score=6)
    course_score += _check_patterns(path, _COURSE_PATH_PATTERNS, score=6)

    # 2. Page title (strong signal)
    text_for_title = page_title or (normalized_text or "")[:300]

    faculty_score += _check_wordlist(text_for_title, _FACULTY_TITLE_WORDS, score=4)
    course_score += _check_wordlist(text_for_title, _COURSE_TITLE_WORDS, score=4)

    # 3. Body text (weaker signal)
    if normalized_text:
        faculty_score += _check_wordlist(normalized_text, _FACULTY_BODY_WORDS, score=2)
        course_score += _check_wordlist(normalized_text, _COURSE_BODY_WORDS, score=2)

    # 4. News / announcement positive signal
    news_score = 0
    news_score += _check_patterns(path, _NEWS_PATH_PATTERNS, score=6)
    news_score += _check_wordlist(text_for_title, _NEWS_TITLE_WORDS, score=4)
    if normalized_text:
        news_score += _check_wordlist(normalized_text, _NEWS_BODY_WORDS, score=2)

    # 5. News penalty on faculty/course (news pages shouldn't be faculty/course)
    news_penalty = 0
    news_penalty += _check_patterns(path, _NEWS_PATH_PATTERNS, score=6)
    if normalized_text:
        news_penalty += _check_wordlist(normalized_text, _NEWS_BODY_WORDS, score=3)
    faculty_score = max(0, faculty_score - news_penalty)
    course_score = max(0, course_score - news_penalty)

    # Decision — news takes priority when it has strong signals
    if news_score >= SCORE_THRESHOLD and news_score > faculty_score and news_score > course_score:
        return "university_news"
    if faculty_score >= SCORE_THRESHOLD and faculty_score > course_score:
        return "faculty_profile"
    if course_score >= SCORE_THRESHOLD and course_score > faculty_score:
        return "course_catalog"
    if faculty_score >= SCORE_THRESHOLD and course_score >= SCORE_THRESHOLD:
        return "faculty_profile" if faculty_score >= course_score else "course_catalog"

    return "other"


def classify_dataframe(
    df: DataFrame,
    url_col: str = "source_url",
    text_col: str = "normalized_text",
    title_col: str = "title",
) -> DataFrame:
    """Add a *record_type* column to a Spark DataFrame.

    Each row is classified via :func:`classify_page` and the result
    stored in ``record_type`` (``"faculty_profile"``, ``"course_catalog"``,
    or ``"other"``).

    If *title_col* does not exist in the DataFrame, an empty string is
    used for the title parameter (classification falls back to URL and body).
    """
    if title_col not in df.columns:
        logger.warning("Column '%s' not found — using empty title for classification", title_col)
        title_expr = F.lit("")
    else:
        title_expr = F.col(title_col)

    classify_udf = F.udf(
        lambda u, t, ti: classify_page(u or "", t or "", ti or ""),
        StringType(),
    )
    return df.withColumn(
        "record_type",
        classify_udf(F.col(url_col), F.col(text_col), title_expr),
    )


def record_type_exists(df: DataFrame) -> bool:
    """Check if *record_type* column is already present."""
    return "record_type" in df.columns


# ── Semantic anchor-based classifier ─────────────────────────────
# Uses sentence-transformers embeddings to classify pages by meaning
# instead of keywords.  Requires no training data — uses hand-crafted
# "anchor" texts that describe each category.

import numpy as np

_semantic_model = None  # lazy-loaded sentence-transformers model

# Anchor texts per category — each group has multiple descriptions
# covering different aspects of the category.
_ANCHOR_TEXTS: dict[str, list[str]] = {
    "faculty_profile": [
        "Professeur d'université, département, faculté, email, bureau, téléphone",
        "Chercheur, publications scientifiques, articles, revues, conférences",
        "Thèse de doctorat, encadrement, supervision, laboratoire de recherche",
        "Curriculum vitae, parcours académique, grade universitaire, HDR",
        "Profil personnel d'un enseignant-chercheur universitaire",
        "ORCID, HAL, Google Scholar, indice h, citations scientifiques",
        "Biographie d'un professeur, parcours d'enseignement et de recherche",
    ],
    "course_catalog": [
        "Module universitaire, semestre, programme d'études, maquette pédagogique",
        "Cours magistral, travaux dirigés, travaux pratiques, volume horaire",
        "Crédits ECTS, coefficient, prérequis, objectifs pédagogiques",
        "Syllabus, plan de cours, contenu pédagogique, compétences visées",
        "Fiche matière, descriptif de cours, évaluation, examen",
        "Licence, master, filière, spécialisation, parcours universitaire",
        "Emploi du temps, calendrier des cours, salle de cours",
    ],
    "university_news": [
        "Actualité universitaire, annonce officielle, information importante",
        "Événement, colloque, séminaire, conférence, atelier, journée d'étude",
        "Avis aux étudiants, convocation, inscription, calendrier des examens",
        "Concours de recrutement, offre d'emploi, appel à candidatures",
        "Résultats, listes de candidats, délibération, décret",
        "Nouvelle universitaire, communication officielle, communiqué de presse",
        "Calendrier académique, rentrée universitaire, vacances, session d'examens",
    ],
}

# Pre-computed anchor vectors (populated on first use)
_anchor_vectors: dict[str, np.ndarray] = {}
_anchor_initialized = False


def _get_semantic_model():
    """Lazy-load the sentence-transformers model."""
    global _semantic_model
    if _semantic_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            model_name = os.getenv(
                "LOCAL_EMBEDDING_MODEL", "paraphrase-multilingual-mpnet-base-v2"
            )
            # Use /tmp as cache to avoid permission issues in containers
            # (default ~/.cache/huggingface may not be writable, e.g. /home/spark)
            cache_dir = os.getenv("HF_HOME", "/tmp/hf_cache")
            os.makedirs(cache_dir, exist_ok=True)
            _semantic_model = SentenceTransformer(model_name, cache_folder=cache_dir)
            logger.info("Loaded semantic classifier model: %s (cache: %s)", model_name, cache_dir)
        except Exception as e:
            logger.warning("Could not load semantic classifier model: %s", e)
            _semantic_model = False
    return _semantic_model if _semantic_model else None


def _init_anchors():
    """Encode anchor texts once (on first call)."""
    global _anchor_vectors, _anchor_initialized
    if _anchor_initialized:
        return
    model = _get_semantic_model()
    if model is None:
        return
    for label, texts in _ANCHOR_TEXTS.items():
        vecs = model.encode(texts, normalize_embeddings=True)
        _anchor_vectors[label] = vecs  # shape: (N, 768)
    _anchor_initialized = True
    logger.info(
        "Semantic anchors initialized: %s",
        {k: len(v) for k, v in _anchor_vectors.items()},
    )


def classify_page_semantic(
    url: str,
    normalized_text: str,
    page_title: str = "",
) -> str:
    """Classify a page using semantic similarity to anchor texts.

    Uses sentence-transformers to embed the page text and compare
    against pre-defined anchor texts for each category.

    Falls back to ``"other"`` if the model is unavailable or the
    text is empty.
    """
    _init_anchors()
    model = _get_semantic_model()
    if model is None or not _anchor_vectors:
        return "other"

    # Combine title + text for richer embedding
    text = " ".join(filter(None, [page_title, normalized_text]))
    if not text.strip():
        return "other"

    # Truncate to 512 tokens (model limit) — keep first 2000 chars
    text = text[:2000]

    # Embed the page
    try:
        page_vec = model.encode([text], normalize_embeddings=True)[0]
    except Exception as e:
        logger.warning("Semantic embedding failed: %s", e)
        return "other"

    # Compare with each anchor group — use max similarity within group
    best_label = "other"
    best_score = 0.0
    for label, anchor_vecs in _anchor_vectors.items():
        # Cosine similarity (vectors already normalized → dot product)
        sims = anchor_vecs @ page_vec
        max_sim = float(sims.max())
        if max_sim > best_score:
            best_score = max_sim
            best_label = label

    # Threshold: if best match is too weak, classify as "other"
    if best_score < 0.25:
        return "other"

    return best_label


def classify_page_semantic_scores(
    url: str,
    normalized_text: str,
    page_title: str = "",
) -> tuple[str, float, float, float, float]:
    """Like :func:`classify_page_semantic` but returns scores per class.

    Returns:
        (label, faculty_score, course_score, news_score, other_score)
        where scores are normalised 0–1.
    """
    _init_anchors()
    model = _get_semantic_model()
    if model is None or not _anchor_vectors:
        return ("other", 0.0, 0.0, 0.0, 1.0)

    text = " ".join(filter(None, [page_title, normalized_text]))
    if not text.strip():
        return ("other", 0.0, 0.0, 0.0, 1.0)

    text = text[:2000]

    try:
        page_vec = model.encode([text], normalize_embeddings=True)[0]
    except Exception:
        return ("other", 0.0, 0.0, 0.0, 1.0)

    scores: dict[str, float] = {}
    for label, anchor_vecs in _anchor_vectors.items():
        sims = anchor_vecs @ page_vec
        scores[label] = float(sims.max())

    # Normalise to 0–1
    max_possible = 1.0
    faculty = min(scores.get("faculty_profile", 0.0) / max_possible, 1.0)
    course = min(scores.get("course_catalog", 0.0) / max_possible, 1.0)
    news = min(scores.get("university_news", 0.0) / max_possible, 1.0)

    best = max(scores, key=scores.get)
    if scores[best] < 0.25:
        return ("other", faculty, course, news, 1.0)

    return (best, faculty, course, news, 0.0)


def classify_semantic_dataframe(
    df: DataFrame,
    url_col: str = "source_url",
    text_col: str = "normalized_text",
    title_col: str = "title",
) -> DataFrame:
    """Add a *record_type* column using semantic anchor classification."""
    _init_anchors()
    model = _get_semantic_model()

    if model is None or not _anchor_vectors:
        logger.warning("Semantic model unavailable — all rows classified as 'other'")
        return df.withColumn("record_type", F.lit("other"))

    def _classify_row(url, text, title):
        return classify_page_semantic(
            url or "", text or "", title or ""
        )

    classify_udf = F.udf(_classify_row, StringType())
    return df.withColumn(
        "record_type",
        classify_udf(F.col(url_col), F.col(text_col), F.col(title_col)),
    )


# ── Scored classification (for combining signals) ───────────────

def classify_page_scores(
    url: str,
    normalized_text: str,
    page_title: str = "",
) -> tuple[str, float, float, float, float]:
    """Classify a page and return label + individual class scores.

    Returns:
        Tuple of (label, faculty_score, course_score, news_score, other_score)
        where scores are normalised 0–1 (each score / max_possible).
    """
    faculty_score = 0
    course_score = 0

    try:
        path = urlparse(url).path
    except Exception:
        path = url

    faculty_score += _check_patterns(path, _FACULTY_PATH_PATTERNS, score=6)
    course_score += _check_patterns(path, _COURSE_PATH_PATTERNS, score=6)

    text_for_title = page_title or (normalized_text or "")[:300]
    faculty_score += _check_wordlist(text_for_title, _FACULTY_TITLE_WORDS, score=4)
    course_score += _check_wordlist(text_for_title, _COURSE_TITLE_WORDS, score=4)

    if normalized_text:
        faculty_score += _check_wordlist(normalized_text, _FACULTY_BODY_WORDS, score=2)
        course_score += _check_wordlist(normalized_text, _COURSE_BODY_WORDS, score=2)

    news_score = 0
    news_score += _check_patterns(path, _NEWS_PATH_PATTERNS, score=6)
    news_score += _check_wordlist(text_for_title, _NEWS_TITLE_WORDS, score=4)
    if normalized_text:
        news_score += _check_wordlist(normalized_text, _NEWS_BODY_WORDS, score=2)

    # News penalty on faculty/course
    news_penalty = 0
    news_penalty += _check_patterns(path, _NEWS_PATH_PATTERNS, score=6)
    if normalized_text:
        news_penalty += _check_wordlist(normalized_text, _NEWS_BODY_WORDS, score=3)
    faculty_score = max(0, faculty_score - news_penalty)
    course_score = max(0, course_score - news_penalty)

    # Decision
    label = "other"
    if news_score >= SCORE_THRESHOLD and news_score > faculty_score and news_score > course_score:
        label = "university_news"
    elif faculty_score >= SCORE_THRESHOLD and faculty_score > course_score:
        label = "faculty_profile"
    elif course_score >= SCORE_THRESHOLD and course_score > faculty_score:
        label = "course_catalog"
    elif faculty_score >= SCORE_THRESHOLD and course_score >= SCORE_THRESHOLD:
        label = "faculty_profile" if faculty_score >= course_score else "course_catalog"

    # Normalise scores to 0–1 (max possible per class is ~30)
    max_score = 30.0
    return (
        label,
        min(faculty_score / max_score, 1.0),
        min(course_score / max_score, 1.0),
        min(news_score / max_score, 1.0),
        1.0 if label == "other" else 0.0,
    )


def combine_semantic_and_regex(
    sem_label: str,
    sem_faculty: float,
    sem_course: float,
    sem_news: float,
    regex_label: str,
    regex_faculty: float,
    regex_course: float,
    regex_news: float,
    target_type: str,
) -> str:
    """Multi-signal classifier: semantic (primary) + regex (correction).

    Rules:
      1. Both agree → keep that label (high confidence)
      2. Semantic is uncertain (< 0.30) but regex is confident (> 0.50)
         → regex wins (regex rata pages that semantic rate)
      3. Semantic says "other" but regex is confident (> 0.50)
         → regex wins (rattrape les pages avec URL patterns clairs)
      4. Semantic has a label but regex says "other"
         → semantic wins (semantic knows the content)
      5. Different labels, both confident → semantic wins (content > URL)
      6. Default → target_type (safety net)
    """
    sem_scores = {
        "faculty_profile": sem_faculty,
        "course_catalog": sem_course,
        "university_news": sem_news,
    }
    regex_scores = {
        "faculty_profile": regex_faculty,
        "course_catalog": regex_course,
        "university_news": regex_news,
    }

    # Rule 1: Both agree → high confidence
    if sem_label == regex_label:
        return sem_label

    # Rule 2: Semantic uncertain, regex confident → regex wins
    sem_best = max(sem_scores.values()) if sem_scores else 0.0
    regex_best_for_diff = max(
        (s, l) for l, s in regex_scores.items() if l != sem_label
    ) if regex_scores else (0.0, "other")

    if sem_best < 0.30 and regex_best_for_diff[0] > 0.50:
        return regex_best_for_diff[1]

    # Rule 3: Semantic says "other", regex confident → regex wins
    if sem_label == "other" and regex_best_for_diff[0] > 0.50:
        return regex_best_for_diff[1]

    # Rule 4: Semantic has a label, regex says "other" → semantic wins
    if regex_label == "other" and sem_label != "other":
        return sem_label

    # Rule 5: Different labels, both confident → semantic wins
    if sem_label != "other" and sem_best >= 0.30:
        return sem_label

    # Rule 6: Default → target_type
    return target_type
