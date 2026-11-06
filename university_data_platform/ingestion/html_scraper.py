"""
HTML scraper — static HTML via requests + BeautifulSoup4.

Stores full raw HTML bytes — no extraction.
Handles 404, 429, 5xx gracefully, records http_status.

Also provides extraction functions for deep scraping:
  extract_internal_links()
  extract_images()
  extract_documents()
"""

from __future__ import annotations

import logging
import os
import re
import warnings
from typing import Any, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup

from university_data_platform.ingestion.base_connector import BaseConnector
from university_data_platform.metadata.schema import IngestionMetadata

# Suppress SSL warnings when verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# ── Browser User-Agent pool (rotate to avoid WAF fingerprinting) ───

BROWSER_USER_AGENTS = [
    # Chrome 124 — Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome 124 — macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox 125 — Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Firefox 125 — macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Edge 124 — Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    # Safari 17.4 — macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

# Realistic browser Accept header
_BROWSER_ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,image/apng,*/*;q=0.8"
)

import random
import time as _time


# ── CAPTCHA / WAF detection ────────────────────────────────────────

_CAPTCHA_TITLE_PATTERN = re.compile(r"Validation request", re.I)
_CAPTCHA_SIZE_THRESHOLD = 1500  # bytes — CAPTCHA pages are ~1021 bytes
_CAPTCHA_CHECKSUM_CACHE: set[str] = set()  # track CAPTCHA checksums seen


def _is_captcha_page(html: str | bytes) -> bool:
    """Detect a WAF/CAPTCHA validation page.

    Signs: very small HTML (< 1.5KB), contains "Validation request" title,
    and/or references /captcha.gif.
    """
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    if len(html) > _CAPTCHA_SIZE_THRESHOLD:
        return False
    if _CAPTCHA_TITLE_PATTERN.search(html):
        return True
    if "/captcha.gif" in html:
        return True
    return False


import hashlib


def _mark_captcha_checksum(raw: bytes) -> None:
    """Remember a captcha checksum so downstream callers can skip."""
    _CAPTCHA_CHECKSUM_CACHE.add(hashlib.md5(raw).hexdigest())


def _is_known_captcha(raw: bytes) -> bool:
    """Check if this content matches a known CAPTCHA page."""
    return hashlib.md5(raw).hexdigest() in _CAPTCHA_CHECKSUM_CACHE


SKIPPED_LINK_PATTERNS = re.compile(r"^(mailto:|tel:|javascript:|#)", re.IGNORECASE)
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp", ".tiff"}
DOCUMENT_EXTENSIONS = {".pdf", ".docx", ".doc", ".xlsx", ".xls", ".pptx", ".ppt", ".csv", ".txt", ".zip"}
MEDIA_EXTENSIONS = {".mp4", ".webm", ".avi", ".mov", ".mkv", ".flv", ".wmv", ".m4v", ".mp3", ".wav", ".ogg", ".flac", ".aac", ".wma"}
SKIPPED_EXTENSIONS = {".dta", ".sav", ".sas7bdat", ".por", ".zsav", ".rar",".dat"}

# Map of known URLs to source_system identifiers
SOURCE_SYSTEM_MAP = {
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
    "fsjesm.ma": "fsjesm_website",
    "wikipedia.org": "wikipedia_website",
    "khanacademy.org": "khanacademy_website",
    "ocw.mit.edu": "mit_ocw_website",
}


def _infer_source_system(url: str) -> str:
    """Infer source_system from URL hostname."""
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    for domain, system in SOURCE_SYSTEM_MAP.items():
        if domain in hostname:
            return system
    return hostname.replace(".", "_").replace("-", "_") or "unknown"


def _resolve_url(url: str, base_url: str) -> str:
    return urljoin(base_url, url)


def _normalize_url(url: str) -> str:
    """Remove fragment and trailing slash for dedup."""
    parsed = urlparse(url)
    cleaned = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
    if parsed.query:
        cleaned += f"?{parsed.query}"
    return cleaned


def _get_domain(url: str) -> str:
    return urlparse(url).hostname or ""


def _get_extension(url: str) -> str:
    """Extract file extension including the dot, e.g. '.png' or '.pdf'.

    Returns '' if no extension found.
    """
    path = urlparse(url).path.rstrip("/")
    _, _, ext = path.rpartition(".")
    return f".{ext.lower()}" if ext else ""


def _is_html_url(url: str) -> bool:
    known_binary = IMAGE_EXTENSIONS | DOCUMENT_EXTENSIONS | MEDIA_EXTENSIONS | SKIPPED_EXTENSIONS
    ext = _get_extension(url)
    if not ext:
        return True
    return ext not in known_binary


def _is_internal_link(url: str, domain: str) -> bool:
    parsed = urlparse(url)
    if not parsed.hostname:
        return True
    # Strip www from both sides to avoid www vs non-www mismatch
    host = parsed.hostname.removeprefix("www.")
    dom = domain.removeprefix("www.")
    if host == dom:
        return True
    # Handle subdomain matching — wikipedia.org matches en.wikipedia.org
    dom_parts = dom.split(".")
    host_parts = host.split(".")
    return host_parts[-len(dom_parts):] == dom_parts if len(dom_parts) >= 2 else host == dom


def is_js_heavy(
    soup: BeautifulSoup,
    html: str,
    min_text_length: int = 200,
) -> tuple[bool, dict]:
    """Detect if a page likely requires JS rendering to show meaningful content.

    Heuristics:
      1. Very small HTML (< 5KB suggests a JS-rendered shell)
      2. No <body> tag or near-empty body text
      3. SPA root elements (#root, #app, #__next, <app-root>, etc.)
      4. Very few ``<a href>`` links (menus generated by JS)
      5. Script content dwarfs visible text

    Returns:
        (is_heavy, details) where details has the indicators for logging.
    """
    details: dict[str, int | str] = {}

    # 1. Very small HTML
    html_size = len(html)
    details["html_size"] = html_size
    if html_size < 5000:
        details["reason"] = f"Very small HTML ({html_size} bytes)"
        return True, details

    # 2. Check body presence and text length
    body = soup.find("body")
    if body is None:
        details["reason"] = "No <body> tag found"
        details["body_text_length"] = 0
        return True, details

    text_len = len(body.get_text(strip=True))
    details["body_text_length"] = text_len
    if text_len < min_text_length:
        details["reason"] = f"Body text too short ({text_len} chars, min {min_text_length})"
        return True, details

    # 3. SPA root elements
    for root_id in ("root", "app", "__next", "___gatsby"):
        if soup.find(id=root_id):
            details["reason"] = f"SPA root element: #{root_id}"
            return True, details

    # 4. Framework-specific component tags
    for tag_name in ("app-root", "app-shell", "mat-app", "ng-component"):
        if soup.find(tag_name):
            details["reason"] = f"Framework component: <{tag_name}>"
            return True, details

    # 5. Very few <a> links (menus likely JS-generated)
    link_count = len(soup.find_all("a", href=True))
    details["link_count"] = link_count
    if link_count < 5:
        details["reason"] = f"Only {link_count} links found"
        return True, details

    # 6. Script-to-text ratio — scripts dominate visible content
    script_tags = soup.find_all("script")
    script_chars = sum(len(s.get_text(strip=True)) for s in script_tags)
    details["script_chars"] = script_chars
    if script_chars > text_len * 2 and text_len < 1000:
        details["reason"] = f"Script-heavy (script:{script_chars} vs text:{text_len})"
        return True, details

    return False, details


def _parse_srcset(srcset: str) -> list[tuple[str, int]]:
    """Parse srcset attribute. Returns [(url, size)] sorted by size descending."""
    candidates = []
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        tokens = part.split()
        url = tokens[0]
        descriptor = tokens[1] if len(tokens) > 1 else "1x"
        if descriptor.endswith("w"):
            size = int(descriptor[:-1]) if descriptor[:-1].isdigit() else 0
        elif descriptor.endswith("x"):
            size = int(descriptor[:-1]) if descriptor[:-1].isdigit() else 1
        else:
            size = 1
        candidates.append((url, size))
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates


# ═══════════════════════════════════════════════════════════════
# Extraction functions (used by DeepScraper)
# ═══════════════════════════════════════════════════════════════

def extract_internal_links(soup: BeautifulSoup, base_url: str, domain: str) -> list[str]:
    """Extract all internal links from a parsed page, deduplicated."""
    links: dict[str, str] = {}
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if SKIPPED_LINK_PATTERNS.match(href):
            continue
        absolute = _resolve_url(href, base_url)
        if _is_internal_link(absolute, domain) and _is_html_url(absolute):
            normalized = _normalize_url(absolute)
            if normalized:
                links[normalized] = absolute
    return list(links.values())


def extract_images(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """Extract all image URLs from a parsed page.

    Sources: <img src>, <img srcset>, <picture> <source>,
             og:image meta tag, inline CSS background-image.

    Returns list of {url, extension} deduplicated by URL.
    """
    found: dict[str, str] = {}

    def add(url: str):
        abs_url = _resolve_url(url.strip(), base_url)
        ext = _get_extension(abs_url)
        if not ext or ext in IMAGE_EXTENSIONS:
            found[abs_url] = ext if ext else "jpg"

    # <img src="...">
    for img in soup.find_all("img", src=True):
        add(img["src"])

    # <img srcset="..."> — take highest resolution
    for img in soup.find_all("img", srcset=True):
        candidates = _parse_srcset(img["srcset"])
        if candidates:
            add(candidates[0][0])

    # <source srcset="..."> inside <picture>
    for source in soup.find_all("source", srcset=True):
        candidates = _parse_srcset(source["srcset"])
        if candidates:
            add(candidates[0][0])

    # <meta property="og:image">
    for og in soup.find_all("meta", property="og:image", content=True):
        add(og["content"])

    # CSS background-image in inline style
    bg_re = re.compile(r"background-image\s*:\s*url\(['\"]?(.*?)['\"]?\)", re.IGNORECASE)
    for tag in soup.find_all(style=True):
        for match in bg_re.finditer(tag.get("style", "")):
            add(match.group(1))

    return [{"url": url, "extension": ext} for url, ext in found.items()]


def extract_documents(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """Extract all document URLs from a parsed page.

    Sources: <a href> with document extension, <iframe> pointing to PDF.

    Returns list of {url, extension} deduplicated by URL.
    """
    found: dict[str, str] = {}

    def add(url: str):
        abs_url = _resolve_url(url.strip(), base_url)
        ext = _get_extension(abs_url)
        if ext in SKIPPED_EXTENSIONS:
            return
        if ext in DOCUMENT_EXTENSIONS:
            found[abs_url] = ext

    for a in soup.find_all("a", href=True):
        add(a["href"])

    for iframe in soup.find_all("iframe", src=True):
        add(iframe["src"])

    return [{"url": url, "extension": ext} for url, ext in found.items()]


# ═══════════════════════════════════════════════════════════════
# JS/Meta redirect detection
# ═══════════════════════════════════════════════════════════════

# Maximum redirect chain for JS-based redirects
MAX_REDIRECT_FOLLOW = 5

# Patterns for redirect detection
_ONLOAD_RX = re.compile(r"""location\.href\s*=\s*['"]([^'"]+)['"]""")
_META_REFRESH_RX = re.compile(r"url\s*=\s*(.+?)(?:\s*;?\s*$)", re.I)
_WINDOW_LOC_RX = re.compile(r"""window\.location\.(?:href|replace)\s*=\s*['"]([^'"]+)['"]""")


def _resolve_js_redirect(html: str, base_url: str) -> Optional[str]:
    """Detect JS-based or meta refresh redirects in HTML.

    Checks:
      1. <meta http-equiv="refresh" content="0;url=...">
      2. <body onLoad="location.href='...'">
      3. <script>window.location.href="..."</script>

    Returns the redirect URL or None if no redirect detected.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    # 1. Meta refresh
    meta = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
    if meta and meta.get("content"):
        match = _META_REFRESH_RX.search(meta["content"])
        if match:
            url = match.group(1).strip().strip("'\"")
            return urljoin(base_url, url)

    # 2. Body onLoad / onload location.href
    for attr in ("onload", "onLoad"):
        body = soup.find("body", attrs={attr: True})
        if body and body.get(attr):
            match = _ONLOAD_RX.search(body[attr])
            if match:
                return urljoin(base_url, match.group(1))

    # 3. Script tags with window.location
    for script in soup.find_all("script"):
        if script.string:
            match = _WINDOW_LOC_RX.search(script.string)
            if match:
                return urljoin(base_url, match.group(1))

    return None


class HtmlScraper(BaseConnector):
    """Static HTML scraper using requests + BeautifulSoup4.

    Stores full raw HTML bytes — no text extraction.
    BS4 is used only for lightweight validation (e.g. checking page title),
    not for content extraction. Raw bytes are preserved.

    Automatically follows JS-based redirects (meta refresh, onLoad, etc.)
    so callers always receive the *real* page content.

    Rotates realistic browser User-Agents and applies random jitter to
    request timing to avoid WAF/CAPTCHA triggers.
    """

    def __init__(
        self,
        *,
        minio_client=None,
        connector_version: str = "1.0.0",
        timeout: int = 30,
        max_retries: int = 3,
        user_agent: str | None = None,
        verify_ssl: bool = True,
        max_redirect_follow: int = MAX_REDIRECT_FOLLOW,
        detect_js: bool = True,
        auto_playwright: bool = False,
        request_delay: float = 0.0,
        request_jitter: float = 1.0,
    ):
        super().__init__(minio_client=minio_client, connector_version=connector_version)
        self.timeout = timeout
        self.max_retries = max_retries
        self.verify_ssl = verify_ssl
        self.max_redirect_follow = max_redirect_follow
        self.detect_js = detect_js
        self.auto_playwright = auto_playwright or os.getenv("AUTO_PLAYWRIGHT", "").lower() in ("true", "1", "yes")
        self.request_delay = request_delay or float(os.getenv("REQUEST_DELAY", "0"))
        self.request_jitter = request_jitter
        self._fixed_ua = user_agent

        # ── Proxy support (HTTP_PROXY / HTTPS_PROXY env vars) ──
        self._proxies = {}
        https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
        http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        if https_proxy:
            self._proxies["https"] = https_proxy
        if http_proxy:
            self._proxies["http"] = http_proxy

        self.session = requests.Session()
        if self._proxies:
            self.session.proxies.update(self._proxies)
            logger.info("Proxy configured: %s", self._proxies)
        self.session.headers.update({
            "Accept": _BROWSER_ACCEPT,
            "Accept-Language": "fr,en;q=0.9,ar;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": self._pick_ua(),
        })

    def _pick_ua(self) -> str:
        """Return a random browser User-Agent, or the fixed one if set."""
        if self._fixed_ua:
            return self._fixed_ua
        return random.choice(BROWSER_USER_AGENTS)

    def _rotate_ua(self) -> None:
        """Rotate the session User-Agent to a different random one."""
        self.session.headers["User-Agent"] = self._pick_ua()

    def _maybe_delay(self) -> None:
        """Sleep for *request_delay* + random jitter between requests."""
        if self.request_delay > 0:
            jitter = random.uniform(0, self.request_jitter)
            _time.sleep(self.request_delay + jitter)

    @property
    def source_type(self) -> str:
        return "html"

    @property
    def bucket_name(self) -> str:
        return "raw-web-html"

    def fetch(
        self,
        *,
        url: str,
        source_system: Optional[str] = None,
        _follow_count: int = 5,
        **kwargs: Any,
    ) -> Tuple[bytes, IngestionMetadata]:
        """Fetch raw HTML from URL.

        Automatically rotates the User-Agent and adds a random delay
        before each request to avoid WAF/CAPTCHA triggers.

        Detects CAPTCHA validation pages and returns them with a
        special ``http_status=418`` so callers can distinguish them
        from real content.

        Automatically follows JS-based redirects (meta refresh, onLoad
        location.href, window.location.href) so callers receive the
        real page content.

        Args:
            url: URL to fetch.
            source_system: Source system identifier (auto-detected if None).
            _follow_count: Internal — remaining redirect hops.

        Returns:
            (raw_html_bytes, IngestionMetadata)
        """
        if not source_system:
            source_system = _infer_source_system(url)

        # Rotate UA and delay before each request
        self._rotate_ua()
        self._maybe_delay()

        last_status = None
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout, verify=self.verify_ssl)
                last_status = resp.status_code

                if resp.status_code == 200:
                    raw_bytes = resp.content

                    # Capture actual Content-Type from response headers
                    actual_ct = (
                        resp.headers.get("Content-Type", "text/html")
                        .split(";")[0]
                        .strip()
                        .lower()
                    )
                    is_html = actual_ct in ("text/html", "application/xhtml+xml")

                    # ── CAPTCHA detection ─────────────────────────
                    if _is_captcha_page(raw_bytes):
                        logger.warning(
                            "CAPTCHA/WAF page detected: %s  (%d bytes)",
                            url, len(raw_bytes),
                        )
                        _mark_captcha_checksum(raw_bytes)
                        # Return as 418 status so callers can skip
                        metadata = IngestionMetadata.create(
                            raw_content=raw_bytes,
                            source_system=source_system,
                            source_url=url,
                            http_status=418,
                            raw_object_path="",
                            content_type="text/html",
                            connector_version=self.connector_version,
                        )
                        return raw_bytes, metadata

                    # Follow JS/meta refresh redirects (up to _follow_count hops)
                    if _follow_count > 0 and raw_bytes:
                        html_text = raw_bytes.decode("utf-8", errors="replace")
                        redirect_url = _resolve_js_redirect(html_text, url)
                        if redirect_url:
                            logger.info(
                                "JS redirect %s → %s  (%d remaining)",
                                url, redirect_url, _follow_count - 1,
                            )
                            return self.fetch(
                                url=redirect_url,
                                source_system=source_system,
                                _follow_count=_follow_count - 1,
                            )

                    # ── JS-heavy detection + optional Playwright fallback ──
                    if self.detect_js and raw_bytes and is_html:
                        try:
                            html_text = raw_bytes.decode("utf-8", errors="replace")
                            soup = BeautifulSoup(html_text, "html.parser")
                            heavy, details = is_js_heavy(soup, html_text)
                        except Exception:
                            heavy = False
                            details = {"reason": "parser_error"}
                        if heavy:
                            reason = details.get("reason", "unknown")
                            logger.warning(
                                "JS-heavy page detected: %s — %s  (auto_playwright=%s)",
                                url, reason, self.auto_playwright,
                            )
                            if self.auto_playwright:
                                try:
                                    from university_data_platform.ingestion.playwright_scraper import PlaywrightScraper
                                    pw = PlaywrightScraper(
                                        minio_client=self.minio,
                                        connector_version=self.connector_version,
                                        timeout=self.timeout * 1000,
                                        wait_for_load_state="networkidle",
                                    )
                                    pw_raw, pw_meta = pw.fetch(
                                        url=url,
                                        source_system=source_system,
                                    )
                                    if pw_meta.http_status == 200 and not _is_captcha_page(pw_raw):
                                        logger.info(
                                            "Playwright fallback succeeded for %s  (%d bytes)",
                                            url, len(pw_raw),
                                        )
                                        return pw_raw, pw_meta
                                    else:
                                        logger.warning(
                                            "Playwright returned CAPTCHA or HTTP %d for %s — keeping requests result",
                                            pw_meta.http_status, url,
                                        )
                                except Exception as e:
                                    logger.warning(
                                        "Playwright fallback failed for %s: %s — keeping requests result",
                                        url, e,
                                    )

                    metadata = IngestionMetadata.create(
                        raw_content=raw_bytes,
                        source_system=source_system,
                        source_url=url,
                        http_status=200,
                        raw_object_path="",
                        content_type=actual_ct,
                        connector_version=self.connector_version,
                    )
                    return raw_bytes, metadata

                elif resp.status_code == 404:
                    logger.warning("404 Not Found: %s", url)
                    raw_bytes = resp.content or b""
                    metadata = IngestionMetadata.create(
                        raw_content=raw_bytes,
                        source_system=source_system,
                        source_url=url,
                        http_status=404,
                        raw_object_path="",
                        content_type="text/html",
                        connector_version=self.connector_version,
                    )
                    return raw_bytes, metadata

                elif resp.status_code == 429:
                    wait = 2 * attempt
                    logger.warning("429 Rate limited on %s — waiting %ds (attempt %d/%d)", url, wait, attempt, self.max_retries)
                    import time
                    time.sleep(wait)
                    continue

                elif resp.status_code >= 500:
                    logger.warning("Server error %d from %s (attempt %d/%d)", resp.status_code, url, attempt, self.max_retries)
                    if attempt == self.max_retries:
                        raw_bytes = resp.content or b""
                        metadata = IngestionMetadata.create(
                            raw_content=raw_bytes,
                            source_system=source_system,
                            source_url=url,
                            http_status=resp.status_code,
                            raw_object_path="",
                            content_type="text/html",
                            connector_version=self.connector_version,
                        )
                        return raw_bytes, metadata
                    import time
                    time.sleep(2 * attempt)
                    continue

                else:
                    # Other 4xx — record and return
                    raw_bytes = resp.content or b""
                    metadata = IngestionMetadata.create(
                        raw_content=raw_bytes,
                        source_system=source_system,
                        source_url=url,
                        http_status=resp.status_code,
                        raw_object_path="",
                        content_type="text/html",
                        connector_version=self.connector_version,
                    )
                    return raw_bytes, metadata

            except requests.RequestException as e:
                last_error = e
                logger.warning("Request error (attempt %d/%d): %s", attempt, self.max_retries, e)
                if attempt == self.max_retries:
                    break
                import time
                time.sleep(2 * attempt)

        # ── Playwright fallback on SSL/TLS failure ─────────────
        ssl_failures = ("SSLEOFError", "SSLError", "CERTIFICATE_VERIFY_FAILED",
                        "ssl_error", "EOF occurred", "WRONG_VERSION_NUMBER")
        if last_error and any(s in str(last_error) for s in ssl_failures):
            logger.warning(
                "SSL handshake failed for %s (WAF/TLS fingerprinting) — trying Playwright fallback",
                url,
            )
            try:
                from university_data_platform.ingestion.playwright_scraper import PlaywrightScraper
                pw = PlaywrightScraper(
                    minio_client=self.minio,
                    connector_version=self.connector_version,
                    timeout=self.timeout * 1000,
                    wait_for_load_state="networkidle",
                )
                pw_raw, pw_meta = pw.fetch(
                    url=url,
                    source_system=source_system,
                )
                if pw_meta.http_status == 200:
                    logger.info(
                        "Playwright fallback succeeded for %s  (%d bytes)",
                        url, len(pw_raw),
                    )
                    return pw_raw, pw_meta
                else:
                    logger.warning(
                        "Playwright returned HTTP %d for %s",
                        pw_meta.http_status, url,
                    )
            except Exception as e:
                logger.warning(
                    "Playwright fallback also failed for %s: %s",
                    url, e,
                )

        # All retries exhausted
        error_body = f"Error: {last_error}".encode("utf-8")
        metadata = IngestionMetadata.create(
            raw_content=error_body,
            source_system=source_system,
            source_url=url,
            http_status=last_status or 0,
            raw_object_path="",
            content_type="text/html",
            connector_version=self.connector_version,
        )
        return error_body, metadata
