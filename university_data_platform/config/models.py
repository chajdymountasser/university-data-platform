"""
Source configuration models — validate sources.yml with Pydantic.

Clients creating custom DAGs can use these models to define their sources:

    from university_data_platform.config.models import HtmlSourceConfig

    my_source = HtmlSourceConfig(
        url="https://www.um5.ac.ma/",
        source_system="um5_website",
        max_depth=2,
    )
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class HtmlSourceConfig(BaseModel):
    """A web page to scrape."""

    type: str = "html"
    url: str
    source_system: Optional[str] = None
    max_depth: int = 2
    verify_ssl: bool = True
    deep: bool = True
    auto_playwright: Optional[bool] = None

    @field_validator("url")
    @classmethod
    def _url_must_be_http(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError(f"URL must start with http:// or https://: {v}")
        return v


class ApiSourceConfig(BaseModel):
    """An API endpoint to query."""

    type: str = "api"
    target: str  # openalex, crossref, orcid
    query: str = ""
    endpoint: str = ""
    max_pages: int = 1

    @field_validator("target")
    @classmethod
    def _target_must_be_known(cls, v: str) -> str:
        allowed = {"openalex", "crossref", "orcid"}
        if v not in allowed:
            raise ValueError(f"target must be one of {allowed}, got {v!r}")
        return v


class FileSourceConfig(BaseModel):
    """A local file to ingest."""

    type: str = "file"
    path: str
    source_system: Optional[str] = None


# Union type for validation / discriminated
SourceConfig = Union[HtmlSourceConfig, ApiSourceConfig, FileSourceConfig]


def _parse_source(raw: dict) -> SourceConfig:
    """Dispatch a raw YAML dict to the correct model."""
    t = raw.get("type", "html")
    if t == "html":
        return HtmlSourceConfig(**raw)
    elif t == "api":
        return ApiSourceConfig(**raw)
    elif t == "file":
        return FileSourceConfig(**raw)
    else:
        raise ValueError(f"Unknown source type: {t!r}")


class SourcesConfig(BaseModel):
    """Full configuration — a list of sources."""

    sources: List[SourceConfig] = Field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: str) -> "SourcesConfig":
        """Load and validate a sources.yml file."""
        import yaml

        with open(path) as f:
            raw = yaml.safe_load(f)
        parsed = [_parse_source(s) for s in raw.get("sources", [])]
        return cls(sources=parsed)

    def by_type(self, t: str) -> list[SourceConfig]:
        return [s for s in self.sources if s.type == t]

    def by_source_system(self, system: str) -> list[SourceConfig]:
        return [s for s in self.sources if getattr(s, "source_system", None) == system]


# ── Convenience helpers ──────────────────────────────────────

def load_sources_from_yaml(path: str) -> List[Dict[str, Any]]:
    """Load and validate sources.yml, return raw dicts (backwards compat)."""
    config = SourcesConfig.from_yaml(path)
    return [s.model_dump() for s in config.sources]


def validate_source_config(raw: dict) -> SourceConfig:
    """Validate a single source dict and return the typed model."""
    return _parse_source(raw)
