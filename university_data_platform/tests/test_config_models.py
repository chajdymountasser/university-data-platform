"""
Tests for configuration models (university_data_platform.config.models).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from university_data_platform.config.models import (
    HtmlSourceConfig,
    ApiSourceConfig,
    FileSourceConfig,
    SourcesConfig,
    validate_source_config,
)


class TestHtmlSourceConfig:
    def test_valid_minimal(self):
        c = HtmlSourceConfig(url="https://www.um5.ac.ma/")
        assert c.type == "html"
        assert c.url == "https://www.um5.ac.ma/"
        assert c.max_depth == 2  # default
        assert c.verify_ssl is True

    def test_valid_full(self):
        c = HtmlSourceConfig(
            url="https://www.usmba.ac.ma/",
            source_system="usmba_website",
            max_depth=3,
            verify_ssl=False,
        )
        assert c.source_system == "usmba_website"
        assert c.max_depth == 3
        assert c.verify_ssl is False

    def test_invalid_url(self):
        with pytest.raises(ValidationError):
            HtmlSourceConfig(url="ftp://bad-scheme.com/")


class TestApiSourceConfig:
    def test_valid_openalex(self):
        c = ApiSourceConfig(target="openalex", query="university")
        assert c.type == "api"
        assert c.target == "openalex"
        assert c.max_pages == 1  # default

    def test_invalid_target(self):
        with pytest.raises(ValidationError, match="target must be one of"):
            ApiSourceConfig(target="unknown_api")


class TestFileSourceConfig:
    def test_valid(self):
        c = FileSourceConfig(path="./data/syllabus.pdf", source_system="syllabus")
        assert c.type == "file"
        assert c.path == "./data/syllabus.pdf"
        assert c.source_system == "syllabus"

    def test_minimal(self):
        c = FileSourceConfig(path="/tmp/x.pdf")
        assert c.source_system is None


class TestValidateSourceConfig:
    def test_html_dispatch(self):
        result = validate_source_config({"type": "html", "url": "https://example.com/"})
        assert isinstance(result, HtmlSourceConfig)

    def test_api_dispatch(self):
        result = validate_source_config({"type": "api", "target": "openalex"})
        assert isinstance(result, ApiSourceConfig)

    def test_file_dispatch(self):
        result = validate_source_config({"type": "file", "path": "/tmp/x.pdf"})
        assert isinstance(result, FileSourceConfig)

    def test_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown source type"):
            validate_source_config({"type": "unknown"})


class TestSourcesConfig:
    def test_from_yaml_loads_and_validates(self, tmp_path):
        yml = tmp_path / "sources.yml"
        yml.write_text("""\
sources:
  - type: html
    url: https://www.um5.ac.ma/
    source_system: um5_website
  - type: api
    target: openalex
    query: university
  - type: file
    path: ./data/test.pdf
    source_system: test_doc
""")
        config = SourcesConfig.from_yaml(str(yml))
        assert len(config.sources) == 3
        assert isinstance(config.sources[0], HtmlSourceConfig)
        assert isinstance(config.sources[1], ApiSourceConfig)
        assert isinstance(config.sources[2], FileSourceConfig)

    def test_by_type_filter(self):
        config = SourcesConfig(sources=[
            HtmlSourceConfig(url="https://a.com/"),
            ApiSourceConfig(target="openalex"),
            HtmlSourceConfig(url="https://b.com/"),
        ])
        html_sources = config.by_type("html")
        assert len(html_sources) == 2

    def test_by_source_system(self):
        config = SourcesConfig(sources=[
            HtmlSourceConfig(url="https://a.com/", source_system="sys_a"),
            HtmlSourceConfig(url="https://b.com/", source_system="sys_b"),
        ])
        result = config.by_source_system("sys_a")
        assert len(result) == 1
        assert result[0].source_system == "sys_a"
