from university_data_platform.ingestion.base_connector import BaseConnector
from university_data_platform.ingestion.api_connector import ApiConnector, OpenAlexConnector, CrossrefConnector, OrcidConnector
from university_data_platform.ingestion.html_scraper import HtmlScraper
from university_data_platform.ingestion.file_connector import FileConnector
from university_data_platform.ingestion.playwright_scraper import PlaywrightScraper
from university_data_platform.ingestion.deep_scraper import DeepScraper

__all__ = [
    "BaseConnector",
    "ApiConnector",
    "OpenAlexConnector",
    "CrossrefConnector",
    "OrcidConnector",
    "HtmlScraper",
    "FileConnector",
    "PlaywrightScraper",
    "DeepScraper",
]
