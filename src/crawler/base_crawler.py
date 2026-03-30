"""
base_crawler.py
---------------
Abstract base class for all crawlers.
Implements rate-limiting, retry-with-backoff, rotating user-agents,
and session management — patterns directly mirroring production
incident-resolution work (API rate-limiting, schema drift handling).
"""

import time
import random
import logging
import requests
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) "
    "Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


class BaseCrawler(ABC):
    """
    Abstract base crawler.

    Subclasses must implement:
        - parse_page(html, category_slug) -> list[dict]

    Config keys consumed:
        scraper.delay_min_seconds
        scraper.delay_max_seconds
        scraper.max_retries
        scraper.retry_backoff_seconds
        scraper.request_timeout_seconds
    """

    def __init__(self, config: dict):
        self.config = config
        self.scraper_cfg = config.get("scraper", {})
        self.session = self._build_session()
        self._audit_records: list[dict] = []

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(self._random_headers())
        return session

    @staticmethod
    def _random_headers() -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "DNT": "1",
        }

    # ------------------------------------------------------------------
    # Rate-limited HTTP GET
    # ------------------------------------------------------------------
    def get(self, url: str, params: Optional[dict] = None) -> Optional[requests.Response]:
        """
        Polite GET with exponential back-off retry.
        Rotates User-Agent on every retry to reduce fingerprinting.
        """
        max_retries = self.scraper_cfg.get("max_retries", 3)
        backoff = self.scraper_cfg.get("retry_backoff_seconds", 5)
        timeout = self.scraper_cfg.get("request_timeout_seconds", 15)

        for attempt in range(1, max_retries + 1):
            try:
                # Rotate headers on each attempt
                self.session.headers.update(self._random_headers())
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                self._polite_delay()
                return response

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response else "unknown"
                logger.warning(
                    "HTTP %s on attempt %d/%d for %s", status, attempt, max_retries, url
                )
                if status == 429:           # Rate-limited — back off hard
                    wait = backoff * (2 ** attempt)
                    logger.info("Rate-limited. Sleeping %ds before retry.", wait)
                    time.sleep(wait)
                elif attempt == max_retries:
                    logger.error("Max retries reached for %s", url)
                    return None
                else:
                    time.sleep(backoff * attempt)

            except requests.exceptions.RequestException as e:
                logger.warning("Request error attempt %d/%d: %s", attempt, max_retries, e)
                if attempt == max_retries:
                    return None
                time.sleep(backoff * attempt)

        return None

    def _polite_delay(self):
        """Random sleep between requests to respect target site."""
        lo = self.scraper_cfg.get("delay_min_seconds", 2)
        hi = self.scraper_cfg.get("delay_max_seconds", 5)
        time.sleep(random.uniform(lo, hi))

    # ------------------------------------------------------------------
    # Audit helpers
    # ------------------------------------------------------------------
    def log_audit(self, category: str, url: str, records_fetched: int,
                  status: str, error: str = ""):
        record = {
            "crawl_timestamp": datetime.utcnow().isoformat(),
            "category": category,
            "url": url,
            "records_fetched": records_fetched,
            "status": status,   # SUCCESS | PARTIAL | FAILED
            "error": error,
        }
        self._audit_records.append(record)
        logger.info("[AUDIT] %s", record)

    def get_audit_records(self) -> list[dict]:
        return self._audit_records

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------
    @abstractmethod
    def parse_page(self, html: str, category_slug: str) -> list[dict]:
        """Parse a single HTML page and return list of product dicts."""
        ...

    @abstractmethod
    def crawl_category(self, category: dict) -> list[dict]:
        """Crawl all pages for a category and return raw records."""
        ...
