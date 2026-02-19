from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class HttpConfig:
    user_agent: str
    request_delay_seconds: float = 1.0
    timeout_seconds: int = 30
    max_retries: int = 3
    backoff_factor: float = 1.5


class EthicalHttpClient:
    def __init__(self, config: HttpConfig, scrape_logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.scrape_logger = scrape_logger
        self.session = self._build_session()
        self._last_request_ts = 0.0
        self._robots_cache: dict[str, RobotFileParser] = {}

    def _build_session(self) -> Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            connect=self.config.max_retries,
            read=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": self.config.user_agent})
        return session

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_ts
        delay = self.config.request_delay_seconds
        if elapsed < delay:
            time.sleep(delay - elapsed)

    def _get_robot_parser(self, url: str) -> RobotFileParser:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base in self._robots_cache:
            return self._robots_cache[base]

        robots_url = f"{base}/robots.txt"
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            parser.read()
        except Exception:
            # Fail closed: if robots cannot be read, disallow crawling for safety.
            parser = RobotFileParser()
            parser.parse(["User-agent: *", "Disallow: /"])

        self._robots_cache[base] = parser
        return parser

    def is_allowed(self, url: str) -> bool:
        parser = self._get_robot_parser(url)
        return parser.can_fetch(self.config.user_agent, url)

    def get(self, url: str, **kwargs) -> Response:
        if not self.is_allowed(url):
            raise PermissionError(f"Blocked by robots.txt: {url}")

        self._rate_limit()
        if self.scrape_logger:
            self.scrape_logger.info(url)

        response = self.session.get(url, timeout=self.config.timeout_seconds, **kwargs)
        self._last_request_ts = time.time()
        return response
