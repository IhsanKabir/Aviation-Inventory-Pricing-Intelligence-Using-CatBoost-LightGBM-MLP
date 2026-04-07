# modules/requester.py
import json
import logging
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError, ProxyError, SSLError, Timeout as RequestsTimeout
from urllib3.util.retry import Retry
from typing import Optional

LOG = logging.getLogger("core.requester")

class RequesterError(Exception):
    """Custom exception for requester errors with context."""
    def __init__(self, message: str, error_type: str = "unknown", original_exception: Exception = None):
        super().__init__(message)
        self.error_type = error_type
        self.original_exception = original_exception


class Requester:
    """
    Enhanced HTTP requester with automatic retries, better error handling,
    and support for proxies and cookies.

    Features:
    - Automatic retry logic with exponential backoff
    - Detailed error classification (DNS, connection, timeout, HTTP)
    - Cookie persistence
    - Proxy support
    - Connection pooling
    """

    def __init__(
        self,
        cookies_path: Optional[Path] = None,
        user_agent: Optional[str] = None,
        timeout: int = 30,
        proxy_url: Optional[str] = None,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ):
        self.session = requests.Session()
        # Avoid inheriting machine-wide proxy settings that can break
        # scraper traffic independently from browser access.
        self.session.trust_env = False
        self.timeout = timeout
        self.cookies_path = Path(cookies_path) if cookies_path else None
        self.proxy_url = proxy_url or None

        # Retry status responses like 429/5xx, but fail fast on connect-level
        # socket errors so blocked hosts do not stall every query lane.
        retry_strategy = Retry(
            total=None,
            connect=0,
            read=0,
            status=max_retries,
            other=0,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
            raise_on_status=False,
        )

        # Mount adapters with retry logic
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=4,
            pool_maxsize=8,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if user_agent:
            self.session.headers.update({"User-Agent": user_agent})

        if self.proxy_url:
            self.session.proxies.update({"http": self.proxy_url, "https": self.proxy_url})
            LOG.info("Using proxy for session: %s", self.proxy_url)

        if self.cookies_path and self.cookies_path.exists():
            try:
                with open(self.cookies_path, "r", encoding="utf-8") as fh:
                    c = json.load(fh)
                    self.session.cookies.update(c)
                LOG.info("Loaded cookies from %s", str(self.cookies_path))
            except Exception as e:
                LOG.warning("Failed to load cookies: %s", e)

    def _classify_error(self, exception: Exception) -> str:
        """Classify the type of network error for better diagnostics."""
        error_str = str(exception).lower()

        if isinstance(exception, ProxyError):
            return "proxy_error"
        elif isinstance(exception, SSLError):
            return "ssl_error"
        elif isinstance(exception, RequestsTimeout):
            return "timeout"
        elif isinstance(exception, RequestsConnectionError):
            if "name resolution" in error_str or "nodename nor servname" in error_str or "no address" in error_str:
                return "dns_resolution"
            if "connection refused" in error_str:
                return "connection_refused"
            if "connection reset" in error_str or "broken pipe" in error_str:
                return "connection_reset"
            if "unreachable" in error_str:
                return "network_unreachable"
            if "forbidden by its access permissions" in error_str or "winerror 10013" in error_str:
                return "socket_access_denied"
            return "connection_error"
        if "name resolution" in error_str or "nodename nor servname" in error_str or "no address" in error_str:
            return "dns_resolution"
        elif "connection refused" in error_str:
            return "connection_refused"
        elif "timeout" in error_str or "timed out" in error_str:
            return "timeout"
        elif "connection reset" in error_str or "broken pipe" in error_str:
            return "connection_reset"
        elif "certificate" in error_str or "ssl" in error_str:
            return "ssl_error"
        elif "proxy" in error_str:
            return "proxy_error"
        elif "unreachable" in error_str:
            return "network_unreachable"
        else:
            return "connection_error"

    def _raise_requester_error(self, exception: Exception):
        error_type = self._classify_error(exception)
        raise RequesterError(str(exception), error_type=error_type, original_exception=exception)

    def post(self, url: str, json_payload: dict, headers: dict | None = None, **kwargs):
        try:
            timeout = kwargs.pop("timeout", self.timeout)
            resp = self.session.post(url, json=json_payload, headers=headers, timeout=timeout, **kwargs)
            ok = resp.status_code in (200, 201)
            # Try to return JSON if possible, else raw text
            try:
                body = resp.json()
            except Exception:
                body = resp.text
            return ok, body, resp.status_code
        except Exception as e:
            self._raise_requester_error(e)

    def post_raw(self, url: str, json_payload: dict, headers: dict | None = None, **kwargs):
        try:
            timeout = kwargs.pop("timeout", self.timeout)
            return self.session.post(url, json=json_payload, headers=headers, timeout=timeout, **kwargs)
        except Exception as e:
            self._raise_requester_error(e)

    def get(self, url: str, params: dict = None, headers: dict = None, **kwargs):
        try:
            timeout = kwargs.pop("timeout", self.timeout)
            resp = self.session.get(url, params=params, headers=headers, timeout=timeout, **kwargs)
            return resp
        except Exception as e:
            self._raise_requester_error(e)

    def save_cookies(self):
        if not self.cookies_path:
            return
        try:
            self.cookies_path.parent.mkdir(parents=True, exist_ok=True)
            # convert cookiejar to dict
            cookie_dict = requests.utils.dict_from_cookiejar(self.session.cookies)
            with open(self.cookies_path, "w", encoding="utf-8") as fh:
                json.dump(cookie_dict, fh, ensure_ascii=False, indent=2)
            LOG.info("Saved cookies to %s", str(self.cookies_path))
        except Exception as e:
            LOG.warning("Failed to save cookies: %s", e)
