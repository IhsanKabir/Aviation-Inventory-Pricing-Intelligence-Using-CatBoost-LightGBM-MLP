# modules/requester.py
import json
import logging
from pathlib import Path
import requests
from typing import Optional

LOG = logging.getLogger("core.requester")

class RequesterError(Exception):
    pass

class Requester:
    def __init__(
        self,
        cookies_path: Optional[Path] = None,
        user_agent: Optional[str] = None,
        timeout: int = 30,
        proxy_url: Optional[str] = None,
    ):
        self.session = requests.Session()
        self.timeout = timeout
        self.cookies_path = Path(cookies_path) if cookies_path else None
        self.proxy_url = proxy_url or None
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
            raise RequesterError(str(e))

    def get(self, url: str, params: dict = None, headers: dict = None, **kwargs):
        try:
            timeout = kwargs.pop("timeout", self.timeout)
            resp = self.session.get(url, params=params, headers=headers, timeout=timeout, **kwargs)
            return resp
        except Exception as e:
            raise RequesterError(e)

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
