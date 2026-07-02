"""
ShareTrip LIVE harvester — mints the short-lived session token itself, so it needs
no manual HAR capture.

How it works (reverse-engineered 2026-06-03):
  1. GET https://sharetrip.net/flight-search?<params>  with header `rsc: 1`
     -> Next.js RSC payload (text/x-component) containing "shortLiveToken":"<64-hex>".
     The token is session-scoped (~hours TTL); we mint once and reuse until it 401s.
  2. GET  api.sharetrip.net/api/v2/flight/search/initialize?<params>
     with header `x-sort-live-token: <token>`  -> {response:{searchId}}.
  3. POST api.sharetrip.net/api/v2/flight/search/available-flights?searchId=<id>
     body {"page":N,"limit":L}  -> {response:{matchedFlights:[...]}} (paginated).

matchedFlights have the same shape as the manual-HAR responses, so we reuse
modules.sharetrip_har._normalize (which already extracts base fare + tax + RBD).
"""
from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from modules.sharetrip_har import _normalize

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
PAGE_URL = "https://sharetrip.net/flight-search"
INIT_URL = "https://api.sharetrip.net/api/v2/flight/search/initialize"
SEARCH_URL = "https://api.sharetrip.net/api/v2/flight/search/available-flights"
_TOKEN_RE = re.compile(r'"shortLiveToken":"([a-f0-9]{64})"')


def _cabin_api(cabin: str) -> str:
    return "BUSINESS" if str(cabin).lower().startswith("b") else "ECONOMY"


def _api_headers(token: str) -> Dict[str, str]:
    return {
        "accept": "application/json, text/plain, */*",
        "origin": "https://sharetrip.net",
        "referer": "https://sharetrip.net/",
        "x-sort-live-token": token,
        "user-agent": UA,
    }


def mint_token(session: requests.Session, origin: str, dest: str, date: str,
               cabin: str = "Economy", timeout: int = 30) -> Optional[str]:
    """Fetch the flight-search RSC payload and scrape a fresh shortLiveToken."""
    params = {
        "adult": 1, "child": 0, "child2To5Count": 0, "child6To12Count": 0,
        "class": "Business" if _cabin_api(cabin) == "BUSINESS" else "Economy",
        "depart": date, "destination": dest, "infant": 0,
        "occupation": "NOT_SELECTED", "origin": origin, "tripType": "OneWay",
    }
    try:
        r = session.get(f"{PAGE_URL}?{urlencode(params)}",
                        headers={"user-agent": UA, "accept": "*/*", "rsc": "1",
                                 "referer": "https://sharetrip.net/"}, timeout=timeout)
    except requests.RequestException:
        return None        # network/DNS blip — caller treats as a transient miss, not a crash
    m = _TOKEN_RE.search(r.text)
    return m.group(1) if m else None


def _initialize(session: requests.Session, token: str, origin: str, dest: str,
                date: str, cabin: str, adt: int, chd: int, inf: int,
                timeout: int = 30, retries: int = 2) -> Optional[str]:
    params = {
        "cabinClass": _cabin_api(cabin), "currency": "BDT",
        "departureDates[]": date, "destinations[]": dest,
        "numOfAdult": adt, "numOfChild": chd, "numOfInfant": inf, "numOfKid": 0,
        "occupation": "NOT_SELECTED", "origins[]": origin, "tripType": "ONEWAY",
    }
    url = f"{INIT_URL}?{urlencode(params)}"
    for attempt in range(retries):
        r = session.get(url, headers=_api_headers(token), timeout=timeout)
        if r.status_code in (200, 201):
            return ((r.json() or {}).get("response") or {}).get("searchId")
        if r.status_code in (401, 403):
            raise PermissionError("token_expired")
        time.sleep(4 * (attempt + 1))   # 429 / 5xx / transient throttle -> back off and retry
    return None


def _poll(session: requests.Session, token: str, search_id: str,
          page_limit: int = 30, max_pages: int = 8, settle_polls: int = 4,
          timeout: int = 30) -> List[Dict[str, Any]]:
    """Poll page 1 until results settle, then page through the rest."""
    hdr = {**_api_headers(token), "content-type": "application/json"}
    url = f"{SEARCH_URL}?searchId={search_id}"
    flights: List[Dict[str, Any]] = []
    # wait for page 1 to populate (search runs async server-side)
    for _ in range(settle_polls):
        time.sleep(5)
        rp = session.post(url, headers=hdr, json={"page": 1, "limit": page_limit}, timeout=timeout)
        resp = (rp.json() or {}).get("response") or {}
        flights = resp.get("matchedFlights") or resp.get("flights") or []
        if flights:
            break
    if not flights:
        return []
    # page through the remainder
    page = 2
    while page <= max_pages:
        rp = session.post(url, headers=hdr, json={"page": page, "limit": page_limit}, timeout=timeout)
        resp = (rp.json() or {}).get("response") or {}
        more = resp.get("matchedFlights") or resp.get("flights") or []
        if not more:
            break
        flights += more
        page += 1
    return flights


def fetch_live(origin: str, dest: str, date: str, cabin: str = "Economy",
               adt: int = 1, chd: int = 0, inf: int = 0,
               session: Optional[requests.Session] = None,
               token: Optional[str] = None) -> Dict[str, Any]:
    """One live ShareTrip search. Returns {ok, rows, token} (token reusable for next call)."""
    own = session is None
    s = session or requests.Session()
    try:
        if not token:
            token = mint_token(s, origin, dest, date, cabin)
            if not token:
                return {"ok": False, "rows": [], "token": None, "reason": "mint_failed"}
        try:
            sid = _initialize(s, token, origin, dest, date, cabin, adt, chd, inf)
        except PermissionError:                       # token expired -> re-mint once
            token = mint_token(s, origin, dest, date, cabin)
            sid = _initialize(s, token, origin, dest, date, cabin, adt, chd, inf) if token else None
        if not sid:
            return {"ok": False, "rows": [], "token": token, "reason": "no_searchId"}
        raw = _poll(s, token, sid)
        rows = [r for r in (_normalize(fl) for fl in raw) if r]
        return {"ok": bool(rows), "rows": rows, "token": token}
    finally:
        if own:
            s.close()
