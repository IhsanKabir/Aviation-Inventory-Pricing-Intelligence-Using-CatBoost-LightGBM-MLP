"""Which GoZayaan coupon lists a manual capture still needs — pure, offline.

A manual capture costs one search-and-click cycle per airline, because GoZayaan
only loads an airline's coupon list when that airline's booking page opens.
The archive shows where that time is wasted:

  * repeating an airline+market inside one session never changed its coupon
    list (14 of 14 repeat groups identical), so a second click adds nothing;
  * the coupon request carries no route, and same-month captures on different
    routes carried the same codes, so ONE route per market is enough.

This reads a session's HAR(s) and reports, per market (DOM / OUTBOUND), which
airlines the searches offered, which already have a coupon list captured, and
which are still missing — the exact short list left to click.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List

from modules.gozayaan_discounts_core import carrier_prices_from_legs, flight_type_for

SEARCH_SUFFIX = "/flight/v4.0/search/"
LEGS_SUFFIX = "/flight/v4.0/search/legs/"
DISCOUNT_SUFFIX = "/business_rules/get_discount_list/"


@dataclass
class MarketCoverage:
    offered: set = field(default_factory=set)       # airlines the searches returned
    captured: Dict[str, int] = field(default_factory=dict)   # airline -> coupon requests
    routes: set = field(default_factory=set)

    @property
    def missing(self) -> List[str]:
        return sorted(self.offered - set(self.captured))

    def missing_among(self, tracked: Iterable[str]) -> List[str]:
        """Missing airlines limited to the ones the report actually shows —
        a search offers ~20 international carriers, and clicking all of them
        is only worth it for the columns someone reads."""
        wanted = {str(a).upper() for a in tracked}
        return [a for a in self.missing if a in wanted]

    @property
    def repeats(self) -> Dict[str, int]:
        return {a: n for a, n in sorted(self.captured.items()) if n > 1}


def _json(text: Any) -> Any:
    try:
        return json.loads(text or "{}")
    except (TypeError, ValueError):
        return {}


def coverage_from_entries(entries: Iterable[Dict[str, Any]]) -> Dict[str, MarketCoverage]:
    """HAR entries -> {"DOM"|"OUTBOUND": MarketCoverage}."""
    market_of_search: Dict[str, str] = {}
    route_of_search: Dict[str, str] = {}
    offered_by_search: Dict[str, set] = {}
    out: Dict[str, MarketCoverage] = {}

    for e in entries:
        request = e.get("request") or {}
        url = str(request.get("url") or "").split("?")[0]
        body = _json((request.get("postData") or {}).get("text"))
        resp = _json(((e.get("response") or {}).get("content") or {}).get("text"))
        if not isinstance(body, dict):
            continue
        if url.endswith(SEARCH_SUFFIX):
            result = resp.get("result") if isinstance(resp, dict) else None
            sid = result.get("search_id") if isinstance(result, dict) else None
            trip = (body.get("trips") or [{}])[0] or {}
            origin, dest = str(trip.get("origin") or ""), str(trip.get("destination") or "")
            if sid and origin and dest:
                market_of_search[str(sid)] = flight_type_for(origin, dest)
                route_of_search[str(sid)] = f"{origin}-{dest}"
        elif url.endswith(LEGS_SUFFIX):
            offered_by_search.setdefault(str(body.get("search_id")), set()).update(
                carrier_prices_from_legs(resp))
        elif url.endswith(DISCOUNT_SUFFIX):
            market = str(body.get("flight_type") or "").upper()
            airline = str(body.get("plating_carrier") or "").upper()
            if market and airline:
                cov = out.setdefault(market, MarketCoverage())
                cov.captured[airline] = cov.captured.get(airline, 0) + 1

    for sid, market in market_of_search.items():
        cov = out.setdefault(market, MarketCoverage())
        cov.offered |= offered_by_search.get(sid, set())
        cov.routes.add(route_of_search[sid])
    return out


def coverage_for_hars(paths: Iterable[Path]) -> Dict[str, MarketCoverage]:
    """Merge several HARs (one session may be split across files)."""
    entries: List[Dict[str, Any]] = []
    for path in paths:
        try:
            har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
        except (OSError, ValueError):
            continue
        entries.extend((har.get("log") or {}).get("entries") or [])
    return coverage_from_entries(entries)
