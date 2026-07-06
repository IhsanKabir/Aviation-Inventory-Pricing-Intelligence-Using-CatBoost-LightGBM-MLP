"""
BDFare B2B (bdfare.com/searchpad) commission — via MANUAL HAR import.

The agent search posts to /bdfare-search/api/v2/Search/AirSearch (and
RefreshAirSearch). Each offer carries totals only:
  grossAmount (base+tax, NO AIT VAT), agentAmount (agent buy, VAT INCLUDED),
  customerNetAmount (customer sell, VAT included).
The agent's real saving = grossAmount - agentAmount. customerNetAmount -
agentAmount is the UI's "Discount" figure but includes the AIT VAT the agent
pays regardless of channel, overstating the commission (field-verified on
BG 247 DAC-DXB: gross 45,834 / agent 43,337 / base 35,138 -> 7.106%, while
customerNet 45,972 - agent = 2,635 -> a false 7.5%).

The report expresses commission as a percent of BASE fare, but the search list
has no base. The itinerary detail does (baseFare + tax), so we derive a
base/(base+tax) ratio per route type and estimate base = gross * ratio. Auth is
a logged-in session, so this is HAR-only.

Usage:
  python tools/import_bdfare_har.py <file.har>
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

DOMESTIC_AIRPORTS = {"DAC", "CGP", "CXB", "ZYL", "SPD", "BZL", "RJH", "JSR", "SAH", "TKR", "IRD", "KMI"}
AIRLINE_ALIAS = {"3L": "G9"}

# Fallback base/(base+tax) ratio when a HAR has no GetAirSearchItinerary detail to
# measure it from. The report expresses commission as a percent of BASE fare, so a
# ratio is required; 1.0 (assume zero tax) systematically UNDERSTATES commission and
# is never realistic. ~0.767 is the empirically observed domestic base/gross split
# (e.g. base 4024 / gross 5249). Capturing a flight-detail view yields the exact
# per-capture ratio and overrides this default.
DEFAULT_BASE_GROSS_RATIO = 0.767


def _alias(code: Any) -> str:
    return AIRLINE_ALIAS.get(str(code or "").upper(), str(code or "").upper())


def _money(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"[-+]?\d[\d,]*\.?\d*", str(value))
    return float(match.group(0).replace(",", "")) if match else None


def _route_type(origin: str, destination: str) -> str:
    return "DOM" if origin in DOMESTIC_AIRPORTS and destination in DOMESTIC_AIRPORTS else "INTL"


def _jload(entry: Dict[str, Any]) -> Any:
    try:
        return json.loads((entry.get("response", {}).get("content", {}) or {}).get("text", "") or "")
    except (ValueError, json.JSONDecodeError):
        return None


def _base_gross_ratio(entries: List[Dict[str, Any]]) -> float:
    """Average base/(base+tax) from itinerary-detail breakdowns; falls back to
    DEFAULT_BASE_GROSS_RATIO when the HAR has no usable detail (assuming zero tax
    via 1.0 would understate every commission)."""
    ratios: List[float] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            base = _money(node.get("baseFare"))
            tax = _money(node.get("tax"))
            if base and tax is not None and (base + tax) > 0:
                ratios.append(base / (base + tax))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for entry in entries:
        if "GetAirSearchItinerary" in entry.get("request", {}).get("url", ""):
            walk(_jload(entry))
    return sum(ratios) / len(ratios) if ratios else DEFAULT_BASE_GROSS_RATIO


def _offer_route(offer: Dict[str, Any]) -> tuple[str, str]:
    journeys = offer.get("journeyWises") or []
    if not journeys:
        return "", ""
    return (str(journeys[0].get("departure") or "").upper(),
            str(journeys[-1].get("arrival") or "").upper())


def parse_commissions(path: str | Path) -> List[Dict[str, Any]]:
    """Per-offer BDFare agent commission rows from a searchpad HAR."""
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    ratio = _base_gross_ratio(entries)

    offers: List[Dict[str, Any]] = []

    def collect(node: Any) -> None:
        if isinstance(node, dict):
            if "grossAmount" in node and "agentAmount" in node:
                offers.append(node)
            for value in node.values():
                collect(value)
        elif isinstance(node, list):
            for value in node:
                collect(value)

    for entry in entries:
        url = entry.get("request", {}).get("url", "")
        if "/Search/AirSearch" in url or "/Search/RefreshAirSearch" in url:
            collect(_jload(entry))

    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for offer in offers:
        airline = _alias(offer.get("airlineCode"))
        gross = _money(offer.get("grossAmount"))
        agent = _money(offer.get("agentAmount"))
        customer_net = _money(offer.get("customerNetAmount"))
        if not airline or not gross or not agent or gross <= 0:
            continue
        # gross excludes AIT VAT while agentAmount includes it, so gross - agent is
        # the VAT-neutral saving; customerNet - agent would count the VAT as discount.
        commission = gross - agent
        base = gross * ratio
        origin, destination = _offer_route(offer)
        sig = (airline, origin, destination, round(gross), round(agent))
        if sig in seen:
            continue
        seen.add(sig)
        rows.append({
            "channel": "bdfare",
            "persona": "B2B",
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "domestic": _route_type(origin, destination) == "DOM",
            "gross_bdt": round(gross),
            "agent_bdt": round(agent),
            "customer_net_bdt": round(customer_net) if customer_net is not None else None,
            "base_est_bdt": round(base),
            "commission_bdt": round(commission),
            "commission_pct": round(commission / base * 100, 2) if base else 0.0,
        })
    return rows


def summarize_commissions(rows: List[Dict[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    """One cell per (route_type, airline): the CHEAPEST offer's commission %.

    Premium fares carry higher commission tiers (field case: a 170k DAC-XNB
    itinerary paid 8.7% while the lead 65k DAC-DXB economy fare paid 7.2%) —
    max() reported a rate nobody sees on the fare they actually compare.
    The cheapest offer is the like-for-like anchor across channels; the full
    spread is returned so callers can surface it (run log)."""
    by_cell: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        rt = "DOM" if r["domestic"] else "INTL"
        by_cell.setdefault((rt, r["airline"]), []).append(r)
    out: Dict[tuple[str, str], Dict[str, Any]] = {}
    for key, items in by_cell.items():
        cheapest = min(items, key=lambda r: r["gross_bdt"])
        pcts = [r["commission_pct"] for r in items]
        out[key] = {"value": cheapest["commission_pct"],
                    "commission_bdt": cheapest["commission_bdt"],
                    "offer_route": f"{cheapest['origin']}-{cheapest['destination']}",
                    "offer_gross_bdt": cheapest["gross_bdt"],
                    "n_offers": len(items),
                    "pct_min": min(pcts), "pct_max": max(pcts)}
    return out
