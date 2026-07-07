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


def _harvest_fare_details(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Exact fare breakdowns from GetAirSearchItinerary (the Fare Summary tab).

    The base/(base+tax) split varies WILDLY per airline (field case, DAC-DXB:
    BG base share 76.7% vs AI 58.8% — AI's taxes are huge), so one global ratio
    misstates most airlines. The Fare Summary response carries the offer's
    itineraryId + baseFare + tax, giving:
      by_itinerary: itineraryId -> exact base (authoritative for that offer)
      by_airline:   (airline, origin, destination) -> measured base ratio
      all_ratios:   every measured ratio (average = last-resort estimate)
    """
    by_itinerary: Dict[str, float] = {}
    by_airline: Dict[tuple, float] = {}
    all_ratios: List[float] = []

    for entry in entries:
        url = entry.get("request", {}).get("url", "")
        if "GetAirSearchItinerary" not in url:
            continue
        data = _jload(entry)
        if not isinstance(data, dict):
            continue
        itin_match = re.search(r"itineraryId=([^&\s]+)", url)
        itin_id = itin_match.group(1) if itin_match else ""
        for info in data.get("flightInfos") or []:
            base = tax = None
            for tf in info.get("travelerFareSummaries") or []:
                if str(tf.get("travelerType", "")).lower() == "adult":
                    base, tax = _money(tf.get("baseFare")), _money(tf.get("tax"))
                    break
            if not base or tax is None or (base + tax) <= 0:
                continue
            ratio = base / (base + tax)
            all_ratios.append(ratio)
            itins = info.get("itineraries") or []
            airline = ""
            origin = dest = ""
            if itins:
                origin = str(itins[0].get("departure") or "").upper()
                dest = str(itins[0].get("arrival") or "").upper()
                legs = itins[0].get("legs") or []
                if legs:
                    airline = _alias(legs[0].get("airlineCode"))
            resp_itin = itin_id or str(data.get("itineraryId") or "")
            if resp_itin:
                by_itinerary[resp_itin] = base
            if airline:
                by_airline[(airline, origin, dest)] = ratio
    return {"by_itinerary": by_itinerary, "by_airline": by_airline,
            "avg_ratio": (sum(all_ratios) / len(all_ratios)) if all_ratios
            else DEFAULT_BASE_GROSS_RATIO,
            "measured": bool(all_ratios)}


def _offer_route(offer: Dict[str, Any]) -> tuple[str, str]:
    journeys = offer.get("journeyWises") or []
    if not journeys:
        return "", ""
    return (str(journeys[0].get("departure") or "").upper(),
            str(journeys[-1].get("arrival") or "").upper())


def fare_match_key(airline: str, origin: str, destination: str,
                   day: int, month: str, dep_time: str,
                   gross: float) -> tuple:
    """Cross-source flight identity: same airline + route + departure day/month
    + departure time + gross fare = the same published fare, so the base fare
    from a solid source (Amy / USBA-FT B2B) is authoritative for it."""
    return (str(airline).upper(), str(origin).upper(), str(destination).upper(),
            int(day), str(month)[:3].lower(), str(dep_time)[:5], round(float(gross)))


def _offer_match_key(offer: Dict[str, Any], airline: str, origin: str,
                     destination: str, gross: float) -> Optional[tuple]:
    """Match key from a search offer's flightSummary (departureDate '30 Jul, Thu'
    + departureTime '15:10')."""
    for fs in offer.get("flightSummary") or []:
        m = re.match(r"\s*(\d{1,2})\s+([A-Za-z]{3})", str(fs.get("departureDate") or ""))
        dep_time = str(fs.get("departureTime") or "")[:5]
        if m and dep_time:
            return fare_match_key(airline, origin, destination,
                                  int(m.group(1)), m.group(2), dep_time, gross)
    return None


def parse_commissions(path: str | Path,
                      base_index: Optional[Dict[tuple, tuple]] = None) -> List[Dict[str, Any]]:
    """Per-offer BDFare agent commission rows from a searchpad HAR.

    Base-fare resolution, best first (never silently guess):
      market  — the SAME flight (airline+route+date+time+gross) found on a solid
                source (Amy / USBA-FT B2B) supplies its base; if BDFare's own
                Fare Summary disagrees, the market base wins and the alteration
                is logged;
      exact   — the offer's own Fare Summary was captured (open Flight Details
                -> Fare Summary before exporting);
      airline_ratio / har_avg_ratio / default_ratio — ESTIMATES, marked so the
                grid can render them as '~x.x'.
    base_index maps fare_match_key(...) -> (base_fare, source_label)."""
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    fares = _harvest_fare_details(entries)
    base_index = base_index or {}

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
    skipped_mixed = skipped_nearby = 0
    for offer in offers:
        airline = _alias(offer.get("airlineCode"))
        gross = _money(offer.get("grossAmount"))
        agent = _money(offer.get("agentAmount"))
        customer_net = _money(offer.get("customerNetAmount"))
        if not airline or not gross or not agent or gross <= 0:
            continue
        # BDFare injects "nearby airport" results into a search (field case: a
        # DAC-DXB search returned DAC-XNB — Dubai Chelsea BUS STATION — via
        # BG+EY with a bus leg). Not the searched route: skip.
        if offer.get("nearbyAirports"):
            skipped_nearby += 1
            continue
        # Mixed-carrier itineraries are attributed to the first marketing
        # carrier but their commission isn't that airline's discount: skip.
        carriers = {_alias(c)
                    for fs in offer.get("flightSummary") or []
                    for c in fs.get("airlineCode") or []}
        if len(carriers) > 1:
            skipped_mixed += 1
            continue
        # gross excludes AIT VAT while agentAmount includes it, so gross - agent is
        # the VAT-neutral saving; customerNet - agent would count the VAT as discount.
        commission = gross - agent
        origin, destination = _offer_route(offer)
        itin_id = str(offer.get("itineraryId") or "")
        own_base = fares["by_itinerary"].get(itin_id) if itin_id else None
        mkey = _offer_match_key(offer, airline, origin, destination, gross)
        market = base_index.get(mkey) if mkey else None
        if market is not None:
            base, base_source = float(market[0]), "market"
            if own_base is not None and abs(own_base - base) > 1:
                print(f"BDFare {airline} {origin}-{destination}: own Fare Summary "
                      f"base {own_base:,.0f} differs from {market[1]} base "
                      f"{base:,.0f} for the same flight - using the market base")
        elif own_base is not None:
            base, base_source = own_base, "exact"
        elif (airline, origin, destination) in fares["by_airline"]:
            base = gross * fares["by_airline"][(airline, origin, destination)]
            base_source = "airline_ratio"
        elif fares["measured"]:
            base, base_source = gross * fares["avg_ratio"], "har_avg_ratio"
        else:
            base, base_source = gross * DEFAULT_BASE_GROSS_RATIO, "default_ratio"
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
            "base_source": base_source,
            "commission_bdt": round(commission),
            "commission_pct": round(commission / base * 100, 2) if base else 0.0,
        })
    if skipped_nearby or skipped_mixed:
        # stdout is captured into the desktop Run log — no silent drops.
        print(f"BDFare: skipped {skipped_nearby} nearby-airport and {skipped_mixed} "
              f"mixed-carrier offer(s) - not a single airline's fare on the searched route")
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
                    "base_source": cheapest.get("base_source", "default_ratio"),
                    "n_offers": len(items),
                    "pct_min": min(pcts), "pct_max": max(pcts)}
    return out
