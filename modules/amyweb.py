"""
AMY public flight-search connector (amyx.amybd.com).

This is the PUBLIC search API behind https://amyweb.amybd.com — distinct from
modules/amybd.py, which drives the sessionized www.amybd.com/atapi.aspx feed used
as a BS/2A OTA fallback. This one needs no cookies and no auth header: a single
JSON POST with a `_FLIGHTFIND_` command returns every airline on the route.

The only stateful bit is a short `TOKEN` string that is session-bound. It is
captured from a browser HAR. Provide a fresh one via the AMYWEB_TOKEN env var
when the bundled default stops returning results (success=False / 0 trips).

Contract: fetch_flights(...) -> {"ok": bool, "rows": [...], "raw": {...}}
Row shape mirrors modules/firsttrip.py so the two sources merge cleanly.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date as _date
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

LOG = logging.getLogger(__name__)

API_URL = os.getenv("AMYWEB_API_URL", "https://amyx.amybd.com/api.aspx")
# Token captured from amyweb.amybd.com HAR (2026-06). Session-bound; override via env.
DEFAULT_TOKEN = "qqLOGjLTOqnXGOLGLTGnqiiK"
ENV_TOKEN = "AMYWEB_TOKEN"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)
CABIN_MAP = {"economy": "Y", "business": "C", "first": "F", "premium_economy": "W"}

# Air Arabia is sold under both 3L (Abu Dhabi) and G9 (Sharjah). Unify to G9.
AIRLINE_ALIAS = {"3L": "G9"}


def _get_token() -> str:
    return os.getenv(ENV_TOKEN) or DEFAULT_TOKEN


def _fmt_layover(mins: Optional[int]) -> Optional[str]:
    """55 -> '55m', 200 -> '3h 20m', 240 -> '4h' (matches FirstTrip layover style)."""
    if not mins or mins <= 0:
        return None
    h, m = divmod(int(mins), 60)
    if h and m:
        return f"{h}h {m}m"
    if h:
        return f"{h}h"
    return f"{m}m"


def _norm_baggage(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return str(raw).replace(" kg", " KG").replace("kg", "KG").strip() or None


def _alias(code: str) -> str:
    return AIRLINE_ALIAS.get(code, code)


def _segments_for_leg(leg: Dict[str, Any], trip_by_id: Dict[Any, Dict]) -> List[Dict]:
    refs = leg.get("TripsR") or []
    segs = [trip_by_id.get(r.get("trip_id")) for r in refs]
    return [s for s in segs if s]


def _normalize_offer(
    trip: Dict[str, Any],
    leg_by_id: Dict[Any, Dict],
    trip_by_id: Dict[Any, Dict],
    requested_cabin: str,
    airline_filter: Optional[str],
) -> Optional[Dict[str, Any]]:
    carrier = _alias(str(trip.get("air_code") or "").upper().strip())
    if not carrier:
        return None
    if airline_filter and carrier != airline_filter.upper():
        return None

    fares = trip.get("Fares") or []
    prices = [float(f.get("tot_fare") or 0) for f in fares if float(f.get("tot_fare") or 0) > 0]
    if not prices:
        return None
    price = min(prices)

    leg_refs = trip.get("LegsRef") or []
    leg = leg_by_id.get(leg_refs[0].get("leg_id")) if leg_refs else None
    if not leg:
        return None

    segs = _segments_for_leg(leg, trip_by_id)
    seg0 = segs[0] if segs else {}
    seg_last = segs[-1] if segs else {}

    # via airports = intermediate stop airports (FirstTrip uses "|" join)
    transits = [t for t in (leg.get("fTransit1"), leg.get("fTransit2")) if t]
    via = "|".join(transits) if transits else None
    layovers = [_fmt_layover(leg.get("fLayover1")), _fmt_layover(leg.get("fLayover2"))]
    layover_times = [lt for lt in layovers if lt]

    origin = str(leg.get("fFrom") or trip.get("fFrom") or "").upper()
    destination = str(leg.get("fDest") or trip.get("fDest") or "").upper()
    departure = leg.get("fDTime") or seg0.get("fDTime")
    arrival = leg.get("fATime") or seg_last.get("fATime")
    if not origin or not destination or not departure or price <= 0:
        return None

    seats = [int(s.get("fSeat") or 0) for s in segs if s.get("fSeat")]
    seat_available = min(seats) if seats else None

    # Operating carriers across segments (codeshare detection). AMY: air_code=operating, m_air_code=marketing.
    operating_airlines = sorted({
        _alias(str(s.get("air_code") or carrier).upper()) for s in segs
    } - {""}) or [carrier]

    return {
        "airline":              carrier,
        "operating_airline":    _alias(str(seg0.get("m_air_code") or carrier).upper()),
        "flight_number":        str(seg0.get("fNo") or "").strip(),
        "origin":               origin,
        "destination":          destination,
        "departure":            departure,
        "arrival":              arrival,
        "cabin":                str(trip.get("fCabin") or requested_cabin or "Economy"),
        "fare_basis":           str(seg0.get("fBasis") or "").strip() or None,
        "brand":                "AMYWEB_OTA",
        "price_total_bdt":      price,
        "fare_amount":          float((fares[0].get("base_fare") or 0)) if fares else 0.0,
        "tax_amount":           float((fares[0].get("tax_fare") or 0)) if fares else 0.0,
        "currency":             "BDT",
        "duration_min":         int(leg.get("fDuration") or 0) or None,
        "stops":                int(leg.get("fStop") or 0),
        "via_airports":         via,
        "layover_times":        layover_times,
        "aircraft":             str(seg0.get("fModel") or "").strip() or None,
        "baggage":              _norm_baggage(seg0.get("fBag")),
        "seat_available":       seat_available,
        "seat_capacity":        None,
        "fare_refundable":      str(trip.get("fRefund") or "").upper() == "REFUND",
        "adt_count":            1,
        "chd_count":            0,
        "inf_count":            0,
        "source_endpoint":      API_URL,
        "inventory_confidence": None,
    }


def fetch_flights(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    airline_code: Optional[str] = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    try:
        j_date = _date.fromisoformat(str(date)).strftime("%d-%b-%Y")
    except ValueError:
        j_date = str(date)

    payload = {
        "CMND": "_FLIGHTFIND_",
        "TRIPS": [{"j_from": origin.upper(), "j_to": destination.upper(), "j_date": j_date}],
        "CABIN": CABIN_MAP.get(cabin.lower(), "Y"),
        "ADT": max(1, int(adt or 1)),
        "CHD": max(0, int(chd or 0)),
        "INF": max(0, int(inf or 0)),
        "DOBC1": "", "DOBC2": "", "DOBC3": "", "DOBC4": "",
        "Umrah": 0,
        "is_micro": 0,
        "TOKEN": _get_token(),
        "is_pack": 0,
        "is_combo": 0,
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://amyweb.amybd.com",
        "Referer": "https://amyweb.amybd.com/",
        "User-Agent": USER_AGENT,
    }

    out: Dict[str, Any] = {"source": "amyweb", "ok": False, "rows": [], "raw": {}}
    try:
        resp = requests.post(API_URL, json=payload, headers=headers, timeout=timeout)
        out["raw"]["status"] = resp.status_code
        if resp.status_code != 200:
            out["raw"]["error"] = f"HTTP {resp.status_code}"
            return out
        data = resp.json()
        if not data.get("success"):
            out["raw"]["error"] = "token_rejected_or_no_results"
            out["raw"]["message"] = data.get("message") or ""
            LOG.warning("[amyweb] %s->%s %s: success=False (token may be stale)",
                        origin, destination, date)
            return out

        leg_by_id = {l.get("leg_id"): l for l in (data.get("LegData") or [])}
        trip_by_id = {t.get("trip_id"): t for t in (data.get("TripData") or [])}
        rows: List[Dict] = []
        for trip in data.get("TripList") or []:
            row = _normalize_offer(trip, leg_by_id, trip_by_id, cabin, airline_code)
            if row:
                rows.append(row)
        out["rows"] = rows
        out["ok"] = len(rows) > 0
        out["raw"]["total_offers"] = len(rows)
        out["raw"]["search_id"] = data.get("SearchID")
    except requests.exceptions.Timeout:
        out["raw"]["error"] = "timeout"
        LOG.warning("[amyweb] %s->%s %s: timeout after %ss", origin, destination, date, timeout)
    except Exception as exc:  # noqa: BLE001 - network/parse failures must not break dispatch
        out["raw"]["error"] = str(exc)
        LOG.warning("[amyweb] %s->%s %s: %s", origin, destination, date, exc)

    return out


def _commission_row(trip: Dict[str, Any], leg_by_id: Dict[Any, Dict]) -> Optional[Dict[str, Any]]:
    """Build one commission row from an AmyBD agent-session trip.

    The logged-in session exposes net_pay (agent cost) alongside tot_fare (sell
    price). Commission = tot_fare - net_pay, expressed as a percent of base_fare
    to match the manual report convention.
    """
    carrier = _alias(str(trip.get("air_code") or "").upper().strip())
    if not carrier:
        return None
    fares = trip.get("Fares") or []
    if not fares:
        return None
    # Representative = cheapest bookable fare (lowest agent net_pay).
    fare = min(fares, key=lambda x: float(x.get("net_pay") or x.get("tot_fare") or 1e12))
    tot = float(fare.get("tot_fare") or 0)
    net = float(fare.get("net_pay") or 0)
    base = float(fare.get("base_fare") or 0)
    if tot <= 0 or net <= 0 or base <= 0:
        return None

    leg_refs = trip.get("LegsRef") or []
    leg = leg_by_id.get(leg_refs[0].get("leg_id")) if leg_refs else None
    origin = str((leg or {}).get("fFrom") or trip.get("fFrom") or "").upper()
    destination = str((leg or {}).get("fDest") or trip.get("fDest") or "").upper()
    departure = (leg or {}).get("fDTime") or trip.get("JDate")

    commission = tot - net
    return {
        "channel": "amy",
        "persona": "B2B",
        "airline": carrier,
        "origin": origin,
        "destination": destination,
        "departure": departure,
        "fare_basis": str(fare.get("fbasis") or "").strip(),
        "tot_fare": round(tot),
        "net_pay": round(net),
        "base_fare": round(base),
        "commission_bdt": round(commission),
        "commission_pct": round(commission / base * 100, 2),       # % off base (report basis)
        "commission_pct_tot": round(commission / tot * 100, 2),    # % off gross total
        "discount": float(fare.get("discount") or 0),
        "cashback": float(fare.get("cashback") or 0),
    }


def parse_agent_har(path: str | Path) -> List[Dict[str, Any]]:
    """
    Parse a logged-in AmyBD agent HAR (amyx.amybd.com api.aspx, CMND=_FLIGHTFIND_)
    into per-airline commission rows. Auth is by session cookie, so the live
    public fetch_flights() cannot see net_pay; this HAR path is the agent surface.
    """
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        try:
            body = json.loads((e.get("request", {}).get("postData") or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        if not isinstance(body, dict) or body.get("CMND") != "_FLIGHTFIND_":
            continue
        try:
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict) or not data.get("TripList"):
            continue
        leg_by_id = {l.get("leg_id"): l for l in (data.get("LegData") or [])}
        for trip in data["TripList"]:
            row = _commission_row(trip, leg_by_id)
            if not row:
                continue
            sig = (row["airline"], row["origin"], row["destination"], row["departure"], row["fare_basis"])
            if sig in seen:
                continue
            seen.add(sig)
            rows.append(row)
    return rows


def summarize_commissions(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """One grid cell per airline: commission % of the representative (cheapest) fare."""
    by_air: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_air.setdefault(r["airline"], []).append(r)
    out: Dict[str, Dict[str, Any]] = {}
    for airline, items in by_air.items():
        best = min(items, key=lambda r: r["net_pay"])
        out[airline] = {
            "commission_pct": best["commission_pct"],
            "commission_pct_tot": best["commission_pct_tot"],
            "commission_bdt": best["commission_bdt"],
            "fare_basis": best["fare_basis"],
        }
    return out
