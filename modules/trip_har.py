"""
Trip.com offer source — via MANUAL HAR import.

www.trip.com FlightListSearchSSE is protected by signed headers (token, x-ctx-fvpc,
x-ctx-wclient-req), so it's a manual-HAR source. Response is a single SSE `data:`
event holding `itineraryList` (each = journeyList[segments] + policies[fares]).
Fares are USD -> converted to BDT via modules.fx.

Usage:
  python tools/import_trip_har.py <file.har> [more.har ...]
"""
from __future__ import annotations

import datetime
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from modules.fx import to_bdt

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = REPO_ROOT / "output" / "manual_sessions" / "trip_cache.json"

AIRLINE_ALIAS = {"3L": "G9"}


def _alias(code: str) -> str:
    return AIRLINE_ALIAS.get(str(code or "").upper(), str(code or "").upper())


def _cabin_bucket(text: Any) -> str:
    t = str(text or "Economy").lower()
    return "business" if ("business" in t or "first" in t) else "economy"


def _fmt_layover(mins: Optional[int]) -> Optional[str]:
    if not mins or int(mins) <= 0:
        return None
    h, m = divmod(int(mins), 60)
    if h and m:
        return f"{h}h {m}m"
    return f"{h}h" if h else f"{m}m"


def _dt(s: str) -> Optional[datetime.datetime]:
    try:
        return datetime.datetime.fromisoformat(str(s).replace(" ", "T"))
    except ValueError:
        return None


def _checked_bag(policy: Dict[str, Any]) -> Optional[str]:
    """Extract checked baggage from a Trip.com policy's tagList; '0 KG' if carry-on only."""
    tags = policy.get("tagList") or []
    keys = {str(t.get("key")) for t in tags}
    for t in tags:
        if str(t.get("key")) != "FREE_CHECKED_BAGGAGE":
            continue
        blob = json.dumps(t)
        m = re.search(r"(\d+)\s*kg", blob, re.I)
        if m:
            return f"{int(m.group(1))} KG"
        return "Included"
    if "FREE_CARRY_ON_BAGGAGE" in keys:
        return "0 KG"  # carry-on only, no checked allowance
    return None


def _normalize(itin: Dict[str, Any], policy: Dict[str, Any], currency: str) -> Optional[Dict[str, Any]]:
    journeys = itin.get("journeyList") or []
    if not journeys:
        return None
    segs = journeys[0].get("transSectionList") or []
    if not segs:
        return None
    seg0, seg_last = segs[0], segs[-1]

    price = float(((policy.get("price") or {}).get("totalPrice")) or 0)
    if price <= 0:
        return None

    fi0 = seg0.get("flightInfo") or {}
    carrier = _alias(fi0.get("airlineCode"))
    if not carrier:
        return None
    operating_airlines = sorted({_alias((s.get("flightInfo") or {}).get("airlineCode")) for s in segs} - {""})

    origin = str((seg0.get("departPoint") or {}).get("airportCode") or "").upper()
    destination = str((seg_last.get("arrivePoint") or {}).get("airportCode") or "").upper()
    departure = seg0.get("departDateTime")
    arrival = seg_last.get("arriveDateTime")
    if not origin or not destination or not departure:
        return None

    via = "|".join((s.get("arrivePoint") or {}).get("airportCode") for s in segs[:-1]) or None
    layover_times: List[str] = []
    for i in range(len(segs) - 1):
        a = _dt(segs[i].get("arriveDateTime"))
        b = _dt(segs[i + 1].get("departDateTime"))
        if a and b:
            lt = _fmt_layover(int((b - a).total_seconds() // 60))
            if lt:
                layover_times.append(lt)

    grade_info = (policy.get("gradeInfoList") or [{}])[0]
    grade = grade_info.get("gradeMultilingual")
    rbd = str(grade_info.get("subClass") or "").upper().strip()
    cabin = _cabin_bucket(grade)
    craft = (fi0.get("craftInfo") or {}).get("shortName") or (fi0.get("craftInfo") or {}).get("name")

    return {
        "airline":            carrier,
        "operating_airline":  carrier,
        "operating_airlines": operating_airlines,
        "flight_number":      str(fi0.get("flightNo") or "").strip(),
        "origin":             origin,
        "destination":        destination,
        "departure_date":     str(departure)[:10],
        "departure":          departure,
        "arrival":            arrival,
        "cabin":              cabin,
        "cabin_class":        cabin,
        "rbd":                rbd,
        "brand":              "TRIP_OTA",
        "fare_class":         str(grade or "").replace(" class", "").strip() or None,
        "price_total_bdt":    round(to_bdt(price, currency)),
        "fare_currency_src":  currency,
        "currency":           "BDT",
        "duration_min":       int(journeys[0].get("duration") or 0) or None,
        "stops":              max(0, len(segs) - 1),
        "via_airports":       via,
        "layover_times":      layover_times,
        "aircraft":           str(craft or "").strip() or None,
        "baggage":            _checked_bag(policy),
        "fare_id":            str(policy.get("policyId") or policy.get("shortPolicyId") or ""),
        "source_endpoint":    "trip:FlightListSearchSSE",
    }


def parse_har(path: str | Path) -> List[Dict[str, Any]]:
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        if "FlightListSearchSSE" not in e.get("request", {}).get("url", ""):
            continue
        txt = (e.get("response", {}).get("content", {}) or {}).get("text", "") or ""
        if "data:" not in txt:
            continue
        try:
            data = json.loads(txt.split("data:", 1)[1].strip())
        except (json.JSONDecodeError, IndexError):
            continue
        currency = str((data.get("basicInfo") or {}).get("currency") or "USD").upper()
        for itin in data.get("itineraryList") or []:
            for policy in itin.get("policies") or []:
                row = _normalize(itin, policy, currency)
                if not row:
                    continue
                sig = (row["fare_id"], row["price_total_bdt"], row["departure"])
                if sig in seen:
                    continue
                seen.add(sig)
                rows.append(row)
    return rows


def _cache_key(origin: str, dest: str, date: str, cabin: str = "economy") -> str:
    return f"{origin.upper()}|{dest.upper()}|{str(date)[:10]}|{str(cabin).lower()}"


def load_cache() -> Dict[str, List[Dict[str, Any]]]:
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(cache: Dict[str, List[Dict[str, Any]]]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=1), encoding="utf-8")


def import_hars(paths: List[str | Path]) -> Dict[str, Any]:
    cache = load_cache()
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for p in paths:
        for row in parse_har(p):
            key = _cache_key(row["origin"], row["destination"],
                             row["departure_date"], row.get("cabin_class", "economy"))
            buckets.setdefault(key, []).append(row)
    for key, rows in buckets.items():
        seen: set = set()
        uniq: List[Dict[str, Any]] = []
        for r in rows:
            sig = (r.get("fare_id"), r.get("price_total_bdt"), r.get("departure"))
            if sig in seen:
                continue
            seen.add(sig)
            uniq.append(r)
        cache[key] = uniq
    save_cache(cache)
    return {"keys_updated": sorted(buckets), "offers_imported": sum(len(v) for v in buckets.values()),
            "total_cache_keys": len(cache)}


def cache_rows(origin: str, dest: str, date: str, cabin: str = "economy") -> List[Dict[str, Any]]:
    return load_cache().get(_cache_key(origin, dest, date, cabin), [])
