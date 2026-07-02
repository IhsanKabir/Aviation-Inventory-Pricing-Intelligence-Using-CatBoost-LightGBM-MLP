"""
AkbarTravels offer source — via MANUAL HAR import.

akbartravels.com sits behind an AWS WAF that runs a per-subdomain JavaScript
"Human Verification" proof-of-work challenge on its API hosts (b2capi/b2capit).
Scripted requests (requests / curl_cffi TLS-impersonation / even in-browser
fetch) are blocked, so there is no live connector. Instead the operator captures
a HAR from a real browser search and we parse it.

Workflow:
  1. Chrome > DevTools > Network, search a route/date on www.akbartravels.com,
     then "Save all as HAR with content".
  2. python tools/import_akbartravels_har.py <file.har> [more.har ...]
  3. The KSA report merges cached offers for matching origin/destination/date.

Notes:
- Unique value vs FirstTrip: carries G9 (Air Arabia) via SHJ. 3L is aliased to G9.
- Every Journey is one fare family -> all fares captured (Lowest..Highest spread).
- Fares are INR; converted to BDT via AKBAR_INR_BDT (default 1.40, env-overridable).
- Codeshare: MAC (marketing) vs OAC (operating) captured per journey.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = REPO_ROOT / "output" / "manual_sessions" / "akbartravels_cache.json"

AIRLINE_ALIAS = {"3L": "G9"}  # Air Arabia Abu Dhabi (3L) -> Air Arabia (G9)
DEFAULT_INR_BDT = 1.40


def _inr_bdt() -> float:
    env = os.getenv("AKBAR_INR_BDT")
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    try:
        from modules.fx import rate
        return rate("INR")
    except Exception:  # noqa: BLE001
        return DEFAULT_INR_BDT


def _alias(code: str) -> str:
    return AIRLINE_ALIAS.get(code, code)


def _cabin_bucket(code: Any) -> str:
    """Map AkbarTravels cabin code to economy/business. E=Economy, B/C/D/F/I/J=premium."""
    c = str(code or "E").upper().strip()
    return "business" if (c and c[0] in ("B", "C", "D", "F", "I", "J", "Z")) else "economy"


def _dur_to_min(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    h = re.search(r"(\d+)\s*h", text)
    m = re.search(r"(\d+)\s*m", text)
    val = (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0)
    return val or None


def _fmt_layover(text: Optional[str]) -> Optional[str]:
    """'04h 30m ' -> '4h 30m'; '00h 55m' -> '55m'."""
    mins = _dur_to_min(text)
    if not mins:
        return None
    h, m = divmod(mins, 60)
    if h and m:
        return f"{h}h {m}m"
    if h:
        return f"{h}h"
    return f"{m}m"


def _fare_currency_amount(journey: Dict[str, Any]) -> tuple[Optional[str], float]:
    """Currency + gross fare. FareKey looks like 'INR,B,41070~INR,B,0'."""
    fk = str(journey.get("FareKey") or "")
    cur = None
    parts = fk.split(",")
    if parts and parts[0].isalpha():
        cur = parts[0].upper()
    amt = float(journey.get("GrossFare") or journey.get("NetFare") or journey.get("TotalFare") or 0)
    return cur, amt


def _checked_bag(raw: Optional[str]) -> Optional[str]:
    """GetFareFeatures 'Checked Bag' -> '30 KG' / '0 KG' (N=none) / None."""
    if raw is None:
        return None
    s = str(raw).split("|")[0].strip()  # first sector
    if not s or s.upper() == "N":
        return "0 KG"
    m = re.search(r"(\d+)\s*KG", s, re.I)
    return f"{int(m.group(1))} KG" if m else (s or None)


def _baggage_map_from_har(entries: list) -> Dict[str, Dict[int, str]]:
    """{Journeykey: {int(amount): checked_bag}} from any GetFareFeatures responses."""
    bmap: Dict[str, Dict[int, str]] = {}
    for e in entries:
        if "GetFareFeatures" not in e.get("request", {}).get("url", ""):
            continue
        text = (e.get("response", {}).get("content", {}) or {}).get("text", "") or ""
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        for ff in data.get("FareFeatures") or []:
            jk = str(ff.get("Journeykey") or "")
            bag = _checked_bag((ff.get("Amenities") or {}).get("Checked Bag"))
            if jk and bag:
                bmap.setdefault(jk, {})[int(ff.get("Amount") or 0)] = bag
    return bmap


def _normalize_journey(j: Dict[str, Any],
                       baggage_map: Optional[Dict[str, Dict[int, str]]] = None) -> Optional[Dict[str, Any]]:
    marketing = _alias(str(j.get("MAC") or j.get("VAC") or "").upper().strip())
    if not marketing:
        return None
    operating = _alias(str(j.get("OAC") or marketing).upper().strip())

    origin = str(j.get("From") or "").upper()
    destination = str(j.get("To") or "").upper()
    departure = j.get("DepartureTime")
    arrival = j.get("ArrivalTime")
    if not origin or not destination or not departure:
        return None

    cur, amt = _fare_currency_amount(j)
    if amt <= 0:
        return None
    price_bdt = amt * _inr_bdt() if (cur in (None, "INR")) else amt

    conns = j.get("Connections") or []
    via = "|".join(c.get("Airport", "") for c in conns if c.get("Airport")) or None
    layovers = [_fmt_layover(c.get("Duration")) for c in conns]
    layover_times = [lt for lt in layovers if lt]

    # operating carriers across journey + connections (codeshare)
    op_set = {operating}
    for c in conns:
        mac = str(c.get("MAC") or "").split("|")[0].strip().upper()
        if mac:
            op_set.add(_alias(mac))
    operating_airlines = sorted(op_set - {""})

    # Baggage from a GetFareFeatures map (only present if "Details/View Fare" was opened)
    baggage = None
    if baggage_map:
        fam = baggage_map.get(str(j.get("JourneyKey") or ""))
        if fam:
            baggage = fam.get(int(amt)) or (next(iter(fam.values())) if len(fam) == 1
                                            else "/".join(sorted(set(fam.values()))))

    return {
        "airline":            marketing,
        "operating_airline":  operating,
        "operating_airlines": operating_airlines,
        "flight_number":      str(j.get("FlightNo") or "").strip(),
        "origin":             origin,
        "destination":        destination,
        "departure_date":     str(departure)[:10],
        "departure":          departure,
        "arrival":            arrival,
        "cabin":              str(j.get("Cabin") or "E"),
        "cabin_class":        _cabin_bucket(j.get("Cabin")),
        "fare_basis":         str(j.get("FBC") or "").strip() or None,
        "rbd":                str(j.get("RBD") or "").upper().strip(),
        "brand":              "AKBAR_OTA",
        "fare_class":         str(j.get("FareClass") or "").strip() or None,
        "price_total_bdt":    round(price_bdt),
        "fare_currency_src":  cur or "INR",
        "currency":           "BDT",
        "duration_min":       _dur_to_min(j.get("Duration")),
        "stops":              int(j.get("Stops") or 0),
        "via_airports":       via,
        "layover_times":      layover_times,
        "aircraft":           str(j.get("AirCraft") or "").strip() or None,
        "baggage":            baggage,  # from GetFareFeatures if Details/View Fare was opened
        "seat_available":     int(j.get("Seats") or 0) or None,
        "fare_refundable":    str(j.get("Refundable") or "").upper() not in ("N", "NONREFUNDABLE", ""),
        "journey_key":        str(j.get("JourneyKey") or ""),
        "source_endpoint":    "akbartravels:GetExpSearch",
    }


def parse_har(path: str | Path) -> List[Dict[str, Any]]:
    """Extract normalized offers from all GetExpSearch responses in a HAR."""
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    baggage_map = _baggage_map_from_har(entries)
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        if "GetExpSearch" not in e.get("request", {}).get("url", ""):
            continue
        text = (e.get("response", {}).get("content", {}) or {}).get("text", "") or ""
        if not text.strip():
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        for trip in data.get("Trips") or []:
            for j in trip.get("Journey") or []:
                sig = (str(j.get("JourneyKey") or ""), str(j.get("FareKey") or ""))
                if sig in seen:
                    continue
                seen.add(sig)
                row = _normalize_journey(j, baggage_map)
                if row:
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
    """Parse HAR(s) and merge offers into the cache (latest capture wins per route+date)."""
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
        for r in rows:  # dedupe across HARs (same fare captured in overlapping files)
            sig = (r.get("journey_key"), r.get("price_total_bdt"), r.get("departure"))
            if sig in seen:
                continue
            seen.add(sig)
            uniq.append(r)
        cache[key] = uniq  # replace: a fresh capture supersedes that route+date+cabin
    save_cache(cache)
    return {
        "keys_updated": sorted(buckets.keys()),
        "offers_imported": sum(len(v) for v in buckets.values()),
        "total_cache_keys": len(cache),
    }


def cache_rows(origin: str, dest: str, date: str, cabin: str = "economy") -> List[Dict[str, Any]]:
    return load_cache().get(_cache_key(origin, dest, date, cabin), [])
