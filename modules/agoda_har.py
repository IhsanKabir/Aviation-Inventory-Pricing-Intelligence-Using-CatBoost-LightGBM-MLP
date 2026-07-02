"""
Agoda (Kayak-powered) offer source — via MANUAL HAR import.

flights.agoda.com search posts to /i/api/search/dynamic/flights/poll and is
protected by an x-csrf token, so it's a manual-HAR source like AkbarTravels.
Fares are USD -> converted to BDT via modules.fx.

Response shape (Kayak): results[].bookingOptions[] carry displayPrice(USD) and
legFarings -> legId; legs{} and segments{} are id-keyed lookups.

Usage:
  python tools/import_agoda_har.py <file.har> [more.har ...]
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from modules.fx import to_bdt

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = REPO_ROOT / "output" / "manual_sessions" / "agoda_cache.json"

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


def _itinerary(leg_farings: list, legs: Dict[str, Any], segments: Dict[str, Any]):
    """Resolve a bookingOption's legFarings into ordered segment dicts + layovers."""
    seg_objs: List[Dict] = []
    layovers: List[Optional[int]] = []
    for lf in leg_farings:
        leg = legs.get(str(lf.get("legId")))
        if not leg:
            continue
        for sref in leg.get("segments") or []:
            seg = segments.get(str(sref.get("id")))
            if seg:
                seg_objs.append(seg)
            lay = (sref.get("layover") or {}).get("duration")
            if lay:
                layovers.append(int(lay))
    return seg_objs, layovers


def _normalize_option(opt: Dict[str, Any], legs: Dict, segments: Dict) -> Optional[Dict[str, Any]]:
    price_obj = opt.get("displayPrice") or {}
    amount = float(price_obj.get("price") or 0)
    currency = str(price_obj.get("currency") or "USD").upper()
    if amount <= 0:
        return None
    # NOTE: Agoda's fees.basePrice == displayPrice (tax baked in), so it is NOT a true
    # pre-tax base. We intentionally do not emit fare_amount; base is derived via the
    # tax model from sources that expose a genuine base (GoZayaan/FirstTrip).

    leg_farings = opt.get("legFarings") or []
    seg_objs, layovers = _itinerary(leg_farings, legs, segments)
    if not seg_objs:
        return None
    seg0, seg_last = seg_objs[0], seg_objs[-1]

    cabin_display = None
    rbd = ""
    for lf in leg_farings:
        for sf in lf.get("segmentFarings") or []:
            cabin_display = cabin_display or sf.get("cabinDisplay")
            rbd = rbd or str(sf.get("cabinCode") or sf.get("bookingCode") or "").upper().strip()
    cabin = _cabin_bucket(cabin_display)

    origin = str(seg0.get("origin") or "").upper()
    destination = str(seg_last.get("destination") or "").upper()
    departure = seg0.get("departure")
    arrival = seg_last.get("arrival")
    if not origin or not destination or not departure:
        return None

    via = "|".join(s.get("destination") for s in seg_objs[:-1]) or None
    operating_airlines = sorted({_alias(s.get("airline")) for s in seg_objs} - {""})
    layover_times = [_fmt_layover(m) for m in layovers]
    layover_times = [lt for lt in layover_times if lt]
    dur = sum(int(s.get("duration") or 0) for s in seg_objs) or None

    return {
        "airline":            _alias(seg0.get("airline")),
        "operating_airline":  _alias(seg0.get("airline")),
        "operating_airlines": operating_airlines,
        "flight_number":      str(seg0.get("flightNumber") or "").strip(),
        "origin":             origin,
        "destination":        destination,
        "departure_date":     str(departure)[:10],
        "departure":          departure,
        "arrival":            arrival,
        "cabin":              cabin,
        "cabin_class":        cabin,
        "rbd":                rbd,
        "brand":              "AGODA_OTA",
        "fare_class":         str(opt.get("providerCode") or "").strip() or None,
        "price_total_bdt":    round(to_bdt(amount, currency)),
        "fare_currency_src":  currency,
        "currency":           "BDT",
        "duration_min":       dur,
        "stops":              max(0, len(seg_objs) - 1),
        "via_airports":       via,
        "layover_times":      layover_times,
        "aircraft":           str(seg0.get("equipmentTypeName") or "").strip() or None,
        "baggage":            None,  # not in poll payload
        "fare_id":            str(opt.get("bookingId") or ""),
        "source_endpoint":    "agoda:flights/poll",
    }


def parse_har(path: str | Path) -> List[Dict[str, Any]]:
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        if not e.get("request", {}).get("url", "").endswith("/flights/poll"):
            continue
        try:
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        legs = data.get("legs") or {}
        segments = data.get("segments") or {}
        for res in data.get("results") or []:
            for opt in res.get("bookingOptions") or []:
                row = _normalize_option(opt, legs, segments)
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
