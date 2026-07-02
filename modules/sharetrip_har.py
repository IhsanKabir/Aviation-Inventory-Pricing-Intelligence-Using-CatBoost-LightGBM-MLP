"""
ShareTrip offer source — via MANUAL HAR import.

ShareTrip's live connector (modules/sharetrip.py) needs a captured access token +
cookies and is disabled/session-dead here, so we parse a browser HAR instead
(consistent with the other OTA HAR importers). All airlines are taken, not just G9.

Search API in the HAR: POST https://api.sharetrip.net/api/v2/flight/search/available-flights
(paginated) -> response.matchedFlights[]. Fares are BDT-native (no FX). Carries
G9 (Air Arabia) with per-segment baggage, RBD (resBookDesigCode) and operatedBy.

Usage:
  python tools/import_sharetrip_har.py <file.har> [more.har ...]
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = REPO_ROOT / "output" / "manual_sessions" / "sharetrip_cache.json"

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


def _baggage(segs: list) -> Optional[str]:
    for s in segs:
        b = s.get("baggage") or {}
        w = b.get("weight")
        if w is not None:
            unit = str(b.get("unit") or "KG").upper()
            return f"{int(w)} {unit}"
    return None


def _normalize(fl: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    legs = fl.get("legs") or []
    if not legs:
        return None
    leg0, legL = legs[0], legs[-1]
    segs: list = []
    for lg in legs:
        segs += lg.get("segments") or []
    if not segs:
        return None
    s0 = segs[0]

    airline = _alias(leg0.get("marketingAirline") or (leg0.get("airlines") or {}).get("code") or "")
    if not airline:
        return None
    _tf = (fl.get("displayPrice") or {}).get("totalFare") or {}
    price = float(_tf.get("total") or 0)
    if price <= 0:
        return None
    base_fare = float(_tf.get("base") or 0)   # BDT-native; base + tax = total

    origin = str((leg0.get("origin") or {}).get("code") or "").upper()
    destination = str((legL.get("destination") or {}).get("code") or "").upper()
    dep = leg0.get("departureDateTime") or {}
    arr = legL.get("arrivalDateTime") or {}
    departure = f"{dep.get('date')}T{dep.get('time')}" if dep.get("date") else None
    arrival = f"{arr.get('date')}T{arr.get('time')}" if arr.get("date") else None
    if not origin or not destination or not departure:
        return None

    via_list: list = []
    for lg in legs:
        via_list += lg.get("layoverIataList") or []
    via = "|".join(via_list) or None
    layover_times = [x for x in (_fmt_layover(s.get("transitTime")) for s in segs) if x]
    operating_airlines = sorted({_alias(s.get("operatedBy") or airline) for s in segs} - {""})
    rbd = str(s0.get("resBookDesigCode") or s0.get("cabinCode") or "").upper().strip()

    return {
        "airline":            airline,
        "operating_airline":  _alias(s0.get("operatedBy") or airline),
        "operating_airlines": operating_airlines,
        "flight_number":      str(s0.get("flightNumber") or "").strip(),
        "origin":             origin,
        "destination":        destination,
        "departure_date":     str(departure)[:10],
        "departure":          departure,
        "arrival":            arrival,
        "cabin":              _cabin_bucket(s0.get("cabin")),
        "cabin_class":        _cabin_bucket(s0.get("cabin")),
        "rbd":                rbd,
        "brand":              "SHARETRIP_OTA",
        "price_total_bdt":    round(price),
        "fare_amount":        round(base_fare),                       # base fare (pre-tax), BDT
        "tax_amount":         round(float(_tf.get("tax") or 0)),
        "currency":           "BDT",
        "duration_min":       int(fl.get("totalDuration") or leg0.get("duration") or 0) or None,
        "stops":              max(0, len(segs) - 1),
        "via_airports":       via,
        "layover_times":      layover_times,
        "aircraft":           str(s0.get("aircraft") or "").strip() or None,
        "baggage":            _baggage(segs),
        "fare_refundable":    bool(fl.get("isRefundable")),
        "fare_id":            str(fl.get("sequenceCode") or fl.get("providerCode") or ""),
        "source_endpoint":    "sharetrip:available-flights",
    }


def parse_har(path: str | Path) -> List[Dict[str, Any]]:
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        if "available-flights" not in e.get("request", {}).get("url", ""):
            continue
        try:
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        for fl in (data.get("response") or {}).get("matchedFlights") or []:
            row = _normalize(fl)
            if not row:
                continue
            sig = (row["fare_id"], row["price_total_bdt"], row["departure"])
            if sig in seen:
                continue
            seen.add(sig)
            rows.append(row)
    return rows


def _discount_row(fl: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Common B2C discount for one ShareTrip matched flight.

    The search response carries the default promotional coupon (FLYINSIDE) as
    displayPrice.discount (a percent off base) plus a `domestic` flag. The
    payment/card-specific coupons (bKash, EBL/Stellar 18%) live in the booking
    flow, not the search, so only the common rate is available here.
    """
    legs = fl.get("legs") or []
    if not legs:
        return None
    airline = _alias(legs[0].get("marketingAirline") or (legs[0].get("airlines") or {}).get("code") or "")
    if not airline:
        return None
    display = fl.get("displayPrice") or {}
    discount = display.get("discount")
    if discount is None:
        return None
    coupon = (fl.get("promotionalCoupon") or {}).get("couponCode")
    base = float((display.get("totalFare") or {}).get("base") or 0)
    return {
        "channel": "sharetrip",
        "persona": "B2C",
        "airline": airline,
        "flight_type": "DOM" if fl.get("domestic") else "INTL",
        "discount_pct": float(discount),
        "coupon_code": coupon,
        "base_fare_bdt": round(base),
    }


def parse_discounts(path: str | Path) -> List[Dict[str, Any]]:
    """
    Extract the common B2C discount per flight from a ShareTrip search HAR
    (POST /api/v2/flight/search/available-flights -> response.matchedFlights).
    Returns one row per (airline, flight_type, coupon). Card/payment-specific
    coupons require a booking-flow capture and are not present here.
    """
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        if "available-flights" not in e.get("request", {}).get("url", ""):
            continue
        try:
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        for fl in (data.get("response") or {}).get("matchedFlights") or []:
            row = _discount_row(fl)
            if not row:
                continue
            sig = (row["airline"], row["flight_type"], row["coupon_code"])
            if sig in seen:
                continue
            seen.add(sig)
            rows.append(row)
    return rows


def summarize_discounts(rows: List[Dict[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    """One cell per (airline, flight_type): the best common discount % (+ coupon)."""
    by_cell: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        by_cell.setdefault((r["airline"], r["flight_type"]), []).append(r)
    out: Dict[tuple[str, str], Dict[str, Any]] = {}
    for key, items in by_cell.items():
        best = max(items, key=lambda r: r["discount_pct"])
        out[key] = {"discount_pct": best["discount_pct"], "coupon_code": best["coupon_code"]}
    return out


# --- booking-flow coupon list (POST /api/v2/flight/search/details) -----------------------
# The fare-details response carries the FULL coupon list with discounts. The grid cell is:
#   common  = displayPrice.discount (the base FLYINSIDE rate) + bKash stackable coupon
#   special = the highest standalone (withDiscount="No") card coupon (e.g. Stellar/EBL)
_CARD_KEYWORDS = ["Stellar", "American Express", "AMEX", "SkyTrip", "Bank Asia", "EBL",
                  "City Bank", "GPStar", "Orange Club", "Robi Elite", "Visa", "Mastercard"]


def _find_coupons(node: Any, acc: list) -> None:
    if isinstance(node, dict):
        if node.get("couponCode") and "discount" in node and "discountType" in node:
            acc.append(node)
        for v in node.values():
            _find_coupons(v, acc)
    elif isinstance(node, list):
        for v in node:
            _find_coupons(v, acc)


def _card_label(coupon: Dict[str, Any]) -> str:
    title = str(coupon.get("title") or "")
    for kw in _CARD_KEYWORDS:
        if kw.lower() in title.lower():
            return kw
    return str(coupon.get("couponCode") or "card")


def _details_row(resp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    airline = ""
    for leg in resp.get("legs") or []:
        a = leg.get("marketingAirline") or (leg.get("airlines") or {}).get("code")
        if a:
            airline = _alias(str(a).upper())
            break
    if not airline:
        return None
    base = float((resp.get("displayPrice") or {}).get("discount") or 0)

    coupons: list = []
    _find_coupons(resp, coupons)
    uniq: Dict[str, Dict[str, Any]] = {}
    for c in coupons:
        uniq.setdefault(c["couponCode"], c)
    coupons = list(uniq.values())
    if not coupons:
        return None

    bkash = next((c for c in coupons
                  if "bkash" in (str(c.get("couponCode", "")) + str(c.get("title", ""))).lower()), None)
    standalone = [c for c in coupons
                  if str(c.get("withDiscount", "")).lower() == "no" and float(c.get("discount") or 0) > 0]
    best = max(standalone, key=lambda c: float(c["discount"]), default=None)

    return {
        "channel": "sharetrip",
        "persona": "B2C",
        "airline": airline,
        "flight_type": "DOM" if resp.get("isDomestic") else "INTL",
        "base_pct": base,
        "common_pct": round(base + float(bkash["discount"]), 2) if bkash else round(base, 2),
        "common_code": bkash["couponCode"] if bkash else None,
        "special_pct": float(best["discount"]) if best else None,
        "special_label": _card_label(best) if best else None,
        "special_code": best["couponCode"] if best else None,
    }


def parse_details_discounts(path: str | Path) -> List[Dict[str, Any]]:
    """
    Full B2C coupon cell from a ShareTrip booking-flow HAR. Each
    POST /api/v2/flight/search/details response (one per selected flight) yields
    one airline's cell. Capture one booking view per airline you want covered.
    """
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in har.get("log", {}).get("entries", []):
        if "/api/v2/flight/search/details" not in e.get("request", {}).get("url", ""):
            continue
        try:
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        row = _details_row(data.get("response") or {})
        if not row:
            continue
        sig = (row["airline"], row["flight_type"], row["common_pct"], row["special_pct"])
        if sig in seen:
            continue
        seen.add(sig)
        rows.append(row)
    return rows


def summarize_details(rows: List[Dict[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    """One cell per (airline, flight_type): best common + its standalone special."""
    by_cell: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        by_cell.setdefault((r["airline"], r["flight_type"]), []).append(r)
    out: Dict[tuple[str, str], Dict[str, Any]] = {}
    for key, items in by_cell.items():
        best = max(items, key=lambda r: r["common_pct"])
        out[key] = {
            "common_pct": best["common_pct"],
            "common_code": best["common_code"],   # None when no bKash coupon (e.g. international)
            "special_pct": best["special_pct"],
            "special_label": best["special_label"],
        }
    return out


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
