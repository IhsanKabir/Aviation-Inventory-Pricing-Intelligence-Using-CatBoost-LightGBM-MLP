"""
GoZayaan offer source — via MANUAL HAR import.

GoZayaan's flight search (production.gozayaan.com/api/flight/v4.0/...) is a clean
JSON API with no auth header, but it is session/rate sensitive (historically JWT +
429 cooldowns), so we follow the same manual-HAR pattern as AkbarTravels rather
than scripting it live. Fares are **BDT-native** (no FX conversion needed).

Flow captured in the HAR:
  POST /api/flight/v4.0/search/           -> search_id (+ the searched cabin)
  POST /api/flight/v4.0/search/legs/      -> {result: {fares, legs, segments, carriers}}

We parse the `legs` responses into firsttrip-compatible offer rows.

Usage:
  python tools/import_gozayaan_har.py <file.har> [more.har ...]
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = REPO_ROOT / "output" / "manual_sessions" / "gozayaan_cache.json"

AIRLINE_ALIAS = {"3L": "G9"}


def _alias(code: str) -> str:
    return AIRLINE_ALIAS.get(str(code or "").upper(), str(code or "").upper())


def _cabin_bucket(cabin_class: Any) -> str:
    c = str(cabin_class or "Economy").lower()
    return "business" if ("business" in c or "first" in c or c == "c") else "economy"


def _fmt_layover(mins: Optional[int]) -> Optional[str]:
    if not mins or int(mins) <= 0:
        return None
    h, m = divmod(int(mins), 60)
    if h and m:
        return f"{h}h {m}m"
    return f"{h}h" if h else f"{m}m"


def _find_search_id(obj: Any) -> Optional[str]:
    if isinstance(obj, dict):
        if obj.get("search_id"):
            return str(obj["search_id"])
        for v in obj.values():
            r = _find_search_id(v)
            if r:
                return r
    return None


def _search_cabin_map(entries: list) -> Dict[str, str]:
    """{search_id: cabin_bucket} from the /search/ request(cabin)+response(search_id) pairs."""
    out: Dict[str, str] = {}
    for e in entries:
        url = e.get("request", {}).get("url", "")
        if not url.endswith("/v4.0/search/") or e["request"].get("method") != "POST":
            continue
        try:
            req = json.loads((e["request"].get("postData") or {}).get("text", "") or "{}")
            resp = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        sid = _find_search_id(resp)
        if sid:
            out[sid] = _cabin_bucket(req.get("cabin_class"))
    return out


def _normalize_fare(fare: Dict[str, Any], legs_by_hash: Dict[str, Dict],
                    segs_by_hash: Dict[str, Dict], cabin: str) -> Optional[Dict[str, Any]]:
    leg_hashes = fare.get("leg_hashes") or []
    if not leg_hashes:
        return None
    leg = legs_by_hash.get(str(leg_hashes[0]))
    if not leg:
        return None
    seg_hashes = leg.get("segment_hashes") or []
    segs = [segs_by_hash.get(str(h)) for h in seg_hashes]
    segs = [s for s in segs if s]
    if not segs:
        return None

    seg0, seg_last = segs[0], segs[-1]
    marketing = _alias(leg.get("marketing_carrier") or seg0.get("marketing_carrier") or "")
    if not marketing:
        return None
    operating_airlines = sorted({_alias(s.get("operating_carrier") or marketing) for s in segs} - {""})

    via = "|".join(s.get("destinationcode") or s.get("destination") for s in segs[:-1]) or None
    layovers = [_fmt_layover((d or {}).get("duration")) for d in (leg.get("lay_over_details") or [])]
    layover_times = [lt for lt in layovers if lt]

    price = float(fare.get("total_fare_amount") or 0)
    if price <= 0:
        return None
    departure = leg.get("departure_date_time") or seg0.get("departure_date_time")
    arrival = leg.get("arrival_date_time") or seg_last.get("arrival_date_time")
    origin = str(seg0.get("origin") or "").upper()
    destination = str(seg_last.get("destination") or "").upper()
    if not origin or not destination or not departure:
        return None

    return {
        "airline":            marketing,
        "operating_airline":  _alias(seg0.get("operating_carrier") or marketing),
        "operating_airlines": operating_airlines,
        "flight_number":      str(seg0.get("flight_number") or "").strip(),
        "origin":             origin,
        "destination":        destination,
        "departure_date":     str(departure)[:10],
        "departure":          departure,
        "arrival":            arrival,
        "cabin":              cabin,
        "cabin_class":        cabin,
        "brand":              "GOZAYAAN_OTA",
        "fare_class":         str(fare.get("fare_type") or "").strip() or None,
        "price_total_bdt":    round(price),
        "fare_amount":        round(float(fare.get("total_base_amount") or 0)),  # base fare, BDT-native
        "tax_amount":         round(float(fare.get("total_tax_amount") or 0)),
        "currency":           str(fare.get("currency") or "BDT"),
        "duration_min":       int(leg.get("travel_time") or 0) or None,
        "stops":              max(0, len(segs) - 1),
        "via_airports":       via,
        "layover_times":      layover_times,
        "aircraft":           str(seg0.get("equipment") or "").strip() or None,
        "baggage":            None,  # not in legs payload
        "fare_id":            str(fare.get("id") or fare.get("hash") or ""),
        "source_endpoint":    "gozayaan:search/legs",
    }


def parse_har(path: str | Path) -> List[Dict[str, Any]]:
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    cabin_map = _search_cabin_map(entries)
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        url = e.get("request", {}).get("url", "")
        if not url.endswith("/v4.0/search/legs/") or e["request"].get("method") != "POST":
            continue
        try:
            body = json.loads((e["request"].get("postData") or {}).get("text", "") or "{}")
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        cabin = cabin_map.get(str(body.get("search_id")), "economy")
        res = data.get("result") or {}
        legs_by_hash = {str(l.get("hash")): l for l in (res.get("legs") or [])}
        segs_by_hash = {str(s.get("hash")): s for s in (res.get("segments") or [])}
        for fare in res.get("fares") or []:
            sig = (str(fare.get("hash_str") or fare.get("id") or ""), cabin)
            if sig in seen or not sig[0]:
                continue
            seen.add(sig)
            row = _normalize_fare(fare, legs_by_hash, segs_by_hash, cabin)
            if row:
                rows.append(row)
    return rows


def parse_surcharge(path: str | Path) -> Dict[str, float]:
    """GoZayaan's flat convenience surcharge (a fee ADDED at payment) per route
    type, from GET /api/business_rules/product_surcharge/ -> result.surcharge.
    Returns {"DOM": pct, "INTL": pct}; unlike ShareTrip's per-gateway fees this
    is one channel-wide charge (field-observed 2.1% for both DOM and INT)."""
    try:
        har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {}
    out: Dict[str, float] = {}
    for e in har.get("log", {}).get("entries", []):
        if "product_surcharge" not in e.get("request", {}).get("url", ""):
            continue
        try:
            r = (json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
                 .get("result") or {})
        except json.JSONDecodeError:
            continue
        pt = str(r.get("product_type") or "").upper()
        sur = r.get("surcharge")
        if pt and sur is not None:
            out["INTL" if pt.startswith("INT") else "DOM"] = round(float(sur), 2)
    return out


def parse_discounts(path: str | Path) -> List[Dict[str, Any]]:
    """
    Extract published coupon/campaign discounts from a GoZayaan HAR.

    GoZayaan does NOT bake coupon discounts into the fare. It serves them from
    POST /api/business_rules/get_discount_list/, whose request body carries the
    per-search context (plating_carrier, flight_type DOM/OUTBOUND, product_price
    = gross total). The response lists campaigns, each with a published
    percentage (discount_markup.markup_amount) and a BDT cap (markup_max_amount),
    applied on the BOOKING amount.

    Realized discount = min(pct% * product_price, cap); realized_pct is that
    amount back as a percent of product_price. Each campaign is returned as its
    own row (no summation), keyed by (airline, flight_type, coupon_code).
    """
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        url = e.get("request", {}).get("url", "")
        if not url.endswith("/api/business_rules/get_discount_list/"):
            continue
        try:
            body = json.loads((e["request"].get("postData") or {}).get("text", "") or "{}")
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue

        airline = _alias(str(body.get("plating_carrier") or "").upper())
        flight_type = str(body.get("flight_type") or "").upper()
        product_price = float(body.get("product_price") or 0)
        if not airline or product_price <= 0:
            continue

        items = data.get("result") or data.get("data") or []
        if isinstance(items, dict):
            items = items.get("results") or items.get("discounts") or []
        if not isinstance(items, list):
            continue

        for it in items:
            if not isinstance(it, dict):
                continue
            markup = it.get("discount_markup") or {}
            if str(markup.get("markup_type") or "").upper() != "PERCENTAGE":
                continue  # only percentage campaigns map to a comparable rate
            code = (it.get("discount_promo_code")
                    or (it.get("discount_campaign") or {}).get("campaign_code") or "")
            pct = float(markup.get("markup_amount") or 0)
            if pct <= 0:
                continue
            cap = float(markup.get("markup_max_amount") or 0)
            realized_amt = pct / 100.0 * product_price
            if cap:
                realized_amt = min(realized_amt, cap)
            realized_pct = round(realized_amt / product_price * 100.0, 2) if product_price else pct

            sig = (airline, flight_type, code)
            if sig in seen:
                continue
            seen.add(sig)
            scope, eligibility = _classify_eligibility(it)
            out.append({
                "channel": "gozayaan",
                "persona": "B2C",
                "airline": airline,
                "flight_type": flight_type,
                "product_price": round(product_price),
                "coupon_code": code,
                "discount_pct": pct,
                "cap_bdt": round(cap) if cap else None,
                "realized_discount_bdt": round(realized_amt),
                "realized_pct": realized_pct,
                "apply_on": markup.get("apply_on"),
                "eligibility_scope": scope,        # "common" | "specific"
                "eligibility": eligibility,        # human label, e.g. "EBL Visa", "Any online payment"
                "name": it.get("discount_name") or it.get("discount_description") or "",
            })
    return out


def _classify_eligibility(campaign: Dict[str, Any]) -> tuple[str, str]:
    """
    Classify a campaign as commonly-available vs card-specific using the
    structured discount_validation.bank_type_details, not the description text.

    Rule (structured-data driven):
      * no bank restriction            -> common  ("Any online payment")
      * AMEX-only scheme               -> specific (premium card)
      * exactly one bank               -> specific (that bank's card)
      * broad multi-bank coverage      -> common  ("Most cards")
    """
    details = (campaign.get("discount_validation") or {}).get("bank_type_details") or []
    banks = sorted({str(b.get("bank_name")).strip() for b in details if b.get("bank_name")})
    schemes = sorted({str(b.get("card_type")).strip() for b in details if b.get("card_type")})

    if not banks:
        return "common", "Any online payment"
    if schemes == ["AMEX"]:
        bank = banks[0] if len(banks) == 1 else "AMEX"
        return "specific", f"{bank} AMEX"
    if len(banks) == 1:
        scheme = f" {schemes[0]}" if schemes else ""
        return "specific", f"{banks[0]}{scheme}"
    return "common", "Most cards"


def summarize_discounts(rows: List[Dict[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    """
    Collapse coupon rows into one grid cell per (airline, flight_type):
      * common  = best realized_pct among commonly-available coupons
      * special = best realized_pct among card-specific coupons, surfaced ONLY
                  when it beats the common rate (with code + card label)
    Mirrors the manual rule: show the commonly-available discount, and if the
    highest is a specific card, show that too with its code/card name.
    """
    by_cell: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        by_cell.setdefault((r["airline"], r["flight_type"]), []).append(r)

    out: Dict[tuple[str, str], Dict[str, Any]] = {}
    for key, items in by_cell.items():
        common = [r for r in items if r["eligibility_scope"] == "common"]
        specific = [r for r in items if r["eligibility_scope"] == "specific"]
        best_common = max(common, key=lambda r: r["realized_pct"], default=None)
        best_special = max(specific, key=lambda r: r["realized_pct"], default=None)
        common_pct = best_common["realized_pct"] if best_common else None
        cell = {
            "common_pct": common_pct,
            "common_code": best_common["coupon_code"] if best_common else None,
            "special": None,
        }
        if best_special and (common_pct is None or best_special["realized_pct"] > common_pct):
            cell["special"] = {
                "pct": best_special["realized_pct"],
                "code": best_special["coupon_code"],
                "eligibility": best_special["eligibility"],
            }
        out[key] = cell
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
    return {
        "keys_updated": sorted(buckets.keys()),
        "offers_imported": sum(len(v) for v in buckets.values()),
        "total_cache_keys": len(cache),
    }


def cache_rows(origin: str, dest: str, date: str, cabin: str = "economy") -> List[Dict[str, Any]]:
    return load_cache().get(_cache_key(origin, dest, date, cabin), [])
