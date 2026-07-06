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
import math
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
    """Automatic B2C discount for one ShareTrip matched flight.

    displayPrice.discount is ShareTrip's AUTOMATIC, airline-specific discount
    (a percent off the BASE fare); promotionalCoupon is the default gateway
    coupon (FLYINSIDE/FLIGHTINT), which validates to 0% extra. The full coupon
    list (wallet stacks, capped card coupons) lives in the booking-flow details,
    so only the automatic rate is available here. NOTE: browser HAR exports
    usually evict available-flights response bodies (multi-MB), so this path
    rarely yields rows in practice — booking-details captures are the reliable
    source.
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
    """One cell per (airline, flight_type): the best automatic discount % (+ coupon).

    Keeps the observed base fare of the winning flight so the market coupon terms
    (from any booking capture) can be judged cap-aware at THIS airline's fare.
    """
    by_cell: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        by_cell.setdefault((r["airline"], r["flight_type"]), []).append(r)
    out: Dict[tuple[str, str], Dict[str, Any]] = {}
    for key, items in by_cell.items():
        best = max(items, key=lambda r: r["discount_pct"])
        out[key] = {"discount_pct": best["discount_pct"], "coupon_code": best["coupon_code"],
                    "base_fare_bdt": best.get("base_fare_bdt", 0)}
    return out


# --- booking-flow coupon list (POST /api/v2/flight/search/details) -----------------------
# The fare-details response carries the FULL coupon list. Every coupon is JUDGED, not
# taken at its advertised %: coupons carry maximumDiscountAmount caps (18% "Stellar"
# capped at 6,000 BDT is only ~7.4% on a 91k intl itinerary), gateway restrictions,
# and a withDiscount flag (Yes = stacks ON TOP of the automatic displayPrice.discount,
# No = replaces it). Verified on 2026-07-06 captures: the automatic discount is a
# percent of the BASE fare (floor(base*d/100) == total - promotionalAmount on all six
# captures) and is airline-specific; the coupon TERMS are market-uniform (identical
# coupon objects across airlines within DOM / within INTL).
# Longest keywords first so "Stellar Signature" doesn't collapse into "Stellar".
_CARD_KEYWORDS = ["Stellar Signature", "Stellar Platinum", "American Express", "AMEX",
                  "SkyTrip", "Bank Asia", "EBL", "City Bank", "GPStar", "Orange Club",
                  "Robi Elite", "bKash", "Nagad", "Visa", "Mastercard", "Stellar"]


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


def _wallet_coupon(coupons: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The stackable wallet coupon for the 'common' rate (bKash preferred, then Nagad).

    Only exists on domestic today — international has no wallet coupon, so the
    common rate there is the automatic discount alone.
    """
    def _is(c: Dict[str, Any], word: str) -> bool:
        blob = (str(c.get("couponCode", "")) + str(c.get("title", ""))).lower()
        return (word in blob and str(c.get("withDiscount", "")).lower() == "yes"
                and float(c.get("discount") or 0) > 0)

    return (next((c for c in coupons if _is(c, "bkash")), None)
            or next((c for c in coupons if _is(c, "nagad")), None))


def _min_gateway_fee(coupon: Dict[str, Any],
                     gateways: Optional[Dict[str, Dict[str, Any]]]) -> Optional[float]:
    """Cheapest convenience charge among the coupon's eligible payment gateways.

    Every ShareTrip rail carries a customer-facing charge (bKash 2%, Nagad 1.5%,
    most cards 2%, EMI 3%, wallets' LCC variants 5%) — a coupon is only as good
    as its cheapest eligible gateway. None when the catalog wasn't captured.
    """
    if not gateways:
        return None
    fees = [float(gateways[str(g)]["charge_pct"])
            for g in (coupon.get("gateway") or []) if str(g) in gateways]
    return min(fees) if fees else None


def judge_coupons(base_fare: float, auto_pct: float, coupons: List[Dict[str, Any]],
                  total_fare: float = 0.0,
                  gateways: Optional[Dict[str, Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Cap-aware effective value of EVERY discount>0 coupon at the observed fare.

    effective saving = min(floor(pct% x base), maximumDiscountAmount if set)
                       + the automatic discount when the coupon stacks (withDiscount=Yes).
    Expressed as % of BASE fare (the same basis ShareTrip's own numbers use), so an
    "18%" coupon whose cap binds ranks below a smaller uncapped stack when that is
    what a customer would actually save. With no observed fare (base<=0) caps cannot
    be evaluated and nominal percentages are used unchanged.

    When the payment-gateway catalog (+ total fare) is available, each coupon also
    gets net_pct = (saving - cheapest eligible gateway fee on the amount paid) /
    base, and the ranking uses NET value — a fee-heavy coupon can't win on its
    sticker rate.
    """
    auto_amt = math.floor(base_fare * auto_pct / 100) if base_fare > 0 else 0
    judged: List[Dict[str, Any]] = []
    for c in coupons:
        pct = float(c.get("discount") or 0)
        if pct <= 0:
            continue    # 0% utility coupons (EMI / BNPL / default gateway markers)
        cap = float(c.get("maximumDiscountAmount") or 0)
        stacks = str(c.get("withDiscount", "")).lower() == "yes"
        if base_fare > 0:
            amt = math.floor(base_fare * pct / 100)
            cap_bound = cap > 0 and amt > cap
            eff = min(amt, cap) if cap > 0 else amt
            saving = auto_amt + eff if stacks else eff
            eff_pct = round(saving / base_fare * 100, 2)
        else:
            cap_bound, saving = False, 0
            eff_pct = round((auto_pct + pct) if stacks else pct, 2)
        fee_pct = _min_gateway_fee(c, gateways)
        net_pct = None
        if fee_pct is not None and base_fare > 0 and total_fare > 0:
            fee_amt = (total_fare - saving) * fee_pct / 100
            net_pct = round((saving - fee_amt) / base_fare * 100, 2)
        judged.append({
            "code": str(c.get("couponCode") or ""),
            "label": _card_label(c),
            "nominal_pct": pct,
            "cap_bdt": cap or None,
            "cap_bound": cap_bound,
            "stacks_with_auto": stacks,
            "saving_bdt": saving,
            "effective_pct": eff_pct,
            "fee_pct": fee_pct,
            "net_pct": net_pct,
        })
    judged.sort(key=lambda j: -(j["net_pct"] if j["net_pct"] is not None else j["effective_pct"]))
    return judged


def judge_cell(auto_pct: float, base_fare: float, coupons: List[Dict[str, Any]],
               total_fare: float = 0.0,
               gateways: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    """One grid cell judged from an airline's automatic rate + the market coupon terms.

    common  = auto + stackable wallet coupon (bKash/Nagad), the rate anyone paying
              online gets; special = the best JUDGED coupon (cap-aware, stack-aware,
              ranked NET of the cheapest eligible gateway fee when known);
              card = the best standalone card coupon when it is not already the winner
              (so a loyalty stack beating a capped card still shows the card rate).
    Displayed percentages stay gross-of-fee (comparable with other channels); the
    fee itself is carried in *_fee_pct for annotation.
    """
    judged = judge_coupons(base_fare, auto_pct, coupons, total_fare, gateways)
    wallet = _wallet_coupon(coupons)
    cell: Dict[str, Any] = {
        "base_pct": round(auto_pct, 2),
        "common_pct": round(auto_pct + float(wallet["discount"]), 2) if wallet else round(auto_pct, 2),
        "common_code": wallet["couponCode"] if wallet else None,
        "common_fee_pct": _min_gateway_fee(wallet, gateways) if wallet else None,
        "special_pct": None, "special_label": None, "special_code": None,
        "special_capped": False, "special_fee_pct": None,
        "card_pct": None, "card_label": None, "card_capped": False, "card_fee_pct": None,
        "judged": judged,
        "base_fare_bdt": round(base_fare) if base_fare > 0 else None,
    }
    contenders = [j for j in judged if not (wallet and j["code"] == wallet["couponCode"])]
    if contenders:
        winner = contenders[0]
        cell.update(special_pct=round(winner["effective_pct"], 1),
                    special_label=winner["label"], special_code=winner["code"],
                    special_capped=winner["cap_bound"], special_fee_pct=winner["fee_pct"])
        cards = [j for j in contenders if not j["stacks_with_auto"]]
        if cards and cards[0]["code"] != winner["code"]:
            cell.update(card_pct=round(cards[0]["effective_pct"], 1),
                        card_label=cards[0]["label"], card_capped=cards[0]["cap_bound"],
                        card_fee_pct=cards[0]["fee_pct"])
    return cell


def _collect_gateways(node: Any, acc: Dict[str, Dict[str, Any]]) -> None:
    if isinstance(node, dict):
        gid = node.get("id") or node.get("gatewayId")
        name = node.get("name") or node.get("title")
        if gid and name and node.get("charge") is not None:
            acc.setdefault(str(gid), {"name": str(name),
                                      "charge_pct": float(node.get("charge") or 0)})
        for v in node.values():
            _collect_gateways(v, acc)
    elif isinstance(node, list):
        for v in node:
            _collect_gateways(v, acc)


def parse_payment_gateways(path: str | Path) -> Dict[str, Dict[str, Any]]:
    """Gateway id -> {name, charge_pct} from GET /api/v1/payment/gateway responses.

    Every rail carries a customer-facing convenience charge (0.5%-5%); the judge
    nets each coupon against its cheapest eligible gateway.
    """
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    gateways: Dict[str, Dict[str, Any]] = {}
    for e in har.get("log", {}).get("entries", []):
        if "/api/v1/payment/gateway" not in e.get("request", {}).get("url", ""):
            continue
        try:
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        _collect_gateways(data, gateways)
    return gateways


def _details_row(resp: Dict[str, Any],
                 gateways: Optional[Dict[str, Dict[str, Any]]] = None) -> Optional[Dict[str, Any]]:
    airline = ""
    for leg in resp.get("legs") or []:
        a = leg.get("marketingAirline") or (leg.get("airlines") or {}).get("code")
        if a:
            airline = _alias(str(a).upper())
            break
    if not airline:
        return None
    display = resp.get("displayPrice") or {}
    auto = float(display.get("discount") or 0)
    tf = display.get("totalFare") or {}
    base_fare = float(tf.get("base") or 0)
    total_fare = float(tf.get("total") or 0)

    coupons: list = []
    _find_coupons(resp, coupons)
    uniq: Dict[str, Dict[str, Any]] = {}
    for c in coupons:
        prev = uniq.get(c["couponCode"])
        if prev is None or len(c) > len(prev):   # keep the richest object per code
            uniq[c["couponCode"]] = c
    coupons = list(uniq.values())
    if not coupons:
        return None

    row = {
        "channel": "sharetrip",
        "persona": "B2C",
        "airline": airline,
        "flight_type": "DOM" if resp.get("isDomestic") else "INTL",
        "coupon_terms": coupons,   # market-uniform: reusable for airlines seen only in search
        # TripCoin earn (~ base/1000 coins) — a small extra, noted but never counted
        # in the % (redemption value isn't exposed in the captures).
        "tripcoin_earn": (resp.get("points") or {}).get("earn"),
    }
    row.update(judge_cell(auto, base_fare, coupons, total_fare, gateways))
    return row


def parse_details_discounts(path: str | Path) -> List[Dict[str, Any]]:
    """
    Full B2C coupon cell from a ShareTrip booking-flow HAR. Each
    GET/POST /api/v2/flight/search/details response (one per selected flight)
    yields one airline's judged cell. Capture one booking view per airline per
    market (domestic and international carry separate coupon sets).
    """
    har = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])

    # First pass: the payment-gateway catalog (fees), wherever it sits in the HAR.
    gateways: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        if "/api/v1/payment/gateway" in e.get("request", {}).get("url", ""):
            try:
                data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
            except json.JSONDecodeError:
                continue
            _collect_gateways(data, gateways)

    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for e in entries:
        if "/api/v2/flight/search/details" not in e.get("request", {}).get("url", ""):
            continue
        try:
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        row = _details_row(data.get("response") or {}, gateways or None)
        if not row:
            continue
        sig = (row["airline"], row["flight_type"], row["common_pct"], row["special_pct"])
        if sig in seen:
            continue
        seen.add(sig)
        rows.append(row)
    return rows


def summarize_details(rows: List[Dict[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    """One judged cell per (airline, flight_type).

    Common comes from the best-common booking; the special is the best JUDGED
    special across all bookings for that cell (a cheaper fare can make a capped
    coupon look better than it is on the fare that matters, so each row was
    already judged at its own observed fare).
    """
    by_cell: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        by_cell.setdefault((r["airline"], r["flight_type"]), []).append(r)
    out: Dict[tuple[str, str], Dict[str, Any]] = {}
    for key, items in by_cell.items():
        best = dict(max(items, key=lambda r: r["common_pct"]))
        specials = [r for r in items if r.get("special_pct") is not None]
        if specials:
            top = max(specials, key=lambda r: r["special_pct"])
            for k in ("special_pct", "special_label", "special_code", "special_capped",
                      "card_pct", "card_label", "card_capped"):
                best[k] = top.get(k)
        out[key] = best
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
