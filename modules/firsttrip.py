"""
FirstTrip OTA connector (b2c-api.firsttrip.com).

No authentication required. Responds as SSE stream with all airlines
on the queried route in a single POST call.

Contract: fetch_flights(...) -> {"ok": bool, "rows": [...], "raw": {...}}

Each row matches the normalised schema expected by saudi_route_scrape.py
and flight_offers / flight_offer_raw_meta.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

LOG = logging.getLogger(__name__)

API_SEARCH = "https://b2c-api.firsttrip.com/flight/api/v1/Search"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)
CABIN_MAP = {"economy": 1, "business": 2, "first": 3, "premium_economy": 4}

CF_HEADER = "cf-ray-status-id-tn"  # server rotates sxsrf via this response header
ENV_SXSRF = "FIRSTTRIP_SXSRF"    # optional override from env


def _duration_to_min(text: str) -> Optional[int]:
    """'3h 55m' -> 235"""
    if not text:
        return None
    h = re.search(r"(\d+)h", text)
    m = re.search(r"(\d+)m", text)
    return (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0)


def _baggage_label(segment: Dict[str, Any]) -> Optional[str]:
    pax = segment.get("passengerBaggages") or []
    if pax:
        p = pax[0]
        kg = p.get("checkInBaggageInKg")
        pc = p.get("checkInBaggageInPieces")
        if kg:
            return f"{int(kg)} KG"
        if pc:
            return f"{int(pc)} Piece{'s' if int(pc) > 1 else ''}"
    bags = segment.get("baggages") or []
    if bags:
        b = bags[0]
        amt = b.get("cargoAmount")
        unit = b.get("cargoUnit", "")
        if amt:
            return f"{int(amt)} {unit}".strip()
    return None


def _normalize(offer: Dict[str, Any], requested_cabin: str,
                airline_filter: Optional[str]) -> Optional[Dict[str, Any]]:
    carrier = str(offer.get("marketingCarrierCode") or "").upper().strip()
    if not carrier:
        return None
    if airline_filter and carrier != airline_filter.upper():
        return None

    directions = offer.get("directions") or []
    if not directions or not directions[0]:
        return None
    leg = directions[0][0]
    segments: List[Dict] = leg.get("segments") or []
    if not segments:
        return None

    seg0   = segments[0]
    seg_last = segments[-1]

    # Operating carriers across all segments (codeshare detection)
    operating_airlines = sorted({
        str(s.get("operatingCarrierCode") or s.get("marketingCarrierCode") or carrier).upper()
        for s in segments
    } - {""})

    # Build via_airports from intermediate stops
    if len(segments) > 1:
        via = "|".join(s["destinationAirportCode"] for s in segments[:-1])
    else:
        via = None

    stops        = int(leg.get("stops") or 0)
    dur_text     = leg.get("totalFlightDuration") or seg0.get("flightDuration") or ""
    dur_min      = _duration_to_min(dur_text)
    layover_times = leg.get("layoverTimes") or []   # e.g. ["3h 20m", "1h 45m"]

    origin      = str(seg0.get("originAirportCode") or "").upper()
    destination = str(seg_last.get("destinationAirportCode") or "").upper()
    departure   = seg0.get("departureTime")
    arrival     = seg_last.get("arrivalTime")
    flight_num  = str(seg0.get("flightNumber") or "").strip()
    aircraft    = str(seg0.get("aircraftModel") or "").strip() or None
    fare_basis  = str(seg0.get("fareBasisCode") or offer.get("brandedFare") or "").strip() or None
    cabin       = str(seg0.get("cabinClass") or requested_cabin or "Economy").strip()
    baggage     = _baggage_label(seg0)
    seats       = int(leg.get("availableSeats") or 0) or None

    price      = float(offer.get("finalTotalPrice") or offer.get("totalPrice") or 0)
    base_price = float(offer.get("finalBasePrice") or offer.get("basePrice") or 0)
    tax        = float(offer.get("finalTaxPrice") or offer.get("taxes") or 0)

    if not origin or not destination or not departure or price <= 0:
        return None

    return {
        "airline":              carrier,
        "operating_airline":    str(seg0.get("operatingCarrierCode") or carrier).upper(),
        "operating_airlines":   operating_airlines,
        "flight_number":        flight_num,
        "origin":               origin,
        "destination":          destination,
        "departure":            departure,
        "arrival":              arrival,
        "cabin":                cabin,
        "fare_basis":           fare_basis,
        "rbd":                  str(seg0.get("rbd") or "").upper().strip(),
        "brand":                "FIRSTTRIP_OTA",
        "price_total_bdt":      price,
        "fare_amount":          base_price,
        "tax_amount":           tax,
        "currency":             "BDT",
        "duration_min":         dur_min,
        "stops":                stops,
        "via_airports":         via,
        "layover_times":        layover_times,
        "aircraft":             aircraft,
        "baggage":              baggage,
        "seat_available":       seats,
        "seat_capacity":        None,
        "fare_refundable":      bool(offer.get("refundable")),
        "adt_count":            1,
        "chd_count":            0,
        "inf_count":            0,
        "source_endpoint":      API_SEARCH,
        "inventory_confidence": None,
    }


def _bootstrap_sxsrf(timeout: int = 15) -> Optional[str]:
    """
    Get a fresh sxsrf by making a no-auth request to AirportsLastUpdateDate.
    Even a 401 response includes cf-ray-status-id-tn which bootstraps the session.
    Client encodes it with btoa(btoa(value)) to produce the sxsrf token.
    """
    import base64
    try:
        r = requests.get(
            "https://b2c-api.firsttrip.com/flight/api/v1/GeneralPurpose/AirportsLastUpdateDate",
            headers={
                "User-Agent":    USER_AGENT,
                "Origin":        "https://firsttrip.com",
                "Referer":       "https://firsttrip.com/",
                "platformtypeid": "1",
            },
            timeout=timeout,
        )
        cf = r.headers.get(CF_HEADER)
        if cf:
            token = base64.b64encode(
                base64.b64encode(cf.encode()).decode().encode()
            ).decode()
            LOG.debug("[firsttrip] bootstrapped sxsrf from cf-ray header")
            return token
    except Exception as exc:
        LOG.warning("[firsttrip] bootstrap failed: %s", exc)
    return None


# Module-level cached sxsrf (rotated per response)
_current_sxsrf: Optional[str] = None


def _get_sxsrf() -> str:
    global _current_sxsrf
    env_val = os.getenv(ENV_SXSRF)
    if env_val:
        return env_val
    if not _current_sxsrf:
        _current_sxsrf = _bootstrap_sxsrf() or ""
    return _current_sxsrf or ""


def _update_sxsrf(response: requests.Response) -> None:
    global _current_sxsrf
    import base64
    cf = response.headers.get(CF_HEADER)
    if cf:
        _current_sxsrf = base64.b64encode(
            base64.b64encode(cf.encode()).decode().encode()
        ).decode()


def fetch_flights(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    airline_code: Optional[str] = None,
    timeout: int = 90,
    return_date: Optional[str] = None,
) -> Dict[str, Any]:
    cabin_id = CABIN_MAP.get(cabin.lower(), 1)
    # Round-trip when return_date given: tripTypeId=2 + an inbound leg. The offer's
    # finalTotalPrice/finalBasePrice then represent the COMBINED round-trip fare.
    routes = [{"origin": origin.upper(), "destination": destination.upper(),
               "departureDate": str(date)}]
    if return_date:
        routes.append({"origin": destination.upper(), "destination": origin.upper(),
                       "departureDate": str(return_date)})
    payload = {
        "tripTypeId": 2 if return_date else 1,
        "routes": routes,
        "adults":   max(1, int(adt or 1)),
        "childs":   max(0, int(chd or 0)),
        "infants":  max(0, int(inf or 0)),
        "cabinClass": cabin_id,
        "preferredCarriers":  [],
        "prohibitedCarriers": [],
        "childrenAges": [],
        "promoCode": "",
        "fareType": 1,
        "isComboFare": False,
    }
    sxsrf = _get_sxsrf()
    headers = {
        "Content-Type":  "application/json",
        "User-Agent":    USER_AGENT,
        "Origin":        "https://firsttrip.com",
        "Referer":       "https://firsttrip.com/",
        "Accept":        "application/json, text/event-stream",
        "platformtypeid": "1",
        **({} if not sxsrf else {"sxsrf": sxsrf}),
    }

    out: Dict[str, Any] = {"source": "firsttrip", "ok": False, "rows": [], "raw": {}}
    t0 = time.time()
    try:
        resp = requests.post(API_SEARCH, json=payload, headers=headers,
                             timeout=timeout, stream=True)
        _update_sxsrf(resp)  # rotate token from response header
        out["raw"]["status"] = resp.status_code
        if resp.status_code == 401:
            # sxsrf expired — bootstrap and retry once
            new_sxsrf = _bootstrap_sxsrf()
            if new_sxsrf:
                headers["sxsrf"] = new_sxsrf
                resp = requests.post(API_SEARCH, json=payload, headers=headers,
                                     timeout=timeout, stream=True)
                _update_sxsrf(resp)
                out["raw"]["status"] = resp.status_code
        if resp.status_code != 200:
            out["raw"]["error"] = f"HTTP {resp.status_code}"
            LOG.warning("[firsttrip] %s->%s %s: HTTP %s", origin, destination,
                        date, resp.status_code)
            return out

        raw_text = resp.text
        out["raw"]["response_len"] = len(raw_text)

        # Parse SSE chunks
        rows: List[Dict] = []
        for chunk in raw_text.split("\ndata: "):
            chunk = chunk.replace("data: ", "", 1).strip()
            if not chunk:
                continue
            try:
                data = json_safe_loads(chunk)
                flights = (
                    (data.get("data") or {})
                    .get("airSearchResponseWithFilters", {})
                    .get("airSearchResponses", [])
                )
                for offer in flights:
                    row = _normalize(offer, cabin, airline_code)
                    if row:
                        rows.append(row)
            except Exception:
                pass

        out["rows"] = rows
        out["ok"]   = len(rows) > 0
        out["raw"]["elapsed_sec"] = round(time.time() - t0, 2)
        out["raw"]["total_offers"] = len(rows)

    except requests.exceptions.Timeout:
        out["raw"]["error"] = "timeout"
        LOG.warning("[firsttrip] %s->%s %s: timeout after %ss",
                    origin, destination, date, timeout)
    except Exception as exc:
        out["raw"]["error"] = str(exc)
        LOG.warning("[firsttrip] %s->%s %s: %s", origin, destination, date, exc)

    return out


def json_safe_loads(text: str) -> Any:
    import json
    return json.loads(text)


def _raw_offers(origin: str, destination: str, date: str, cabin: str = "Economy",
                promo: str = "", timeout: int = 90) -> List[Dict[str, Any]]:
    """
    Low-level search returning the RAW airSearchResponses offers, which embed the
    full coupon/discount breakdown FirstTrip applies per offer. Kept separate from
    fetch_flights() so the live pipeline contract is untouched.
    """
    cabin_id = CABIN_MAP.get(cabin.lower(), 1)
    payload = {
        "tripTypeId": 1,
        "routes": [{"origin": origin.upper(), "destination": destination.upper(),
                    "departureDate": str(date)}],
        "adults": 1, "childs": 0, "infants": 0, "cabinClass": cabin_id,
        "preferredCarriers": [], "prohibitedCarriers": [], "childrenAges": [],
        "promoCode": promo, "fareType": 1, "isComboFare": False,
    }
    sxsrf = _get_sxsrf()
    headers = {
        "Content-Type": "application/json", "User-Agent": USER_AGENT,
        "Origin": "https://firsttrip.com", "Referer": "https://firsttrip.com/",
        "Accept": "application/json, text/event-stream", "platformtypeid": "1",
        **({} if not sxsrf else {"sxsrf": sxsrf}),
    }
    resp = requests.post(API_SEARCH, json=payload, headers=headers,
                         timeout=timeout, stream=True)
    _update_sxsrf(resp)
    if resp.status_code != 200:
        return []
    offers: List[Dict[str, Any]] = []
    for chunk in resp.text.split("\ndata: "):
        chunk = chunk.replace("data: ", "", 1).strip()
        if not chunk:
            continue
        try:
            data = json_safe_loads(chunk)
            offers += ((data.get("data") or {})
                       .get("airSearchResponseWithFilters", {})
                       .get("airSearchResponses", []))
        except Exception:
            pass
    return offers


def fetch_b2c_discounts(origin: str, destination: str, date: str,
                        cabin: str = "Economy",
                        airline_code: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    FirstTrip B2C coupon discounts, read directly from the live search response.

    Each offer carries the auto-applied coupon (couponCode / couponDiscountRate /
    couponMaximumDiscountAmount / totalCouponAmount) plus a dynamic-discount slot.
      * headline_rate = couponDiscountRate  -> % off BASE fare (what the manual
        report shows, e.g. 16 for BS domestic)
      * realized_pct  = totalCouponAmount / gross_total * 100 -> effective % off
        the gross TOTAL after the cap is applied
    Returns one row per offer; use summarize_b2c_discounts() for the grid cell.
    """
    return _b2c_rows_from_offers(_raw_offers(origin, destination, date, cabin),
                                 cabin, airline_code)


def _b2c_rows_from_offers(offers: List[Dict[str, Any]], cabin: str = "Economy",
                          airline_code: Optional[str] = None) -> List[Dict[str, Any]]:
    """Offer dicts -> B2C discount rows. Shared by the LIVE fetch and the HAR
    failsafe (parse_b2c_har) — same response shape either way."""
    rows: List[Dict[str, Any]] = []
    for o in offers:
        base = _normalize(o, cabin, airline_code)
        if not base:
            continue
        gross = float(o.get("finalTotalPrice") or o.get("totalPrice")
                      or base.get("price_total_bdt") or 0)
        if gross <= 0:
            continue
        coupon_amt = float(o.get("totalCouponAmount") or 0)
        rows.append({
            "channel": "firsttrip",
            "persona": "B2C",
            "airline": base["airline"],
            "origin": base["origin"],
            "destination": base["destination"],
            "departure": base["departure"],
            "flight_number": base["flight_number"],
            "gross_total_bdt": round(gross),
            "base_fare_bdt": round(float(o.get("finalBasePrice") or base.get("fare_amount") or 0)),
            "coupon_code": o.get("couponCode") or "",
            "headline_rate": float(o.get("couponDiscountRate") or 0),
            "coupon_cap_bdt": float(o.get("couponMaximumDiscountAmount") or 0) or None,
            "coupon_amount_bdt": round(coupon_amt),
            "price_with_coupon_bdt": round(float(o.get("finalTotalPriceWithCoupon") or 0)) or None,
            "realized_pct": round(coupon_amt / gross * 100, 2) if gross else 0.0,
            "dynamic_code": o.get("dynamicDiscountCode") or None,
            "dynamic_rate": float(o.get("dynamicDiscountRate") or 0) or None,
            "special_code": o.get("specialCouponCode") or None,
            "special_rate": float(o.get("specialCouponDiscountRate") or 0) or None,
        })
    return rows


def parse_b2c_har(har_path: str) -> List[Dict[str, Any]]:
    """FirstTrip B2C rows from a SAVED b2c-api.firsttrip.com Search HAR.

    FAILSAFE for fetch_b2c_discounts: when the live fetch is blocked (offline,
    firewall, Cloudflare challenge), capture the same search in the browser and
    export it — the HAR body carries the identical SSE stream, so this yields the
    same rows and feeds the same summarize/true-base pipeline.
    """
    import json as _json
    har = _json.loads(Path(har_path).read_text(encoding="utf-8-sig"))
    offers: List[Dict[str, Any]] = []
    for entry in (har.get("log") or {}).get("entries", []):
        url = (entry.get("request") or {}).get("url", "")
        if "b2c-api.firsttrip.com" not in url or "/flight/api/v1/Search" not in url:
            continue
        text = ((entry.get("response") or {}).get("content") or {}).get("text") or ""
        for chunk in text.split("\ndata: "):
            chunk = chunk.replace("data: ", "", 1).strip()
            if not chunk:
                continue
            try:
                data = json_safe_loads(chunk)
                offers += ((data.get("data") or {})
                           .get("airSearchResponseWithFilters", {})
                           .get("airSearchResponses", []))
            except Exception:  # noqa: BLE001 — skip malformed SSE chunks
                pass
    return _b2c_rows_from_offers(offers)


def _ft_coupon_label(code: Optional[str]) -> str:
    """Readable card/offer label from a FirstTrip coupon code, e.g.
    FTEBLDOM07 -> 'EBL', FTCITYDOM -> 'City'. Falls back to the raw code."""
    if not code:
        return ""
    core = re.sub(r"^FT", "", str(code).upper())
    core = re.sub(r"(DOM|INT|INTL|OW|RT).*$", "", core)
    core = re.sub(r"\d+$", "", core).strip()
    known = {"EBL": "EBL", "CITY": "City Bank", "DBBL": "DBBL", "BRAC": "BRAC",
             "GPSTAR": "GPStar", "GP": "GPStar", "SCB": "SCB", "MTB": "MTB"}
    return known.get(core, core or str(code))


def summarize_b2c_discounts(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    One grid cell per airline, split into TWO tiers like ShareTrip/GoZayaan:
      * common  = the dynamic discount ANYONE gets (e.g. FTBSDOM 14%)
      * special = the coupon that beats it but needs a specific card/loyalty
                  (e.g. FTEBLDOM07 16% = EBL cardholders) — shown with its label
    Carriers with only one rate (e.g. VQ) get common alone. `rate` (the max) is
    kept for backward-compatible ranking.
    """
    by_air: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_air.setdefault(r["airline"], []).append(r)

    def best_rate(r: Dict[str, Any]) -> float:
        return max(r["headline_rate"], r.get("dynamic_rate") or 0.0)

    out: Dict[str, Dict[str, Any]] = {}
    for airline, items in by_air.items():
        best = max(items, key=best_rate)
        coupon_rate = best["headline_rate"]
        coupon_code = best["coupon_code"]
        dynamic_rate = best.get("dynamic_rate") or 0.0
        dynamic_code = best.get("dynamic_code")

        # common = the general (dynamic) discount; if there's no dynamic slot the
        # coupon itself is the common one.
        if dynamic_rate > 0:
            common_rate, common_code = dynamic_rate, dynamic_code
            # the coupon is a card/loyalty special only when it beats the common rate
            special_rate = coupon_rate if coupon_rate > dynamic_rate else 0.0
            special_code = coupon_code if special_rate else None
        else:
            common_rate, common_code = coupon_rate, coupon_code
            special_rate, special_code = 0.0, None

        out[airline] = {
            "rate": max(common_rate, special_rate),   # backward-compat ranking
            "code": special_code or common_code,
            "source": "coupon" if special_rate else "dynamic",
            "common_rate": common_rate,
            "common_code": common_code,
            "special_rate": special_rate or None,
            "special_code": special_code,
            "special_label": _ft_coupon_label(special_code),
            "realized_pct": best["realized_pct"],
            "cap_bdt": best["coupon_cap_bdt"],
        }
    return out


# --- B2C convenience fee (payment-step, gateway-dependent) --------------------------------
# FirstTrip adds a convenience fee at checkout that is NOT in the search response — it comes
# from GetActivePaymentGateway (per-gateway chargePercentage, e.g. Nagad 1.0 / Bkash 1.5 /
# cards ~2). Like the ShareTrip gateway fee, we annotate the cheapest eligible charge.
_GATEWAY_ENDPOINT = "GetActivePaymentGateway"


_CARD_HINTS = ("visa", "master", "amex", "card", "ebl", "city", "brac", "dbbl",
               "scb", "standard chartered", "mtb", "bank", "credit", "debit")


def _gateways(node: Any, acc: List[Dict[str, Any]]) -> None:
    """Every {name, chargePercentage} found in a GetActivePaymentGateway payload."""
    if isinstance(node, dict):
        if "chargePercentage" in node:
            try:
                acc.append({"name": str(node.get("name") or node.get("detailsName") or ""),
                            "charge": float(node.get("chargePercentage") or 0)})
            except (TypeError, ValueError):
                pass
        for v in node.values():
            _gateways(v, acc)
    elif isinstance(node, list):
        for v in node:
            _gateways(v, acc)


def parse_b2c_gateways(har_path: str) -> List[Dict[str, Any]]:
    """All payment gateways ({name, charge}) from a FirstTrip booking HAR."""
    try:
        har = json.loads(Path(har_path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return []
    gws: List[Dict[str, Any]] = []
    for e in har.get("log", {}).get("entries", []):
        if _GATEWAY_ENDPOINT not in e.get("request", {}).get("url", ""):
            continue
        try:
            data = json.loads((e.get("response", {}).get("content", {}) or {}).get("text", "") or "{}")
        except json.JSONDecodeError:
            continue
        _gateways(data, gws)
    return gws


def b2c_gateway_fees(gws: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """{'common': cheapest wallet/any charge, 'card': cheapest CARD charge} from a
    gateway list. Card falls back to the dearest non-zero when no card is captured
    (wallet-only B2B lists), so the special tier still shows a sensible fee."""
    nonzero = [g for g in gws if g["charge"] > 0]
    if not nonzero:
        return {"common": None, "card": None}
    common = round(min(g["charge"] for g in nonzero), 2)
    cards = [g for g in nonzero if any(h in g["name"].lower() for h in _CARD_HINTS)]
    card = round(min(g["charge"] for g in cards), 2) if cards else round(max(g["charge"] for g in nonzero), 2)
    return {"common": common, "card": card}


def parse_b2c_gateway_fee(har_path: str) -> Optional[float]:
    """Cheapest non-zero gateway charge % from a FirstTrip booking HAR (compat helper)."""
    return b2c_gateway_fees(parse_b2c_gateways(har_path)).get("common")


def _b2b_commission_row(offer: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One agent-commission row from a FirstTrip B2B Search/Progressive offer.

    The logged-in agent offer carries platingCarrier + basePrice and a negative
    passengerFares.adt.discountPrice (the agent commission). Commission % is
    taken on base fare to match the manual report.
    """
    carrier = str(offer.get("platingCarrier") or "").upper().strip()
    base = float(offer.get("basePrice") or 0)
    pax = (offer.get("passengerFares") or {}).get("adt") or {}
    disc = float(pax.get("discountPrice") or 0)
    if not carrier or base <= 0:
        return None

    origin = destination = ""
    departure = None
    directions = offer.get("directions") or []
    if directions and directions[0]:
        leg = directions[0][0] or {}
        origin = str(leg.get("from") or "").upper()
        destination = str(leg.get("to") or "").upper()
        segments = leg.get("segments") or []
        if segments:
            departure = segments[0].get("departure")

    commission = abs(disc)
    return {
        "channel": "firsttrip_b2b",
        "persona": "B2B",
        "airline": carrier,
        "origin": origin,
        "destination": destination,
        "departure": departure,
        "base_fare_bdt": round(base),
        "gross_total_bdt": round(float(offer.get("previousTotalFare") or 0)),
        "net_total_bdt": round(float(offer.get("totalPrice") or 0)),
        "commission_bdt": round(commission, 2),
        "commission_pct": round(commission / base * 100, 2),
    }


def parse_b2b_commissions(har_path: str | Path) -> List[Dict[str, Any]]:
    """
    FirstTrip B2B agent commissions from a logged-in booking.firsttrip.com HAR.

    The agent search hits api.firsttrip.com/api/Search/Progressive, an SSE stream
    of `data: {"searchResponse": {"airSearchResponses": [...]}}` chunks. Each offer
    exposes the agent commission as a negative passengerFares.adt.discountPrice;
    commission % = |discountPrice| / basePrice. HAR-based (cookie/JWT session).
    """
    import json as _json
    har = _json.loads(Path(har_path).read_text(encoding="utf-8-sig"))
    entries = har.get("log", {}).get("entries", [])
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for entry in entries:
        if "/api/Search/Progressive" not in entry.get("request", {}).get("url", ""):
            continue
        text = (entry.get("response", {}).get("content", {}) or {}).get("text", "") or ""
        for chunk in text.split("\ndata: "):
            chunk = chunk.replace("data: ", "", 1).strip()
            try:
                data = _json.loads(chunk)
            except (ValueError, _json.JSONDecodeError):
                continue
            search_response = data.get("searchResponse") or {}
            for offer in search_response.get("airSearchResponses") or []:
                row = _b2b_commission_row(offer)
                if not row:
                    continue
                sig = (row["airline"], row["origin"], row["destination"],
                       row["departure"], row["base_fare_bdt"])
                if sig in seen:
                    continue
                seen.add(sig)
                rows.append(row)
    return rows


def summarize_b2b_commissions(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Per airline: best (highest) agent commission % for the grid cell."""
    by_air: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_air.setdefault(r["airline"], []).append(r)
    out: Dict[str, Dict[str, Any]] = {}
    for airline, items in by_air.items():
        best = max(items, key=lambda r: r["commission_pct"])
        out[airline] = {
            "commission_pct": best["commission_pct"],
            "commission_bdt": best["commission_bdt"],
        }
    return out
