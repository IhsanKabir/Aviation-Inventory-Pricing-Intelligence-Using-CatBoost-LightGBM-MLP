"""
ShareTrip OTA connector for normalized multi-airline fare feeds.

Contract:
- fetch_flights_for_airline(...) returns:
  { raw, originalResponse, rows, ok }
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional

from modules.requester import Requester


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

API_BASE = os.getenv("SHARETRIP_API_BASE", "https://api.sharetrip.net").rstrip("/")
INIT_URL = f"{API_BASE}/api/v2/flight/search/initialize"
SEARCH_URL_TMPL = f"{API_BASE}/api/v2/flight/search/{{search_id}}"
DEFAULT_ACCESS_TOKEN = "$2b$10$Fzhdo.0wIriRI9BfMIBKsuEZapnZdFAurXYwqDesf7DiFAHPmF6zm"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

ENV_ACCESS_TOKEN = "SHARETRIP_ACCESS_TOKEN"
ENV_COOKIES_PATH = "SHARETRIP_COOKIES_PATH"
ENV_PROXY_URL = "SHARETRIP_PROXY_URL"
ENV_ORIGIN = "SHARETRIP_ORIGIN"
ENV_REFERER = "SHARETRIP_REFERER"
ENV_CURRENCY = "SHARETRIP_CURRENCY"
ENV_OCCUPATION = "SHARETRIP_OCCUPATION"
ENV_POLL_MAX = "SHARETRIP_POLL_MAX_ATTEMPTS"
ENV_POLL_SLEEP_SEC = "SHARETRIP_POLL_SLEEP_SEC"
ENV_PAGE_LIMIT = "SHARETRIP_PAGE_LIMIT"
ENV_MAX_PAGES = "SHARETRIP_MAX_PAGES"
ENV_DEFAULT_AIRLINE_CODE = "SHARETRIP_AIRLINE_CODE"


def _safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if v in (None, ""):
            return default
        return int(v)
    except Exception:
        return default


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def _clip_text(v: Any, size: int = 600) -> str:
    text = str(v or "")
    if len(text) <= size:
        return text
    return text[: size - 3] + "..."


def _cabin_to_sharetrip_code(cabin: str) -> str:
    c = str(cabin or "").strip().lower()
    if "business" in c:
        return "BUSINESS"
    if "premium" in c:
        return "PREMIUM_ECONOMY"
    return "ECONOMY"


def _trip_type_to_sharetrip(trip_type: str = "OW") -> str:
    t = str(trip_type or "OW").strip().upper()
    if t in {"RT", "ROUNDTRIP", "ROUND_TRIP"}:
        return "ROUNDTRIP"
    return "ONEWAY"


def _default_headers() -> Dict[str, str]:
    token = str(os.getenv(ENV_ACCESS_TOKEN, DEFAULT_ACCESS_TOKEN)).strip() or DEFAULT_ACCESS_TOKEN
    origin = str(os.getenv(ENV_ORIGIN, "https://sharetrip.net")).strip() or "https://sharetrip.net"
    referer = str(os.getenv(ENV_REFERER, "https://sharetrip.net/")).strip() or "https://sharetrip.net/"
    return {
        "Accept": "application/json, text/plain, */*",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": USER_AGENT,
        "accesstoken": token,
    }


def build_initialize_params(
    *,
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    return {
        "cabinClass": _cabin_to_sharetrip_code(cabin),
        "currency": str(os.getenv(ENV_CURRENCY, "BDT")).strip() or "BDT",
        "departureDates[]": str(date),
        "destinations[]": str(destination).upper().strip(),
        "numOfAdult": max(1, int(adt or 1)),
        "numOfChild": max(0, int(chd or 0)),
        "numOfInfant": max(0, int(inf or 0)),
        "numOfKid": 0,
        "occupation": str(os.getenv(ENV_OCCUPATION, "NOT_SELECTED")).strip() or "NOT_SELECTED",
        "origins[]": str(origin).upper().strip(),
        "tripType": _trip_type_to_sharetrip("OW"),
    }


def _post_search_page(req: Requester, search_id: str, page: int, limit: int, headers: Dict[str, str]) -> tuple[int, Any]:
    url = SEARCH_URL_TMPL.format(search_id=search_id)
    payload = {"page": page, "limit": limit}
    response = req.session.post(url, json=payload, headers={**headers, "Content-Type": "application/json"}, timeout=req.timeout)
    try:
        body: Any = response.json()
    except Exception:
        body = response.text
    return response.status_code, body


def _dt_to_iso(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        d = str(value.get("date") or "").strip()
        t = str(value.get("time") or "").strip()
        if d and t:
            return f"{d}T{t}"
        if d:
            return d
    if isinstance(value, str):
        return value.strip() or None
    return None


def _extract_penalty_text(offer: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    display = offer.get("displayPrice") or {}
    mini_rules = display.get("miniRules")
    if mini_rules:
        return "SHARETRIP_MINI_RULES", _clip_text(mini_rules, 1000)
    msg = str(offer.get("refundableMsg") or "").strip()
    if msg:
        return "SHARETRIP_REFUNDABLE_MSG", msg
    return None, None


def _normalize_offer(
    *,
    airline_code: str,
    offer: Dict[str, Any],
    requested_cabin: str,
    adt: int,
    chd: int,
    inf: int,
    search_id: str,
) -> Optional[Dict[str, Any]]:
    legs = offer.get("legs") or []
    if not isinstance(legs, list) or not legs:
        return None
    leg0 = legs[0] if isinstance(legs[0], dict) else {}
    airline = str((leg0.get("airlines") or {}).get("code") or "").upper().strip()
    if airline != str(airline_code).upper().strip():
        return None

    segments: List[Dict[str, Any]] = []
    for leg in legs:
        if isinstance(leg, dict):
            for seg in (leg.get("segments") or []):
                if isinstance(seg, dict):
                    segments.append(seg)
    if not segments:
        return None
    seg0 = segments[0]
    seg_last = segments[-1]

    departure = _dt_to_iso(seg0.get("departureDateTime") or leg0.get("departureDateTime"))
    arrival = _dt_to_iso(seg_last.get("arrivalDateTime") or leg0.get("arrivalDateTime"))
    origin = str((seg0.get("origin") or {}).get("code") or (leg0.get("origin") or {}).get("code") or "").upper().strip()
    destination = str((seg_last.get("destination") or {}).get("code") or (leg0.get("destination") or {}).get("code") or "").upper().strip()

    display = offer.get("displayPrice") or {}
    total_fare = display.get("totalFare") or {}
    base_amount = _safe_float(total_fare.get("base"))
    tax_amount = _safe_float(total_fare.get("tax"))
    gross_total_amount = _safe_float(total_fare.get("total"))
    display_total_amount = _safe_float(total_fare.get("promotionalAmount"))
    if display_total_amount is None and gross_total_amount is not None:
        display_total_amount = gross_total_amount
    if display_total_amount is None and base_amount is not None and tax_amount is not None:
        display_total_amount = base_amount + tax_amount
    if gross_total_amount is None:
        gross_total_amount = display_total_amount

    ota_discount_pct = _safe_float(display.get("discount"))
    ota_discount_amount = None
    ota_markup_amount = None
    if gross_total_amount is not None and display_total_amount is not None:
        ota_discount_amount = max(gross_total_amount - display_total_amount, 0.0)
        ota_markup_amount = max(display_total_amount - gross_total_amount, 0.0)

    baggage = seg0.get("baggage") or {}
    bag_weight = _safe_int(baggage.get("weight"))
    bag_unit = str(baggage.get("unit") or "").strip()
    bag_text = None
    if bag_weight is not None:
        bag_text = f"{bag_weight} {bag_unit}".strip()

    equipment = (seg0.get("aircraft") or {}).get("code") or leg0.get("aircraftCode")
    aircraft_model = (seg0.get("aircraft") or {}).get("model") or (leg0.get("aircraft") or {}).get("model")
    penalty_source, penalty_text = _extract_penalty_text(offer)

    row: Dict[str, Any] = {
        "airline": airline,
        "operating_airline": str(seg0.get("operatedBy") or seg0.get("marketingAirline") or airline).upper().strip() or None,
        "flight_number": str(seg0.get("flightNumber") or "").strip(),
        "origin": origin,
        "destination": destination,
        "departure": departure,
        "arrival": arrival,
        "cabin": str(seg0.get("cabin") or requested_cabin or "Economy").title(),
        "fare_basis": str(seg0.get("resBookDesigCode") or seg0.get("cabinCode") or "").strip() or None,
        "brand": "SHARETRIP_OTA",
        "price_total_bdt": display_total_amount,
        "fare_amount": base_amount,
        "tax_amount": tax_amount,
        "currency": str(offer.get("currency") or total_fare.get("currency") or "BDT"),
        "duration_min": _safe_int(seg0.get("duration")) or _safe_int(leg0.get("duration")) or _safe_int(offer.get("totalDuration")),
        "stops": max(0, len(segments) - 1),
        "booking_class": str(seg0.get("resBookDesigCode") or seg0.get("cabinCode") or "").strip() or None,
        "baggage": bag_text,
        "equipment_code": str(equipment or "").strip() or None,
        "aircraft": str(aircraft_model or "").strip() or None,
        "seat_capacity": None,
        "seat_available": None,
        "inventory_confidence": "unknown_ota",
        "estimated_load_factor_pct": None,
        "soldout": False,
        "adt_count": max(1, int(adt or 1)),
        "chd_count": max(0, int(chd or 0)),
        "inf_count": max(0, int(inf or 0)),
        "fare_ref_num": str(offer.get("providerCode") or offer.get("sequenceCode") or ""),
        "fare_search_reference": str(search_id or ""),
        "fare_search_signature": str(offer.get("sequenceCode") or ""),
        "source_endpoint": "sharetrip:v2/search",
        "ota_name": "sharetrip",
        "ota_gross_fare": gross_total_amount,
        "ota_display_fare": display_total_amount,
        "ota_discount_pct": ota_discount_pct,
        "ota_discount_amount": ota_discount_amount,
        "ota_markup_amount": ota_markup_amount,
        "ota_has_brand_inventory_signal": bool(offer.get("isBrandFareAvailable")) if offer.get("isBrandFareAvailable") is not None else None,
        "fare_refundable": bool(offer.get("isRefundable")) if offer.get("isRefundable") is not None else None,
        "raw_offer": {
            "search_id": search_id,
            "offer": offer,
        },
    }
    if penalty_text:
        row["penalty_source"] = penalty_source
        row["penalty_rule_text"] = penalty_text
    return row


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[tuple] = set()
    for r in rows:
        key = (
            r.get("airline"),
            r.get("origin"),
            r.get("destination"),
            r.get("departure"),
            r.get("flight_number"),
            r.get("cabin"),
            r.get("fare_basis"),
            r.get("brand"),
            r.get("fare_ref_num"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def fetch_flights_for_airline(
    *,
    airline_code: str,
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    cookies_path: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    cookies = cookies_path or os.getenv(ENV_COOKIES_PATH) or None
    proxy = proxy_url or os.getenv(ENV_PROXY_URL) or None
    req = Requester(cookies_path=cookies, user_agent=USER_AGENT, proxy_url=proxy)
    headers = _default_headers()
    params = build_initialize_params(
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )

    out: Dict[str, Any] = {
        "raw": {
            "source": "sharetrip",
            "airline": str(airline_code).upper(),
            "initialize_params": params,
            "headers_hint": {
                "origin": headers.get("Origin"),
                "referer": headers.get("Referer"),
                "has_cookie_session": bool(req.session.cookies),
                "has_access_token": bool(headers.get("accesstoken")),
            },
            "poll_attempts": [],
            "page_attempts": [],
        },
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }

    init_resp = req.get(INIT_URL, params=params, headers=headers)
    out["raw"]["initialize_status"] = init_resp.status_code
    init_body: Any
    try:
        init_body = init_resp.json()
    except Exception:
        init_body = init_resp.text
    out["raw"]["initialize_response"] = init_body
    if init_resp.status_code not in (200, 201) or not isinstance(init_body, dict):
        out["raw"]["error"] = "initialize_failed"
        return out

    search_id = str((init_body.get("response") or {}).get("searchId") or "").strip()
    if not search_id:
        out["raw"]["error"] = "search_id_missing"
        return out
    out["raw"]["search_id"] = search_id

    poll_max = max(1, _safe_int(os.getenv(ENV_POLL_MAX), 8) or 8)
    poll_sleep = max(0.2, _safe_float(os.getenv(ENV_POLL_SLEEP_SEC), 1.0) or 1.0)
    page_limit = max(1, min(50, _safe_int(os.getenv(ENV_PAGE_LIMIT), 50) or 50))
    max_pages = max(1, _safe_int(os.getenv(ENV_MAX_PAGES), 6) or 6)

    final_body: Any = None
    final_response: Dict[str, Any] = {}
    for i in range(1, poll_max + 1):
        status, body = _post_search_page(req, search_id, page=1, limit=page_limit, headers=headers)
        poll_info = {
            "attempt": i,
            "status": status,
        }
        if isinstance(body, dict):
            response = body.get("response") or {}
            poll_info.update(
                {
                    "code": body.get("code"),
                    "message": body.get("message"),
                    "progressBar": response.get("progressBar"),
                    "totalFlightsCount": response.get("totalFlightsCount"),
                    "matchedFlights_count": len(response.get("matchedFlights") or []),
                }
            )
            final_body = body
            final_response = response if isinstance(response, dict) else {}
            if response.get("progressBar") == 1:
                break
        else:
            poll_info["body_preview"] = _clip_text(body, 240)
            final_body = body
        out["raw"]["poll_attempts"].append(poll_info)
        time.sleep(poll_sleep)

    if not isinstance(final_body, dict):
        out["raw"]["error"] = "search_failed_non_json"
        out["raw"]["search_response"] = final_body
        return out

    out["originalResponse"] = final_body
    out["raw"]["search_response"] = final_body
    out["raw"]["search_status"] = 200

    if str(final_body.get("code") or "").upper() != "SUCCESS":
        out["raw"]["error"] = "search_not_ok"
        return out

    total_flights = _safe_int(final_response.get("totalFlightsCount"), 0) or 0
    pages = max(1, min(max_pages, int(math.ceil(total_flights / float(page_limit))) if total_flights > 0 else 1))
    all_offers: List[Dict[str, Any]] = []

    # page=1 from final poll result
    matched_1 = final_response.get("matchedFlights") or []
    if isinstance(matched_1, list):
        all_offers.extend([x for x in matched_1 if isinstance(x, dict)])

    for page in range(2, pages + 1):
        status, body = _post_search_page(req, search_id, page=page, limit=page_limit, headers=headers)
        page_info = {"page": page, "status": status}
        if isinstance(body, dict):
            response = body.get("response") or {}
            page_rows = response.get("matchedFlights") or []
            page_info["matchedFlights_count"] = len(page_rows) if isinstance(page_rows, list) else 0
            if isinstance(page_rows, list):
                all_offers.extend([x for x in page_rows if isinstance(x, dict)])
        else:
            page_info["body_preview"] = _clip_text(body, 240)
        out["raw"]["page_attempts"].append(page_info)

    rows: List[Dict[str, Any]] = []
    wanted = str(airline_code or "").upper().strip()
    for offer in all_offers:
        row = _normalize_offer(
            airline_code=wanted,
            offer=offer,
            requested_cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
            search_id=search_id,
        )
        if row:
            rows.append(row)

    out["rows"] = _dedupe_rows(rows)
    out["ok"] = True
    return out


def fetch_flights(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    airline_code: Optional[str] = None,
):
    """
    Unified contract for run_all.py:
    { raw, originalResponse, rows, ok }
    """
    code = str(airline_code or os.getenv(ENV_DEFAULT_AIRLINE_CODE, "")).upper().strip()
    if not code:
        return {
            "raw": {
                "source": "sharetrip",
                "error": "airline_code_missing",
                "hint": "Pass airline_code in fetch_flights(...) or set SHARETRIP_AIRLINE_CODE.",
            },
            "originalResponse": None,
            "rows": [],
            "ok": False,
        }

    return fetch_flights_for_airline(
        airline_code=code,
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )


def cli_main():
    parser = argparse.ArgumentParser(description="ShareTrip OTA connector tester")
    parser.add_argument("--airline", required=True)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--cookies-path", default=None)
    parser.add_argument("--proxy-url", default=None)
    args = parser.parse_args()

    output = fetch_flights_for_airline(
        airline_code=args.airline,
        origin=args.origin,
        destination=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
        cookies_path=args.cookies_path,
        proxy_url=args.proxy_url,
    )
    print(json.dumps(output, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
