"""
Indigo (6E) connector.

Modes:
- auto (default): try Indigo direct API first, then fallback to ShareTrip.
- direct: Indigo direct API only.
- sharetrip: ShareTrip OTA only.

Contract:
- fetch_flights(...) returns:
  { raw, originalResponse, rows, ok }
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from modules.requester import Requester
from modules.sharetrip import fetch_flights_for_airline as fetch_from_sharetrip


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

SESSION_API_BASE = os.getenv(
    "INDIGO_SESSION_API_BASE",
    "https://api-prod-session-skyplus6e.goindigo.in",
).rstrip("/")
FLIGHT_API_BASE = os.getenv(
    "INDIGO_FLIGHT_API_BASE",
    "https://api-prod-flight-skyplus6e.goindigo.in",
).rstrip("/")

TOKEN_REFRESH_URL = f"{SESSION_API_BASE}/v1/token/refresh"
SEARCH_URL = f"{FLIGHT_API_BASE}/v1/flight/search"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

ENV_SOURCE_MODE = "INDIGO_SOURCE_MODE"
ENV_COOKIES_PATH = "INDIGO_COOKIES_PATH"
ENV_PROXY_URL = "INDIGO_PROXY_URL"
ENV_ORIGIN = "INDIGO_ORIGIN"
ENV_REFERER = "INDIGO_REFERER"
ENV_HEADERS_FILE = "INDIGO_HEADERS_FILE"
ENV_RESIDENT_COUNTRY = "INDIGO_RESIDENT_COUNTRY"
ENV_CURRENCY = "INDIGO_CURRENCY"
ENV_PROMOTION_CODE = "INDIGO_PROMOTION_CODE"
ENV_TAXES_AND_FEES = "INDIGO_TAXES_AND_FEES"
ENV_FLIGHT_FILTER_TYPE = "INDIGO_FLIGHT_FILTER_TYPE"
ENV_TOKEN_REFRESH_ENABLED = "INDIGO_TOKEN_REFRESH_ENABLED"
DEFAULT_HEADERS_FILE = "output/manual_sessions/indigo_headers_latest.json"


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v in (None, ""):
            return None
        return int(v)
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v in (None, ""):
            return None
        return float(v)
    except Exception:
        return None


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def _clip_text(v: Any, size: int = 500) -> str:
    text = str(v or "")
    if len(text) <= size:
        return text
    return text[: size - 3] + "..."


def _default_headers() -> Dict[str, str]:
    origin = str(os.getenv(ENV_ORIGIN, "https://www.goindigo.in")).strip() or "https://www.goindigo.in"
    referer = str(os.getenv(ENV_REFERER, "https://www.goindigo.in/")).strip() or "https://www.goindigo.in/"
    return {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": USER_AGENT,
    }


def _headers_cache_file() -> str:
    return str(os.getenv(ENV_HEADERS_FILE, DEFAULT_HEADERS_FILE) or DEFAULT_HEADERS_FILE)


def _load_extra_headers_from_cache() -> Dict[str, str]:
    path = Path(_headers_cache_file())
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    excluded = {"host", "content-length", "cookie", "connection", "accept-encoding"}
    out: Dict[str, str] = {}
    for k, v in raw.items():
        name = str(k or "").strip()
        if not name:
            continue
        if name.lower() in excluded:
            continue
        out[name] = str(v or "")
    return out


def _build_search_payload(
    *,
    origin: str,
    destination: str,
    date: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    pax_types: List[Dict[str, Any]] = []
    adt_n = max(1, int(adt or 1))
    chd_n = max(0, int(chd or 0))
    inf_n = max(0, int(inf or 0))
    pax_types.append({"count": adt_n, "discountCode": "", "type": "ADT"})
    if chd_n > 0:
        pax_types.append({"count": chd_n, "discountCode": "", "type": "CHD"})
    if inf_n > 0:
        pax_types.append({"count": inf_n, "discountCode": "", "type": "INF"})

    return {
        "codes": {
            "currency": str(os.getenv(ENV_CURRENCY, "BDT")).strip() or "BDT",
            "promotionCode": str(os.getenv(ENV_PROMOTION_CODE, "")).strip(),
        },
        "criteria": [
            {
                "dates": {"beginDate": str(date).strip()},
                "flightFilters": {
                    "type": str(os.getenv(ENV_FLIGHT_FILTER_TYPE, "All")).strip() or "All",
                },
                "stations": {
                    "originStationCodes": [str(origin).upper().strip()],
                    "destinationStationCodes": [str(destination).upper().strip()],
                },
            }
        ],
        "passengers": {
            "residentCountry": str(os.getenv(ENV_RESIDENT_COUNTRY, "IN")).strip() or "IN",
            "types": pax_types,
        },
        "taxesAndFees": str(os.getenv(ENV_TAXES_AND_FEES, "TaxesAndFees")).strip() or "TaxesAndFees",
        "tripCriteria": "oneWay",
        "isRedeemTransaction": False,
    }


def _request_json_or_text(response: Any) -> Any:
    try:
        return response.json()
    except Exception:
        return response.text


def _is_blocked(status_code: int, body: Any) -> bool:
    text = ""
    if isinstance(body, dict):
        text = json.dumps(body, ensure_ascii=False)
    elif isinstance(body, str):
        text = body
    lower = text.lower()
    if status_code in {401, 403, 429}:
        return True
    return any(
        x in lower
        for x in (
            "access denied",
            "captcha",
            "akamai",
            "bot",
            "forbidden",
        )
    )


def _parse_iso(v: Any) -> Optional[dt.datetime]:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _duration_min_from_designator(designator: Dict[str, Any]) -> Optional[int]:
    dep = _parse_iso(designator.get("departure"))
    arr = _parse_iso(designator.get("arrival"))
    if dep is None or arr is None:
        return None
    return max(0, int((arr - dep).total_seconds() // 60))


def _flight_number_from_journey(journey: Dict[str, Any]) -> str:
    segments = journey.get("segments") or []
    if isinstance(segments, list) and segments and isinstance(segments[0], dict):
        ident = (segments[0].get("identifier") or {})
        num = str(ident.get("identifier") or "").strip()
        if num:
            return num
    seg_key = str(journey.get("segKey") or "").strip()
    m = re.search(r"^[A-Z]{3}(\d{1,5})[A-Z]{3}$", seg_key)
    if m:
        return m.group(1)
    return ""


def _operating_airline_from_journey(journey: Dict[str, Any], fallback: str) -> Optional[str]:
    segments = journey.get("segments") or []
    if isinstance(segments, list) and segments and isinstance(segments[0], dict):
        ident = (segments[0].get("identifier") or {})
        code = str(ident.get("carrierCode") or "").upper().strip()
        if code:
            return code
    return str(fallback or "").upper().strip() or None


def _equipment_from_journey(journey: Dict[str, Any]) -> Optional[str]:
    segments = journey.get("segments") or []
    if isinstance(segments, list) and segments and isinstance(segments[0], dict):
        ident = (segments[0].get("identifier") or {})
        value = str(ident.get("equipmentType") or "").strip()
        if value:
            return value
    return None


def _baggage_text(baggage_data: Dict[str, Any]) -> Optional[str]:
    if not isinstance(baggage_data, dict):
        return None
    chunks: List[str] = []
    checkin_kg = _safe_float(baggage_data.get("checkinBaggageWeight"))
    hand_kg = _safe_float(baggage_data.get("handBaggageWeight"))
    if checkin_kg is not None:
        chunks.append(f"Check-in {int(checkin_kg) if checkin_kg.is_integer() else checkin_kg}kg")
    if hand_kg is not None:
        chunks.append(f"Cabin {int(hand_kg) if hand_kg.is_integer() else hand_kg}kg")
    if chunks:
        return "; ".join(chunks)
    return None


def _normalize_journey_rows(
    *,
    airline_code: str,
    trip: Dict[str, Any],
    journey: Dict[str, Any],
    requested_cabin: str,
    adt: int,
    chd: int,
    inf: int,
    currency_code: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    passenger_fares = journey.get("passengerFares") or []
    if not isinstance(passenger_fares, list):
        passenger_fares = []
    if not passenger_fares:
        return rows

    designator = journey.get("designator") or {}
    origin = str(designator.get("origin") or trip.get("origin") or "").upper().strip()
    destination = str(designator.get("destination") or trip.get("destination") or "").upper().strip()
    departure = str(designator.get("departure") or "").strip()
    arrival = str(designator.get("arrival") or "").strip()
    flight_number = _flight_number_from_journey(journey)
    operating_airline = _operating_airline_from_journey(journey, fallback=airline_code)
    duration_min = _duration_min_from_designator(designator)
    equipment_code = _equipment_from_journey(journey)
    stops = _safe_int(journey.get("stops"))
    if stops is None:
        segments = journey.get("segments") or []
        stops = max(0, len(segments) - 1) if isinstance(segments, list) else 0
    soldout = bool(journey.get("isSold")) if journey.get("isSold") is not None else False
    filling_fast = journey.get("fillingFast")

    # Keep only identity-complete rows to avoid downstream DB issues.
    if not (origin and destination and departure and flight_number):
        return rows

    for pf in passenger_fares:
        if not isinstance(pf, dict):
            continue
        fare_total = _safe_float(pf.get("totalFareAmount"))
        fare_amount = _safe_float(pf.get("totalPublishFare"))
        tax_amount = _safe_float(pf.get("totalTax"))
        if tax_amount is None and fare_total is not None and fare_amount is not None:
            tax_amount = max(0.0, fare_total - fare_amount)

        product_class = str(pf.get("productClass") or "").strip()
        cabin_label = str(pf.get("FareClass") or requested_cabin or "Economy").strip()
        if not cabin_label:
            cabin_label = "Economy"

        row: Dict[str, Any] = {
            "airline": str(airline_code).upper(),
            "operating_airline": operating_airline,
            "flight_number": flight_number,
            "origin": origin,
            "destination": destination,
            "departure": departure,
            "arrival": arrival,
            "cabin": cabin_label,
            "fare_basis": product_class or None,
            "brand": "INDIGO_DIRECT",
            "price_total_bdt": fare_total,
            "fare_amount": fare_amount,
            "tax_amount": tax_amount,
            "currency": str(currency_code or "BDT"),
            "duration_min": duration_min,
            "stops": stops,
            "booking_class": product_class or None,
            "baggage": _baggage_text(pf.get("baggageData") or {}),
            "equipment_code": equipment_code,
            "aircraft": equipment_code,
            "seat_capacity": None,
            "seat_available": None,
            "inventory_confidence": "unknown",
            "estimated_load_factor_pct": None,
            "soldout": soldout,
            "adt_count": max(1, int(adt or 1)),
            "chd_count": max(0, int(chd or 0)),
            "inf_count": max(0, int(inf or 0)),
            "fare_ref_num": str(pf.get("fareAvailabilityKey") or ""),
            "fare_search_reference": str(journey.get("journeyKey") or journey.get("segKey") or ""),
            "fare_search_signature": str(journey.get("segKey") or ""),
            "source_endpoint": "goindigo:v1/flight/search",
            "fare_refundable": None,
            "raw_offer": {
                "trip": trip,
                "journey": journey,
                "passenger_fare": pf,
                "signal_filling_fast": filling_fast,
                "indigo_total_publish_fare": _safe_float(pf.get("totalPublishFare")),
                "indigo_original_fare_amount": _safe_float(pf.get("originalFareAmount")),
                "indigo_original_published_amount": _safe_float(pf.get("originalPublishedAmount")),
                "indigo_original_total_discount": _safe_float(pf.get("originalTotalDiscount")),
            },
        }
        rows.append(row)
    return rows


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


def fetch_direct(
    *,
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
    extra_headers = _load_extra_headers_from_cache()
    headers.update(extra_headers)
    payload = _build_search_payload(
        origin=origin,
        destination=destination,
        date=date,
        adt=adt,
        chd=chd,
        inf=inf,
    )

    out: Dict[str, Any] = {
        "raw": {
            "source": "indigo_direct",
            "airline": "6E",
            "search_payload": payload,
            "headers_hint": {
                "origin": headers.get("Origin"),
                "referer": headers.get("Referer"),
                "has_cookie_session": bool(req.session.cookies),
                "headers_cache_file": _headers_cache_file(),
                "extra_headers_loaded": bool(extra_headers),
            },
        },
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }

    if _env_truthy(ENV_TOKEN_REFRESH_ENABLED, default=True):
        try:
            refresh_resp = req.session.put(
                TOKEN_REFRESH_URL,
                json={},
                headers=headers,
                timeout=req.timeout,
            )
            refresh_body = _request_json_or_text(refresh_resp)
            out["raw"]["token_refresh_status"] = refresh_resp.status_code
            out["raw"]["token_refresh_response"] = (
                refresh_body if isinstance(refresh_body, dict) else _clip_text(refresh_body, 300)
            )
        except Exception as exc:
            out["raw"]["token_refresh_error"] = str(exc)

    try:
        search_resp = req.session.post(
            SEARCH_URL,
            json=payload,
            headers=headers,
            timeout=req.timeout,
        )
    except Exception as exc:
        out["raw"]["error"] = "search_request_failed"
        out["raw"]["detail"] = str(exc)
        return out

    search_body = _request_json_or_text(search_resp)
    out["raw"]["search_status"] = search_resp.status_code
    out["raw"]["search_response"] = (
        search_body if isinstance(search_body, dict) else _clip_text(search_body, 1200)
    )

    if _is_blocked(search_resp.status_code, search_body):
        out["raw"]["error"] = "blocked"
        out["raw"]["hint"] = (
            "Indigo direct endpoint blocked this request. "
            "Use INDIGO_SOURCE_MODE=sharetrip for fallback, or retry with valid session cookies via INDIGO_COOKIES_PATH."
        )
        return out

    if search_resp.status_code != 200 or not isinstance(search_body, dict):
        out["raw"]["error"] = "search_failed"
        return out

    out["originalResponse"] = search_body
    data = search_body.get("data") or {}
    trips = data.get("trips") or []
    if not isinstance(trips, list):
        trips = []
    rows: List[Dict[str, Any]] = []
    currency_code = str(data.get("currencyCode") or payload.get("codes", {}).get("currency") or "BDT")
    for trip in trips:
        if not isinstance(trip, dict):
            continue
        for journey in (trip.get("journeysAvailable") or []):
            if not isinstance(journey, dict):
                continue
            rows.extend(
                _normalize_journey_rows(
                    airline_code="6E",
                    trip=trip,
                    journey=journey,
                    requested_cabin=cabin,
                    adt=adt,
                    chd=chd,
                    inf=inf,
                    currency_code=currency_code,
                )
            )

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
    requested_code = str(airline_code or "6E").upper().strip()
    if requested_code != "6E":
        return {
            "raw": {
                "source": "indigo_direct",
                "error": "unsupported_airline_code",
                "hint": "modules.indigo only supports airline_code=6E.",
            },
            "originalResponse": None,
            "rows": [],
            "ok": False,
        }

    mode = str(os.getenv(ENV_SOURCE_MODE, "auto")).strip().lower()
    if mode == "sharetrip":
        return fetch_from_sharetrip(
            airline_code="6E",
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )

    direct = fetch_direct(
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    if mode == "direct":
        return direct

    if bool(direct.get("ok")) and isinstance(direct.get("rows"), list) and direct.get("rows"):
        return direct

    fallback = fetch_from_sharetrip(
        airline_code="6E",
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    wrapped = {
        "raw": {
            "source": "indigo_auto",
            "direct": direct.get("raw", {}),
            "fallback_source": "sharetrip",
            "fallback_ok": bool(fallback.get("ok")),
        },
        "originalResponse": fallback.get("originalResponse"),
        "rows": fallback.get("rows") if isinstance(fallback.get("rows"), list) else [],
        "ok": bool(fallback.get("ok")),
    }
    return wrapped


def cli_main():
    parser = argparse.ArgumentParser(description="Indigo connector tester (direct with ShareTrip fallback)")
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--mode", choices=["auto", "direct", "sharetrip"], default=None)
    args = parser.parse_args()

    if args.mode:
        os.environ[ENV_SOURCE_MODE] = args.mode
    out = fetch_flights(
        origin=args.origin,
        destination=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
        airline_code="6E",
    )
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
