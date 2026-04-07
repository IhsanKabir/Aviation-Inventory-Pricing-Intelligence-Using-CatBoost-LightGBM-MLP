"""
BDFare OTA connector for normalized multi-airline fare feeds.

Flow (from HAR + runtime verification):
- POST /bdfare-search/api/v2/Search/AirSearch
- POST /bdfare-search/api/v2/Search/GetAirSearch?requestId=...
- POST /bdfare-search/api/v2/Search/RefreshAirSearch?requestId=... (poll as needed)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

from modules.requester import Requester


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

API_BASE = str(os.getenv("BDFARE_API_BASE", "https://bdfare.com")).rstrip("/")
SEARCH_BASE = f"{API_BASE}/bdfare-search/api/v2/Search"
AIRSEARCH_URL = f"{SEARCH_BASE}/AirSearch"
GET_AIRSEARCH_URL_TMPL = f"{SEARCH_BASE}/GetAirSearch?requestId={{request_id}}"
REFRESH_AIRSEARCH_URL_TMPL = f"{SEARCH_BASE}/RefreshAirSearch?requestId={{request_id}}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

ENV_COOKIES_PATH = "BDFARE_COOKIES_PATH"
ENV_PROXY_URL = "BDFARE_PROXY_URL"
ENV_ORIGIN = "BDFARE_ORIGIN"
ENV_REFERER = "BDFARE_REFERER"
ENV_MAX_POLLS = "BDFARE_MAX_POLLS"
ENV_POLL_SLEEP_SEC = "BDFARE_POLL_SLEEP_SEC"
ENV_SEARCH_FARE_TYPE = "BDFARE_SEARCH_FARE_TYPE"
ENV_DEFAULT_AIRLINE_CODE = "BDFARE_AIRLINE_CODE"


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


def _clip_text(v: Any, size: int = 800) -> str:
    text = str(v or "")
    if len(text) <= size:
        return text
    return text[: size - 3] + "..."


def _parse_money(v: Any) -> Optional[float]:
    if isinstance(v, (int, float)):
        return float(v)
    text = str(v or "").strip()
    if not text:
        return None
    m = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except Exception:
        return None


def _parse_duration_min(v: Any) -> Optional[int]:
    if isinstance(v, (int, float)):
        return max(0, int(v))
    s = str(v or "").strip().lower()
    if not s:
        return None
    h = 0
    m = 0
    hm = re.search(r"(\d+)\s*h", s)
    mm = re.search(r"(\d+)\s*m", s)
    if hm:
        h = int(hm.group(1))
    if mm:
        m = int(mm.group(1))
    total = h * 60 + m
    return total if total > 0 else None


def _parse_day_month_with_fallback(raw: Any, fallback: dt.date) -> dt.date:
    text = str(raw or "").strip()
    if not text:
        return fallback
    # Example: "31 Mar, Tue"
    piece = text.split(",")[0].strip()
    try:
        dm = dt.datetime.strptime(f"{piece} {fallback.year}", "%d %b %Y")
        return dt.date(dm.year, dm.month, dm.day)
    except Exception:
        return fallback


def _combine_iso(d: dt.date, hhmm: Any) -> str:
    t = str(hhmm or "").strip()
    if not re.fullmatch(r"\d{1,2}:\d{2}", t):
        t = "00:00"
    hh, mm = t.split(":")
    hh_i = max(0, min(23, int(hh)))
    mm_i = max(0, min(59, int(mm)))
    return dt.datetime(d.year, d.month, d.day, hh_i, mm_i, 0).isoformat()


def _cabin_to_bdfare(cabin: str) -> int:
    c = str(cabin or "").strip().lower()
    if "first" in c:
        return 4
    if "business" in c:
        return 3
    if "premium" in c:
        return 2
    return 1


def _default_headers(request_id: Optional[str] = None) -> Dict[str, str]:
    origin = str(os.getenv(ENV_ORIGIN, "https://bdfare.com")).strip() or "https://bdfare.com"
    referer_default = "https://bdfare.com/flight-search-result"
    if request_id:
        referer_default = f"{referer_default}?requestId={request_id}"
    referer = str(os.getenv(ENV_REFERER, referer_default)).strip() or referer_default
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": USER_AGENT,
    }


def _build_payload(
    *,
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    travelers: List[Dict[str, Any]] = []
    adt_n = max(1, int(adt or 1))
    chd_n = max(0, int(chd or 0))
    inf_n = max(0, int(inf or 0))
    travelers.append({"travelerType": 1, "travelerCount": adt_n})
    if chd_n > 0:
        travelers.append(
            {
                "travelerType": 2,
                "travelerCount": chd_n,
                "travelerAgeCode": "",
            }
        )
    if inf_n > 0:
        travelers.append({"travelerType": 3, "travelerCount": inf_n})

    payload: Dict[str, Any] = {
        "channelType": 1,
        "travelType": 1,  # One-way
        "airTravelPreference": {
            "cabinClass": _cabin_to_bdfare(cabin),
            "airlinePreference": "",
            "stopOver": "",
        },
        "airTravelInformation": [
            {
                "departure": str(origin).upper().strip(),
                "arrival": str(destination).upper().strip(),
                "travelDate": str(date).strip(),
            }
        ],
        "airTravelerInformation": travelers,
        "searchFareType": int(os.getenv(ENV_SEARCH_FARE_TYPE, "0") or 0),
    }
    return payload


def _post_json(req: Requester, url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> tuple[int, Any]:
    response = req.post_raw(url, json_payload=payload, headers=headers)
    try:
        body: Any = response.json()
    except Exception:
        body = response.text
    return response.status_code, body


def _normalize_row(
    *,
    info: Dict[str, Any],
    request_id: str,
    requested_date: str,
    requested_cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    summary_list = info.get("flightSummary") or []
    s0 = summary_list[0] if isinstance(summary_list, list) and summary_list and isinstance(summary_list[0], dict) else {}
    fallback_day = dt.date.fromisoformat(str(requested_date))
    dep_day = _parse_day_month_with_fallback(s0.get("departureDate"), fallback_day)
    arr_day_raw = _parse_day_month_with_fallback(s0.get("arrivalDate"), dep_day)
    extra_days = _safe_int(s0.get("numberOfAdditionalDaysTravel")) or 0
    arr_day = arr_day_raw + dt.timedelta(days=max(0, extra_days))
    departure_iso = _combine_iso(dep_day, s0.get("departureTime"))
    arrival_iso = _combine_iso(arr_day, s0.get("arrivalTime"))

    gross = _parse_money(info.get("grossAmount"))
    display = _parse_money(info.get("customerNetAmount"))
    if display is None:
        display = _parse_money(info.get("netAmount"))
    # Some rows intermittently return customerNetAmount as 0 while gross/net are non-zero.
    # Treat that as an unreliable display value to avoid false 100% discount/markup signals.
    if display is None or (display <= 0 and (gross or 0) > 0):
        display = gross
    agent = _parse_money(info.get("agentAmount"))
    amount = _parse_money(info.get("amount"))
    if amount is None:
        amount = _parse_money(info.get("netAmount"))
    tax_amount = None
    if gross is not None and amount is not None:
        tax_amount = max(gross - amount, 0.0)

    ota_discount_amount = None
    ota_markup_amount = None
    ota_discount_pct = None
    if gross is not None and display is not None:
        ota_discount_amount = max(gross - display, 0.0)
        ota_markup_amount = max(display - gross, 0.0)
        if gross > 0:
            ota_discount_pct = (ota_discount_amount / gross) * 100.0

    stop_key = info.get("stopKey") or []
    if isinstance(stop_key, list) and stop_key:
        first_stop = str(stop_key[0] or "").upper()
        stops = 0 if first_stop == "NS" else max(1, len(info.get("layoverAirports") or []))
    else:
        stops = max(0, len(info.get("layoverAirports") or []))
    via_airports = list(
        dict.fromkeys(
            str(item.get("code") if isinstance(item, dict) else item or "").upper().strip()
            for item in (info.get("layoverAirports") or [])
            if str(item.get("code") if isinstance(item, dict) else item or "").upper().strip()
        )
    )

    row: Dict[str, Any] = {
        "airline": str(info.get("airlineCode") or "").upper().strip(),
        "operating_airline": str(info.get("airlineCode") or "").upper().strip() or None,
        "flight_number": str(s0.get("airlineFlightNumber") or "").strip(),
        "origin": str(s0.get("departureAirportCode") or "").upper().strip(),
        "destination": str(s0.get("arrivalAirportCode") or "").upper().strip(),
        "departure": departure_iso,
        "arrival": arrival_iso,
        "cabin": str(requested_cabin or "Economy"),
        "fare_basis": str(info.get("productClass") or "").strip() or None,
        "brand": f"BDFare_{str(info.get('itineraryType') or 'OTA').upper()}",
        "price_total_bdt": display,
        "fare_amount": amount,
        "tax_amount": tax_amount,
        "currency": str(info.get("currency") or "BDT"),
        "duration_min": _safe_int(info.get("duration")) or _parse_duration_min(s0.get("journeyDuration")),
        "stops": stops,
        "via_airports": "|".join(via_airports) if via_airports else None,
        "booking_class": str(info.get("productClass") or "").strip() or None,
        "baggage": None,
        "equipment_code": None,
        "aircraft": None,
        "seat_capacity": None,
        "seat_available": None,
        "inventory_confidence": "unknown_ota",
        "estimated_load_factor_pct": None,
        "soldout": False,
        "adt_count": max(1, int(adt or 1)),
        "chd_count": max(0, int(chd or 0)),
        "inf_count": max(0, int(inf or 0)),
        "fare_ref_num": str(info.get("itineraryId") or ""),
        "fare_search_reference": str(request_id),
        "fare_search_signature": str(info.get("itineraryId") or ""),
        "source_endpoint": "bdfare:v2/Search/GetAirSearch",
        "ota_name": "bdfare",
        "ota_gross_fare": gross,
        "ota_display_fare": display,
        "ota_discount_pct": ota_discount_pct,
        "ota_discount_amount": ota_discount_amount,
        "ota_markup_amount": ota_markup_amount,
        "fare_refundable": bool(info.get("refundable")) if info.get("refundable") is not None else None,
        "raw_offer": {
            "request_id": request_id,
            "flight_info": info,
            "agent_amount": agent,
        },
    }

    penalties = {
        "change": info.get("changePenality"),
        "cancel": info.get("cancelPenality"),
    }
    if penalties["change"] not in (None, "", [], {}) or penalties["cancel"] not in (None, "", [], {}):
        row["penalty_source"] = "BDFARE_PENALTY_OBJECT"
        row["penalty_rule_text"] = _clip_text(json.dumps(penalties, ensure_ascii=False), 1000)
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
            r.get("via_airports"),
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
    payload = _build_payload(
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    headers = _default_headers()

    out: Dict[str, Any] = {
        "raw": {
            "source": "bdfare",
            "airline": str(airline_code).upper(),
            "search_payload": payload,
            "headers_hint": {
                "origin": headers.get("Origin"),
                "referer": headers.get("Referer"),
                "has_cookie_session": bool(req.session.cookies),
            },
            "attempts": [],
        },
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }

    start_status, start_body = _post_json(req, AIRSEARCH_URL, payload=payload, headers=headers)
    out["raw"]["airsearch_status"] = start_status
    out["raw"]["airsearch_response"] = start_body
    out["raw"]["attempts"].append(
        {
            "step": "airsearch",
            "status": start_status,
            "has_json": isinstance(start_body, dict),
            "request_id": start_body.get("requestId") if isinstance(start_body, dict) else None,
        }
    )
    if start_status != 200 or not isinstance(start_body, dict):
        out["raw"]["error"] = "airsearch_failed"
        return out

    request_id = str(start_body.get("requestId") or "").strip()
    out["raw"]["request_id"] = request_id
    if not request_id:
        out["raw"]["error"] = "request_id_missing"
        return out

    max_polls = max(1, _safe_int(os.getenv(ENV_MAX_POLLS)) or 8)
    poll_sleep = max(0.2, _safe_float(os.getenv(ENV_POLL_SLEEP_SEC)) or 1.0)
    final_body: Any = None
    get_headers = _default_headers(request_id=request_id)

    get_url = GET_AIRSEARCH_URL_TMPL.format(request_id=request_id)
    refresh_url = REFRESH_AIRSEARCH_URL_TMPL.format(request_id=request_id)
    for idx in range(1, max_polls + 1):
        get_status, get_body = _post_json(req, get_url, payload={}, headers=get_headers)
        info_count = len(get_body.get("flightInfos") or []) if isinstance(get_body, dict) else 0
        result_completed = bool(get_body.get("resultCompleted")) if isinstance(get_body, dict) else False
        err = (get_body.get("error") or {}) if isinstance(get_body, dict) else {}
        out["raw"]["attempts"].append(
            {
                "step": "get_airsearch",
                "attempt": idx,
                "status": get_status,
                "flight_infos_count": info_count,
                "result_completed": result_completed,
                "error_code": err.get("errorCode") if isinstance(err, dict) else None,
                "error_message": err.get("message") if isinstance(err, dict) else None,
            }
        )
        if get_status == 200 and isinstance(get_body, dict):
            final_body = get_body
            if info_count > 0 or result_completed:
                break
        ref_status, ref_body = _post_json(req, refresh_url, payload={}, headers=get_headers)
        out["raw"]["attempts"].append(
            {
                "step": "refresh_airsearch",
                "attempt": idx,
                "status": ref_status,
                "has_json": isinstance(ref_body, dict),
                "result_completed": bool(ref_body.get("resultCompleted")) if isinstance(ref_body, dict) else None,
            }
        )
        time.sleep(poll_sleep)

    if not isinstance(final_body, dict):
        out["raw"]["error"] = "get_airsearch_failed"
        return out

    out["originalResponse"] = final_body
    infos = final_body.get("flightInfos") or []
    if not isinstance(infos, list):
        infos = []

    wanted = str(airline_code or "").upper().strip()
    rows: List[Dict[str, Any]] = []
    for info in infos:
        if not isinstance(info, dict):
            continue
        if str(info.get("airlineCode") or "").upper().strip() != wanted:
            continue
        rows.append(
            _normalize_row(
                info=info,
                request_id=request_id,
                requested_date=str(date),
                requested_cabin=cabin,
                adt=adt,
                chd=chd,
                inf=inf,
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
    code = str(airline_code or os.getenv(ENV_DEFAULT_AIRLINE_CODE, "")).upper().strip()
    if not code:
        return {
            "raw": {
                "source": "bdfare",
                "error": "airline_code_missing",
                "hint": "Pass airline_code in fetch_flights(...) or set BDFARE_AIRLINE_CODE.",
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
    p = argparse.ArgumentParser(description="BDFare OTA connector tester")
    p.add_argument("--airline", required=True, help="Airline code filter (e.g. BS, BG, 6E)")
    p.add_argument("--origin", required=True)
    p.add_argument("--destination", required=True)
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--cabin", default="Economy")
    p.add_argument("--adt", type=int, default=1)
    p.add_argument("--chd", type=int, default=0)
    p.add_argument("--inf", type=int, default=0)
    args = p.parse_args()

    out = fetch_flights_for_airline(
        airline_code=args.airline,
        origin=args.origin,
        destination=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
    )
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
