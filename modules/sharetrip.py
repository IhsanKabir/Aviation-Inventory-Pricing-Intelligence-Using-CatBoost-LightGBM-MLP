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

from modules.bdfare import fetch_flights_for_airline as fetch_from_bdfare
from modules.requester import Requester, RequesterError


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
ENV_ADAPTIVE_POLL_STOP = "SHARETRIP_ADAPTIVE_POLL_STOP"
ENV_EARLY_STOP_MIN_PROGRESS = "SHARETRIP_EARLY_STOP_MIN_PROGRESS"
ENV_MULTI_PAGE_STABLE_POLLS = "SHARETRIP_MULTI_PAGE_STABLE_POLLS"
ENV_INIT_MAX_ATTEMPTS = "SHARETRIP_INIT_MAX_ATTEMPTS"
ENV_INIT_RETRY_SLEEP_SEC = "SHARETRIP_INIT_RETRY_SLEEP_SEC"
ENV_SOURCE_POLICY = "SHARETRIP_SOURCE_POLICY"
ENV_BDFARE_AIRLINES = "SHARETRIP_BDFARE_AIRLINES"
ENV_SOURCE_OVERRIDES = "SHARETRIP_SOURCE_OVERRIDES"


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


def _env_true(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _progress_fraction(v: Any) -> Optional[float]:
    num = _safe_float(v)
    if num is None:
        return None
    return max(0.0, min(1.0, float(num)))


def _clip_text(v: Any, size: int = 600) -> str:
    text = str(v or "")
    if len(text) <= size:
        return text
    return text[: size - 3] + "..."


def _preview_body(v: Any, size: int = 320) -> str:
    if isinstance(v, (dict, list)):
        try:
            return _clip_text(json.dumps(v, ensure_ascii=False), size)
        except Exception:
            return _clip_text(str(v), size)
    return _clip_text(v, size)


def _has_usable_rows(result: Any) -> bool:
    rows = result.get("rows") if isinstance(result, dict) else None
    ok = bool(result.get("ok")) if isinstance(result, dict) else False
    return ok and isinstance(rows, list) and bool(rows)


def _source_attempt_summary(source: str, result: Any) -> Dict[str, Any]:
    raw = result.get("raw") if isinstance(result, dict) else {}
    rows = result.get("rows") if isinstance(result, dict) else None
    return {
        "source": source,
        "ok": bool(result.get("ok")) if isinstance(result, dict) else False,
        "rows": len(rows) if isinstance(rows, list) else None,
        "error": (raw or {}).get("error") if isinstance(raw, dict) else None,
        "message": (raw or {}).get("message") if isinstance(raw, dict) else None,
    }


def _sharetrip_source_overrides() -> Dict[str, str]:
    raw = str(os.getenv(ENV_SOURCE_OVERRIDES, "") or "").strip()
    if not raw:
        return {}
    overrides: Dict[str, str] = {}
    for part in raw.split(","):
        piece = str(part or "").strip()
        if not piece or "=" not in piece:
            continue
        airline, policy = piece.split("=", 1)
        airline_code = str(airline or "").upper().strip()
        policy_name = str(policy or "").strip().lower()
        if airline_code and policy_name:
            overrides[airline_code] = policy_name
    return overrides


def _sharetrip_source_policy(airline_code: Optional[str] = None) -> str:
    airline = str(airline_code or "").upper().strip()
    if airline:
        override = _sharetrip_source_overrides().get(airline)
        if override:
            return override
    return str(os.getenv(ENV_SOURCE_POLICY, "sharetrip_only") or "sharetrip_only").strip().lower()


def _bdfare_airline_scope_allows(airline_code: str) -> bool:
    raw = str(os.getenv(ENV_BDFARE_AIRLINES, "all") or "all").strip().lower()
    if raw in {"", "all", "*"}:
        return True
    allowed = {part.strip().upper() for part in raw.split(",") if part.strip()}
    return str(airline_code or "").upper().strip() in allowed


def _fetch_bdfare_for_airline(
    *,
    airline_code: str,
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Dict[str, Any]:
    return fetch_from_bdfare(
        airline_code=str(airline_code).upper().strip(),
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )


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


def _build_requester(*, cookies: Optional[str], proxy: Optional[str]) -> Requester:
    return Requester(cookies_path=cookies, user_agent=USER_AGENT, proxy_url=proxy)


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
    response = req.post_raw(
        url,
        json_payload=payload,
        headers={**headers, "Content-Type": "application/json"},
    )
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
    via_airports = list(
        dict.fromkeys(
            str((seg.get("destination") or {}).get("code") or "").upper().strip()
            for seg in segments[:-1]
            if str((seg.get("destination") or {}).get("code") or "").upper().strip()
            and str((seg.get("destination") or {}).get("code") or "").upper().strip() not in {origin, destination}
        )
    )

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
        "via_airports": "|".join(via_airports) if via_airports else None,
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
            r.get("via_airports"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _fetch_sharetrip_core(
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
    req = _build_requester(cookies=cookies, proxy=proxy)
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

    init_max_attempts = max(1, _safe_int(os.getenv(ENV_INIT_MAX_ATTEMPTS), 3) or 3)
    init_retry_sleep = max(0.5, _safe_float(os.getenv(ENV_INIT_RETRY_SLEEP_SEC), 1.5) or 1.5)
    init_resp = None
    init_body: Any = None
    search_id = ""
    init_error_text = None
    initialize_attempts: List[Dict[str, Any]] = []
    for attempt in range(1, init_max_attempts + 1):
        init_error_text = None
        try:
            init_resp = req.get(INIT_URL, params=params, headers=headers)
            try:
                init_body = init_resp.json()
            except Exception:
                init_body = init_resp.text
        except (RequesterError, Exception) as exc:
            init_resp = None
            init_body = None
            init_error_text = _clip_text(exc, 320)

        attempt_info = {
            "attempt": attempt,
            "status": init_resp.status_code if init_resp is not None else None,
            "has_cookie_session": bool(req.session.cookies),
            "has_access_token": bool(headers.get("accesstoken")),
        }
        if init_error_text:
            attempt_info["error"] = init_error_text
        elif init_body is not None:
            attempt_info["response_preview"] = _preview_body(init_body)
        initialize_attempts.append(attempt_info)

        if init_resp is not None and init_resp.status_code in (200, 201) and isinstance(init_body, dict):
            search_id = str((init_body.get("response") or {}).get("searchId") or "").strip()
            if search_id:
                break
        if attempt < init_max_attempts:
            sleep_sec = round(init_retry_sleep * attempt, 2)
            LOG.warning(
                "[%s] ShareTrip initialize attempt %d/%d failed %s->%s on %s (%s); retrying in %.2fs",
                str(airline_code).upper().strip() or "?",
                attempt,
                init_max_attempts,
                str(origin).upper().strip(),
                str(destination).upper().strip(),
                str(date).strip(),
                str(cabin).strip() or "Economy",
                sleep_sec,
            )
            time.sleep(sleep_sec)
            req = _build_requester(cookies=cookies, proxy=proxy)

    out["raw"]["initialize_attempts"] = initialize_attempts
    out["raw"]["initialize_status"] = init_resp.status_code if init_resp is not None else None
    out["raw"]["initialize_response"] = init_body
    if init_resp is None or init_resp.status_code not in (200, 201) or not isinstance(init_body, dict):
        out["raw"]["error"] = "initialize_failed"
        out["raw"]["initialize_response_preview"] = _preview_body(init_body if init_body is not None else init_error_text)
        LOG.warning(
            "[%s] ShareTrip initialize failed %s->%s on %s (%s): status=%s cookies=%s token=%s preview=%s",
            str(airline_code).upper().strip() or "?",
            str(origin).upper().strip(),
            str(destination).upper().strip(),
            str(date).strip(),
            str(cabin).strip() or "Economy",
            init_resp.status_code if init_resp is not None else None,
            bool(req.session.cookies),
            bool(headers.get("accesstoken")),
            out["raw"]["initialize_response_preview"],
        )
        return out

    if not search_id:
        out["raw"]["error"] = "search_id_missing"
        out["raw"]["initialize_response_preview"] = _preview_body(init_body)
        LOG.warning(
            "[%s] ShareTrip initialize missing searchId %s->%s on %s (%s): status=%s preview=%s",
            str(airline_code).upper().strip() or "?",
            str(origin).upper().strip(),
            str(destination).upper().strip(),
            str(date).strip(),
            str(cabin).strip() or "Economy",
            init_resp.status_code if init_resp is not None else None,
            out["raw"]["initialize_response_preview"],
        )
        return out
    out["raw"]["search_id"] = search_id

    poll_max = max(1, _safe_int(os.getenv(ENV_POLL_MAX), 8) or 8)
    poll_sleep = max(0.2, _safe_float(os.getenv(ENV_POLL_SLEEP_SEC), 1.0) or 1.0)
    page_limit = max(1, min(50, _safe_int(os.getenv(ENV_PAGE_LIMIT), 50) or 50))
    max_pages = max(1, _safe_int(os.getenv(ENV_MAX_PAGES), 6) or 6)
    adaptive_poll_stop = _env_true(ENV_ADAPTIVE_POLL_STOP, default=True)
    early_stop_min_progress = _progress_fraction(os.getenv(ENV_EARLY_STOP_MIN_PROGRESS))
    if early_stop_min_progress is None:
        early_stop_min_progress = 0.90
    multi_page_stable_polls = max(1, _safe_int(os.getenv(ENV_MULTI_PAGE_STABLE_POLLS), 2) or 2)

    final_body: Any = None
    final_response: Dict[str, Any] = {}
    last_full_page_signature: Optional[tuple[int, int]] = None
    full_page_stable_polls = 0
    for i in range(1, poll_max + 1):
        status, body = _post_search_page(req, search_id, page=1, limit=page_limit, headers=headers)
        poll_info = {
            "attempt": i,
            "status": status,
        }
        if isinstance(body, dict):
            response = body.get("response") or {}
            progress = _progress_fraction(response.get("progressBar"))
            total_flights = max(0, _safe_int(response.get("totalFlightsCount"), 0) or 0)
            matched_count = len(response.get("matchedFlights") or [])
            poll_info.update(
                {
                    "code": body.get("code"),
                    "message": body.get("message"),
                    "progressBar": progress,
                    "totalFlightsCount": total_flights,
                    "matchedFlights_count": matched_count,
                }
            )
            final_body = body
            final_response = response if isinstance(response, dict) else {}

            full_page_ready = total_flights > 0 and matched_count >= min(total_flights, page_limit)
            if full_page_ready:
                signature = (total_flights, matched_count)
                if signature == last_full_page_signature:
                    full_page_stable_polls += 1
                else:
                    last_full_page_signature = signature
                    full_page_stable_polls = 1
            else:
                last_full_page_signature = None
                full_page_stable_polls = 0

            poll_info["full_page_ready"] = full_page_ready
            poll_info["full_page_stable_polls"] = full_page_stable_polls

            stop_reason = None
            if progress == 1.0:
                stop_reason = "progress_complete"
            elif adaptive_poll_stop and progress is not None and progress >= early_stop_min_progress and full_page_ready:
                if total_flights <= page_limit:
                    stop_reason = "adaptive_single_page_full"
                elif full_page_stable_polls >= multi_page_stable_polls:
                    stop_reason = "adaptive_multi_page_stable"
            if stop_reason:
                poll_info["stop_reason"] = stop_reason
                out["raw"]["poll_stop_reason"] = stop_reason
        else:
            poll_info["body_preview"] = _clip_text(body, 240)
            final_body = body
        out["raw"]["poll_attempts"].append(poll_info)
        if isinstance(body, dict) and poll_info.get("stop_reason"):
            break
        if i < poll_max:
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
        LOG.warning(
            "[%s] ShareTrip search not ok %s->%s on %s (%s): code=%s message=%s progress=%s preview=%s",
            str(airline_code).upper().strip() or "?",
            str(origin).upper().strip(),
            str(destination).upper().strip(),
            str(date).strip(),
            str(cabin).strip() or "Economy",
            final_body.get("code"),
            _clip_text(final_body.get("message"), 120),
            (final_response or {}).get("progressBar"),
            _preview_body(final_body),
        )
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
    policy = _sharetrip_source_policy(airline_code)
    airline = str(airline_code or "").upper().strip()
    can_try_bdfare = _bdfare_airline_scope_allows(airline)
    attempts: List[Dict[str, Any]] = []

    def _annotate(result: Dict[str, Any]) -> Dict[str, Any]:
        raw = result.setdefault("raw", {})
        if isinstance(raw, dict):
            raw["source_policy"] = policy
            raw["source_attempts"] = list(attempts)
        return result

    def _run_bdfare() -> Dict[str, Any]:
        result = _fetch_bdfare_for_airline(
            airline_code=airline,
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
        attempts.append(_source_attempt_summary("bdfare", result))
        return result

    def _run_sharetrip() -> Dict[str, Any]:
        result = _fetch_sharetrip_core(
            airline_code=airline,
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
            cookies_path=cookies_path,
            proxy_url=proxy_url,
        )
        attempts.append(_source_attempt_summary("sharetrip", result))
        return result

    if policy == "bdfare_only" and can_try_bdfare:
        return _annotate(_run_bdfare())

    if policy in {"bdfare_first", "bdfare_first_then_sharetrip"} and can_try_bdfare:
        bdfare_result = _run_bdfare()
        if _has_usable_rows(bdfare_result) or policy == "bdfare_first":
            return _annotate(bdfare_result)
        sharetrip_result = _run_sharetrip()
        if _has_usable_rows(sharetrip_result):
            return _annotate(sharetrip_result)
        return _annotate(bdfare_result if bool(bdfare_result.get("ok")) else sharetrip_result)

    sharetrip_result = _run_sharetrip()
    if _has_usable_rows(sharetrip_result) or policy not in {"sharetrip_then_bdfare", "bdfare_first_then_sharetrip"}:
        return _annotate(sharetrip_result)
    if can_try_bdfare:
        bdfare_result = _run_bdfare()
        if _has_usable_rows(bdfare_result):
            return _annotate(bdfare_result)
        return _annotate(sharetrip_result if bool(sharetrip_result.get("ok")) else bdfare_result)
    return _annotate(sharetrip_result)


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
