"""
AMYBD OTA connector for normalized BS / 2A fare feeds.

Contract:
- fetch_flights_for_airline(...) returns:
  { raw, originalResponse, rows, ok }
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from modules.requester import Requester


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

API_URL = os.getenv("AMYBD_API_URL", "https://www.amybd.com/atapi.aspx")
DEFAULT_FALLBACK_TOKEN = "OqOqKXGLTLKLKGLXXTiKqnnn"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

ENV_COOKIES_PATH = "AMYBD_COOKIES_PATH"
ENV_PROXY_URL = "AMYBD_PROXY_URL"
ENV_ORIGIN = "AMYBD_ORIGIN"
ENV_REFERER = "AMYBD_REFERER"
ENV_TOKEN = "AMYBD_TOKEN"
ENV_AUTHID = "AMYBD_AUTHID"
ENV_CAUTH = "AMYBD_CAUTH"
ENV_SEARCH_CMND = "AMYBD_SEARCH_CMND"
ENV_ENABLE_PRICECOMBO = "AMYBD_ENABLE_PRICECOMBO"
ENV_PRICECOMBO_MAX_OFFERS = "AMYBD_PRICECOMBO_MAX_OFFERS"
ENV_PRICECOMBO_DISP_VALUES = "AMYBD_PRICECOMBO_DISP_VALUES"
ENV_DISABLE_DEFAULT_TOKEN = "AMYBD_DISABLE_DEFAULT_TOKEN"

AIRPORT_LABELS = {
    "DAC": "Dhaka - DAC - BANGLADESH",
    "CGP": "Chittagong - CGP - BANGLADESH",
    "CXB": "Cox's Bazar - CXB - BANGLADESH",
    "JSR": "Jessore - JSR - BANGLADESH",
    "RJH": "Rajshahi - RJH - BANGLADESH",
    "SPD": "Saidpur - SPD - BANGLADESH",
    "ZYL": "Sylhet - ZYL - BANGLADESH",
    "BZL": "Barisal - BZL - BANGLADESH",
}


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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _clip_text(v: Any, size: int = 300) -> str:
    text = str(v or "")
    if len(text) <= size:
        return text
    return text[: size - 3] + "..."


def _cabin_to_amybd_code(cabin: str) -> str:
    c = str(cabin or "").strip().lower()
    if "business" in c:
        return "C"
    return "Y"


def _cabin_from_source(raw_cabin: Any, requested: str) -> str:
    v = str(raw_cabin or "").strip().upper()
    if v == "C":
        return "Business"
    if v == "Y":
        return "Economy"
    return str(requested or "Economy")


def _airport_label(iata: str) -> str:
    code = str(iata or "").strip().upper()
    env_key = f"AMYBD_LABEL_{code}"
    override = os.getenv(env_key, "").strip()
    if override:
        return override
    if code in AIRPORT_LABELS:
        return AIRPORT_LABELS[code]
    return f"{code} - {code} - BANGLADESH"


def _date_to_amybd(d: str) -> str:
    try:
        return dt.date.fromisoformat(str(d).strip()).strftime("%d-%b-%Y")
    except Exception:
        return str(d)


def _default_headers() -> Dict[str, str]:
    origin = os.getenv(ENV_ORIGIN, "https://www.amybd.com").strip() or "https://www.amybd.com"
    referer = os.getenv(ENV_REFERER, "https://www.amybd.com/flights").strip() or "https://www.amybd.com/flights"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
    }
    authid = os.getenv(ENV_AUTHID, "").strip()
    chauth = os.getenv(ENV_CAUTH, "").strip()
    if authid:
        headers["authid"] = authid
    if chauth:
        headers["chauth"] = chauth
    return headers


def build_search_payload(
    *,
    origin: str,
    destination: str,
    date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
    cmnd: str = "_FLIGHTSEARCH_",
    include_token: Optional[bool] = None,
    token_override: Optional[str] = None,
) -> Dict[str, Any]:
    today_text = dt.date.today().strftime("%d-%b-%Y")
    payload: Dict[str, Any] = {
        "is_combo": 0,
        "CMND": cmnd,
        "TRIP": "OW",
        "FROM": _airport_label(origin),
        "DEST": _airport_label(destination),
        "JDT": _date_to_amybd(date),
        "RDT": today_text,
        "ACLASS": _cabin_to_amybd_code(cabin),
        "AD": max(1, int(adt or 1)),
        "CH": max(0, int(chd or 0)),
        "INF": max(0, int(inf or 0)),
        "Umrah": "0",
        "DOBC1": "01-Mar-2017",
        "DOBC2": "01-Mar-2017",
        "DOBC3": "01-Mar-2017",
        "DOBC4": "01-Mar-2017",
    }
    token = str(token_override if token_override is not None else os.getenv(ENV_TOKEN, "")).strip()
    should_include_token = include_token if include_token is not None else bool(token)
    if should_include_token and token:
        payload["TOKEN"] = token
    return payload


def _post_form(req: Requester, payload: Dict[str, Any], headers: Dict[str, str]) -> tuple[int, Any]:
    body_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    response = req.session.post(API_URL, data=body_text, headers=headers, timeout=req.timeout)
    try:
        body: Any = response.json()
    except Exception:
        body = response.text
    return response.status_code, body


def _build_search_attempts() -> List[str]:
    override = os.getenv(ENV_SEARCH_CMND, "").strip().upper()
    valid = {"_FLIGHTSEARCH_", "_FLIGHTSEARCHOPEN_"}
    if override in valid:
        other = "_FLIGHTSEARCHOPEN_" if override == "_FLIGHTSEARCH_" else "_FLIGHTSEARCH_"
        return [override, other]
    return ["_FLIGHTSEARCH_", "_FLIGHTSEARCHOPEN_"]


def _token_candidates() -> List[Optional[str]]:
    env_token = os.getenv(ENV_TOKEN, "").strip()
    if env_token:
        return [None, env_token]
    if _env_bool(ENV_DISABLE_DEFAULT_TOKEN, default=False):
        return [None]
    return [None, DEFAULT_FALLBACK_TOKEN]


def _extract_svdid(search_body: Any) -> Optional[str]:
    if isinstance(search_body, dict):
        svdid = str(search_body.get("svdid") or "").strip()
        if svdid:
            return svdid
    return None


def _pricecombo_urls(search_id: Any, aid: Any, disp_values: List[str]) -> List[str]:
    urls: List[str] = []
    for disp in disp_values:
        query = urlencode(
            {
                "CMND": "_PRICECOMBO_",
                "sid1": str(search_id),
                "sid2": "0",
                "aid1": str(aid),
                "aid2": "",
                "disp": str(disp),
            }
        )
        urls.append(f"{API_URL}?{query}")
    return urls


def _safe_json_or_text(response_text: Any) -> Any:
    text = str(response_text or "")
    if not text.strip():
        return ""
    try:
        return json.loads(text)
    except Exception:
        return text


def _extract_penalty_text(candidate: Any) -> Optional[str]:
    if isinstance(candidate, (dict, list)):
        text = json.dumps(candidate, ensure_ascii=False)
    else:
        text = str(candidate or "")
    if not text.strip():
        return None
    if re.search(r"(?i)(penalt|refund|cancel|change fee|no[- ]show)", text):
        return _clip_text(text, size=600)
    return None


def _normalize_trip_row(
    *,
    airline_code: str,
    trip: Dict[str, Any],
    requested_cabin: str,
    adt: int,
    chd: int,
    inf: int,
    search_id: Any,
    search_svdid: Optional[str],
    search_command: str,
) -> Dict[str, Any]:
    legs = trip.get("fLegs")
    leg0 = legs[0] if isinstance(legs, list) and legs and isinstance(legs[0], dict) else {}

    flight_number = (
        str(leg0.get("xFlight") or "").strip()
        or str(trip.get("fNo") or "").replace(str(airline_code), "").strip()
    )
    departure = trip.get("fDTime") or leg0.get("DTime")
    arrival = trip.get("fATime") or leg0.get("ATime") or departure
    origin = str(trip.get("fFrom") or leg0.get("xFrom") or "").upper().strip()
    destination = str(trip.get("fDest") or leg0.get("xDest") or "").upper().strip()

    total_amount = _safe_float(trip.get("fTFare"))
    if total_amount is None:
        total_amount = _safe_float(trip.get("fFare"))
    base_amount = _safe_float(trip.get("fTBFare"))
    if base_amount is None:
        base_amount = _safe_float(trip.get("fBFare"))
    tax_amount = _safe_float(trip.get("fCFare"))
    if tax_amount is None and total_amount is not None and base_amount is not None:
        tax_amount = max(0.0, total_amount - base_amount)

    seat_available = _safe_int(trip.get("fSeat"))
    inventory_confidence = "reported_ota" if seat_available is not None else "unknown_ota"
    soldout = bool(seat_available == 0) if seat_available is not None else False
    model = str(trip.get("fModel") or "").strip() or None
    refund_text = str(trip.get("fRefund") or "").strip()
    refundable = True if "REFUND" in refund_text.upper() else (False if refund_text else None)

    row: Dict[str, Any] = {
        "airline": str(airline_code).upper(),
        "operating_airline": str(trip.get("stAirCode") or "").upper().strip() or None,
        "flight_number": flight_number,
        "origin": origin,
        "destination": destination,
        "departure": departure,
        "arrival": arrival,
        "cabin": _cabin_from_source(trip.get("fCabin"), requested_cabin),
        "fare_basis": str(trip.get("fClsNam") or leg0.get("xClass") or ""),
        "brand": f"AMYBD_{str(trip.get('csource') or 'OTA').upper()}",
        "price_total_bdt": total_amount,
        "fare_amount": base_amount,
        "tax_amount": tax_amount,
        "currency": "BDT",
        "duration_min": _safe_int(trip.get("fDursec")) or _safe_int(leg0.get("xDur")),
        "stops": max(0, len(legs) - 1) if isinstance(legs, list) else 0,
        "booking_class": str(trip.get("fClsNam") or leg0.get("xClass") or "").strip() or None,
        "baggage": str(trip.get("fBag") or "").strip() or None,
        "equipment_code": model,
        "aircraft": model,
        "seat_capacity": None,
        "seat_available": seat_available,
        "inventory_confidence": inventory_confidence,
        "estimated_load_factor_pct": None,
        "soldout": soldout,
        "adt_count": max(1, int(adt or 1)),
        "chd_count": max(0, int(chd or 0)),
        "inf_count": max(0, int(inf or 0)),
        "fare_ref_num": str(trip.get("fAMYid") or trip.get("fGDSid") or trip.get("fSoft") or ""),
        "fare_search_reference": str(trip.get("search_id") or search_id or ""),
        "fare_search_signature": str(search_svdid or ""),
        "source_endpoint": f"atapi.aspx:{str(search_command or '').upper()}",
        "fare_refundable": refundable,
        "raw_offer": {
            "search_id": search_id,
            "svdid": search_svdid,
            "trip": trip,
        },
    }
    if refund_text:
        row["penalty_source"] = "AMYBD_REFUND_FLAG"
        row["penalty_rule_text"] = refund_text
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

    out: Dict[str, Any] = {
        "raw": {
            "source": "amybd",
            "airline": str(airline_code).upper(),
            "search_payload": None,
            "headers_hint": {
                "origin": headers.get("Origin"),
                "referer": headers.get("Referer"),
                "has_cookie_session": bool(req.session.cookies),
                "has_authid": bool(headers.get("authid")),
                "has_chauth": bool(headers.get("chauth")),
            },
            "search_attempts": [],
        },
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }

    selected_status: Optional[int] = None
    selected_body: Any = None
    selected_payload: Optional[Dict[str, Any]] = None
    selected_cmd = ""
    for cmd in _build_search_attempts():
        for token_candidate in _token_candidates():
            payload = build_search_payload(
                origin=origin,
                destination=destination,
                date=date,
                cabin=cabin,
                adt=adt,
                chd=chd,
                inf=inf,
                cmnd=cmd,
                include_token=bool(token_candidate),
                token_override=token_candidate or "",
            )
            status, body = _post_form(req, payload, headers)
            attempt = {
                "cmnd": cmd,
                "token_included": bool(token_candidate),
                "status": status,
                "is_json_dict": isinstance(body, dict),
                "success_flag": bool(body.get("success")) if isinstance(body, dict) else None,
            }
            out["raw"]["search_attempts"].append(attempt)
            selected_status = status
            selected_body = body
            selected_payload = payload
            selected_cmd = cmd
            if status == 200 and isinstance(body, dict) and bool(body.get("success", False)):
                break
        if selected_status == 200 and isinstance(selected_body, dict) and bool(selected_body.get("success", False)):
            break

    out["raw"]["search_command_used"] = selected_cmd
    out["raw"]["search_payload"] = selected_payload
    out["raw"]["search_status"] = selected_status
    out["raw"]["search_response"] = selected_body
    if selected_status != 200 or not isinstance(selected_body, dict):
        out["raw"]["error"] = "search_failed"
        return out

    out["originalResponse"] = selected_body
    out["raw"]["search_svdid"] = _extract_svdid(selected_body)
    if not bool(selected_body.get("success", False)):
        out["raw"]["error"] = "search_not_ok"
        out["raw"]["message"] = str(selected_body.get("message") or "")
        return out

    trips = selected_body.get("Trips") or []
    if not isinstance(trips, list):
        trips = []
    rows: List[Dict[str, Any]] = []
    wanted = str(airline_code or "").upper().strip()
    for trip in trips:
        if not isinstance(trip, dict):
            continue
        if str(trip.get("stAirCode") or "").upper().strip() != wanted:
            continue
        row = _normalize_trip_row(
            airline_code=wanted,
            trip=trip,
            requested_cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
            search_id=selected_body.get("SearchID"),
            search_svdid=_extract_svdid(selected_body),
            search_command=selected_cmd,
        )
        rows.append(row)

    out["rows"] = _dedupe_rows(rows)
    if _env_bool(ENV_ENABLE_PRICECOMBO, default=False):
        disp_raw = str(os.getenv(ENV_PRICECOMBO_DISP_VALUES, "1") or "1")
        disp_values = [d.strip() for d in disp_raw.split(",") if d.strip()]
        if not disp_values:
            disp_values = ["1"]
        max_offers = _safe_int(os.getenv(ENV_PRICECOMBO_MAX_OFFERS, "5")) or 5
        sid = selected_body.get("SearchID")
        out["raw"]["pricecombo_attempts"] = []
        if sid:
            for row in out["rows"][: max(1, max_offers)]:
                trip = ((row.get("raw_offer") or {}).get("trip") or {})
                aid = trip.get("fAMYid") or trip.get("fGDSid")
                if aid in (None, ""):
                    continue
                for url in _pricecombo_urls(sid, aid, disp_values):
                    try:
                        response = req.get(url, headers=headers)
                        body_pc = _safe_json_or_text(response.text)
                        penalty_text = _extract_penalty_text(body_pc)
                        entry = {
                            "url": url,
                            "status": response.status_code,
                            "aid": str(aid),
                            "has_body": bool(str(response.text or "").strip()),
                            "penalty_hint_found": bool(penalty_text),
                            "body_preview": _clip_text(body_pc, 300),
                        }
                        out["raw"]["pricecombo_attempts"].append(entry)
                        if penalty_text and not row.get("penalty_rule_text"):
                            row["penalty_source"] = "AMYBD_PRICECOMBO"
                            row["penalty_rule_text"] = penalty_text
                    except Exception as exc:
                        out["raw"]["pricecombo_attempts"].append(
                            {
                                "url": url,
                                "aid": str(aid),
                                "error": str(exc),
                            }
                        )
    out["ok"] = True
    return out


def cli_main():
    parser = argparse.ArgumentParser(description="AMYBD OTA connector tester")
    parser.add_argument("--airline", required=True, choices=["BS", "2A", "BG", "VQ"])
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
