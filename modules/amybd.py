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
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from core.source_switches import disabled_source_response, source_enabled, source_switch_status
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
ENV_HEADERS_FILE = "AMYBD_HEADERS_FILE"
ENV_SESSION_AUTO_REFRESH = "AMYBD_SESSION_AUTO_REFRESH"
ENV_SESSION_REFRESH_CMD = "AMYBD_SESSION_REFRESH_CMD"
ENV_SESSION_REFRESH_TIMEOUT_SEC = "AMYBD_SESSION_REFRESH_TIMEOUT_SEC"
ENV_SESSION_SUMMARY_FILE = "AMYBD_SESSION_SUMMARY_FILE"

DEFAULT_SESSION_SUMMARY_FILE = "output/manual_sessions/amybd_session_latest.json"
DEFAULT_COOKIES_CACHE_FILE = "output/manual_sessions/amybd_cookies.json"
DEFAULT_HEADERS_CACHE_FILE = "output/manual_sessions/amybd_headers_latest.json"

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


def _safe_str(v: Any) -> str:
    return str(v or "").strip()


def _session_summary_file() -> str:
    return _safe_str(os.getenv(ENV_SESSION_SUMMARY_FILE)) or DEFAULT_SESSION_SUMMARY_FILE


def _cookies_cache_file() -> str:
    return _safe_str(os.getenv(ENV_COOKIES_PATH)) or DEFAULT_COOKIES_CACHE_FILE


def _headers_cache_file() -> str:
    return _safe_str(os.getenv(ENV_HEADERS_FILE)) or DEFAULT_HEADERS_CACHE_FILE


def _load_json_dict(path_str: str) -> Dict[str, Any]:
    path = Path(path_str)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _load_session_summary() -> Dict[str, Any]:
    return _load_json_dict(_session_summary_file())


def _load_extra_headers_from_cache() -> Dict[str, str]:
    raw = _load_json_dict(_headers_cache_file())
    excluded = {
        "host",
        "content-length",
        "cookie",
        "connection",
        "accept-encoding",
    }
    out: Dict[str, str] = {}
    for k, v in raw.items():
        name = _safe_str(k)
        if not name or name.lower() in excluded:
            continue
        out[name] = str(v or "")
    return out


def _session_value(*keys: str) -> str:
    summary = _load_session_summary()
    for key in keys:
        value = _safe_str(summary.get(key))
        if value:
            return value
    return ""


def _resolve_initial_cookies_path(explicit: Optional[str]) -> Optional[str]:
    if explicit:
        return explicit
    env_path = _safe_str(os.getenv(ENV_COOKIES_PATH))
    if env_path:
        return env_path
    default_path = Path(DEFAULT_COOKIES_CACHE_FILE)
    if default_path.exists():
        return str(default_path)
    return None


def _has_reusable_session_material(cookies_path: Optional[str]) -> bool:
    summary = _load_session_summary()
    search_success = bool(summary.get("search_success"))
    authid = _safe_str(os.getenv(ENV_AUTHID)) or _safe_str(summary.get("authid"))
    chauth = _safe_str(os.getenv(ENV_CAUTH)) or _safe_str(summary.get("chauth"))
    token = _safe_str(os.getenv(ENV_TOKEN)) or _safe_str(summary.get("token"))
    cookies_ok = bool(cookies_path and Path(cookies_path).exists())
    return search_success and cookies_ok and bool(authid or chauth or token)


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
    summary_origin = _session_value("origin")
    summary_referer = _session_value("referer")
    origin = _safe_str(os.getenv(ENV_ORIGIN)) or summary_origin or "https://www.amybd.com"
    referer = _safe_str(os.getenv(ENV_REFERER)) or summary_referer or "https://www.amybd.com/flights"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
    }
    authid = _safe_str(os.getenv(ENV_AUTHID)) or _session_value("authid")
    chauth = _safe_str(os.getenv(ENV_CAUTH)) or _session_value("chauth")
    if authid:
        headers["authid"] = authid
    if chauth:
        headers["chauth"] = chauth
    headers.update(_load_extra_headers_from_cache())
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
    token = _safe_str(token_override if token_override is not None else os.getenv(ENV_TOKEN, "")) or _session_value("token")
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
    env_token = _safe_str(os.getenv(ENV_TOKEN)) or _session_value("token")
    if env_token:
        return [None, env_token]
    if _env_bool(ENV_DISABLE_DEFAULT_TOKEN, default=False):
        return [None]
    return [None, DEFAULT_FALLBACK_TOKEN]


def _refresh_command_context(
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
        "origin": _safe_str(origin),
        "destination": _safe_str(destination),
        "date": _safe_str(date),
        "cabin": _safe_str(cabin or "Economy"),
        "adt": int(max(1, int(adt or 1))),
        "chd": int(max(0, int(chd or 0))),
        "inf": int(max(0, int(inf or 0))),
        "session_file": _session_summary_file(),
        "cookies_file": _cookies_cache_file(),
        "headers_file": _headers_cache_file(),
        "python": sys.executable or "python",
    }


def _default_refresh_command(**ctx: Any) -> str:
    return (
        f'"{ctx["python"]}" tools/refresh_amybd_session.py '
        f'--non-interactive '
        f'--origin "{ctx["origin"]}" --destination "{ctx["destination"]}" '
        f'--date "{ctx["date"]}" --cabin "{ctx["cabin"]}" '
        f'--adt {ctx["adt"]} --chd {ctx["chd"]} --inf {ctx["inf"]} '
        f'--out "{ctx["session_file"]}" '
        f'--cookies-out "{ctx["cookies_file"]}" '
        f'--headers-out "{ctx["headers_file"]}"'
    )


def _run_refresh_command(**ctx: Any) -> Dict[str, Any]:
    command_template = _safe_str(os.getenv(ENV_SESSION_REFRESH_CMD))
    if command_template:
        try:
            command = command_template.format(**ctx)
        except Exception:
            command = command_template
    else:
        command = _default_refresh_command(**ctx)
    timeout_sec = _safe_float(os.getenv(ENV_SESSION_REFRESH_TIMEOUT_SEC))
    timeout_sec = float(timeout_sec if timeout_sec is not None else 180.0)
    timeout_sec = max(30.0, timeout_sec)
    LOG.info("Running AMYBD session refresh command")
    try:
        proc = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        return {
            "ok": proc.returncode == 0,
            "command": command,
            "returncode": proc.returncode,
            "context": ctx,
            "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-10:]),
            "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-10:]),
            "timeout_sec": timeout_sec,
        }
    except Exception as exc:
        return {
            "ok": False,
            "command": command,
            "context": ctx,
            "error": str(exc),
            "timeout_sec": timeout_sec,
        }


def _missing_session_material() -> bool:
    return not any(
        (
            _safe_str(os.getenv(ENV_AUTHID)),
            _safe_str(os.getenv(ENV_CAUTH)),
            _safe_str(os.getenv(ENV_TOKEN)),
            _session_value("authid"),
            _session_value("chauth"),
            _session_value("token"),
        )
    )


def _body_indicates_invalid_session(body: Any) -> bool:
    text = ""
    if isinstance(body, dict):
        text = " ".join(
            str(body.get(k) or "")
            for k in ("message", "error", "detail", "status")
        ).lower()
    else:
        text = str(body or "").lower()
    return any(
        needle in text
        for needle in (
            "invalid login",
            "login",
            "auth",
            "unauthor",
            "session expired",
            "forbidden",
        )
    )


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
    via_airports = list(
        dict.fromkeys(
            str(leg.get("xDest") or "").upper().strip()
            for leg in (legs[:-1] if isinstance(legs, list) else [])
            if str(leg.get("xDest") or "").upper().strip()
            and str(leg.get("xDest") or "").upper().strip() not in {origin, destination}
        )
    )

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
        "via_airports": "|".join(via_airports) if via_airports else None,
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
    if not source_enabled("amybd"):
        return disabled_source_response("amybd")

    cookies = _resolve_initial_cookies_path(cookies_path)
    proxy = proxy_url or os.getenv(ENV_PROXY_URL) or None
    auto_refresh_session = _env_bool(ENV_SESSION_AUTO_REFRESH, default=False)
    refresh_ctx = _refresh_command_context(
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    if auto_refresh_session and not _has_reusable_session_material(cookies):
        pre_refresh = _run_refresh_command(**refresh_ctx)
        cookies = _resolve_initial_cookies_path(cookies) or _cookies_cache_file()
    else:
        pre_refresh = None
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
                "session_summary_file": _session_summary_file(),
                "headers_cache_file": _headers_cache_file(),
                "cookies_cache_file": _cookies_cache_file(),
                "session_auto_refresh": auto_refresh_session,
            },
            "search_attempts": [],
        },
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }
    if pre_refresh is not None:
        out["raw"]["pre_search_session_refresh"] = pre_refresh

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

    should_retry_refresh = (
        auto_refresh_session
        and (
            selected_status in {401, 403}
            or _body_indicates_invalid_session(selected_body)
        )
    )
    if should_retry_refresh:
        refresh_meta = _run_refresh_command(**refresh_ctx)
        out["raw"]["session_refresh"] = refresh_meta
        retry_cookies = _cookies_cache_file()
        retry_req = Requester(cookies_path=retry_cookies, user_agent=USER_AGENT, proxy_url=proxy)
        retry_headers = _default_headers()
        retry_status: Optional[int] = None
        retry_body: Any = None
        retry_payload: Optional[Dict[str, Any]] = None
        retry_cmd = selected_cmd
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
                status, body = _post_form(retry_req, payload, retry_headers)
                out["raw"]["search_attempts"].append(
                    {
                        "cmnd": cmd,
                        "token_included": bool(token_candidate),
                        "status": status,
                        "is_json_dict": isinstance(body, dict),
                        "success_flag": bool(body.get("success")) if isinstance(body, dict) else None,
                        "after_session_refresh": True,
                    }
                )
                retry_status = status
                retry_body = body
                retry_payload = payload
                retry_cmd = cmd
                if status == 200 and isinstance(body, dict) and bool(body.get("success", False)):
                    break
            if retry_status == 200 and isinstance(retry_body, dict) and bool(retry_body.get("success", False)):
                break
        selected_status = retry_status
        selected_body = retry_body
        selected_payload = retry_payload
        selected_cmd = retry_cmd
        headers = retry_headers
        req = retry_req
        out["raw"]["search_command_used"] = selected_cmd
        out["raw"]["search_payload"] = selected_payload
        out["raw"]["search_status"] = selected_status
        out["raw"]["search_response"] = selected_body
        out["raw"]["search_retry_attempted"] = True

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


def check_source_health(*, dry_run: bool = True, **_: Any) -> Dict[str, Any]:
    from core.source_health import ok, warn

    status = source_switch_status("amybd")
    if not status.get("enabled"):
        return warn(
            "amybd",
            message="; ".join(status.get("reasons") or []) or "AMYBD connector is disabled",
            blocking=False,
            configured_enabled=False,
        )

    missing = _missing_session_material()
    payload = {
        "source": "amybd",
        "session_summary_file": _session_summary_file(),
        "cookies_cache_file": DEFAULT_COOKIES_CACHE_FILE,
        "manual_action_required": missing,
    }
    if missing:
        return warn(
            "amybd",
            message="AMYBD session/token material is missing; refresh manual session before relying on this source",
            blocking=False,
            **payload,
        )
    return ok(
        "amybd",
        message="session material present; validity is measured per extraction attempt",
        **payload,
    )


def check_session(*, dry_run: bool = True, **kwargs: Any) -> Dict[str, Any]:
    return check_source_health(dry_run=dry_run, **kwargs)


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
