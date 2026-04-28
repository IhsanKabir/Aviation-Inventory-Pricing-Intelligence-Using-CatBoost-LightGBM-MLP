from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from modules.requester import Requester, RequesterError
    from modules.sharetrip import fetch_flights_for_airline as fetch_from_sharetrip
except ModuleNotFoundError:
    # Allow direct script execution (`python modules\airasia.py ...`) from repo root.
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from modules.requester import Requester, RequesterError
    from modules.sharetrip import fetch_flights_for_airline as fetch_from_sharetrip

from core.source_switches import disabled_source_response, source_enabled

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

AIRLINE_CODE = "AK"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
SEARCH_URL = "https://flights.airasia.com/web/fp/search/flights/v5/aggregated-results"
REFRESH_URL = "https://ssor.airasia.com/sso/v2/authorization/by-refresh-token?clientId=PRD-AAWWW-5NS5DMQ6"

ENV_SOURCE_MODE = "AIRASIA_SOURCE_MODE"
ENV_SESSION_FILE = "AIRASIA_SESSION_FILE"
ENV_CAPTURE_ROOT = "AIRASIA_CAPTURE_ROOT"
ENV_COOKIES_PATH = "AIRASIA_COOKIES_PATH"
ENV_PROXY_URL = "AIRASIA_PROXY_URL"
ENV_CURRENCY = "AIRASIA_CURRENCY"
ENV_LOCALE = "AIRASIA_LOCALE"
ENV_GEO_ID = "AIRASIA_GEO_ID"
ENV_TIMEOUT_SEC = "AIRASIA_TIMEOUT_SEC"

DEFAULT_SESSION_FILE = "output/manual_sessions/airasia_session_latest.json"
DEFAULT_CAPTURE_ROOT = "output/manual_sessions"


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except Exception:
        try:
            s = str(value).strip()
            return int(float(s)) if s else None
        except Exception:
            return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        try:
            s = str(value).replace(",", "").strip()
            return float(s) if s else None
        except Exception:
            return None


def _clip_text(value: Any, size: int = 260) -> str:
    text = str(value or "")
    return text if len(text) <= size else text[: size - 3] + "..."


def _safe_json_loads(value: Any) -> Any:
    if not isinstance(value, str):
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def _parse_iso(value: Any) -> Optional[dt.datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ):
        try:
            return dt.datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _normalize_dt_text(value: Any) -> Optional[str]:
    s = str(value or "").strip()
    if not s:
        return None
    parsed = _parse_iso(s)
    if parsed is None:
        return s
    return parsed.isoformat() if parsed.tzinfo else parsed.strftime("%Y-%m-%dT%H:%M:%S")


def _format_airasia_date(iso_date: str) -> str:
    return dt.date.fromisoformat(str(iso_date).strip()).strftime("%d/%m/%Y")


def _normalize_cabin_name(cabin: str) -> str:
    value = str(cabin or "Economy").strip().lower()
    if value in {"business", "premium flatbed", "premium_flatbed", "flatbed"}:
        return "premiumFlatbed"
    if value in {"premium economy", "premium_economy"}:
        return "premiumEconomy"
    return "economy"


def _fare_class_to_cabin(category: Any, requested_cabin: str) -> str:
    code = str(category or "").strip().upper()
    if code in {"PM", "PF", "BUSINESS", "BUS", "FLATBED"}:
        return "Business"
    if code in {"PE", "PY", "PREMIUM", "PREMIUMECONOMY"}:
        return "Premium Economy"
    requested = str(requested_cabin or "Economy").strip()
    return requested.title() if requested else "Economy"


def _normalize_flight_number(value: Any, carrier_code: Any = None) -> str:
    text = str(value or "").strip().upper()
    carrier = str(carrier_code or "").strip().upper()
    if carrier and text.startswith(carrier):
        text = text[len(carrier) :].strip()
    return text


def _total_guest_count(adt: int, chd: int, inf: int) -> int:
    return max(1, int(adt or 0)) + max(0, int(chd or 0)) + max(0, int(inf or 0))


def _session_file_path() -> Path:
    return Path(os.getenv(ENV_SESSION_FILE, DEFAULT_SESSION_FILE) or DEFAULT_SESSION_FILE)


def _capture_root_path() -> Path:
    return Path(os.getenv(ENV_CAPTURE_ROOT, DEFAULT_CAPTURE_ROOT) or DEFAULT_CAPTURE_ROOT)


def _load_json_file(path: str | Path | None) -> Any:
    if not path:
        return None
    file_path = Path(path)
    if not file_path.exists():
        return None
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _extract_search_query_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    flight_journey = payload.get("searchContext", {}).get("flightJourney", {})
    details = flight_journey.get("journeyDetails") or []
    detail0 = details[0] if isinstance(details, list) and details and isinstance(details[0], dict) else {}
    passengers = flight_journey.get("passengers") or {}
    depart_raw = str(detail0.get("departDate") or "").strip()
    depart_date = depart_raw
    if "/" in depart_raw:
        try:
            depart_date = dt.datetime.strptime(depart_raw, "%d/%m/%Y").date().isoformat()
        except Exception:
            pass
    return {
        "origin": str(detail0.get("origin") or "").upper().strip() or None,
        "destination": str(detail0.get("destination") or "").upper().strip() or None,
        "date": depart_date or None,
        "cabin": payload.get("searchContext", {}).get("cabinClass"),
        "adt": _safe_int(passengers.get("adult")),
        "chd": _safe_int(passengers.get("child")),
        "inf": _safe_int(passengers.get("infant")),
    }


def _summary_query(summary: Dict[str, Any]) -> Dict[str, Any]:
    explicit = summary.get("search_query")
    if isinstance(explicit, dict) and explicit:
        return {
            "origin": str(explicit.get("origin") or "").upper().strip() or None,
            "destination": str(explicit.get("destination") or "").upper().strip() or None,
            "date": str(explicit.get("date") or "").strip() or None,
            "cabin": explicit.get("cabin"),
            "adt": _safe_int(explicit.get("adt")),
            "chd": _safe_int(explicit.get("chd")),
            "inf": _safe_int(explicit.get("inf")),
        }
    request_body = (summary.get("search_request") or {}).get("request_body_json")
    return _extract_search_query_from_payload(request_body) if isinstance(request_body, dict) else {}


def _summary_matches_query(summary: Dict[str, Any], origin: str, destination: str, date: str) -> bool:
    query = _summary_query(summary)
    return (
        str(query.get("origin") or "").upper().strip() == str(origin).upper().strip()
        and str(query.get("destination") or "").upper().strip() == str(destination).upper().strip()
        and str(query.get("date") or "").strip() == str(date).strip()
    )


def _candidate_capture_files(origin: str, destination: str, date: str) -> List[Path]:
    out: List[Path] = []
    session_file = _session_file_path()
    if session_file.exists():
        out.append(session_file)
    runs_dir = _capture_root_path() / "runs"
    if runs_dir.exists():
        pattern = f"ak_{str(origin).upper()}_{str(destination).upper()}_{date}_*"
        for run_dir in sorted(runs_dir.glob(pattern), reverse=True):
            for filename in ("airasia_capture_summary.json", "airasia_session_latest.json"):
                candidate = run_dir / filename
                if candidate.exists():
                    out.append(candidate)
    deduped: List[Path] = []
    seen: set[str] = set()
    for path in out:
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _sort_key_for_summary(path: Path, summary: Dict[str, Any]) -> Tuple[float, str]:
    captured = str(summary.get("captured_at_utc") or "").strip()
    parsed = _parse_iso(captured) if captured else None
    if parsed is not None:
        return (parsed.timestamp(), str(path))
    try:
        return (path.stat().st_mtime, str(path))
    except Exception:
        return (0.0, str(path))


def _find_exact_capture(origin: str, destination: str, date: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    matches: List[Tuple[Path, Dict[str, Any]]] = []
    for path in _candidate_capture_files(origin, destination, date):
        summary = _load_json_file(path)
        response = (summary or {}).get("search_response") if isinstance(summary, dict) else None
        if not isinstance(summary, dict) or not _summary_matches_query(summary, origin, destination, date):
            continue
        if not isinstance(response, dict):
            continue
        if response.get("response_body_json") is None and not response.get("response_body_text"):
            continue
        matches.append((path, summary))
    if not matches:
        return None, None
    matches.sort(key=lambda item: _sort_key_for_summary(item[0], item[1]), reverse=True)
    return matches[0]


def _load_latest_session_summary() -> Optional[Dict[str, Any]]:
    summary = _load_json_file(_session_file_path())
    return summary if isinstance(summary, dict) else None


def _response_payload_from_summary(summary: Dict[str, Any]) -> Any:
    response = summary.get("search_response") or {}
    if isinstance(response.get("response_body_json"), (dict, list)):
        return response.get("response_body_json")
    return _safe_json_loads(response.get("response_body_text"))


def _default_search_payload(*, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int) -> Dict[str, Any]:
    return {
        "searchContext": {
            "flightJourney": {
                "journeyType": "O",
                "journeyDetails": [{
                    "departDate": _format_airasia_date(date),
                    "returnDate": "",
                    "origin": str(origin).upper().strip(),
                    "destination": str(destination).upper().strip(),
                    "isOriginCity": True,
                    "isDestinationCity": True,
                }],
                "passengers": {"adult": max(1, int(adt or 1)), "child": max(0, int(chd or 0)), "infant": max(0, int(inf or 0))},
            },
            "promocode": None,
            "cabinClass": _normalize_cabin_name(cabin),
        },
        "userContext": {
            "currency": str(os.getenv(ENV_CURRENCY, "BDT")).strip() or "BDT",
            "geoId": str(os.getenv(ENV_GEO_ID, "BD")).strip() or "BD",
            "locale": str(os.getenv(ENV_LOCALE, "en-gb")).strip() or "en-gb",
            "platform": "web",
        },
        "inventoryContext": {"airlineProfile": "all", "type": "paired", "provider": "AAB"},
    }


def _build_payload_from_template(template: Optional[Dict[str, Any]], *, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int, refreshed_tokens: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    payload = copy.deepcopy(template) if isinstance(template, dict) else _default_search_payload(origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
    search_context = payload.setdefault("searchContext", {})
    flight_journey = search_context.setdefault("flightJourney", {})
    details = flight_journey.get("journeyDetails")
    if not isinstance(details, list) or not details:
        details = [{}]
        flight_journey["journeyDetails"] = details
    detail0 = details[0] if isinstance(details[0], dict) else {}
    detail0.update({
        "departDate": _format_airasia_date(date),
        "returnDate": "",
        "origin": str(origin).upper().strip(),
        "destination": str(destination).upper().strip(),
        "isOriginCity": bool(detail0.get("isOriginCity", True)),
        "isDestinationCity": bool(detail0.get("isDestinationCity", True)),
    })
    details[0] = detail0
    passengers = flight_journey.setdefault("passengers", {})
    passengers.update({"adult": max(1, int(adt or 1)), "child": max(0, int(chd or 0)), "infant": max(0, int(inf or 0))})
    flight_journey["journeyType"] = str(flight_journey.get("journeyType") or "O")
    search_context["cabinClass"] = _normalize_cabin_name(cabin)
    search_context.setdefault("promocode", None)
    user_context = payload.setdefault("userContext", {})
    user_context.setdefault("currency", str(os.getenv(ENV_CURRENCY, "BDT")).strip() or "BDT")
    user_context.setdefault("geoId", str(os.getenv(ENV_GEO_ID, "BD")).strip() or "BD")
    user_context.setdefault("locale", str(os.getenv(ENV_LOCALE, "en-gb")).strip() or "en-gb")
    user_context.setdefault("platform", "web")
    if refreshed_tokens:
        sso_details = user_context.setdefault("ssoDetails", {})
        if refreshed_tokens.get("accessToken"):
            sso_details["accessToken"] = refreshed_tokens["accessToken"]
        if refreshed_tokens.get("refreshToken"):
            sso_details["refreshToken"] = refreshed_tokens["refreshToken"]
    return payload


def _clean_headers(headers: Dict[str, Any] | None) -> Dict[str, str]:
    excluded = {"host", "content-length", "cookie", "accept-encoding", "connection", "content-encoding", "transfer-encoding"}
    out: Dict[str, str] = {}
    for raw_name, raw_value in (headers or {}).items():
        name = str(raw_name or "").strip()
        if not name or name.startswith(":") or name.lower() in excluded:
            continue
        out[name] = str(raw_value or "")
    return out


def _build_default_headers(referer: str | None = None) -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://www.airasia.com",
        "Referer": referer or "https://www.airasia.com/",
        "User-Agent": USER_AGENT,
        "x-platform-id": "WEB",
        "x-ui-action": "listing_search",
    }


def _first_string_by_keys(node: Any, keys: Iterable[str]) -> Optional[str]:
    wanted = {str(k) for k in keys}
    if isinstance(node, dict):
        for key, value in node.items():
            if key in wanted and isinstance(value, str) and value.strip():
                return value.strip()
            found = _first_string_by_keys(value, wanted)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _first_string_by_keys(item, wanted)
            if found:
                return found
    return None


def _timeout_seconds() -> float:
    return max(10.0, float(_safe_float(os.getenv(ENV_TIMEOUT_SEC)) or 45.0))


def _refresh_tokens_from_session(session_summary: Dict[str, Any], *, cookies_path: Optional[str], proxy_url: Optional[str]) -> Tuple[Optional[Dict[str, str]], Dict[str, Any]]:
    refresh_request = session_summary.get("refresh_request") or {}
    refresh_url = str(refresh_request.get("request_url") or REFRESH_URL).strip() or REFRESH_URL
    refresh_headers = _build_default_headers()
    refresh_headers.update(_clean_headers(refresh_request.get("request_headers") or {}))
    template_payload = (session_summary.get("search_request") or {}).get("request_body_json")
    refresh_token = _first_string_by_keys(template_payload, {"refreshToken"}) or _first_string_by_keys(refresh_request.get("request_body_json"), {"refreshToken"})
    if not refresh_token:
        return None, {"status": None, "error": "missing_refresh_token"}
    req = Requester(cookies_path=Path(cookies_path) if cookies_path else None, user_agent=refresh_headers.get("User-Agent") or USER_AGENT, timeout=int(_timeout_seconds()), proxy_url=proxy_url)
    try:
        resp = req.post_raw(refresh_url, json_payload={"refreshToken": refresh_token}, headers=refresh_headers)
    except Exception as exc:
        return None, {"status": None, "error": "refresh_request_failed", "detail": str(exc)}
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    meta = {"status": getattr(resp, "status_code", None), "ok": bool(getattr(resp, "ok", False)), "body_preview": _clip_text(body, 320)}
    if not getattr(resp, "ok", False):
        return None, meta
    access_token = _first_string_by_keys(body, {"accessToken"})
    next_refresh_token = _first_string_by_keys(body, {"refreshToken"}) or refresh_token
    if not access_token:
        meta["error"] = "refresh_missing_access_token"
        return None, meta
    return {"accessToken": access_token, "refreshToken": next_refresh_token}, meta


def _extract_search_results_root(payload: Dict[str, Any]) -> Dict[str, Any]:
    for candidate in (
        payload.get("searchResults"),
        (payload.get("data") or {}).get("searchResults") if isinstance(payload.get("data"), dict) else None,
        (payload.get("content") or {}).get("searchResults") if isinstance(payload.get("content"), dict) else None,
    ):
        if isinstance(candidate, dict):
            return candidate
    return {}


def _flight_designator(flight: Dict[str, Any]) -> Dict[str, Any]:
    details = flight.get("flightDetails")
    if isinstance(details, dict) and isinstance(details.get("designator"), dict):
        return details.get("designator") or {}
    return flight.get("designator") if isinstance(flight.get("designator"), dict) else {}


def _flight_segments(flight: Dict[str, Any]) -> List[Dict[str, Any]]:
    details = flight.get("flightDetails")
    if isinstance(details, dict) and isinstance(details.get("segments"), list):
        return [seg for seg in details.get("segments") or [] if isinstance(seg, dict)]
    return [seg for seg in flight.get("segments") or [] if isinstance(seg, dict)]


def _iter_segment_legs(segments: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for segment in segments:
        legs = segment.get("legs")
        if isinstance(legs, list) and legs:
            out.extend((segment, leg) for leg in legs if isinstance(leg, dict))
        else:
            out.append((segment, segment))
    return out


def _journey_duration_min(segments: List[Dict[str, Any]], departure: Optional[str], arrival: Optional[str]) -> Optional[int]:
    dep_dt, arr_dt = _parse_iso(departure), _parse_iso(arrival)
    if dep_dt is not None and arr_dt is not None:
        return max(0, int((arr_dt - dep_dt).total_seconds() // 60))
    total = sum(_safe_int(segment.get("duration")) or 0 for segment in segments)
    return total or None


def _bundle_candidates(flight: Dict[str, Any], segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for candidate in (flight.get("bundleList"), flight.get("ancillarylist")):
        if isinstance(candidate, list):
            out.extend([item for item in candidate if isinstance(item, dict)])
    for segment in segments:
        for candidate in (segment.get("bundleList"), segment.get("ancillarylist")):
            if isinstance(candidate, list):
                out.extend([item for item in candidate if isinstance(item, dict)])
    return out


def _bundle_total(bundle: Dict[str, Any], guest_count: int, base_total: Optional[float]) -> Optional[float]:
    details = bundle.get("bundlePriceDetails") or {}
    for key in ("totalFareWithBundleAllPax", "basetotalFareWithBundleAllPax"):
        value = _safe_float(details.get(key))
        if value is not None:
            return value
    for key in ("totalFareWithBundlePerPax", "basetotalFareWithBundlePerPax"):
        value = _safe_float(details.get(key))
        if value is not None:
            return value * max(1, int(guest_count or 1))
    extra_total, tax_total = _safe_float(bundle.get("totalPrice")), _safe_float(bundle.get("taxTotal"))
    if extra_total is None and tax_total is None:
        return None
    bundle_add_on = (extra_total or 0.0) + (tax_total or 0.0)
    return bundle_add_on if base_total is None else base_total + bundle_add_on


def _base_currency(payload: Dict[str, Any], flight: Dict[str, Any]) -> str:
    return (
        str(flight.get("userCurrencyCode") or "").strip()
        or str(flight.get("currencyCode") or "").strip()
        or str(payload.get("currency") or "").strip()
        or str((payload.get("userContext") or {}).get("currency") or "").strip()
        or "BDT"
    )


def _base_total(flight: Dict[str, Any]) -> Optional[float]:
    return _safe_float(flight.get("convertedPrice")) or _safe_float(flight.get("price")) or _safe_float(flight.get("priceAmount"))


def _search_id(payload: Dict[str, Any], search_results: Dict[str, Any]) -> Optional[str]:
    for candidate in (search_results.get("searchId"), payload.get("searchId"), (payload.get("content") or {}).get("searchId") if isinstance(payload.get("content"), dict) else None):
        if candidate not in (None, ""):
            return str(candidate)
    return None


def _normalize_flight_rows(*, payload: Dict[str, Any], search_results: Dict[str, Any], flight: Dict[str, Any], requested_cabin: str, adt: int, chd: int, inf: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    designator, segments = _flight_designator(flight), _flight_segments(flight)
    segment_legs = _iter_segment_legs(segments)
    if not segments or not segment_legs:
        return rows
    first_segment, first_leg = segment_legs[0]
    _, last_leg = segment_legs[-1]
    origin = str(designator.get("departureStation") or first_leg.get("designator", {}).get("departureStation") or "").upper().strip() or None
    destination = str(designator.get("arrivalStation") or last_leg.get("designator", {}).get("arrivalStation") or "").upper().strip() or None
    departure = _normalize_dt_text(designator.get("departureTime") or first_leg.get("designator", {}).get("departureTime"))
    arrival = _normalize_dt_text(designator.get("arrivalTime") or last_leg.get("designator", {}).get("arrivalTime"))
    carrier_code = str(first_segment.get("marketingCarrierCode") or first_leg.get("carrier", {}).get("code") or AIRLINE_CODE).upper().strip()
    flight_number = _normalize_flight_number(first_segment.get("marketingFlightNo") or first_leg.get("carrier", {}).get("flightNumber"), carrier_code=carrier_code)
    if not (origin and destination and departure and flight_number):
        return rows
    fare_class = str(first_segment.get("fareClassCategory") or first_leg.get("carrier", {}).get("cabinClass") or "").strip() or None
    guest_count = _total_guest_count(adt, chd, inf)
    base_total = _base_total(flight)
    currency = _base_currency(payload, flight)
    trip_id = str(flight.get("tripId") or flight.get("id") or "").strip()
    via_airports: List[str] = []
    for _, leg in segment_legs[:-1]:
        station = str((leg.get("designator") or {}).get("arrivalStation") or "").upper().strip()
        if station and station not in {origin, destination} and station not in via_airports:
            via_airports.append(station)
    base_row: Dict[str, Any] = {
        "airline": AIRLINE_CODE,
        "operating_airline": str(first_segment.get("operatingCarrierCode") or carrier_code).upper().strip() or None,
        "flight_number": flight_number,
        "origin": origin,
        "destination": destination,
        "departure": departure,
        "arrival": arrival,
        "cabin": _fare_class_to_cabin(fare_class, requested_cabin),
        "fare_basis": fare_class,
        "brand": str(first_segment.get("fareType") or flight.get("fareType") or "AIRASIA_DIRECT").strip() or "AIRASIA_DIRECT",
        "price_total_bdt": base_total,
        "fare_amount": base_total,
        "tax_amount": None,
        "currency": currency,
        "duration_min": _journey_duration_min(segments, departure, arrival),
        "stops": max(0, len(segment_legs) - 1),
        "via_airports": "|".join(via_airports) if via_airports else None,
        "booking_class": fare_class,
        "baggage": None,
        "equipment_code": None,
        "aircraft": None,
        "seat_capacity": None,
        "seat_available": None,
        "inventory_confidence": "unknown",
        "estimated_load_factor_pct": None,
        "soldout": bool(flight.get("isSoldOut")) if flight.get("isSoldOut") is not None else False,
        "adt_count": max(1, int(adt or 1)),
        "chd_count": max(0, int(chd or 0)),
        "inf_count": max(0, int(inf or 0)),
        "fare_ref_num": trip_id or None,
        "fare_search_reference": _search_id(payload, search_results),
        "fare_search_signature": trip_id or None,
        "source_endpoint": "airasia:aggregated-results",
        "fare_refundable": None,
        "raw_offer": {"flight": flight, "segments": segments},
    }
    rows.append(base_row)
    for bundle in _bundle_candidates(flight, segments):
        bundle_code = str(bundle.get("bundleCode") or bundle.get("type") or bundle.get("bundleId") or "").strip()
        bundle_total = _bundle_total(bundle, guest_count=guest_count, base_total=base_total)
        if not bundle_code and bundle_total is None:
            continue
        row = copy.deepcopy(base_row)
        row["brand"] = bundle_code or f"{base_row['brand']}_BUNDLE"
        row["price_total_bdt"] = bundle_total if bundle_total is not None else row.get("price_total_bdt")
        row["fare_amount"] = row.get("price_total_bdt")
        row["fare_ref_num"] = "|".join(part for part in [trip_id, bundle_code or str(bundle.get("bundleId") or "").strip()] if part) or row.get("fare_ref_num")
        row["raw_offer"] = {"flight": flight, "segments": segments, "bundle": bundle}
        rows.append(row)
    return rows


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[tuple] = set()
    for row in rows:
        key = (row.get("airline"), row.get("origin"), row.get("destination"), row.get("departure"), row.get("flight_number"), row.get("cabin"), row.get("fare_basis"), row.get("brand"), row.get("fare_ref_num"))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def parse_aggregated_results(payload: Any, *, requested_cabin: str = "Economy", adt: int = 1, chd: int = 0, inf: int = 0) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    search_results = _extract_search_results_root(payload)
    if not search_results:
        return []
    rows: List[Dict[str, Any]] = []
    for bucket_name in ("trips", "recommendedFlights"):
        buckets = search_results.get(bucket_name) or []
        if not isinstance(buckets, list):
            continue
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            flights = bucket.get("flightsList") or []
            if not isinstance(flights, list):
                continue
            for flight in flights:
                if isinstance(flight, dict):
                    rows.extend(_normalize_flight_rows(payload=payload, search_results=search_results, flight=flight, requested_cabin=requested_cabin, adt=adt, chd=chd, inf=inf))
    return _dedupe_rows(rows)


def _direct_search_hint(origin: str, destination: str, date: str) -> str:
    return (
        "Run a fresh AirAsia browser capture first, for example: "
        f"python tools/refresh_airasia_session.py --origin {str(origin).upper()} "
        f"--destination {str(destination).upper()} --date {date}"
    )


def _fetch_from_capture(*, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int) -> Dict[str, Any]:
    capture_path, summary = _find_exact_capture(origin, destination, date)
    out: Dict[str, Any] = {"raw": {"source": "airasia_capture", "airline": AIRLINE_CODE, "capture_file": str(capture_path) if capture_path else None}, "originalResponse": None, "rows": [], "ok": False}
    if capture_path is None or not isinstance(summary, dict):
        out["raw"]["error"] = "capture_not_found"
        out["raw"]["hint"] = _direct_search_hint(origin, destination, date)
        return out
    from core.source_health import capture_is_stale, max_capture_age_hours

    staleness = capture_is_stale(
        generated_at=summary.get("captured_at_utc") or summary.get("generated_at_utc"),
        path=capture_path,
        max_age_hours=max_capture_age_hours("AIRASIA_MAX_CAPTURE_AGE_HOURS"),
    )
    out["raw"].update(
        {
            "capture_age_hours": staleness.get("age_hours"),
            "max_capture_age_hours": staleness.get("max_age_hours"),
        }
    )
    if staleness.get("stale"):
        out["raw"]["error"] = "stale_capture"
        out["raw"]["hint"] = _direct_search_hint(origin, destination, date)
        return out
    payload = _response_payload_from_summary(summary)
    out["raw"]["captured_at_utc"] = summary.get("captured_at_utc")
    out["originalResponse"] = payload
    out["rows"] = parse_aggregated_results(payload, requested_cabin=cabin, adt=adt, chd=chd, inf=inf)
    out["ok"] = payload is not None
    if payload is None:
        out["raw"]["error"] = "capture_missing_response_body"
    return out


def _replay_latest_session(*, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int, cookies_path: Optional[str] = None, proxy_url: Optional[str] = None) -> Dict[str, Any]:
    summary = _load_latest_session_summary()
    out: Dict[str, Any] = {"raw": {"source": "airasia_direct", "airline": AIRLINE_CODE, "session_file": str(_session_file_path())}, "originalResponse": None, "rows": [], "ok": False}
    if not isinstance(summary, dict):
        out["raw"]["error"] = "session_not_found"
        out["raw"]["hint"] = _direct_search_hint(origin, destination, date)
        return out
    search_request = summary.get("search_request") or {}
    request_headers = _build_default_headers(referer=summary.get("search_page_url"))
    request_headers.update(_clean_headers(search_request.get("request_headers") or {}))
    request_url = str(search_request.get("request_url") or SEARCH_URL).strip() or SEARCH_URL
    payload_template = search_request.get("request_body_json")
    cookies_file = cookies_path or os.getenv(ENV_COOKIES_PATH) or summary.get("cookies_out")
    proxy = proxy_url or os.getenv(ENV_PROXY_URL) or None
    refreshed_tokens, refresh_meta = (None, None)
    if _first_string_by_keys(payload_template, {"refreshToken"}):
        refreshed_tokens, refresh_meta = _refresh_tokens_from_session(summary, cookies_path=cookies_file, proxy_url=proxy)
        out["raw"]["refresh"] = refresh_meta
    payload = _build_payload_from_template(payload_template if isinstance(payload_template, dict) else None, origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf, refreshed_tokens=refreshed_tokens)
    out["raw"]["search_payload"] = payload
    out["raw"]["headers_hint"] = {
        "has_cookie_session": bool(cookies_file),
        "has_x_api_key": bool(request_headers.get("x-api-key") or request_headers.get("X-API-Key")),
        "has_channel_hash": bool(request_headers.get("channel_hash")),
        "has_sso_id": bool(request_headers.get("sso-id")),
    }
    req = Requester(cookies_path=Path(cookies_file) if cookies_file else None, user_agent=request_headers.get("User-Agent") or request_headers.get("user-agent") or USER_AGENT, timeout=int(_timeout_seconds()), proxy_url=proxy)
    try:
        resp = req.post_raw(request_url, json_payload=payload, headers=request_headers)
    except RequesterError as exc:
        out["raw"]["error"] = "request_failed"
        out["raw"]["detail"] = str(exc)
        return out
    except Exception as exc:
        out["raw"]["error"] = "request_exception"
        out["raw"]["detail"] = str(exc)
        return out
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    out["raw"]["status"] = resp.status_code
    out["raw"]["request_url"] = request_url
    out["raw"]["response_preview"] = _clip_text(body, 360)
    out["originalResponse"] = body
    if not resp.ok:
        out["raw"]["error"] = "direct_request_failed"
        if resp.status_code in {401, 403, 429}:
            out["raw"]["hint"] = _direct_search_hint(origin, destination, date)
        return out
    out["rows"] = parse_aggregated_results(body, requested_cabin=cabin, adt=adt, chd=chd, inf=inf)
    out["ok"] = True
    return out


def fetch_direct(*, origin: str, destination: str, date: str, cabin: str = "Economy", adt: int = 1, chd: int = 0, inf: int = 0, replay_enabled: bool = True, capture_enabled: bool = True, cookies_path: Optional[str] = None, proxy_url: Optional[str] = None) -> Dict[str, Any]:
    replay_result: Optional[Dict[str, Any]] = None
    if replay_enabled:
        replay_result = _replay_latest_session(origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf, cookies_path=cookies_path, proxy_url=proxy_url)
        if replay_result.get("ok"):
            return replay_result
    if capture_enabled:
        capture_result = _fetch_from_capture(origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
        if capture_result.get("ok"):
            return capture_result
        if replay_result is None:
            return capture_result
    return replay_result or {"raw": {"source": "airasia_direct", "error": "no_direct_path_attempted", "hint": _direct_search_hint(origin, destination, date)}, "originalResponse": None, "rows": [], "ok": False}


def fetch_flights(origin: str, destination: str, date: str, cabin: str = "Economy", adt: int = 1, chd: int = 0, inf: int = 0, airline_code: Optional[str] = None):
    if not source_enabled("airasia"):
        return disabled_source_response("airasia")

    requested_code = str(airline_code or AIRLINE_CODE).upper().strip()
    if requested_code != AIRLINE_CODE:
        return {"raw": {"source": "airasia_direct", "error": "unsupported_airline_code", "hint": f"modules.airasia only supports airline_code={AIRLINE_CODE}."}, "originalResponse": None, "rows": [], "ok": False}
    mode = str(os.getenv(ENV_SOURCE_MODE, "auto") or "auto").strip().lower()
    cookies_path, proxy_url = os.getenv(ENV_COOKIES_PATH) or None, os.getenv(ENV_PROXY_URL) or None
    if mode == "sharetrip":
        return fetch_from_sharetrip(airline_code=AIRLINE_CODE, origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
    if mode == "capture":
        return _fetch_from_capture(origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
    direct = fetch_direct(origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf, replay_enabled=True, capture_enabled=True, cookies_path=cookies_path, proxy_url=proxy_url)
    if mode == "direct" or bool(direct.get("ok")):
        return direct
    fallback = fetch_from_sharetrip(airline_code=AIRLINE_CODE, origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
    return {"raw": {"source": "airasia_auto", "direct": direct.get("raw", {}), "fallback_source": "sharetrip", "fallback_ok": bool(fallback.get("ok"))}, "originalResponse": fallback.get("originalResponse"), "rows": fallback.get("rows") if isinstance(fallback.get("rows"), list) else [], "ok": bool(fallback.get("ok"))}


def check_source_health(*, dry_run: bool = True, **_: Any) -> Dict[str, Any]:
    from core.source_health import ok

    return ok(
        "airasia",
        message="session/capture-backed connector; session replay and fresh captures are validated during extraction",
        mode=str(os.getenv(ENV_SOURCE_MODE, "auto") or "auto"),
        session_file=str(_session_file_path()),
        capture_root=str(_capture_root_path()),
        manual_action_required=True,
    )


def check_session(*, dry_run: bool = True, **kwargs: Any) -> Dict[str, Any]:
    return check_source_health(dry_run=dry_run, **kwargs)


def cli_main() -> None:
    parser = argparse.ArgumentParser(description="AirAsia connector tester")
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--mode", choices=["auto", "direct", "capture", "sharetrip"], default=None)
    args = parser.parse_args()
    if args.mode:
        os.environ[ENV_SOURCE_MODE] = args.mode
    out = fetch_flights(origin=args.origin, destination=args.destination, date=args.date, cabin=args.cabin, adt=args.adt, chd=args.chd, inf=args.inf, airline_code=AIRLINE_CODE)
    print(json.dumps(out, indent=2, ensure_ascii=True, default=str))


if __name__ == "__main__":
    cli_main()
