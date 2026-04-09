"""
Maldivian (Q2) PLNext scaffold.

This HAR-derived scaffold integrates Maldivian into the pipeline with the standard
fetch_flights() contract, but leaves live fare search disabled/not implemented until
we capture a successful PLNext availability request/response payload.

Useful today:
- Bootstrap page probing (PLNext / reCAPTCHA detection)
- HAR-derived session + airport-list request shape
- Seed route entries from observed HAR traffic (DAC <-> MLE)
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse

try:
    from modules.requester import Requester
except ModuleNotFoundError:
    # Allow direct script execution (`python modules\maldivian.py ...`) from repo root.
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from modules.requester import Requester


LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

AIRLINE_CODE = "Q2"
BASE_URL = "https://book.maldivian.aero"
INDEX_URL = f"{BASE_URL}/plnext/MaldivianAero/Override.action"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

BOOTSTRAP_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": USER_AGENT,
}

AIRPORT_LIST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": BASE_URL,
    "Referer": INDEX_URL,
    "User-Agent": USER_AGENT,
    "X-Requested-With": "XMLHttpRequest",
}

# Extracted from the provided HAR (book.maldivian.aero).
PLNEXT_FORM_DEFAULTS = {
    "LANGUAGE": "GB",
    "COUNTRY_SITE": "GB",
    "SITE": "J06GJ06G",
    "BOOKING_FLOW": "REVENUE",
    "TRIP_FLOW": "YES",
    "EXTERNAL_ID": "BOOKING",
    "OFFICE_ID": "DACQ208AA",
}

HAR_SEED_ROUTE_PAIRS = [
    ("DAC", "MLE"),
    ("MLE", "DAC"),
]

ENV_COOKIES_PATH = "MALDIVIAN_COOKIES_PATH"
ENV_PROXY_URL = "MALDIVIAN_PROXY_URL"
FARE_UID = "FARE"
FARE_SOURCE_ENDPOINT = "AjaxCall.action?UID=FARE&UI_ACTION=ajax"
RUNS_DIR = Path(__file__).resolve().parents[1] / "output" / "manual_sessions" / "runs"


def _safe_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _load_json_object(path: Path) -> Optional[Dict[str, Any]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        try:
            s = str(value).strip()
            if not s:
                return None
            return int(float(s))
        except Exception:
            return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        try:
            s = str(value).replace(",", "").strip()
            if not s:
                return None
            return float(s)
        except Exception:
            return None


def _parse_plnext_datetime(value: Any) -> Optional[str]:
    s = str(value or "").strip()
    if not s:
        return None
    for fmt in ("%b %d, %Y %I:%M:%S %p", "%b %d, %Y %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            continue
    return None


def _parse_plnext_request_date(value: Any) -> Optional[str]:
    s = str(value or "").strip()
    if len(s) >= 8 and s[:8].isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return None


def _is_q2_fare_ajax_url(url: str) -> bool:
    if not url or "book.maldivian.aero" not in url or "AjaxCall.action" not in url:
        return False
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    return (q.get("UID", [""])[0].upper() == FARE_UID) and (q.get("UI_ACTION", [""])[0].lower() == "ajax")


def _har_response_text(entry: Dict[str, Any]) -> str:
    response = entry.get("response") or {}
    content = response.get("content") or {}
    text = content.get("text")
    if isinstance(text, str):
        if str(content.get("encoding") or "").lower() == "base64":
            try:
                return base64.b64decode(text).decode("utf-8", errors="replace")
            except Exception:
                return ""
        return text
    return ""


def _derive_capture_route_date(payload: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    data = payload.get("data") or {}
    basefacts = data.get("basefacts") or {}
    origin = str(basefacts.get("request.B_LOCATION_1") or basefacts.get("originAirportCode") or "").upper().strip()
    destination = str(basefacts.get("request.E_LOCATION_1") or basefacts.get("destinationAirportCode") or "").upper().strip()
    date = _parse_plnext_request_date(basefacts.get("request.B_DATE_1")) or _parse_plnext_request_date(basefacts.get("departureDate"))
    if not origin and rows:
        origin = str(rows[0].get("origin") or "").upper().strip()
    if not destination and rows:
        destination = str(rows[0].get("destination") or "").upper().strip()
    if not date and rows:
        date = str(rows[0].get("search_date") or rows[0].get("departure") or "")[:10] or None
    return {
        "origin": origin or None,
        "destination": destination or None,
        "date": date or None,
    }


def _looks_captcha_or_bot_block(status_code: int, headers: Dict[str, str], body: str) -> bool:
    lower = (body or "").lower()
    if status_code in (403, 429):
        return True
    if "recaptcha" in lower or "g-recaptcha" in lower:
        return True
    if "captcha" in lower and "google.com/recaptcha" in lower:
        return True
    if "please wait" in lower and "maldivian" in lower:
        return True
    if "access is temporarily restricted" in lower:
        return True
    for key, value in (headers or {}).items():
        if key.lower() == "x-datadome" and value:
            return True
    return False


def _extract_jsessionid(url_or_html: str) -> Optional[str]:
    if not url_or_html:
        return None
    match = re.search(r";jsessionid=([^?\"'<>\\s]+)", url_or_html, re.I)
    if match:
        return match.group(1)
    match = re.search(r"sessionId[\"']?\s*[:=]\s*[\"']([^\"']+)[\"']", url_or_html, re.I)
    if match:
        return match.group(1)
    return None


def _build_airport_list_url(jsessionid: str) -> str:
    return (
        f"{BASE_URL}/plnext/MaldivianAero/AjaxCall.action"
        f";jsessionid={jsessionid}?UID=RETRIEVE_AIRPORT_LIST&UI_ACTION="
    )


def _extract_airports_from_payload(payload: Any) -> Dict[str, Dict[str, Any]]:
    """
    Best-effort parser for PLNext airport-list payload variants.

    The provided HAR captured an error/minimal shell response, so this parser is intentionally
    tolerant and returns an empty dict when no airport records are present.
    """

    airports: Dict[str, Dict[str, Any]] = {}

    def maybe_add(obj: Dict[str, Any]) -> None:
        code = None
        for key in ("IATACode", "iataCode", "airportCode", "code", "stationCode"):
            value = obj.get(key)
            if isinstance(value, str) and len(value.strip()) == 3:
                code = value.strip().upper()
                break
        if not code:
            return
        label = None
        for key in ("Label", "label", "name", "airportName"):
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                label = value.strip()
                break
        country = None
        for key in ("Country", "country", "countryCode", "ISOCountry"):
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                country = value.strip().upper()
                break
        airports.setdefault(
            code,
            {
                "code": code,
                "label": label,
                "country": country,
            },
        )

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            maybe_add(node)
            for value in node.values():
                walk(value)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return airports


def _first_segment(itinerary_elem: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(itinerary_elem, dict):
        return {}
    segments = itinerary_elem.get("listSegment") or []
    if segments and isinstance(segments[0], dict):
        return segments[0]
    return {}


def _extract_fare_basis_map(payload: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    rows = (
        payload.get("data", {})
        .get("business", {})
        .get("FareBasisInformationView", {})
        .get("fareBasisRows", [])
    )
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        seg = row.get("segmentBean") or {}
        if not isinstance(seg, dict):
            continue
        seg_key = str(seg.get("uniqueId") or seg.get("id") or "").strip()
        if not seg_key:
            continue
        infos = row.get("fareBasisInformations") or []
        fare_basis_name = None
        for info in infos:
            if not isinstance(info, dict):
                continue
            name = str(info.get("fareBasisName") or "").strip()
            if name:
                fare_basis_name = name
                break
        if fare_basis_name:
            out[seg_key] = fare_basis_name
    return out


def _extract_pax_counts(payload: Dict[str, Any], default_adt: int, default_chd: int, default_inf: int) -> tuple[int, int, int]:
    basefacts = payload.get("data", {}).get("basefacts", {}) or {}
    adt = _safe_int(basefacts.get("nbOfADT"))
    chd = _safe_int(basefacts.get("nbOfCHD"))
    inf = _safe_int(basefacts.get("nbOfINF"))
    return (
        max(0, adt if adt is not None else int(default_adt or 0)),
        max(0, chd if chd is not None else int(default_chd or 0)),
        max(0, inf if inf is not None else int(default_inf or 0)),
    )


def _cabin_from_segment(seg: Dict[str, Any], itinerary_elem: Dict[str, Any], requested_cabin: str) -> str:
    cabin_name = ""
    cabin_code = ""
    try:
        cabin0 = ((seg.get("listCabin") or [])[0]) if isinstance(seg, dict) else {}
    except Exception:
        cabin0 = {}
    if isinstance(cabin0, dict):
        cabin_name = str(cabin0.get("name") or "").strip()
        cabin_code = str(cabin0.get("code") or "").strip().upper()
    brand = str((itinerary_elem.get("fareFamily") or {}).get("ffName") or "").strip()
    source = f"{cabin_name} {brand}".lower()
    if "business" in source or cabin_code == "C":
        return "Business"
    if "premium" in source or cabin_code == "P":
        return "Premium Economy"
    return requested_cabin or "Economy"


def _segment_rbd_status(seg: Dict[str, Any]) -> tuple[Optional[str], Optional[int]]:
    cabin0 = ((seg.get("listCabin") or [])[0]) if isinstance(seg, dict) and (seg.get("listCabin") or []) else {}
    if not isinstance(cabin0, dict):
        return None, None
    rbds = cabin0.get("listRbd") or []
    if not rbds or not isinstance(rbds[0], dict):
        return None, None
    rbd = str(rbds[0].get("rbd") or "").strip() or None
    status_n = _safe_int(rbds[0].get("status"))
    return rbd, status_n


def _extract_rows_from_fare_ajax(
    payload: Any,
    *,
    requested_cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        return []
    business = data.get("business") or {}
    if not isinstance(business, dict):
        return []

    itinerary_view = business.get("ListItineraryView") or {}
    itinerary_list = itinerary_view.get("listItineraryElem") or []
    if not isinstance(itinerary_list, list):
        return []

    price = business.get("Price") or {}
    price_currency = ((price.get("currency") or {}).get("code")) if isinstance(price, dict) else None
    currency = str(price_currency or "BDT").upper().strip() or "BDT"
    base_fare = _safe_float((price.get("baseFare") or {}).get("amount")) if isinstance(price, dict) else None
    total_taxes = _safe_float((price.get("totalTaxes") or {}).get("amount")) if isinstance(price, dict) else None
    total_amount = _safe_float((price.get("totalAmount") or {}).get("amount")) if isinstance(price, dict) else None

    basefacts = data.get("basefacts") or {}
    req_origin = str(basefacts.get("request.B_LOCATION_1") or basefacts.get("originAirportCode") or "").upper().strip() or None
    req_dest = str(basefacts.get("request.E_LOCATION_1") or basefacts.get("destinationAirportCode") or "").upper().strip() or None
    search_date = _parse_plnext_request_date(basefacts.get("request.B_DATE_1")) or _parse_plnext_request_date(basefacts.get("departureDate"))

    adt_n, chd_n, inf_n = _extract_pax_counts(payload, adt, chd, inf)
    fare_basis_map = _extract_fare_basis_map(payload)

    rows: List[Dict[str, Any]] = []
    seen = set()

    for idx, itin in enumerate(itinerary_list):
        if not isinstance(itin, dict):
            continue

        segments = itin.get("listSegment") or []
        if not isinstance(segments, list) or not segments:
            continue

        first_seg = _first_segment(itin)
        last_seg = segments[-1] if isinstance(segments[-1], dict) else first_seg
        if not first_seg:
            continue

        marketing_codes: List[str] = []
        flight_numbers: List[str] = []
        booking_classes: List[str] = []
        seat_statuses: List[int] = []
        operating_codes: List[str] = []
        duration_ms = 0
        stops_total = 0
        equipment_names: List[str] = []
        equipment_codes: List[str] = []
        fare_basis_parts: List[str] = []

        for seg in segments:
            if not isinstance(seg, dict):
                continue
            airline_code = str((seg.get("airline") or {}).get("code") or AIRLINE_CODE).upper().strip()
            if airline_code:
                marketing_codes.append(airline_code)
            op_code = str((seg.get("opAirline") or {}).get("code") or "").upper().strip()
            if op_code:
                operating_codes.append(op_code)
            fn = str(seg.get("flightNumber") or "").strip()
            if fn:
                flight_numbers.append(fn)
            rbd, status_n = _segment_rbd_status(seg)
            if rbd:
                booking_classes.append(rbd)
            if status_n is not None:
                seat_statuses.append(status_n)
            seg_ms = _safe_int(seg.get("segmentTime")) or 0
            duration_ms += max(0, seg_ms)
            stops_total += max(0, _safe_int(seg.get("nbrOfStops")) or 0)
            eq = seg.get("equipment") or {}
            if isinstance(eq, dict):
                eq_name = str(eq.get("name") or "").strip()
                eq_code = str(eq.get("code") or "").strip()
                if eq_name:
                    equipment_names.append(eq_name)
                if eq_code:
                    equipment_codes.append(eq_code)
            seg_key = str(seg.get("uniqueId") or seg.get("id") or "").strip()
            fb = fare_basis_map.get(seg_key)
            if fb:
                fare_basis_parts.append(fb)

        marketing_carrier = marketing_codes[0] if marketing_codes else AIRLINE_CODE
        operating_carrier = operating_codes[0] if operating_codes else None
        flight_number = "/".join(flight_numbers) if flight_numbers else None
        booking_class = "/".join(dict.fromkeys(booking_classes)) if booking_classes else None
        fare_basis = "/".join(dict.fromkeys(fare_basis_parts)) if fare_basis_parts else None
        seats_remaining = min(seat_statuses) if seat_statuses else None
        duration_min = int(duration_ms / 60000) if duration_ms > 0 else None

        origin = str(((first_seg.get("beginLocation") or {}).get("locationCode")) or req_origin or "").upper().strip() or None
        destination = str(((last_seg.get("endLocation") or {}).get("locationCode")) or req_dest or "").upper().strip() or None
        departure = _parse_plnext_datetime(first_seg.get("beginDate")) or _parse_plnext_datetime(first_seg.get("beginDateGMT"))
        arrival = _parse_plnext_datetime(last_seg.get("endDate")) or _parse_plnext_datetime(last_seg.get("endDateGMT"))

        brand = str((itin.get("fareFamily") or {}).get("ffName") or (first_seg.get("fareFamily") or {}).get("ffName") or "").strip() or None
        cabin = _cabin_from_segment(first_seg, itin, requested_cabin)

        if not (marketing_carrier and origin and destination and departure):
            continue

        key = (marketing_carrier, flight_number, origin, destination, departure, cabin, brand, fare_basis)
        if key in seen:
            continue
        seen.add(key)

        row = {
            "airline": marketing_carrier,
            "operating_airline": operating_carrier,
            "brand": brand or cabin,
            "flight_number": flight_number,
            "origin": origin,
            "destination": destination,
            "departure": departure,
            "arrival": arrival,
            "duration_min": duration_min,
            "stops": stops_total if len(segments) > 1 else (_safe_int(first_seg.get("nbrOfStops")) or 0),
            "cabin": cabin,
            "booking_class": booking_class,
            "fare_basis": fare_basis,
            "fare_amount": base_fare,
            "tax_amount": total_taxes,
            "total_amount": total_amount,
            "currency": currency,
            "seats_remaining": seats_remaining,
            # Core DB schema aliases (flight_offers uses price_total_bdt / seat_available)
            "price_total_bdt": total_amount if total_amount is not None else None,
            "seat_available": seats_remaining,
            "seat_capacity": None,
            "aircraft": equipment_names[0] if len(set(equipment_names)) == 1 and equipment_names else (" / ".join(dict.fromkeys(equipment_names)) if equipment_names else None),
            "equipment_code": equipment_codes[0] if len(set(equipment_codes)) == 1 and equipment_codes else (" / ".join(dict.fromkeys(equipment_codes)) if equipment_codes else None),
            "baggage": None,
            "soldout": False,
            "adt_count": adt_n,
            "chd_count": chd_n,
            "inf_count": inf_n,
            "search_date": search_date,
            "source_endpoint": FARE_SOURCE_ENDPOINT,
            "raw_offer": {
                "source": "plnext_fare_ajax",
                "uid": FARE_UID,
                "sessionId": payload.get("sessionId"),
                "pageTicket": (data.get("pageTicket") if isinstance(data, dict) else None),
                "itinerary_index": idx,
                "itinerary": itin,
                "price_summary": {
                    "baseFare": (price.get("baseFare") if isinstance(price, dict) else None),
                    "totalTaxes": (price.get("totalTaxes") if isinstance(price, dict) else None),
                    "totalAmount": (price.get("totalAmount") if isinstance(price, dict) else None),
                },
            },
        }
        rows.append(row)

    return rows


def extract_fare_capture_from_har(
    har_payload: Any,
    *,
    requested_cabin: str = "Economy",
    adt: Optional[int] = None,
    chd: Optional[int] = None,
    inf: Optional[int] = None,
    source_har_path: Optional[str] = None,
) -> Dict[str, Any]:
    if not isinstance(har_payload, dict):
        return {
            "ok": False,
            "error": "invalid_har_payload",
            "rows": [],
            "rows_count": 0,
            "source_har_path": source_har_path,
        }

    entries = ((har_payload.get("log") or {}).get("entries") or [])
    if not isinstance(entries, list):
        entries = []

    seen_fare_calls: List[Dict[str, Any]] = []
    last_candidate: Optional[Dict[str, Any]] = None

    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        req = entry.get("request") or {}
        url = str(req.get("url") or "")
        if not _is_q2_fare_ajax_url(url):
            continue

        resp = entry.get("response") or {}
        status = _safe_int(resp.get("status")) or 0
        seen_fare_calls.append({"url": url, "status": status})

        payload = _safe_json_loads(_har_response_text(entry))
        if not isinstance(payload, dict):
            last_candidate = {
                "ok": False,
                "error": "invalid_fare_response_json",
                "rows": [],
                "rows_count": 0,
                "status": status,
                "fare_uid_url": url,
                "fare_entry_index": idx,
                "fare_uid_request_body": ((req.get("postData") or {}).get("text") or ""),
                "source_har_path": source_har_path,
                "seen_fare_calls": seen_fare_calls,
            }
            continue

        eff_adt = int(adt if adt is not None else ((_extract_pax_counts(payload, 1, 0, 0))[0]))
        eff_chd = int(chd if chd is not None else ((_extract_pax_counts(payload, 1, 0, 0))[1]))
        eff_inf = int(inf if inf is not None else ((_extract_pax_counts(payload, 1, 0, 0))[2]))
        rows = _extract_rows_from_fare_ajax(
            payload,
            requested_cabin=requested_cabin,
            adt=eff_adt,
            chd=eff_chd,
            inf=eff_inf,
        )
        route_date = _derive_capture_route_date(payload, rows)
        cabin = requested_cabin
        if rows:
            cabins = [str(r.get("cabin") or "").strip() for r in rows if str(r.get("cabin") or "").strip()]
            if cabins and len(set(cabins)) == 1:
                cabin = cabins[0]

        last_candidate = {
            "ok": bool(rows),
            "error": None if rows else "fare_payload_has_no_rows",
            "rows": rows,
            "rows_count": len(rows),
            "status": status,
            "fare_uid_url": url,
            "fare_entry_index": idx,
            "fare_uid_request_body": ((req.get("postData") or {}).get("text") or ""),
            "fare_payload": payload,
            "origin": route_date.get("origin"),
            "destination": route_date.get("destination"),
            "date": route_date.get("date"),
            "cabin": cabin,
            "adt": eff_adt,
            "chd": eff_chd,
            "inf": eff_inf,
            "source_har_path": source_har_path,
            "seen_fare_calls": seen_fare_calls,
        }
        if rows:
            return last_candidate

    if last_candidate is not None:
        return last_candidate

    return {
        "ok": False,
        "error": "fare_uid_not_found_in_har",
        "rows": [],
        "rows_count": 0,
        "source_har_path": source_har_path,
        "seen_fare_calls": seen_fare_calls,
    }


def _find_matching_saved_capture(
    origin: str,
    dest: str,
    date: str,
    *,
    requested_cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> Optional[Dict[str, Any]]:
    if not RUNS_DIR.exists():
        return None

    origin_u = str(origin or "").upper().strip()
    dest_u = str(dest or "").upper().strip()
    date_s = str(date or "")[:10]

    candidates = sorted(
        (p for p in RUNS_DIR.glob("q2_*/*q2_probe_response.json") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    for summary_path in candidates:
        summary = _load_json_object(summary_path)
        if not summary:
            continue
        if str(summary.get("carrier") or "").upper() not in {"Q2", ""}:
            continue
        if origin_u and str(summary.get("origin") or "").upper() != origin_u:
            continue
        if dest_u and str(summary.get("destination") or "").upper() != dest_u:
            continue
        if date_s and str(summary.get("date") or "")[:10] != date_s:
            continue

        fare_path_raw = summary.get("fare_uid_response_path")
        fare_path = Path(fare_path_raw) if isinstance(fare_path_raw, str) and fare_path_raw.strip() else (summary_path.parent / "q2_fare_uid_response.json")
        if not fare_path.is_absolute():
            fare_path = (summary_path.parent / fare_path).resolve()
        if not fare_path.exists():
            continue

        payload = _load_json_object(fare_path)
        if not payload:
            continue

        rows = _extract_rows_from_fare_ajax(
            payload,
            requested_cabin=requested_cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
        if not rows:
            continue

        return {
            "summary_path": str(summary_path.resolve()),
            "fare_json_path": str(fare_path.resolve()),
            "summary": summary,
            "fare_payload": payload,
            "rows": rows,
        }

    return None


def _seed_route_entries(allowed_origins: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    allowed = {str(x).upper().strip() for x in (allowed_origins or []) if str(x).strip()}
    entries: List[Dict[str, Any]] = []
    for origin, destination in HAR_SEED_ROUTE_PAIRS:
        if allowed and origin not in allowed:
            continue
        entries.append(
            {
                "airline": AIRLINE_CODE,
                "origin": origin,
                "destination": destination,
                "cabins": ["Economy"],
            }
        )
    return entries


def probe_bootstrap(
    cookies_path: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    req = Requester(cookies_path=cookies_path, user_agent=USER_AGENT, proxy_url=proxy_url)
    resp = req.get(INDEX_URL, headers=BOOTSTRAP_HEADERS)
    text = resp.text or ""
    headers = {str(k): str(v) for k, v in (resp.headers or {}).items()}
    return {
        "status_code": resp.status_code,
        "url": str(resp.url),
        "headers": headers,
        "body_snippet": text[:4000],
        "is_captcha_or_block": _looks_captcha_or_bot_block(resp.status_code, headers, text),
        "has_plnext": "/plnext/" in text.lower() or "plnextv2" in text.lower(),
        "has_recaptcha": "recaptcha" in text.lower(),
        "jsessionid": _extract_jsessionid(str(resp.url)) or _extract_jsessionid(text),
    }


def retrieve_airport_list(
    cookies_path: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    req = Requester(cookies_path=cookies_path, user_agent=USER_AGENT, proxy_url=proxy_url)
    bootstrap_resp = req.get(INDEX_URL, headers=BOOTSTRAP_HEADERS)
    bootstrap_text = bootstrap_resp.text or ""
    bootstrap_headers = {str(k): str(v) for k, v in (bootstrap_resp.headers or {}).items()}

    out: Dict[str, Any] = {
        "bootstrap_status": bootstrap_resp.status_code,
        "bootstrap_url": str(bootstrap_resp.url),
        "bootstrap_is_captcha_or_block": _looks_captcha_or_bot_block(
            bootstrap_resp.status_code,
            bootstrap_headers,
            bootstrap_text,
        ),
        "bootstrap_has_recaptcha": "recaptcha" in bootstrap_text.lower(),
    }

    jsessionid = _extract_jsessionid(str(bootstrap_resp.url)) or _extract_jsessionid(bootstrap_text)
    out["jsessionid"] = jsessionid
    if not jsessionid:
        out["error"] = "missing_jsessionid"
        return out

    form = dict(PLNEXT_FORM_DEFAULTS)
    form.update(
        {
            "TYPE": "AIRPORT_LIST",
            "PAGE_TICKET": "0",
        }
    )

    url = _build_airport_list_url(jsessionid)
    headers = dict(AIRPORT_LIST_HEADERS)
    headers["Referer"] = str(bootstrap_resp.url or INDEX_URL)

    resp = req.session.post(url, data=form, headers=headers, timeout=req.timeout)
    body: Any = _safe_json_loads(resp.text)
    if body is None:
        body = resp.text

    out["airport_list_status"] = resp.status_code
    out["airport_list_url"] = str(resp.url)
    out["airport_list_body"] = body
    if isinstance(body, dict):
        out["airport_count_detected"] = len(_extract_airports_from_payload(body))
    else:
        out["airport_count_detected"] = 0
    return out


def discover_route_entries(
    *,
    allowed_origins: Optional[Iterable[str]] = None,
    use_live_probe: bool = False,
    cookies_path: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    # Current HAR only proves DAC<->MLE. Keep live probe optional until we capture a
    # successful airport list payload with route graph.
    if not use_live_probe:
        return _seed_route_entries(allowed_origins=allowed_origins)

    probe = retrieve_airport_list(cookies_path=cookies_path, proxy_url=proxy_url)
    body = probe.get("airport_list_body")
    airports = _extract_airports_from_payload(body) if isinstance(body, dict) else {}
    if not airports:
        return _seed_route_entries(allowed_origins=allowed_origins)

    # Airport list payload does not imply route graph, so keep seed routes for now.
    return _seed_route_entries(allowed_origins=allowed_origins)


def maldivian_search(
    origin: str,
    dest: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    cookies_path: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Standard run_all contract wrapper.

    Live automated search is not implemented yet (PLNext flow + reCAPTCHA/Imperva).
    This function returns a clean failure payload with bootstrap diagnostics instead of crashing.
    The parser for `UID=FARE&UI_ACTION=ajax` responses is implemented separately and can be used
    on captured JSON responses.
    """
    output: Dict[str, Any] = {"raw": {}, "originalResponse": None, "rows": [], "ok": False}
    output["raw"]["request"] = {
        "origin": str(origin).upper().strip(),
        "destination": str(dest).upper().strip(),
        "date": date,
        "cabin": cabin,
        "adt": int(adt or 0),
        "chd": int(chd or 0),
        "inf": int(inf or 0),
    }
    if cookies_path or proxy_url:
        output["raw"]["access_path"] = {
            "cookies_path": cookies_path,
            "proxy_url": proxy_url,
        }

    saved_capture = _find_matching_saved_capture(
        origin=origin,
        dest=dest,
        date=date,
        requested_cabin=cabin,
        adt=int(adt or 0),
        chd=int(chd or 0),
        inf=int(inf or 0),
    )
    if saved_capture:
        output["raw"]["source"] = "maldivian_capture"
        output["raw"]["capture_summary_path"] = saved_capture["summary_path"]
        output["raw"]["capture_fare_json_path"] = saved_capture["fare_json_path"]
        output["raw"]["capture_summary"] = {
            "origin": (saved_capture.get("summary") or {}).get("origin"),
            "destination": (saved_capture.get("summary") or {}).get("destination"),
            "date": (saved_capture.get("summary") or {}).get("date"),
            "parsed_selected_days_rows_count": (saved_capture.get("summary") or {}).get("parsed_selected_days_rows_count"),
        }
        output["rows"] = saved_capture["rows"]
        output["originalResponse"] = saved_capture["fare_payload"]
        output["ok"] = bool(saved_capture["rows"])
        return output

    try:
        probe = probe_bootstrap(cookies_path=cookies_path, proxy_url=proxy_url)
    except Exception as exc:
        LOG.error("[Q2] bootstrap probe failed: %s", exc)
        output["raw"]["error"] = "bootstrap_probe_failed"
        output["raw"]["detail"] = str(exc)
        return output

    output["raw"]["bootstrap_probe"] = {
        k: v for k, v in probe.items() if k != "body_snippet"
    }
    output["raw"]["bootstrap_body_snippet"] = probe.get("body_snippet")

    if probe.get("is_captcha_or_block"):
        output["raw"]["error"] = "captcha_or_bot_blocked"
        output["raw"]["hint"] = (
            "Maldivian PLNext bootstrap appears protected (reCAPTCHA/anti-bot). "
            "Use a clean browser/HAR capture of a successful search before implementing live fare scraping."
        )
        return output

    output["raw"]["error"] = "search_flow_not_implemented"
    output["raw"]["hint"] = (
        "Maldivian airline scaffold is integrated. Live PLNext search flow is not implemented yet, "
        "but the FARE Ajax JSON parser is ready. Capture `AjaxCall.action?UID=FARE&UI_ACTION=ajax` "
        "responses (or export HAR with content) to parse/integrate fares."
    )
    return output


def fetch_flights(
    origin: str,
    destination: str,
    date: str,
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
):
    """
    Unified contract for run_all.py:
    { raw, originalResponse, rows, ok }
    """
    cookies_path = os.getenv(ENV_COOKIES_PATH) or None
    proxy_url = os.getenv(ENV_PROXY_URL) or None
    return maldivian_search(
        origin=origin,
        dest=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
        cookies_path=cookies_path,
        proxy_url=proxy_url,
    )


def cli_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--discover-routes", action="store_true")
    parser.add_argument("--use-live-probe", action="store_true", help="Attempt live PLNext airport-list probe (best-effort)")
    parser.add_argument("--origin-filter", action="append", default=[])
    parser.add_argument("--cookies-path", help=f"Cookie JSON path (Requester-compatible dict) or use {ENV_COOKIES_PATH}")
    parser.add_argument("--proxy-url", help=f"Proxy URL (e.g. http://host:port) or use {ENV_PROXY_URL}")
    parser.add_argument("--probe-airport-list", action="store_true", help="Print airport-list probe diagnostics and exit")
    parser.add_argument("--parse-fare-json", help="Parse a saved Maldivian UID=FARE Ajax JSON response file and print normalized rows")
    parser.add_argument("--parse-har", help="Parse a Maldivian HAR file, extract the latest UID=FARE Ajax response, and print normalized rows")
    args = parser.parse_args()

    cookies_path = args.cookies_path or os.getenv(ENV_COOKIES_PATH) or None
    proxy_url = args.proxy_url or os.getenv(ENV_PROXY_URL) or None

    if args.probe_airport_list:
        out = retrieve_airport_list(cookies_path=cookies_path, proxy_url=proxy_url)
        print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
        return

    if args.parse_fare_json:
        payload = json.loads(open(args.parse_fare_json, "r", encoding="utf-8-sig").read())
        rows = _extract_rows_from_fare_ajax(
            payload,
            requested_cabin=args.cabin,
            adt=args.adt,
            chd=args.chd,
            inf=args.inf,
        )
        print(json.dumps({"ok": bool(rows), "rows_count": len(rows), "rows": rows}, indent=2, ensure_ascii=False, default=str))
        return

    if args.parse_har:
        har_payload = json.loads(open(args.parse_har, "r", encoding="utf-8-sig").read())
        extracted = extract_fare_capture_from_har(
            har_payload,
            requested_cabin=args.cabin,
            adt=args.adt,
            chd=args.chd,
            inf=args.inf,
            source_har_path=args.parse_har,
        )
        out = dict(extracted)
        out.pop("fare_payload", None)
        print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
        return

    if args.discover_routes:
        entries = discover_route_entries(
            allowed_origins=args.origin_filter,
            use_live_probe=args.use_live_probe,
            cookies_path=cookies_path,
            proxy_url=proxy_url,
        )
        print(json.dumps(entries, indent=2, ensure_ascii=False))
        return

    if not (args.origin and args.destination and args.date):
        parser.error("--origin, --destination, and --date are required unless --discover-routes or --probe-airport-list is used")

    out = maldivian_search(
        origin=args.origin,
        dest=args.destination,
        date=args.date,
        cabin=args.cabin,
        adt=args.adt,
        chd=args.chd,
        inf=args.inf,
        cookies_path=cookies_path,
        proxy_url=proxy_url,
    )
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
