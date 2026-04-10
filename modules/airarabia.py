from __future__ import annotations

import argparse
import base64
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

try:
    from modules.sharetrip import fetch_flights_for_airline as fetch_from_sharetrip
except ModuleNotFoundError:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from modules.sharetrip import fetch_flights_for_airline as fetch_from_sharetrip


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

AIRLINE_CODE = "G9"
ENV_SOURCE_MODE = "AIRARABIA_SOURCE_MODE"
ENV_CAPTURE_ROOT = "AIRARABIA_CAPTURE_ROOT"
DEFAULT_CAPTURE_ROOT = "output/manual_sessions"
FARE_URL_TOKEN = "/api/flight-results/FlightSearchFare"


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        try:
            text = str(value).replace(",", "").strip()
            return float(text) if text else None
        except Exception:
            return None


_FX_TO_BDT: dict[str, float] = {
    "AED": 32.5,   # UAE Dirham
    "SAR": 30.5,   # Saudi Riyal
    "OMR": 300.0,  # Omani Rial
    "KWD": 375.0,  # Kuwaiti Dinar
    "QAR": 31.5,   # Qatari Riyal
    "BHD": 305.0,  # Bahraini Dinar
    "JOD": 162.0,  # Jordanian Dinar
    "USD": 110.0,
    "EUR": 120.0,
    "SGD": 90.0,
    "MYR": 26.0,
    "THB": 3.3,
    "INR": 1.38,
    "MVR": 7.5,
}


def _to_bdt(amount: Optional[float], currency: Optional[str]) -> Optional[float]:
    if amount is None:
        return None
    if not currency or currency.upper() == "BDT":
        return round(float(amount), 2)
    rate = _FX_TO_BDT.get(currency.upper())
    return round(amount * rate, 2) if rate is not None else None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except Exception:
        try:
            text = str(value).strip()
            return int(float(text)) if text else None
        except Exception:
            return None


def _normalize_dt_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return text


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


def _response_text_from_har_entry(entry: Dict[str, Any]) -> str:
    response = entry.get("response") or {}
    content = response.get("content") or {}
    text = content.get("text")
    if not isinstance(text, str):
        return ""
    if str(content.get("encoding") or "").lower() == "base64":
        try:
            return base64.b64decode(text).decode("utf-8", errors="replace")
        except Exception:
            return ""
    return text


def _candidate_capture_files(origin: str, destination: str, date: str) -> List[Path]:
    runs_dir = _capture_root_path() / "runs"
    if not runs_dir.exists():
        return []
    pattern = f"g9_{str(origin).upper()}_{str(destination).upper()}_{date}_*"
    out: List[Path] = []
    for run_dir in sorted(runs_dir.glob(pattern), reverse=True):
        summary_path = run_dir / "airarabia_capture_summary.json"
        if summary_path.exists():
            out.append(summary_path)
    return out


def _sort_key_for_summary(path: Path, summary: Dict[str, Any]) -> Tuple[float, str]:
    captured_at = str(summary.get("captured_at_utc") or "").strip()
    if captured_at:
        try:
            normalized = captured_at.replace("Z", "+00:00")
            return (float(__import__("datetime").datetime.fromisoformat(normalized).timestamp()), str(path))
        except Exception:
            pass
    try:
        return (path.stat().st_mtime, str(path))
    except Exception:
        return (0.0, str(path))


def _find_exact_capture(origin: str, destination: str, date: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    matches: List[Tuple[Path, Dict[str, Any]]] = []
    for path in _candidate_capture_files(origin, destination, date):
        summary = _load_json_file(path)
        if not isinstance(summary, dict):
            continue
        if (
            str(summary.get("origin") or "").upper().strip() == str(origin).upper().strip()
            and str(summary.get("destination") or "").upper().strip() == str(destination).upper().strip()
            and str(summary.get("date") or "").strip() == str(date).strip()
        ):
            matches.append((path, summary))
    if not matches:
        return None, None
    matches.sort(key=lambda item: _sort_key_for_summary(item[0], item[1]), reverse=True)
    return matches[0]


def _response_payload_from_summary(summary: Dict[str, Any]) -> Any:
    payload = summary.get("response_body")
    if isinstance(payload, dict):
        return payload
    payload_path = summary.get("response_json_path")
    loaded = _load_json_file(payload_path)
    if isinstance(loaded, dict):
        return loaded
    return None


def parse_fare_response(payload: Any, *, requested_cabin: str = "Economy", adt: int = 1, chd: int = 0, inf: int = 0) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") or {}
    currency = str(data.get("currency") or "").strip() or None
    pricing = data.get("selectedFlightPricing") or {}
    pricing_total = pricing.get("total") or {}
    rules = pricing.get("fareRules") or []
    ond_list = data.get("originDestinationResponse") or []
    rows: List[Dict[str, Any]] = []
    for ond in ond_list:
        if not isinstance(ond, dict):
            continue
        for option in ond.get("availableOptions") or []:
            if not isinstance(option, dict):
                continue
            segments = option.get("segments") or []
            if not segments:
                continue
            first = segments[0] if isinstance(segments[0], dict) else {}
            last = segments[-1] if isinstance(segments[-1], dict) else {}
            origin = str(option.get("originAirportCode") or ond.get("origin") or "").upper().strip()
            destination = str(option.get("destinationAirportCode") or ond.get("destination") or "").upper().strip()
            departure = _normalize_dt_text(((first.get("departureDateTime") or {}).get("local")))
            arrival = _normalize_dt_text(((last.get("arrivalDateTime") or {}).get("local")))
            flight_number = str(first.get("filghtDesignator") or first.get("segmentCode") or "").strip()
            if not (origin and destination and departure and flight_number):
                continue
            via_airports: List[str] = []
            for segment in segments[:-1]:
                airport = str((segment.get("description") or {}).get("destinationAirportCode") or "").upper().strip()
                if airport and airport not in {origin, destination} and airport not in via_airports:
                    via_airports.append(airport)
            fare_classes = option.get("availableFareClasses") or []
            if not fare_classes:
                fare_classes = [{"fareClassCode": requested_cabin, "description": "Basic", "price": pricing_total.get("price"), "availableSeats": None, "selected": True, "soldOut": False}]
            for fare_class in fare_classes:
                if not isinstance(fare_class, dict):
                    continue
                selected = bool(fare_class.get("selected"))
                price_total = _safe_float(fare_class.get("price"))
                tax_amount = None
                if selected:
                    tax_amount = (_safe_float(pricing_total.get("tax")) or 0.0) + (_safe_float(pricing_total.get("surcharge")) or 0.0)
                row = {
                    "airline": AIRLINE_CODE,
                    "operating_airline": str(first.get("carrierCode") or AIRLINE_CODE).upper().strip() or AIRLINE_CODE,
                    "flight_number": flight_number,
                    "origin": origin,
                    "destination": destination,
                    "departure": departure,
                    "arrival": arrival,
                    "cabin": requested_cabin,
                    "fare_basis": str(fare_class.get("fareClassCode") or "").strip() or None,
                    "brand": str(fare_class.get("description") or "AIRARABIA").strip() or "AIRARABIA",
                    "price_total_bdt": _to_bdt(price_total, currency),
                    "fare_amount": price_total,
                    "tax_amount": tax_amount,
                    "currency": currency,
                    "duration_min": _safe_int(first.get("durationInMinutes")) or _safe_int(option.get("totalDuration")),
                    "stops": max(0, len(segments) - 1),
                    "via_airports": "|".join(via_airports) if via_airports else None,
                    "booking_class": str(fare_class.get("fareClassCode") or "").strip() or None,
                    "baggage": None,
                    "equipment_code": str(first.get("equipmentModelNumber") or "").strip() or None,
                    "aircraft": str(first.get("equipmentModelInfo") or "").strip() or None,
                    "seat_capacity": None,
                    "seat_available": None if _safe_int(fare_class.get("availableSeats")) in (None, -1) else _safe_int(fare_class.get("availableSeats")),
                    "inventory_confidence": "unknown",
                    "estimated_load_factor_pct": None,
                    "soldout": bool(fare_class.get("soldOut")) or not bool(option.get("seatAvailable", True)),
                    "adt_count": max(1, int(adt or 1)),
                    "chd_count": max(0, int(chd or 0)),
                    "inf_count": max(0, int(inf or 0)),
                    "fare_ref_num": str(data.get("transactionId") or "").strip() or None,
                    "fare_search_reference": str(data.get("transactionId") or "").strip() or None,
                    "fare_search_signature": str(fare_class.get("fareClassCode") or "").strip() or None,
                    "source_endpoint": "airarabia:FlightSearchFare",
                    "fare_refundable": None,
                    "raw_offer": {"option": option, "fare_class": fare_class, "fare_rules": rules},
                }
                rows.append(row)
    deduped: List[Dict[str, Any]] = []
    seen: set[tuple] = set()
    for row in rows:
        key = (row.get("airline"), row.get("origin"), row.get("destination"), row.get("departure"), row.get("flight_number"), row.get("brand"), row.get("fare_basis"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def extract_fare_capture_from_har(
    har_payload: Dict[str, Any],
    *,
    requested_cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    source_har_path: Optional[str] = None,
) -> Dict[str, Any]:
    entries = ((har_payload.get("log") or {}).get("entries") or [])
    matches: List[Dict[str, Any]] = []
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        request = entry.get("request") or {}
        url = str(request.get("url") or "")
        if FARE_URL_TOKEN not in url:
            continue
        response_text = _response_text_from_har_entry(entry)
        try:
            response_body = json.loads(response_text)
        except Exception:
            continue
        try:
            request_body = json.loads(((request.get("postData") or {}).get("text") or ""))
        except Exception:
            request_body = None
        rows = parse_fare_response(response_body, requested_cabin=requested_cabin, adt=adt, chd=chd, inf=inf)
        if not rows:
            continue
        first_row = rows[0]
        matches.append(
            {
                "entry_index": idx,
                "request_url": url,
                "request_body": request_body,
                "response_body": response_body,
                "rows": rows,
                "origin": first_row.get("origin"),
                "destination": first_row.get("destination"),
                "date": str(first_row.get("departure") or "")[:10] or None,
                "carrier": AIRLINE_CODE,
                "source_har_path": source_har_path,
                "path_hint": urlparse(url).path,
            }
        )
    if not matches:
        return {"ok": False, "error": "fare_capture_not_found", "source_har_path": source_har_path}
    matches.sort(key=lambda item: (str(item.get("date") or ""), int(item.get("entry_index") or 0)), reverse=True)
    selected = matches[0]
    return {
        "ok": True,
        "carrier": AIRLINE_CODE,
        "origin": selected.get("origin"),
        "destination": selected.get("destination"),
        "date": selected.get("date"),
        "rows": selected.get("rows") or [],
        "request_url": selected.get("request_url"),
        "request_body": selected.get("request_body"),
        "response_body": selected.get("response_body"),
        "fare_entry_index": selected.get("entry_index"),
        "source_har_path": source_har_path,
    }


def _hint(origin: str, destination: str, date: str) -> str:
    return (
        "Import a saved Air Arabia HAR first, for example: "
        "python tools/import_airarabia_har.py --har path\\to\\www.airarabia.com.har"
        f"  # then retry {str(origin).upper()}->{str(destination).upper()} {date}"
    )


def _fetch_from_capture(*, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int) -> Dict[str, Any]:
    capture_path, summary = _find_exact_capture(origin, destination, date)
    out: Dict[str, Any] = {"raw": {"source": "airarabia_capture", "airline": AIRLINE_CODE, "capture_file": str(capture_path) if capture_path else None}, "originalResponse": None, "rows": [], "ok": False}
    if capture_path is None or not isinstance(summary, dict):
        out["raw"]["error"] = "capture_not_found"
        out["raw"]["hint"] = _hint(origin, destination, date)
        return out
    payload = _response_payload_from_summary(summary)
    out["originalResponse"] = payload
    out["rows"] = parse_fare_response(payload, requested_cabin=cabin, adt=adt, chd=chd, inf=inf)
    out["ok"] = bool(out["rows"])
    return out


def fetch_flights(origin: str, destination: str, date: str, cabin: str = "Economy", adt: int = 1, chd: int = 0, inf: int = 0):
    mode = str(os.getenv(ENV_SOURCE_MODE, "auto") or "auto").strip().lower()
    if mode == "sharetrip":
        return fetch_from_sharetrip(airline_code=AIRLINE_CODE, origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
    capture = _fetch_from_capture(origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
    if mode == "capture" or bool(capture.get("ok")):
        return capture
    fallback = fetch_from_sharetrip(airline_code=AIRLINE_CODE, origin=origin, destination=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
    return {
        "raw": {"source": "airarabia_auto", "capture": capture.get("raw", {}), "fallback_source": "sharetrip", "fallback_ok": bool(fallback.get("ok"))},
        "originalResponse": fallback.get("originalResponse"),
        "rows": fallback.get("rows") if isinstance(fallback.get("rows"), list) else [],
        "ok": bool(fallback.get("ok")),
    }


def cli_main() -> None:
    parser = argparse.ArgumentParser(description="Air Arabia connector tester")
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--mode", choices=["auto", "capture", "sharetrip"], default=None)
    parser.add_argument("--parse-har")
    args = parser.parse_args()

    if args.parse_har:
        har_payload = json.loads(Path(args.parse_har).read_text(encoding="utf-8-sig"))
        out = extract_fare_capture_from_har(har_payload, requested_cabin=args.cabin, adt=args.adt, chd=args.chd, inf=args.inf, source_har_path=args.parse_har)
        out.pop("response_body", None)
        print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
        return

    if args.mode:
        os.environ[ENV_SOURCE_MODE] = args.mode
    out = fetch_flights(origin=args.origin, destination=args.destination, date=args.date, cabin=args.cabin, adt=args.adt, chd=args.chd, inf=args.inf)
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
