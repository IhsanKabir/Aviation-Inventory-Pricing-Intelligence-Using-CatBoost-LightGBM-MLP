from __future__ import annotations

import argparse
import base64
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

AIRLINE_CODE = "OV"
ENV_CAPTURE_ROOT = "SALAMAIR_CAPTURE_ROOT"
DEFAULT_CAPTURE_ROOT = "output/manual_sessions"
ENV_COOKIES_PATH = "SALAMAIR_COOKIES_PATH"
ENV_PROXY_URL = "SALAMAIR_PROXY_URL"
ENV_SOURCE_MODE = "SALAMAIR_SOURCE_MODE"
ENV_BROWSER_CAPTURE_AUTO = "SALAMAIR_BROWSER_CAPTURE_AUTO"
ENV_BROWSER_CAPTURE_CMD = "SALAMAIR_BROWSER_CAPTURE_CMD"
ENV_BROWSER_CAPTURE_TIMEOUT_SEC = "SALAMAIR_BROWSER_CAPTURE_TIMEOUT_SEC"
FARES_URL_TOKEN = "/api/flights/flightFares"
CONFIRM_URL_TOKEN = "/api/flights/confirm"


def _safe_json_loads(text: Any) -> Any:
    if not isinstance(text, str):
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


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


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _source_mode() -> str:
    raw = str(os.getenv(ENV_SOURCE_MODE, "capture_then_browser") or "capture_then_browser").strip().lower()
    return raw or "capture_then_browser"


def _preferred_python() -> str:
    repo_root = Path(__file__).resolve().parents[1]
    venv_py = repo_root / ".venv" / "Scripts" / "python.exe"
    if venv_py.exists():
        return str(venv_py)
    return sys.executable or "python"


def _candidate_capture_files(origin: str, destination: str, date: str) -> List[Path]:
    runs_dir = _capture_root_path() / "runs"
    if not runs_dir.exists():
        return []
    pattern = f"ov_{str(origin).upper()}_{str(destination).upper()}_{date}_*"
    out: List[Path] = []
    for run_dir in sorted(runs_dir.glob(pattern), reverse=True):
        summary_path = run_dir / "salamair_capture_summary.json"
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


def _payload_from_summary(summary: Dict[str, Any], key: str) -> Any:
    payload = summary.get(key)
    if isinstance(payload, dict):
        return payload
    loaded = _load_json_file(summary.get(f"{key}_path"))
    if isinstance(loaded, dict):
        return loaded
    return None


def _confirm_permission_hints(confirm_payload: Dict[str, Any], brand: str) -> Dict[str, Any]:
    booking = (confirm_payload.get("booking") or {}) if isinstance(confirm_payload, dict) else {}
    summary = (confirm_payload.get("summary") or {}) if isinstance(confirm_payload, dict) else {}
    permissions = booking.get("permissions") or {}
    selected_brand = None
    for segment in summary.get("segments") or []:
        if isinstance(segment, dict) and segment.get("fareTypeName"):
            selected_brand = str(segment.get("fareTypeName"))
            break
    if selected_brand and selected_brand.strip().lower() != str(brand or "").strip().lower():
        return {}
    can_cancel = permissions.get("canCancelFlight")
    can_change = permissions.get("canChangeFlight")
    return {
        "fare_refundable": bool(can_cancel) if can_cancel is not None else None,
        "can_change": bool(can_change) if can_change is not None else None,
    }


def parse_flight_fares_payload(
    payload: Any,
    *,
    requested_cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    confirm_payload: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    currency = str((confirm_payload or {}).get("summary", {}).get("currencyCode") or "BDT").strip() or "BDT"
    rows: List[Dict[str, Any]] = []
    for flight in payload.get("flights") or []:
        if not isinstance(flight, dict):
            continue
        segments = flight.get("segments") or []
        if not segments:
            continue
        first = segments[0] if isinstance(segments[0], dict) else {}
        legs = first.get("legs") or []
        first_leg = legs[0] if legs and isinstance(legs[0], dict) else {}
        origin = str(first.get("originCode") or "").upper().strip()
        destination = str(first.get("destinationCode") or "").upper().strip()
        departure = str(first.get("departureDate") or "").strip() or None
        arrival = str(first.get("arrivalDate") or "").strip() or None
        flight_number = str(first.get("flightNumber") or "").strip()
        if not (origin and destination and departure and flight_number):
            continue
        via_airports: List[str] = []
        for leg in legs[:-1]:
            if not isinstance(leg, dict):
                continue
            airport = str(leg.get("destination") or "").upper().strip()
            if airport and airport not in {origin, destination} and airport not in via_airports:
                via_airports.append(airport)
        for fare in flight.get("fares") or []:
            if not isinstance(fare, dict):
                continue
            brand = str(fare.get("fareTypeName") or "").strip() or "SALAMAIR"
            confirm_hints = _confirm_permission_hints(confirm_payload or {}, brand)
            for fare_info in fare.get("fareInfos") or []:
                if not isinstance(fare_info, dict):
                    continue
                price_total = _safe_float(fare_info.get("fareWithTaxes")) or _safe_float(fare_info.get("baseFareWithTaxes"))
                fare_without_tax = _safe_float(fare_info.get("fareWithoutTaxes")) or _safe_float(fare_info.get("fareAmt"))
                tax_amount = None
                if price_total is not None and fare_without_tax is not None:
                    tax_amount = price_total - fare_without_tax
                rows.append(
                    {
                        "airline": AIRLINE_CODE,
                        "operating_airline": str(first.get("operatingCarrierCode") or AIRLINE_CODE).upper().strip() or AIRLINE_CODE,
                        "flight_number": flight_number,
                        "origin": origin,
                        "destination": destination,
                        "departure": departure,
                        "arrival": arrival,
                        "cabin": requested_cabin,
                        "fare_basis": str(fare_info.get("fareBasisCode") or "").strip() or None,
                        "brand": brand,
                        "price_total_bdt": price_total,
                        "fare_amount": fare_without_tax if fare_without_tax is not None else price_total,
                        "tax_amount": tax_amount,
                        "currency": currency,
                        "duration_min": _safe_int(first.get("flightTime")),
                        "stops": _safe_int(first.get("stops")) if _safe_int(first.get("stops")) is not None else max(0, len(legs) - 1),
                        "via_airports": "|".join(via_airports) if via_airports else None,
                        "booking_class": str(fare_info.get("fareClassCode") or "").strip() or None,
                        "baggage": None,
                        "equipment_code": str(first_leg.get("aircraftType") or "").strip() or None,
                        "aircraft": str(first_leg.get("aircraftDescription") or "").strip() or None,
                        "seat_capacity": None,
                        "seat_available": _safe_int(fare_info.get("seatsAvailable")) if _safe_int(fare_info.get("seatsAvailable")) not in (-1,) else None,
                        "inventory_confidence": "observed",
                        "estimated_load_factor_pct": None,
                        "soldout": bool(_safe_int(fare_info.get("seatsAvailable")) == 0),
                        "adt_count": max(1, int(adt or 1)),
                        "chd_count": max(0, int(chd or 0)),
                        "inf_count": max(0, int(inf or 0)),
                        "fare_ref_num": str(fare_info.get("fareID") or "").strip() or None,
                        "fare_search_reference": str(flight.get("logicalFlightId") or "").strip() or None,
                        "fare_search_signature": str(fare_info.get("fareSellKey") or "").strip() or None,
                        "source_endpoint": "salamair:flightFares",
                        "fare_refundable": confirm_hints.get("fare_refundable"),
                        "raw_offer": {"flight": flight, "fare": fare, "fare_info": fare_info, "confirm_payload": confirm_payload},
                    }
                )
    deduped: List[Dict[str, Any]] = []
    seen: set[tuple] = set()
    for row in rows:
        key = (row.get("airline"), row.get("origin"), row.get("destination"), row.get("departure"), row.get("flight_number"), row.get("brand"), row.get("fare_basis"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def extract_capture_from_har(
    har_payload: Dict[str, Any],
    *,
    requested_cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    source_har_path: Optional[str] = None,
) -> Dict[str, Any]:
    entries = ((har_payload.get("log") or {}).get("entries") or [])
    fare_matches: List[Dict[str, Any]] = []
    confirm_matches: List[Dict[str, Any]] = []
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        request = entry.get("request") or {}
        url = str(request.get("url") or "")
        request_body = _safe_json_loads(((request.get("postData") or {}).get("text") or ""))
        response_body = _safe_json_loads(_response_text_from_har_entry(entry))
        if FARES_URL_TOKEN in url and isinstance(response_body, dict):
            fare_matches.append({"entry_index": idx, "request_url": url, "request_body": request_body, "response_body": response_body})
        elif CONFIRM_URL_TOKEN in url and isinstance(response_body, dict):
            confirm_matches.append({"entry_index": idx, "request_url": url, "request_body": request_body, "response_body": response_body})
    if not fare_matches:
        return {"ok": False, "error": "flight_fares_capture_not_found", "source_har_path": source_har_path}
    fare_matches.sort(key=lambda item: int(item.get("entry_index") or 0), reverse=True)
    selected = fare_matches[0]
    selected_request = selected.get("request_body") or {}
    confirm_payload = None
    for confirm in sorted(confirm_matches, key=lambda item: int(item.get("entry_index") or 0), reverse=True):
        body = confirm.get("response_body") or {}
        booking = body.get("booking") or {}
        segments = booking.get("segments") or []
        if not segments:
            continue
        first = segments[0] if isinstance(segments[0], dict) else {}
        if (
            str(first.get("originCode") or "").upper().strip() == str(selected_request.get("origin") or "").upper().strip()
            and str(first.get("destinationCode") or "").upper().strip() == str(selected_request.get("destination") or "").upper().strip()
            and str(first.get("departureDate") or "")[:10] == str(selected_request.get("departureDate") or "")[:10]
        ):
            confirm_payload = body
            break
    rows = parse_flight_fares_payload(selected.get("response_body"), requested_cabin=requested_cabin, adt=adt, chd=chd, inf=inf, confirm_payload=confirm_payload)
    if not rows:
        return {"ok": False, "error": "no_rows_parsed_from_capture", "source_har_path": source_har_path}
    first_row = rows[0]
    return {
        "ok": True,
        "carrier": AIRLINE_CODE,
        "origin": first_row.get("origin"),
        "destination": first_row.get("destination"),
        "date": str(first_row.get("departure") or "")[:10] or None,
        "rows": rows,
        "flight_fares_request_body": selected.get("request_body"),
        "flight_fares_response_body": selected.get("response_body"),
        "flight_fares_entry_index": selected.get("entry_index"),
        "confirm_response_body": confirm_payload,
        "source_har_path": source_har_path,
    }


def _hint(origin: str, destination: str, date: str) -> str:
    return (
        "Capture live SalamAir data first, for example: "
        f"python tools/capture_salamair_live.py --origin {str(origin).upper()} --destination {str(destination).upper()} --date {date}"
        " or import a saved HAR with "
        "python tools/import_salamair_har.py --har path\\to\\booking.salamair.com.har"
        f"  # then retry {str(origin).upper()}->{str(destination).upper()} {date}"
    )


def _default_browser_capture_command(*, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int) -> str:
    py = _preferred_python()
    return (
        f'"{py}" tools/capture_salamair_live.py '
        f'--origin "{str(origin).upper().strip()}" '
        f'--destination "{str(destination).upper().strip()}" '
        f'--date "{str(date).strip()}" '
        f'--cabin "{str(cabin or "Economy").strip()}" '
        f'--adt {int(max(1, adt or 1))} --chd {int(max(0, chd or 0))} --inf {int(max(0, inf or 0))} '
        f'--headless'
    )


def _run_browser_capture_command(*, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int) -> Dict[str, Any]:
    command_template = str(os.getenv(ENV_BROWSER_CAPTURE_CMD, "")).strip()
    fmt_ctx = {
        "python": _preferred_python(),
        "origin": str(origin).upper().strip(),
        "destination": str(destination).upper().strip(),
        "date": str(date).strip(),
        "cabin": str(cabin or "Economy").strip(),
        "adt": int(max(1, adt or 1)),
        "chd": int(max(0, chd or 0)),
        "inf": int(max(0, inf or 0)),
    }
    if command_template:
        try:
            command = command_template.format(**fmt_ctx)
        except Exception:
            command = command_template
    else:
        command = _default_browser_capture_command(
            origin=origin,
            destination=destination,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
    timeout_sec = _safe_float(os.getenv(ENV_BROWSER_CAPTURE_TIMEOUT_SEC))
    timeout_sec = float(timeout_sec if timeout_sec is not None else 180.0)
    timeout_sec = max(30.0, timeout_sec)
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
            "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-20:]),
            "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-20:]),
            "timeout_sec": timeout_sec,
        }
    except Exception as exc:
        return {
            "ok": False,
            "command": command,
            "error": str(exc),
            "timeout_sec": timeout_sec,
        }


def _try_browser_capture_fallback(*, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int) -> Dict[str, Any]:
    if not _env_truthy(ENV_BROWSER_CAPTURE_AUTO, default=True):
        return {"capture_out": None, "capture_meta": None}
    capture_meta = _run_browser_capture_command(
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    capture_out = _fetch_from_capture(
        origin=origin,
        destination=destination,
        date=date,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    if capture_out.get("ok"):
        capture_out["raw"]["browser_capture"] = capture_meta
        capture_out["raw"]["source"] = "salamair_auto_capture"
        return {"capture_out": capture_out, "capture_meta": capture_meta}
    return {"capture_out": None, "capture_meta": capture_meta}


def _fetch_from_capture(*, origin: str, destination: str, date: str, cabin: str, adt: int, chd: int, inf: int) -> Dict[str, Any]:
    capture_path, summary = _find_exact_capture(origin, destination, date)
    out: Dict[str, Any] = {"raw": {"source": "salamair_capture", "airline": AIRLINE_CODE, "capture_file": str(capture_path) if capture_path else None}, "originalResponse": None, "rows": [], "ok": False}
    if capture_path is None or not isinstance(summary, dict):
        out["raw"]["error"] = "capture_not_found"
        out["raw"]["hint"] = _hint(origin, destination, date)
        return out
    fares_payload = _payload_from_summary(summary, "flight_fares_response_body")
    confirm_payload = _payload_from_summary(summary, "confirm_response_body")
    out["originalResponse"] = fares_payload
    out["rows"] = parse_flight_fares_payload(fares_payload, requested_cabin=cabin, adt=adt, chd=chd, inf=inf, confirm_payload=confirm_payload if isinstance(confirm_payload, dict) else None)
    out["ok"] = bool(out["rows"])
    return out


def salamair_search(origin: str, dest: str, date: str, cabin: str = "Economy", adt: int = 1, chd: int = 0, inf: int = 0, cookies_path: Optional[str] = None, proxy_url: Optional[str] = None) -> Dict[str, Any]:
    saved = _fetch_from_capture(origin=origin, destination=dest, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf)
    if saved.get("ok"):
        return saved
    mode = _source_mode()
    if mode in {"auto", "capture_then_browser", "browser_fallback"}:
        attempted = _try_browser_capture_fallback(
            origin=origin,
            destination=dest,
            date=date,
            cabin=cabin,
            adt=adt,
            chd=chd,
            inf=inf,
        )
        if attempted.get("capture_out"):
            return attempted["capture_out"]
        return {
            "raw": {
                "source": "salamair_capture",
                "error": "search_flow_not_implemented",
                "hint": _hint(origin, dest, date),
                "cookies_path": cookies_path,
                "proxy_url": proxy_url,
                "browser_capture": attempted.get("capture_meta"),
            },
            "originalResponse": None,
            "rows": [],
            "ok": False,
        }
    return {
        "raw": {"source": "salamair_capture", "error": "search_flow_not_implemented", "hint": _hint(origin, dest, date), "cookies_path": cookies_path, "proxy_url": proxy_url},
        "originalResponse": None,
        "rows": [],
        "ok": False,
    }


def fetch_flights(origin: str, destination: str, date: str, cabin: str = "Economy", adt: int = 1, chd: int = 0, inf: int = 0):
    return salamair_search(origin=origin, dest=destination, date=date, cabin=cabin, adt=adt, chd=chd, inf=inf, cookies_path=os.getenv(ENV_COOKIES_PATH) or None, proxy_url=os.getenv(ENV_PROXY_URL) or None)


def cli_main() -> None:
    parser = argparse.ArgumentParser(description="SalamAir connector tester")
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--parse-har")
    args = parser.parse_args()

    if args.parse_har:
        har_payload = json.loads(Path(args.parse_har).read_text(encoding="utf-8-sig"))
        out = extract_capture_from_har(har_payload, requested_cabin=args.cabin, adt=args.adt, chd=args.chd, inf=args.inf, source_har_path=args.parse_har)
        out.pop("flight_fares_response_body", None)
        out.pop("confirm_response_body", None)
        print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
        return

    out = fetch_flights(origin=args.origin, destination=args.destination, date=args.date, cabin=args.cabin, adt=args.adt, chd=args.chd, inf=args.inf)
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    cli_main()
