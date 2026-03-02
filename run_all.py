"""
run_all.py (patched)

- Uses unified response contract from modules.* modules (fetch_flights / biman_search)
- Friendly logs, no tracebacks.
- Soft-fail fallback logic.
"""
import json
import importlib
import logging
import argparse
import hashlib
import re
import time
import os
from pathlib import Path
from typing import Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from comparison_engine import ComparisonEngine
from strategy_engine import StrategyEngine
from sqlalchemy import func, text
from models.flight_offer import FlightOfferORM
from db import (
    init_db,
    bulk_insert_offers,
    #save_raw_response_meta,
    normalize_for_db,
    save_change_events,
    save_column_change_events,
    get_session,
    bulk_insert_raw_meta,
    normalize_raw_meta,
)
import uuid
import datetime
from engines.route_scope import (
    load_airport_countries,
    parse_csv_upper_codes,
    route_matches_scope,
)
from modules.penalties import apply_penalty_inference

def is_valid_core_offer(o: dict) -> bool:
    required = [
        "airline",
        "flight_number",
        "origin",
        "destination",
        "departure",
        "cabin",
        "brand",
    ]
    return all(o.get(k) is not None for k in required)

init_db()
#session = get_session()


LOG = logging.getLogger("run_all")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

AIRLINES_FILE = Path("config/airlines.json")
ROUTES_FILE = Path("config/routes.json")
AIRPORT_TZ_FILE = Path("config/airport_timezones.json")
AIRPORT_COUNTRY_FILE = Path("config/airport_countries.json")
SCHEDULE_FILE = Path("config/schedule.json")
OUTPUT_DIR = Path("output/latest")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ROUTE_AUDIT_OUTPUT_DIR = Path("output/reports")
ROUTE_AUDIT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_STATUS_OUTPUT_DIR = Path("output/reports")
RUN_STATUS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_AIRPORT_UTC_OFFSET_MINUTES = {
    "DAC": 360,
    "CGP": 360,
    "CXB": 360,
    "JSR": 360,
    "RJH": 360,
    "SPD": 360,
    "ZYL": 360,
    "BZL": 360,
}


def parse_args():
    parser = argparse.ArgumentParser(description="Airline accumulation runner")
    parser.add_argument("--quick", action="store_true", help="Run fast mode (single day offset: today)")
    parser.add_argument("--airline", help="Filter airline code(s), comma-separated (e.g., BG,VQ)")
    parser.add_argument("--origin", help="Filter routes by origin airport (e.g., DAC)")
    parser.add_argument("--destination", help="Filter routes by destination airport (e.g., CXB)")
    parser.add_argument("--date", help="Run a single departure date in YYYY-MM-DD format")
    parser.add_argument("--date-start", help="Inclusive range start date (YYYY-MM-DD)")
    parser.add_argument("--date-end", help="Inclusive range end date (YYYY-MM-DD)")
    parser.add_argument("--dates", help="Comma-separated departure dates in YYYY-MM-DD format")
    parser.add_argument("--date-offsets", help="Comma-separated day offsets from today, e.g. 0,3,7,30")
    parser.add_argument("--dates-file", default="config/dates.json", help="Optional JSON file for dynamic date settings")
    parser.add_argument("--schedule-file", default=str(SCHEDULE_FILE), help="Optional scheduler config file for auto-run date defaults")
    parser.add_argument("--cabin", help="Filter to a single cabin name (e.g., Economy)")
    parser.add_argument("--adt", type=int, default=1, help="Adult passenger count for search requests (default: 1)")
    parser.add_argument("--chd", type=int, default=0, help="Child passenger count for search requests (default: 0)")
    parser.add_argument("--inf", type=int, default=0, help="Infant passenger count for search requests (default: 0)")
    parser.add_argument(
        "--probe-group-id",
        help="Optional identifier to link multiple passenger-mix probe runs for the same observation window",
    )
    parser.add_argument(
        "--route-scope",
        choices=["all", "domestic", "international"],
        default="all",
        help="Route scope filter based on airport-country mapping",
    )
    parser.add_argument(
        "--market-country",
        default="BD",
        help="Domestic market country (ISO2 or country name, e.g., BD, IN, Bangladesh, India)",
    )
    parser.add_argument("--limit-routes", type=int, help="Process only first N matched routes per airline")
    parser.add_argument("--limit-dates", type=int, help="Process only first N selected dates")
    parser.add_argument(
        "--strict-route-audit",
        action="store_true",
        help="Fail fast on route configuration issues (duplicates/malformed rows/missing airport-country mappings)",
    )
    parser.add_argument("--profile-runtime", action="store_true", help="Write per-search runtime profile")
    parser.add_argument("--profile-output-dir", default="output/reports", help="Runtime profile output directory")
    parser.add_argument(
        "--query-timeout-seconds",
        type=float,
        default=180.0,
        help="Soft timeout for a single fetch query (seconds). Timed-out queries are skipped as soft-fail.",
    )
    return parser.parse_args()


def _parse_iso_date_list(values: list[str]) -> list[str]:
    out = []
    seen = set()
    for raw in values:
        s = str(raw or "").strip()
        if not s:
            continue
        try:
            d = datetime.date.fromisoformat(s)
            key = d.isoformat()
            if key not in seen:
                seen.add(key)
                out.append(key)
        except Exception:
            LOG.warning("Ignoring invalid date value: %s", s)
    return out


def _parse_offsets(raw: str) -> list[int]:
    out = []
    for part in str(raw or "").split(","):
        s = part.strip()
        if not s:
            continue
        if not re.fullmatch(r"[-+]?\d+", s):
            LOG.warning("Ignoring invalid day offset: %s", s)
            continue
        out.append(int(s))
    # Keep order but dedupe
    deduped = []
    seen = set()
    for v in out:
        if v not in seen:
            seen.add(v)
            deduped.append(v)
    return deduped


def _expand_date_range(start_raw: str | None, end_raw: str | None) -> list[str]:
    try:
        start = datetime.date.fromisoformat(str(start_raw or "").strip())
        end = datetime.date.fromisoformat(str(end_raw or "").strip())
    except Exception:
        return []
    if end < start:
        start, end = end, start
    span = (end - start).days
    return [(start + datetime.timedelta(days=i)).isoformat() for i in range(span + 1)]


def _load_dates_from_file(path: Path, today: datetime.date) -> list[str]:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return []
        obj = json.loads(text)
    except Exception as exc:
        LOG.warning("Failed to parse dates file %s: %s", path, exc)
        return []

    # Supported shapes:
    # 1) ["2026-03-01", "2026-03-07"]
    # 2) {"dates": [...]} or {"day_offsets": [0,3,7,30]}
    # 3) {"date_ranges": [{"start":"2026-03-10","end":"2026-03-20"}, ...]}
    if isinstance(obj, list):
        return _parse_iso_date_list(obj)

    if isinstance(obj, dict):
        if isinstance(obj.get("dates"), list):
            parsed = _parse_iso_date_list(obj["dates"])
            if parsed:
                return parsed
        if obj.get("date_start") and obj.get("date_end"):
            parsed = _expand_date_range(obj.get("date_start"), obj.get("date_end"))
            if parsed:
                return parsed
        if obj.get("start_date") and obj.get("end_date"):
            parsed = _expand_date_range(obj.get("start_date"), obj.get("end_date"))
            if parsed:
                return parsed
        if isinstance(obj.get("date_range"), dict):
            parsed = _expand_date_range(
                obj["date_range"].get("start") or obj["date_range"].get("date_start"),
                obj["date_range"].get("end") or obj["date_range"].get("date_end"),
            )
            if parsed:
                return parsed
        if isinstance(obj.get("date_ranges"), list):
            merged = []
            for item in obj["date_ranges"]:
                if not isinstance(item, dict):
                    continue
                parsed = _expand_date_range(
                    item.get("start") or item.get("date_start"),
                    item.get("end") or item.get("date_end"),
                )
                for d in parsed:
                    if d not in merged:
                        merged.append(d)
            if merged:
                return merged
        if isinstance(obj.get("day_offsets"), list):
            offs = []
            for v in obj["day_offsets"]:
                try:
                    offs.append(int(v))
                except Exception:
                    continue
            offs = list(dict.fromkeys(offs))
            return [(today + datetime.timedelta(days=o)).isoformat() for o in offs]
    return []


def _has_explicit_date_selection(args) -> bool:
    return bool(
        args.date
        or args.dates
        or args.date_start
        or args.date_end
        or args.date_offsets
    )


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _prepare_public_export_rows(rows: list[dict]) -> list[dict]:
    """
    Redact connector/source internals from public-facing CSV/JSON exports.
    Local DB/raw-meta still retains full details.
    """
    if not _truthy_env("PUBLIC_EXPORT_REDACT_SOURCES", default=True):
        return rows
    redacted = []
    drop_keys = {
        "source_endpoint",
        "penalty_source",
        "raw_offer",
        "raw_response",
    }
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        redacted.append({k: v for k, v in row.items() if k not in drop_keys})
    return redacted


def _load_schedule_date_defaults(path: Path) -> dict:
    """
    Supported schedule.json shape (only relevant keys shown):
    {
      "auto_run_date_ranges": {
        "run_all": {
          "date_ranges": [{"start":"2026-03-10","end":"2026-03-20"}],
          "date_start": "2026-03-10",
          "date_end": "2026-03-20",
          "dates": ["2026-03-10","2026-03-11"],
          "date_offsets": [0,1,2],
          "dates_file": "config/dates.json"
        },
        "run_pipeline": { ...same keys... },
        "default": { ...same keys... }
      }
    }
    """
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.warning("Failed to parse schedule file %s for date defaults: %s", path, exc)
        return {}

    if not isinstance(obj, dict):
        return {}

    root = obj.get("auto_run_date_ranges")
    if not isinstance(root, dict):
        return {}

    merged = {}
    # Backward-compatible section names:
    # - "run_all" (existing)
    # - "accumulation" (preferred user-facing term)
    for key in ("default", "run_all", "accumulation"):
        section = root.get(key)
        if isinstance(section, dict):
            merged.update(section)
    return merged


def _collect_schedule_dates_union_run_all(schedule_defaults: dict) -> list[str]:
    today = datetime.datetime.now(datetime.timezone.utc).date()
    merged: list[str] = []

    def _add_many(values: list[str]):
        for v in values:
            if v and v not in merged:
                merged.append(v)

    if schedule_defaults.get("date"):
        _add_many(_parse_iso_date_list([schedule_defaults.get("date")]))
    if schedule_defaults.get("dates"):
        dates_val = schedule_defaults.get("dates")
        if isinstance(dates_val, list):
            _add_many(_parse_iso_date_list(dates_val))
        else:
            _add_many(_parse_iso_date_list(str(dates_val).split(",")))

    ds = schedule_defaults.get("date_start")
    de = schedule_defaults.get("date_end")
    if ds and de:
        _add_many(_expand_date_range(ds, de))
    elif ds or de:
        _add_many(_parse_iso_date_list([ds or de]))

    date_ranges = schedule_defaults.get("date_ranges")
    if isinstance(date_ranges, list):
        for item in date_ranges:
            if not isinstance(item, dict):
                continue
            _add_many(
                _expand_date_range(
                    item.get("start") or item.get("date_start"),
                    item.get("end") or item.get("date_end"),
                )
            )

    offs = schedule_defaults.get("date_offsets")
    if isinstance(offs, list):
        parsed_offs = []
        for v in offs:
            try:
                parsed_offs.append(int(v))
            except Exception:
                continue
        _add_many([(today + datetime.timedelta(days=o)).isoformat() for o in parsed_offs])
    elif isinstance(offs, str) and offs.strip():
        parsed_offs = _parse_offsets(offs)
        _add_many([(today + datetime.timedelta(days=o)).isoformat() for o in parsed_offs])

    dates_file = schedule_defaults.get("dates_file")
    if dates_file:
        _add_many(_load_dates_from_file(Path(str(dates_file)), today=today))

    return sorted(merged)


def _apply_schedule_date_defaults_run_all(args) -> None:
    if _has_explicit_date_selection(args):
        return
    schedule_defaults = _load_schedule_date_defaults(Path(args.schedule_file))
    if not schedule_defaults:
        return

    if bool(schedule_defaults.get("combine")):
        combined = _collect_schedule_dates_union_run_all(schedule_defaults)
        if combined:
            args.dates = ",".join(combined)
            LOG.info(
                "Applied auto-run date defaults from %s with combine=true: dates=%s",
                args.schedule_file,
                combined,
            )
            return

    applied = []
    for attr in ("date", "date_start", "date_end", "dates", "dates_file"):
        if getattr(args, attr, None):
            continue
        v = schedule_defaults.get(attr)
        if v in (None, "", []):
            continue
        setattr(args, attr, str(v) if attr != "dates" else (",".join(v) if isinstance(v, list) else str(v)))
        applied.append(f"{attr}={getattr(args, attr)}")

    if not getattr(args, "date_offsets", None):
        offs = schedule_defaults.get("date_offsets")
        if isinstance(offs, list) and offs:
            try:
                args.date_offsets = ",".join(str(int(x)) for x in offs)
                applied.append(f"date_offsets={args.date_offsets}")
            except Exception:
                pass
        elif isinstance(offs, str) and offs.strip():
            args.date_offsets = offs.strip()
            applied.append(f"date_offsets={args.date_offsets}")

    if applied:
        LOG.info("Applied auto-run date defaults from %s: %s", args.schedule_file, ", ".join(applied))

def build_current_snapshot(rows):
    snapshot = {}
    for r in rows:
        normalized = dict(r)
        identity = _flight_offer_identity_key(
            airline=r.get("airline"),
            origin=r.get("origin"),
            destination=r.get("destination"),
            departure=r.get("departure"),
            flight_number=r.get("flight_number"),
            cabin=r.get("cabin"),
            fare_basis=r.get("fare_basis"),
            brand=r.get("brand"),
        )
        # Normalize departure key format to match DB-derived snapshots.
        normalized["departure"] = identity[3]
        snapshot[identity] = normalized
    return snapshot


def preload_previous_snapshots(
    *,
    session,
    current_scrape_id,
    airline: str,
    origin: str,
    destination: str,
    cabin: str,
    departure_days: list[str],
):
    parsed_days = []
    for d in departure_days:
        try:
            parsed_days.append(datetime.date.fromisoformat(str(d)))
        except Exception:
            continue
    if not parsed_days:
        return {}

    min_day = min(parsed_days)
    max_day = max(parsed_days) + datetime.timedelta(days=1)
    wanted_days = {d.isoformat() for d in parsed_days}

    sql = text(
        """
        WITH ranked AS (
            SELECT
                fo.airline,
                fo.origin,
                fo.destination,
                fo.departure,
                fo.flight_number,
                fo.cabin,
                fo.fare_basis,
                fo.brand,
                fo.price_total_bdt,
                fo.seat_available,
                fo.seat_capacity,
                fo.scraped_at,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        fo.airline,
                        fo.origin,
                        fo.destination,
                        fo.departure,
                        fo.flight_number,
                        fo.cabin,
                        COALESCE(fo.fare_basis, ''),
                        COALESCE(fo.brand, '')
                    ORDER BY fo.scraped_at DESC, fo.id DESC
                ) AS rn
            FROM flight_offers fo
            WHERE fo.airline = :airline
              AND fo.origin = :origin
              AND fo.destination = :destination
              AND fo.cabin = :cabin
              AND fo.scrape_id <> :current_scrape_id
              AND fo.departure >= :min_day
              AND fo.departure < :max_day
        )
        SELECT
            airline,
            origin,
            destination,
            departure,
            flight_number,
            cabin,
            fare_basis,
            brand,
            price_total_bdt,
            seat_available,
            seat_capacity,
            scraped_at
        FROM ranked
        WHERE rn = 1
        """
    )

    rows = session.execute(
        sql,
        {
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "cabin": cabin,
            "current_scrape_id": current_scrape_id,
            "min_day": datetime.datetime.combine(min_day, datetime.time.min),
            "max_day": datetime.datetime.combine(max_day, datetime.time.min),
        },
    ).mappings().all()

    by_day = {d: {} for d in wanted_days}
    for r in rows:
        dep = r.get("departure")
        dep_day = None
        if isinstance(dep, datetime.datetime):
            dep_day = dep.date().isoformat()
        elif dep is not None:
            try:
                dep_day = datetime.datetime.fromisoformat(str(dep)).date().isoformat()
            except Exception:
                dep_day = None
        if dep_day not in by_day:
            continue

        dep_key = _flight_offer_identity_key(
            airline=r.get("airline"),
            origin=r.get("origin"),
            destination=r.get("destination"),
            departure=r.get("departure"),
            flight_number=r.get("flight_number"),
            cabin=r.get("cabin"),
            fare_basis=r.get("fare_basis"),
            brand=r.get("brand"),
        )[3]
        key = (
            r.get("airline"),
            r.get("origin"),
            r.get("destination"),
            dep_key,
            r.get("flight_number"),
            r.get("cabin"),
            r.get("fare_basis"),
            r.get("brand"),
        )
        row_dict = dict(r)
        row_dict["departure"] = dep_key
        by_day[dep_day][key] = row_dict

    return by_day

def load_airlines() -> Dict[str, Dict[str, Any]]:
    with AIRLINES_FILE.open("r", encoding="utf-8") as f:
        items = json.load(f)
    airlines = {}
    for a in items:
        if not a.get("enabled", False):
            continue
        code = a["code"]
        airlines[code] = {
            "module": a["module"],
            "throttle": a.get("throttle_per_minute", 30),
            "cabins": a.get("cabin_classes", ["Economy"])
        }
    LOG.info("Enabled airlines: %s", list(airlines.keys()))
    return airlines


def load_routes_for_airline(airline_code: str):
    with ROUTES_FILE.open("r", encoding="utf-8") as f:
        routes = json.load(f)
    # expected schema is list of dicts with 'airline', 'origin', 'destination', optional 'cabins'
    return [r for r in routes if r.get("airline") == airline_code]


def resolve_route_cabins(route: Dict[str, Any], airline_cfg: Dict[str, Any]) -> list[str]:
    """
    Resolve effective cabins per route with airline-level guardrails.

    Route-level cabin lists may be broader than currently supported connector
    capability. We intersect route cabins with airline-config cabins to avoid
    unsupported queries.
    """
    airline_cabins = [str(c).strip() for c in (airline_cfg.get("cabins") or ["Economy"]) if str(c).strip()]
    if not airline_cabins:
        airline_cabins = ["Economy"]

    route_cabins = route.get("cabins")
    if not isinstance(route_cabins, list) or not route_cabins:
        return airline_cabins

    allowed = {c.lower(): c for c in airline_cabins}
    resolved: list[str] = []
    for c in route_cabins:
        key = str(c).strip().lower()
        if not key or key not in allowed:
            continue
        canonical = allowed[key]
        if canonical not in resolved:
            resolved.append(canonical)

    return resolved or airline_cabins


def audit_route_config(
    *,
    airlines_enabled: Dict[str, Dict[str, Any]],
    all_enabled_airline_codes=None,
    airport_countries: Dict[str, str],
) -> Dict[str, int]:
    """
    Lightweight configuration audit to catch route coverage / data-quality issues
    before a long accumulation run starts.
    """
    try:
        routes = json.loads(ROUTES_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.warning("Route audit skipped: failed to read %s (%s)", ROUTES_FILE, exc)
        return {
            "duplicate_count": 0,
            "malformed_count": 0,
            "unknown_airline_count": 0,
            "not_selected_airline_count": 0,
            "unknown_airport_count": 0,
            "missing_reverse_count": 0,
            "duplicates_sample": [],
            "malformed_rows_sample": [],
            "unknown_airline_rows_sample": [],
            "not_selected_airline_rows_sample": [],
            "unknown_airports_sample": [],
            "missing_reverse_sample": [],
        }

    if not isinstance(routes, list):
        LOG.warning("Route audit skipped: %s is not a JSON list", ROUTES_FILE)
        return {
            "duplicate_count": 0,
            "malformed_count": 0,
            "unknown_airline_count": 0,
            "not_selected_airline_count": 0,
            "unknown_airport_count": 0,
            "missing_reverse_count": 0,
            "duplicates_sample": [],
            "malformed_rows_sample": [],
            "unknown_airline_rows_sample": [],
            "not_selected_airline_rows_sample": [],
            "unknown_airports_sample": [],
            "missing_reverse_sample": [],
        }

    selected_codes = set(airlines_enabled.keys())
    globally_enabled_codes = (
        set(str(x).upper() for x in (all_enabled_airline_codes or []))
        if all_enabled_airline_codes is not None
        else set(selected_codes)
    )
    duplicate_keys = set()
    seen_keys = set()
    malformed_rows = []
    unknown_airline_rows = []
    not_selected_airline_rows = []
    unknown_airports = []
    missing_reverse = []
    per_airline_count: Dict[str, int] = {}

    # Normalize for reverse-pair checks.
    simple_keys = set()
    for idx, r in enumerate(routes):
        if not isinstance(r, dict):
            malformed_rows.append((idx, "not-an-object"))
            continue

        airline = str(r.get("airline") or "").upper().strip()
        origin = str(r.get("origin") or "").upper().strip()
        destination = str(r.get("destination") or "").upper().strip()
        cabins = r.get("cabins")
        if not airline or not origin or not destination:
            malformed_rows.append((idx, f"missing fields airline/origin/destination: {r}"))
            continue

        if airline not in globally_enabled_codes:
            unknown_airline_rows.append((idx, airline, origin, destination))
        elif airline not in selected_codes:
            not_selected_airline_rows.append((idx, airline, origin, destination))

        if origin not in airport_countries:
            unknown_airports.append((idx, "origin", airline, origin))
        if destination not in airport_countries:
            unknown_airports.append((idx, "destination", airline, destination))

        # Duplicate key includes cabins so same route with different cabin sets is not falsely flagged.
        cabin_key = tuple(cabins) if isinstance(cabins, list) else ()
        full_key = (airline, origin, destination, cabin_key)
        if full_key in seen_keys:
            duplicate_keys.add(full_key)
        else:
            seen_keys.add(full_key)

        simple_keys.add((airline, origin, destination))
        if airline in selected_codes:
            per_airline_count[airline] = per_airline_count.get(airline, 0) + 1

    for airline, origin, destination in sorted(simple_keys):
        if (airline, destination, origin) not in simple_keys:
            missing_reverse.append((airline, origin, destination))

    LOG.info(
        "Route audit: %d rows in %s | enabled-airline routes=%s",
        len(routes),
        ROUTES_FILE,
        ", ".join(f"{a}:{per_airline_count.get(a, 0)}" for a in sorted(selected_codes)),
    )

    if duplicate_keys:
        sample = sorted(list(duplicate_keys))[:10]
        LOG.warning("Route audit: duplicate route entries detected (%d). Sample=%s", len(duplicate_keys), sample)
    if malformed_rows:
        LOG.warning("Route audit: malformed rows detected (%d). Sample=%s", len(malformed_rows), malformed_rows[:10])
    if unknown_airline_rows:
        LOG.warning(
            "Route audit: routes configured for disabled/unknown airlines (%d). Sample=%s",
            len(unknown_airline_rows),
            unknown_airline_rows[:10],
        )
    if not_selected_airline_rows:
        LOG.info(
            "Route audit: routes configured for enabled airlines not selected in this run (%d). Sample=%s",
            len(not_selected_airline_rows),
            not_selected_airline_rows[:10],
        )
    if unknown_airports:
        LOG.warning(
            "Route audit: airports missing in %s (%d). Sample=%s",
            AIRPORT_COUNTRY_FILE,
            len(unknown_airports),
            unknown_airports[:10],
        )
    if missing_reverse:
        # Not always wrong, so keep as INFO. This still catches accidental omissions.
        LOG.info("Route audit: routes missing reverse pair (%d). Sample=%s", len(missing_reverse), missing_reverse[:15])

    return {
        "duplicate_count": len(duplicate_keys),
        "malformed_count": len(malformed_rows),
        "unknown_airline_count": len(unknown_airline_rows),
        "not_selected_airline_count": len(not_selected_airline_rows),
        "unknown_airport_count": len(unknown_airports),
        "missing_reverse_count": len(missing_reverse),
        "duplicates_sample": sorted(list(duplicate_keys))[:10],
        "malformed_rows_sample": malformed_rows[:10],
        "unknown_airline_rows_sample": unknown_airline_rows[:10],
        "not_selected_airline_rows_sample": not_selected_airline_rows[:10],
        "unknown_airports_sample": unknown_airports[:10],
        "missing_reverse_sample": missing_reverse[:15],
    }


def write_route_audit_report(*, route_audit: Dict[str, Any], airlines_enabled: Dict[str, Dict[str, Any]]):
    ts_local = datetime.datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "routes_file": str(ROUTES_FILE),
        "airport_country_file": str(AIRPORT_COUNTRY_FILE),
        "enabled_airlines": sorted(list(airlines_enabled.keys())),
        "audit": route_audit,
    }
    latest = ROUTE_AUDIT_OUTPUT_DIR / "route_audit_report_latest.json"
    run = ROUTE_AUDIT_OUTPUT_DIR / f"route_audit_report_{ts_local}.json"
    try:
        latest.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        run.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        LOG.info("Route audit report written: %s", latest)
    except Exception as exc:
        LOG.warning("Failed to write route audit report JSON: %s", exc)


def _heartbeat_paths(scrape_id) -> tuple[Path, Path]:
    ts_local = datetime.datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    latest = RUN_STATUS_OUTPUT_DIR / "run_all_status_latest.json"
    run = RUN_STATUS_OUTPUT_DIR / f"run_all_status_{ts_local}_{scrape_id}.json"
    return latest, run


def _write_run_status(status: Dict[str, Any], *, latest_path: Path, run_path: Path | None = None) -> None:
    try:
        payload = dict(status)
        payload["written_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # Compatibility aliases for terminology migration (non-breaking).
        if payload.get("scrape_id") and not payload.get("accumulation_run_id"):
            payload["accumulation_run_id"] = payload.get("scrape_id")
        if payload.get("started_at_utc") and not payload.get("accumulation_started_at_utc"):
            payload["accumulation_started_at_utc"] = payload.get("started_at_utc")
        if payload.get("last_query_at_utc") and not payload.get("accumulation_last_query_at_utc"):
            payload["accumulation_last_query_at_utc"] = payload.get("last_query_at_utc")
        payload["accumulation_written_at_utc"] = payload.get("written_at_utc")
        text_out = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
        latest_path.write_text(text_out, encoding="utf-8")
        if run_path is not None:
            run_path.write_text(text_out, encoding="utf-8")
        # Also write accumulation-named heartbeat aliases (same payload, same content).
        acc_latest = latest_path.parent / "run_all_accumulation_status_latest.json"
        acc_run = None
        if run_path is not None:
            acc_run = run_path.parent / run_path.name.replace("run_all_status_", "run_all_accumulation_status_")
        acc_latest.write_text(text_out, encoding="utf-8")
        if acc_run is not None:
            acc_run.write_text(text_out, encoding="utf-8")
    except Exception as exc:
        LOG.debug("Failed to write run heartbeat/status file: %s", exc)


def _call_with_timeout(fn, timeout_seconds: float, *args, **kwargs):
    if timeout_seconds is None or timeout_seconds <= 0:
        return fn(*args, **kwargs)
    ex = ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=float(timeout_seconds))
    except FutureTimeoutError:
        fut.cancel()
        ex.shutdown(wait=False, cancel_futures=True)
        raise
    except Exception:
        ex.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        if fut.done():
            ex.shutdown(wait=False, cancel_futures=True)


def _safe_call_fetch(fetch_fn, origin, dest, dt, cabin, *, timeout_seconds: float | None = None, **fetch_kwargs):
    """Call fetch_fn and guarantee unified contract back; trap exceptions."""
    try:
        try:
            resp = _call_with_timeout(fetch_fn, timeout_seconds, origin, dest, dt, cabin, **fetch_kwargs)
        except TypeError as exc:
            # Backward compatibility with older modules that do not accept passenger mix kwargs.
            if fetch_kwargs and "unexpected keyword argument" in str(exc):
                resp = _call_with_timeout(fetch_fn, timeout_seconds, origin, dest, dt, cabin)
            else:
                raise
    except FutureTimeoutError:
        LOG.warning(
            "[%s->%s %s %s] fetch function timed out after %.1fs (soft-fail)",
            origin, dest, dt, cabin, float(timeout_seconds or 0),
        )
        resp = None
    except Exception as exc:
        LOG.warning("[%s->%s %s %s] fetch function raised an exception (soft-fail): %s", origin, dest, dt, cabin, exc)
        LOG.debug("exception details", exc_info=True)
        resp = None
    # If resp is None or not a dict, normalize
    if not isinstance(resp, dict):
        return {"ok": False, "raw": {}, "originalResponse": None, "rows": []}

    # ensure keys exist
    return {
        "ok": bool(resp.get("ok")),
        "raw": resp.get("raw", resp),
        "originalResponse": resp.get("originalResponse"),
        "rows": resp.get("rows") if isinstance(resp.get("rows"), list) else []
    }


def load_airport_offsets() -> Dict[str, int]:
    offsets = dict(DEFAULT_AIRPORT_UTC_OFFSET_MINUTES)
    if AIRPORT_TZ_FILE.exists():
        try:
            user_map = json.loads(AIRPORT_TZ_FILE.read_text(encoding="utf-8"))
            for k, v in user_map.items():
                try:
                    offsets[str(k).upper()] = int(v)
                except Exception:
                    continue
        except Exception as exc:
            LOG.warning("Failed to load airport timezone config: %s", exc)
    return offsets


def _parse_iso_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        # Handles "YYYY-MM-DDTHH:MM:SS" and offsets.
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _format_offset(minutes: int | None) -> str | None:
    if minutes is None:
        return None
    sign = "+" if minutes >= 0 else "-"
    m = abs(int(minutes))
    hh = m // 60
    mm = m % 60
    return f"{sign}{hh:02d}:{mm:02d}"


def _to_utc(local_dt, airport_code: str | None, airport_offsets: Dict[str, int]):
    if local_dt is None:
        return None, None
    offset_min = airport_offsets.get(str(airport_code or "").upper())
    if offset_min is None:
        return None, None
    tzinfo = datetime.timezone(datetime.timedelta(minutes=offset_min))
    if local_dt.tzinfo is None:
        aware_local = local_dt.replace(tzinfo=tzinfo)
    else:
        aware_local = local_dt
    utc_dt = aware_local.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return utc_dt, _format_offset(offset_min)


def _inventory_confidence(row: dict) -> str:
    if row.get("seat_available") is not None:
        return "reported"
    return "unknown"


def _raw_meta_hash_key(meta: dict) -> str:
    payload = {
        "flight_offer_id": meta.get("flight_offer_id"),
        "currency": meta.get("currency"),
        "fare_amount": meta.get("fare_amount"),
        "tax_amount": meta.get("tax_amount"),
        "baggage": meta.get("baggage"),
        "aircraft": meta.get("aircraft"),
        "equipment_code": meta.get("equipment_code"),
        "duration_min": meta.get("duration_min"),
        "stops": meta.get("stops"),
        "arrival": str(meta.get("arrival")) if meta.get("arrival") is not None else None,
        "booking_class": meta.get("booking_class"),
        "soldout": meta.get("soldout"),
        "adt_count": meta.get("adt_count"),
        "chd_count": meta.get("chd_count"),
        "inf_count": meta.get("inf_count"),
        "probe_group_id": meta.get("probe_group_id"),
        "inventory_confidence": meta.get("inventory_confidence"),
        "departure_utc": str(meta.get("departure_utc")) if meta.get("departure_utc") is not None else None,
        "arrival_utc": str(meta.get("arrival_utc")) if meta.get("arrival_utc") is not None else None,
        "fare_ref_num": meta.get("fare_ref_num"),
        "fare_search_reference": meta.get("fare_search_reference"),
        "source_endpoint": meta.get("source_endpoint"),
        "penalty_source": meta.get("penalty_source"),
        "penalty_currency": meta.get("penalty_currency"),
        "penalty_rule_text": meta.get("penalty_rule_text"),
        "fare_change_fee_before_24h": meta.get("fare_change_fee_before_24h"),
        "fare_change_fee_within_24h": meta.get("fare_change_fee_within_24h"),
        "fare_change_fee_no_show": meta.get("fare_change_fee_no_show"),
        "fare_cancel_fee_before_24h": meta.get("fare_cancel_fee_before_24h"),
        "fare_cancel_fee_within_24h": meta.get("fare_cancel_fee_within_24h"),
        "fare_cancel_fee_no_show": meta.get("fare_cancel_fee_no_show"),
        "fare_changeable": meta.get("fare_changeable"),
        "fare_refundable": meta.get("fare_refundable"),
        "raw_offer": meta.get("raw_offer"),
    }
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _flight_offer_identity_key(
    *,
    airline: str | None,
    origin: str | None,
    destination: str | None,
    departure,
    flight_number,
    cabin: str | None,
    fare_basis: str | None,
    brand: str | None,
) -> Tuple[str, str, str, str, str, str, str, str]:
    dt = _parse_iso_datetime(departure)
    if dt is not None and dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    dep_key = dt.isoformat(sep="T", timespec="seconds") if dt is not None else str(departure or "")
    return (
        str(airline or "").upper(),
        str(origin or "").upper(),
        str(destination or "").upper(),
        dep_key,
        str(flight_number or ""),
        str(cabin or ""),
        str(fare_basis or ""),
        str(brand or ""),
    )


def _flight_offer_identity_key_no_brand(key: Tuple[str, str, str, str, str, str, str, str]):
    return key[:-1]


def _load_inserted_offer_id_maps(
    *,
    session,
    scrape_id,
    airline: str,
    origin: str,
    destination: str,
    cabin: str,
):
    rows = (
        session.query(
            FlightOfferORM.id,
            FlightOfferORM.airline,
            FlightOfferORM.origin,
            FlightOfferORM.destination,
            FlightOfferORM.departure,
            FlightOfferORM.flight_number,
            FlightOfferORM.cabin,
            FlightOfferORM.fare_basis,
            FlightOfferORM.brand,
        )
        .filter(
            FlightOfferORM.scrape_id == scrape_id,
            FlightOfferORM.airline == airline,
            FlightOfferORM.origin == origin,
            FlightOfferORM.destination == destination,
            FlightOfferORM.cabin == cabin,
        )
        .all()
    )

    keyed = {}
    keyed_no_brand = {}
    for r in rows:
        k = _flight_offer_identity_key(
            airline=r.airline,
            origin=r.origin,
            destination=r.destination,
            departure=r.departure,
            flight_number=r.flight_number,
            cabin=r.cabin,
            fare_basis=r.fare_basis,
            brand=r.brand,
        )
        keyed[k] = r.id
        kb = _flight_offer_identity_key_no_brand(k)
        if kb not in keyed_no_brand:
            keyed_no_brand[kb] = r.id
    return keyed, keyed_no_brand



def main():
    args = parse_args()
    args.adt = max(1, int(args.adt or 1))
    args.chd = max(0, int(args.chd or 0))
    args.inf = max(0, int(args.inf or 0))
    _apply_schedule_date_defaults_run_all(args)
    scrape_id = uuid.uuid4()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    scraped_at = now_utc.replace(tzinfo=None)
    init_db(create_tables=True)
    comparison_engine = ComparisonEngine()
    strategy_engine = StrategyEngine()
    airport_offsets = load_airport_offsets()
    airport_countries = load_airport_countries(AIRPORT_COUNTRY_FILE)
    if args.route_scope != "all" and not airport_countries:
        LOG.warning(
            "Route-scope filter is active but airport country mapping is empty (%s).",
            AIRPORT_COUNTRY_FILE,
        )

    LOG.info("Loading configuration...")
    heartbeat_latest, heartbeat_run = _heartbeat_paths(scrape_id)
    run_status = {
        "state": "starting",
        "pid": os.getpid(),
        "scrape_id": str(scrape_id),
        "accumulation_run_id": str(scrape_id),
        "started_at_utc": now_utc.isoformat(),
        "accumulation_started_at_utc": now_utc.isoformat(),
        "query_timeout_seconds": float(args.query_timeout_seconds or 0),
        "search_passengers": {"adt": int(args.adt), "chd": int(args.chd), "inf": int(args.inf)},
        "probe_group_id": (str(args.probe_group_id).strip() if args.probe_group_id else None),
    }
    _write_run_status(run_status, latest_path=heartbeat_latest, run_path=heartbeat_run)
    airlines = load_airlines()
    all_enabled_airline_codes = sorted(list(airlines.keys()))

    selected_airlines = parse_csv_upper_codes(args.airline) if args.airline else []
    if selected_airlines:
        selected_set = set(selected_airlines)
        airlines = {k: v for k, v in airlines.items() if k.upper() in selected_set}
        LOG.info("Airline filter active: %s", ",".join(selected_airlines))

    route_audit = audit_route_config(
        airlines_enabled=airlines,
        all_enabled_airline_codes=all_enabled_airline_codes,
        airport_countries=airport_countries,
    )
    write_route_audit_report(
        route_audit=route_audit,
        airlines_enabled=airlines,
    )
    if args.strict_route_audit:
        fatal_route_issues = (
            route_audit.get("duplicate_count", 0)
            + route_audit.get("malformed_count", 0)
            + route_audit.get("unknown_airline_count", 0)
            + route_audit.get("unknown_airport_count", 0)
        )
        if fatal_route_issues > 0:
            LOG.error(
                "Strict route audit failed: duplicates=%d malformed=%d unknown_airlines=%d unknown_airports=%d. "
                "Fix route config before scraping.",
                route_audit.get("duplicate_count", 0),
                route_audit.get("malformed_count", 0),
                route_audit.get("unknown_airline_count", 0),
                route_audit.get("unknown_airport_count", 0),
            )
            return 2

    if not airlines:
        LOG.error("No active airlines. Nothing to do.")
        return

    today = now_utc.date()
    dates = []
    if args.date:
        dates = _parse_iso_date_list([args.date])
    elif args.dates:
        dates = _parse_iso_date_list(args.dates.split(","))
    elif args.date_start and args.date_end:
        dates = _expand_date_range(args.date_start, args.date_end)
    elif args.date_start or args.date_end:
        single = args.date_start or args.date_end
        dates = _parse_iso_date_list([single])
    elif args.date_offsets:
        offsets = _parse_offsets(args.date_offsets)
        dates = [(today + datetime.timedelta(days=d)).isoformat() for d in offsets]
    else:
        file_dates = _load_dates_from_file(Path(args.dates_file), today=today)
        if file_dates:
            dates = file_dates
        else:
            day_offsets = [0] if args.quick else [0, 3, 5, 7, 15, 30]
            dates = [(today + datetime.timedelta(days=d)).strftime("%Y-%m-%d") for d in day_offsets]

    if not dates:
        LOG.warning("No valid dates resolved from args/config; falling back to today.")
        dates = [today.isoformat()]
    if args.limit_dates and args.limit_dates > 0:
        dates = dates[: args.limit_dates]

    if args.quick:
        LOG.info("Quick mode enabled: using single-day accumulation window.")
    LOG.info("Accumulation dates: %s", dates)
    LOG.info("Route scope: %s (market country=%s)", args.route_scope, args.market_country)
    LOG.info("Accumulation passenger mix: ADT=%d CHD=%d INF=%d", args.adt, args.chd, args.inf)
    if args.probe_group_id:
        LOG.info("Probe group id: %s", args.probe_group_id)

    all_rows = []
    runtime_records = []
    overall_query_completed = 0
    overall_started = time.perf_counter()
    overall_query_total = 0
    for code, cfg in airlines.items():
        routes_preview = load_routes_for_airline(code)
        if args.origin:
            routes_preview = [r for r in routes_preview if str(r.get("origin", "")).upper() == args.origin.strip().upper()]
        if args.destination:
            routes_preview = [r for r in routes_preview if str(r.get("destination", "")).upper() == args.destination.strip().upper()]
        if args.route_scope != "all":
            routes_preview = [
                r for r in routes_preview
                if route_matches_scope(
                    r.get("origin"), r.get("destination"), scope=args.route_scope,
                    airport_countries=airport_countries, market_country=args.market_country,
                )
            ]
        if args.limit_routes and args.limit_routes > 0:
            routes_preview = routes_preview[: args.limit_routes]
        for r in routes_preview:
            cabin_list_preview = resolve_route_cabins(r, cfg)
            if args.cabin:
                cabin_list_preview = [c for c in cabin_list_preview if str(c).lower() == args.cabin.strip().lower()]
            overall_query_total += len(cabin_list_preview) * len(dates)
    run_status.update(
        {
            "state": "running",
            "overall_query_total": overall_query_total,
            "selected_dates": dates,
            "route_scope": args.route_scope,
            "market_country": args.market_country,
            "search_passengers": {"adt": int(args.adt), "chd": int(args.chd), "inf": int(args.inf)},
            "probe_group_id": (str(args.probe_group_id).strip() if args.probe_group_id else None),
        }
    )
    _write_run_status(run_status, latest_path=heartbeat_latest, run_path=heartbeat_run)

    for code, cfg in airlines.items():
        LOG.info("\n=== Airline loaded: %s module: %s ===", code, cfg["module"])
        routes = load_routes_for_airline(code)
        if args.origin:
            routes = [r for r in routes if str(r.get("origin", "")).upper() == args.origin.strip().upper()]
        if args.destination:
            routes = [r for r in routes if str(r.get("destination", "")).upper() == args.destination.strip().upper()]

        if args.route_scope != "all":
            routes = [
                r
                for r in routes
                if route_matches_scope(
                    r.get("origin"),
                    r.get("destination"),
                    scope=args.route_scope,
                    airport_countries=airport_countries,
                    market_country=args.market_country,
                )
            ]
        if args.limit_routes and args.limit_routes > 0:
            routes = routes[: args.limit_routes]

        if not routes:
            LOG.warning("No routes found for airline %s", code)
            continue

        airline_query_total = 0
        airline_route_cabin_pairs = 0
        for r in routes:
            planned_cabins = list(resolve_route_cabins(r, cfg))
            if args.cabin:
                planned_cabins = [c for c in planned_cabins if str(c).lower() == args.cabin.strip().lower()]
            airline_route_cabins = len(planned_cabins)
            airline_route_cabin_pairs += airline_route_cabins
            airline_query_total += airline_route_cabins * len(dates)
        airline_query_completed = 0
        airline_elapsed_total = 0.0
        LOG.info(
            "[%s] Work plan: routes=%d route-cabin-pairs=%d dates=%d planned_queries=%d",
            code,
            len(routes),
            airline_route_cabin_pairs,
            len(dates),
            airline_query_total,
        )

        try:
            mod = importlib.import_module(f"modules.{cfg['module']}")
        except Exception as e:
            LOG.error("Cannot import module for %s: %s", code, e)
            continue

        fetch_fn = getattr(mod, "fetch_flights", None)
        # legacy fallback name
        biman_fn = getattr(mod, "biman_search", None)

        for r in routes:
            origin = r["origin"]
            dest = r["destination"]
            cabin_list = resolve_route_cabins(r, cfg)
            if args.cabin:
                cabin_list = [c for c in cabin_list if str(c).lower() == args.cabin.strip().lower()]
                if not cabin_list:
                    LOG.info("[%s] Skipping %s->%s; cabin filter '%s' not available for this route.", code, origin, dest, args.cabin)
                    continue

            for cabin in cabin_list:
                session_cmp = get_session()
                try:
                    previous_by_day = preload_previous_snapshots(
                        session=session_cmp,
                        current_scrape_id=scrape_id,
                        airline=code,
                        origin=origin,
                        destination=dest,
                        cabin=cabin,
                        departure_days=dates,
                    )
                finally:
                    session_cmp.close()
                LOG.info(
                    "[%s] Prefetched previous snapshots for %s->%s (%s): days=%d",
                    code,
                    origin,
                    dest,
                    cabin,
                    len(previous_by_day),
                )

                for dt in dates:
                    airline_query_completed += 1
                    overall_query_completed += 1
                    run_status.update(
                        {
                            "state": "running",
                            "current_airline": code,
                            "current_origin": origin,
                            "current_destination": dest,
                            "current_date": dt,
                            "current_cabin": cabin,
                            "airline_query_completed": airline_query_completed,
                            "airline_query_total": airline_query_total,
                            "overall_query_completed": overall_query_completed,
                            "overall_query_total": overall_query_total,
                            "phase": "fetching",
                        }
                    )
                    _write_run_status(run_status, latest_path=heartbeat_latest, run_path=heartbeat_run)
                    LOG.info(
                        "[%s] Query %d/%d (overall_completed=%d) %s -> %s on %s (%s)",
                        code,
                        airline_query_completed,
                        airline_query_total,
                        overall_query_completed,
                        origin,
                        dest,
                        dt,
                        cabin,
                    )
                    query_start = time.perf_counter()

                    # 1) Primary attempt: fetch_flights if provided
                    resp = None
                    if callable(fetch_fn):
                        resp = _safe_call_fetch(
                            fetch_fn, origin, dest, dt, cabin,
                            timeout_seconds=args.query_timeout_seconds,
                            airline_code=code,
                            adt=args.adt,
                            chd=args.chd,
                            inf=args.inf,
                        )

                    # 2) If primary failed or not provided, try legacy biman_search fallback
                    if not (resp and resp.get("ok")):
                        if callable(biman_fn):
                            LOG.info("[%s] Primary fetch failed or returned no rows; trying legacy fallback for %s->%s %s (%s).", code, origin, dest, dt, cabin)
                            try:
                                # legacy returns may be (ok, raw_json, status) in some setups
                                result = _call_with_timeout(
                                    biman_fn,
                                    args.query_timeout_seconds,
                                    origin,
                                    dest,
                                    dt,
                                    cabin=cabin,
                                    adt=args.adt,
                                    chd=args.chd,
                                    inf=args.inf,
                                )
                                # If result is tuple-like, try to normalize
                                if isinstance(result, tuple) and (len(result) in (2, 3)):
                                    # (ok, raw_json) or (ok, raw_json, status)
                                    ok = bool(result[0])
                                    raw = result[1] if len(result) >= 2 else {}
                                    original = raw.get("data", {}).get("bookingAirSearch", {}).get("originalResponse") if isinstance(raw, dict) else None
                                    # try parser in run_all (parser available in modules.parser)
                                    rows = []
                                    try:
                                        from modules.parser import extract_offers_from_response
                                        if original:
                                            rows = extract_offers_from_response(original)
                                    except Exception:
                                        rows = []
                                    resp = {"ok": ok, "raw": raw, "originalResponse": original, "rows": rows}
                                elif isinstance(result, dict):
                                    # assume it's already in unified form
                                    resp = {
                                        "ok": bool(result.get("ok")),
                                        "raw": result.get("raw", result),
                                        "originalResponse": result.get("originalResponse"),
                                        "rows": result.get("rows") if isinstance(result.get("rows"), list) else []
                                    }
                                else:
                                    # unknown shape
                                    resp = {"ok": False, "raw": {}, "originalResponse": None, "rows": []}
                            except Exception as exc:
                                LOG.warning("[%s->%s %s %s] legacy fallback raised exception (soft-fail): %s", origin, dest, dt, cabin, exc)
                                LOG.debug("exception details", exc_info=True)
                                resp = {"ok": False, "raw": {}, "originalResponse": None, "rows": []}
                        else:
                            # No fallback available
                            LOG.info(
                                "[%s] Primary fetch returned ok=false and no legacy fallback is defined for module %s; skipping %s->%s %s (%s).",
                                code,
                                cfg["module"],
                                origin,
                                dest,
                                dt,
                                cabin,
                            )
                            if isinstance(resp, dict):
                                # Preserve raw error context from primary fetch for diagnostics.
                                resp = {
                                    "ok": False,
                                    "raw": resp.get("raw", resp),
                                    "originalResponse": resp.get("originalResponse"),
                                    "rows": resp.get("rows") if isinstance(resp.get("rows"), list) else [],
                                }
                            else:
                                resp = {"ok": False, "raw": {}, "originalResponse": None, "rows": []}

                    # At this point resp exists and follows unified contract
                    rows = resp.get("rows", [])
                    elapsed = round(time.perf_counter() - query_start, 4)
                    airline_elapsed_total += elapsed
                    runtime_records.append(
                        {
                            "airline": code,
                            "origin": origin,
                            "destination": dest,
                            "date": dt,
                            "cabin": cabin,
                            "ok": bool(resp.get("ok")),
                            "rows": int(len(rows)),
                            "elapsed_sec": elapsed,
                        }
                    )
                    if airline_query_total > 0:
                        airline_avg = airline_elapsed_total / airline_query_completed
                        airline_remaining = max(0, airline_query_total - airline_query_completed)
                        airline_eta_sec = round(airline_avg * airline_remaining, 1)
                    else:
                        airline_avg = 0.0
                        airline_eta_sec = 0.0
                    overall_elapsed = round(time.perf_counter() - overall_started, 1)
                    LOG.info(
                        "[%s] Progress: %d/%d queries done | last=%.2fs avg=%.2fs | airline_eta=%.1fs | overall_elapsed=%.1fs | rows=%d",
                        code,
                        airline_query_completed,
                        airline_query_total,
                        elapsed,
                        airline_avg,
                        airline_eta_sec,
                        overall_elapsed,
                        len(rows),
                    )
                    run_status.update(
                        {
                            "phase": "post_fetch",
                            "last_query_elapsed_sec": elapsed,
                            "last_query_rows": int(len(rows)),
                            "last_query_ok": bool(resp.get("ok")),
                            "last_query_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        }
                    )
                    _write_run_status(run_status, latest_path=heartbeat_latest, run_path=heartbeat_run)
                    if rows:
                        # ----------------------------
                        # 1. Normalize CORE rows
                        # ----------------------------
                        normalized_core_rows = normalize_for_db(
                            rows,
                            scraped_at=scraped_at,
                            scrape_id=scrape_id
                        )

                        # ----------------------------
                        # 1a. FILTER invalid identity rows (CRITICAL)
                        # ----------------------------
                        core_rows = []
                        valid_core_identity_keys = set()
                        skipped = 0

                        for o in normalized_core_rows:
                            if is_valid_core_offer(o):
                                core_rows.append(o)
                                valid_core_identity_keys.add(
                                    (
                                        o["airline"],
                                        o["origin"],
                                        o["destination"],
                                        o["departure"],
                                        o["flight_number"],
                                        o["cabin"],
                                        o["fare_basis"],
                                        o["brand"],
                                    )
                                )
                            else:
                                skipped += 1
                                LOG.warning(
                                    "Skipping non-flight row (missing identity): airline=%s flight=%s origin=%s dest=%s departure=%s",
                                    o.get("airline"),
                                    o.get("flight_number"),
                                    o.get("origin"),
                                    o.get("destination"),
                                    o.get("departure"),
                                )

                        # ----------------------------
                        # 1b. Insert ONLY valid CORE rows
                        # ----------------------------

                        def dedupe_core_rows(rows):
                            seen = set()
                            out = []
                            for r in rows:
                                key = (
                                    r["scrape_id"],
                                    r["airline"],
                                    r["origin"],
                                    r["destination"],
                                    r["departure"],
                                    r["flight_number"],
                                    r["cabin"],
                                    r.get("fare_basis"),
                                    r.get("brand"),
                                )
                                if key not in seen:
                                    seen.add(key)
                                    out.append(r)
                            return out

                        core_rows = dedupe_core_rows(core_rows)
                        if core_rows:
                            bulk_insert_offers(core_rows)

                        LOG.info(
                            "[%s] CORE normalization: %d valid rows inserted, %d skipped",
                            code,
                            len(core_rows),
                            skipped,
                        )

                        # ----------------------------
                        # 2. Fetch inserted CORE rows
                        # ----------------------------
                        session = get_session()
                        try:
                            raw_meta_to_insert = []
                            raw_meta_matched = 0
                            raw_meta_unmatched = 0
                            offer_id_map, offer_id_map_no_brand = _load_inserted_offer_id_maps(
                                session=session,
                                scrape_id=scrape_id,
                                airline=code,
                                origin=origin,
                                destination=dest,
                                cabin=cabin,
                            )

                            for r in rows:
                                departure_local = _parse_iso_datetime(r.get("departure"))
                                arrival_local = _parse_iso_datetime(r.get("arrival"))
                                departure_utc, departure_tz_offset = _to_utc(
                                    departure_local, r.get("origin"), airport_offsets
                                )
                                arrival_utc, arrival_tz_offset = _to_utc(
                                    arrival_local, r.get("destination"), airport_offsets
                                )

                                raw_offer = r.get("raw_offer") or {}
                                identity = _flight_offer_identity_key(
                                    airline=r.get("airline"),
                                    origin=r.get("origin"),
                                    destination=r.get("destination"),
                                    departure=r.get("departure"),
                                    flight_number=r.get("flight_number"),
                                    cabin=r.get("cabin"),
                                    fare_basis=r.get("fare_basis"),
                                    brand=r.get("brand"),
                                )
                                flight_offer_id = offer_id_map.get(identity)
                                if flight_offer_id is None:
                                    flight_offer_id = offer_id_map_no_brand.get(
                                        _flight_offer_identity_key_no_brand(identity)
                                    )
                                if flight_offer_id is None:
                                    raw_meta_unmatched += 1
                                    continue
                                raw_meta_matched += 1

                                penalty_payload = apply_penalty_inference(
                                    {
                                        "airline": r.get("airline"),
                                        "origin": r.get("origin"),
                                        "destination": r.get("destination"),
                                        "brand": r.get("brand"),
                                        "fare_basis": r.get("fare_basis"),
                                        "penalty_source": r.get("penalty_source") or raw_offer.get("penalty_source"),
                                        "penalty_currency": r.get("penalty_currency") or raw_offer.get("penalty_currency"),
                                        "penalty_rule_text": r.get("penalty_rule_text") or raw_offer.get("penalty_rule_text"),
                                        "fare_change_fee_before_24h": r.get("fare_change_fee_before_24h") if r.get("fare_change_fee_before_24h") is not None else raw_offer.get("fare_change_fee_before_24h"),
                                        "fare_change_fee_within_24h": r.get("fare_change_fee_within_24h") if r.get("fare_change_fee_within_24h") is not None else raw_offer.get("fare_change_fee_within_24h"),
                                        "fare_change_fee_no_show": r.get("fare_change_fee_no_show") if r.get("fare_change_fee_no_show") is not None else raw_offer.get("fare_change_fee_no_show"),
                                        "fare_cancel_fee_before_24h": r.get("fare_cancel_fee_before_24h") if r.get("fare_cancel_fee_before_24h") is not None else raw_offer.get("fare_cancel_fee_before_24h"),
                                        "fare_cancel_fee_within_24h": r.get("fare_cancel_fee_within_24h") if r.get("fare_cancel_fee_within_24h") is not None else raw_offer.get("fare_cancel_fee_within_24h"),
                                        "fare_cancel_fee_no_show": r.get("fare_cancel_fee_no_show") if r.get("fare_cancel_fee_no_show") is not None else raw_offer.get("fare_cancel_fee_no_show"),
                                        "fare_changeable": r.get("fare_changeable") if r.get("fare_changeable") is not None else raw_offer.get("fare_changeable"),
                                        "fare_refundable": r.get("fare_refundable") if r.get("fare_refundable") is not None else raw_offer.get("fare_refundable"),
                                    }
                                )

                                raw_meta_to_insert.append({
                                    "flight_offer_id": flight_offer_id,
                                    "currency": r.get("currency"),
                                    "fare_amount": r.get("fare_amount"),
                                    "tax_amount": r.get("tax_amount"),
                                    "baggage": r.get("baggage"),
                                    "aircraft": r.get("aircraft"),
                                    "equipment_code": r.get("equipment_code"),
                                    "duration_min": r.get("duration_min"),
                                    "stops": r.get("stops"),
                                    "arrival": r.get("arrival"),
                                    "estimated_load_factor_pct": r.get("estimated_load_factor_pct"),
                                    "inventory_confidence": r.get("inventory_confidence") or _inventory_confidence(r),
                                    "booking_class": r.get("booking_class"),
                                    "soldout": r.get("soldout"),
                                    "adt_count": r.get("adt_count"),
                                    "chd_count": r.get("chd_count"),
                                    "inf_count": r.get("inf_count"),
                                    "probe_group_id": (str(args.probe_group_id).strip() if args.probe_group_id else None),
                                    "departure_local": departure_local,
                                    "departure_utc": departure_utc,
                                    "departure_tz_offset": departure_tz_offset,
                                    "arrival_utc": arrival_utc,
                                    "arrival_tz_offset": arrival_tz_offset,
                                    "fare_ref_num": r.get("fare_ref_num") or raw_offer.get("fare_ref_num"),
                                    "fare_search_reference": r.get("fare_search_reference") or raw_offer.get("fare_search_reference"),
                                    "source_endpoint": r.get("source_endpoint"),
                                    "penalty_source": penalty_payload.get("penalty_source"),
                                    "penalty_currency": penalty_payload.get("penalty_currency"),
                                    "penalty_rule_text": penalty_payload.get("penalty_rule_text"),
                                    "fare_change_fee_before_24h": penalty_payload.get("fare_change_fee_before_24h"),
                                    "fare_change_fee_within_24h": penalty_payload.get("fare_change_fee_within_24h"),
                                    "fare_change_fee_no_show": penalty_payload.get("fare_change_fee_no_show"),
                                    "fare_cancel_fee_before_24h": penalty_payload.get("fare_cancel_fee_before_24h"),
                                    "fare_cancel_fee_within_24h": penalty_payload.get("fare_cancel_fee_within_24h"),
                                    "fare_cancel_fee_no_show": penalty_payload.get("fare_cancel_fee_no_show"),
                                    "fare_changeable": penalty_payload.get("fare_changeable"),
                                    "fare_refundable": penalty_payload.get("fare_refundable"),
                                    "raw_offer": raw_offer,
                                    "scraped_at": scraped_at,
                                })

                            if raw_meta_to_insert:
                                deduped_raw_meta = []
                                seen_raw_meta = set()
                                for item in raw_meta_to_insert:
                                    hk = _raw_meta_hash_key(item)
                                    if hk in seen_raw_meta:
                                        continue
                                    seen_raw_meta.add(hk)
                                    deduped_raw_meta.append(item)
                                bulk_insert_raw_meta(deduped_raw_meta)
                                raw_meta_to_insert = deduped_raw_meta

                        finally:
                            session.close()


                        LOG.info(
                            "[%s] Persisted %d core rows + %d raw-meta rows (matched=%d unmatched=%d)",
                            code,
                            len(core_rows),
                            len(raw_meta_to_insert),
                            raw_meta_matched,
                            raw_meta_unmatched,
                        )

                        # ----------------------------
                        # 6. Prepare rows for comparison + export (FILTERED)
                        # ----------------------------
                        filtered_rows_for_compare = [
                            r for r in rows
                            if (
                                   r.get("airline"),
                                   r.get("origin"),
                                   r.get("destination"),
                                   r.get("departure"),
                                   r.get("flight_number"),
                                   r.get("cabin"),
                                   r.get("fare_basis"),
                                   r.get("brand"),
                               ) in valid_core_identity_keys
                        ]

                        # Keep only identity-clean rows for export
                        all_rows.extend(filtered_rows_for_compare)

                        # ----------------------------
                        # 7. Comparison logic (UNCHANGED)
                        # ----------------------------
                        previous = previous_by_day.get(dt, {})

                        current = build_current_snapshot(filtered_rows_for_compare)

                        events = comparison_engine.compare(previous, current)
                        if events:
                            save_change_events(events)
                            strategy_engine.process(events)
                        column_events = comparison_engine.compare_column_changes(previous, current)
                        if column_events:
                            saved = save_column_change_events(column_events)
                            LOG.info("[%s] Saved %d column-level change rows", code, saved)
                        run_status.update(
                            {
                                "phase": "query_complete",
                                "total_rows_accumulated": len(all_rows),
                            }
                        )
                        _write_run_status(run_status, latest_path=heartbeat_latest, run_path=heartbeat_run)



                    else:
                        # Friendly message — we don't error out here.
                        # If fetch returned ok=false, include compact reason/hint.
                        reason_bits = []
                        try:
                            raw = resp.get("raw") if isinstance(resp, dict) else {}
                            if isinstance(raw, dict):
                                err = raw.get("error")
                                if err:
                                    reason_bits.append(f"error={err}")
                                msg = raw.get("message")
                                if msg:
                                    reason_bits.append(f"message={msg}")
                                search_body = raw.get("search_response")
                                if isinstance(search_body, dict):
                                    s_msg = search_body.get("message")
                                    if s_msg:
                                        reason_bits.append(f"search_message={s_msg}")
                                    s_err = search_body.get("error")
                                    if isinstance(s_err, dict) and s_err.get("message"):
                                        reason_bits.append(f"search_error={s_err.get('message')}")
                                hint = raw.get("hint")
                                if hint:
                                    reason_bits.append(f"hint={str(hint)[:120]}")
                        except Exception:
                            pass

                        if reason_bits:
                            LOG.info(
                                "[%s] No rows for %s->%s on %s (%s). Details: %s",
                                code,
                                origin,
                                dest,
                                dt,
                                cabin,
                                " | ".join(reason_bits),
                            )
                        else:
                            LOG.info(
                                "[%s] No rows for %s->%s on %s (%s). This can be normal (none scheduled / sold out / non-operated).",
                                code,
                                origin,
                                dest,
                                dt,
                                cabin,
                            )
                        run_status.update(
                            {
                                "phase": "query_complete",
                                "total_rows_accumulated": len(all_rows),
                            }
                        )
                        _write_run_status(run_status, latest_path=heartbeat_latest, run_path=heartbeat_run)

    # ----------------------------
    # Save results
    # ----------------------------

    csv_path = OUTPUT_DIR / "combined_results.csv"
    json_path = OUTPUT_DIR / "combined_results.json"
    archive_path = OUTPUT_DIR / f"combined_results_{scrape_id}.json"

    if json_path.exists():
        try:
            json_path.rename(archive_path)
            LOG.info("ARCHIVE: %s -> %s", json_path.name, archive_path.name)
        except Exception:
            LOG.debug("Unable to archive previous results (continuing).", exc_info=True)

    export_rows = _prepare_public_export_rows(all_rows)

    # Save JSON
    try:
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(export_rows, f, indent=2)
    except Exception as e:
        LOG.error("Failed to write combined results JSON: %s", e)

    # Save CSV
    if export_rows:
        try:
            import csv
            keys = sorted({k for row in export_rows for k in row.keys()})
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for row in export_rows:
                    # ensure fields match header
                    safe_row = {k: row.get(k, "") for k in keys}
                    w.writerow(safe_row)
            LOG.info("Saved CSV: %s (%d rows)", csv_path, len(export_rows))
        except Exception as e:
            LOG.error("Failed to write CSV: %s", e)
    else:
        LOG.warning("No rows to write.")

    LOG.info("Done. Total rows: %d", len(all_rows))
    run_status.update(
        {
            "state": "completed",
            "phase": "done",
            "completed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "total_rows_accumulated": len(all_rows),
            "overall_query_completed": overall_query_completed,
            "overall_query_total": overall_query_total,
        }
    )
    _write_run_status(run_status, latest_path=heartbeat_latest, run_path=heartbeat_run)

    if args.profile_runtime:
        out_dir = Path(args.profile_output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        profile_ts = datetime.datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")

        by_airline = {}
        by_route = {}
        for r in runtime_records:
            a = r["airline"]
            route = f"{r['airline']}:{r['origin']}-{r['destination']}:{r['cabin']}"
            by_airline.setdefault(a, []).append(r["elapsed_sec"])
            by_route.setdefault(route, []).append(r["elapsed_sec"])

        summary = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "query_count": len(runtime_records),
            "by_airline": {
                k: {
                    "count": len(v),
                    "avg_sec": round(sum(v) / len(v), 4) if v else 0.0,
                    "max_sec": round(max(v), 4) if v else 0.0,
                }
                for k, v in sorted(by_airline.items())
            },
            "slowest_routes": sorted(
                [
                    {
                        "route": k,
                        "count": len(v),
                        "avg_sec": round(sum(v) / len(v), 4) if v else 0.0,
                        "max_sec": round(max(v), 4) if v else 0.0,
                    }
                    for k, v in by_route.items()
                ],
                key=lambda x: x["avg_sec"],
                reverse=True,
            )[:50],
            "records": runtime_records,
        }
        latest = out_dir / "runtime_profile_latest.json"
        run = out_dir / f"runtime_profile_{profile_ts}.json"
        latest.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        run.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        LOG.info("Runtime profile written: %s", latest)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Best-effort final heartbeat for manual interruption.
        try:
            latest = RUN_STATUS_OUTPUT_DIR / "run_all_status_latest.json"
            if latest.exists():
                data = json.loads(latest.read_text(encoding="utf-8"))
                data["state"] = "interrupted"
                data["phase"] = "interrupted"
                data["interrupted_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                latest.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        except Exception:
            pass
        raise
