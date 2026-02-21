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
from pathlib import Path
from typing import Dict, Any
from comparison_engine import ComparisonEngine
from strategy_engine import StrategyEngine
from sqlalchemy import func
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
OUTPUT_DIR = Path("output/latest")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
    parser = argparse.ArgumentParser(description="Airline scraper runner")
    parser.add_argument("--quick", action="store_true", help="Run fast mode (single day offset: today)")
    parser.add_argument("--airline", help="Filter to a single airline code (e.g., BG)")
    parser.add_argument("--origin", help="Filter routes by origin airport (e.g., DAC)")
    parser.add_argument("--destination", help="Filter routes by destination airport (e.g., CXB)")
    parser.add_argument("--date", help="Run a single departure date in YYYY-MM-DD format")
    parser.add_argument("--dates", help="Comma-separated departure dates in YYYY-MM-DD format")
    parser.add_argument("--date-offsets", help="Comma-separated day offsets from today, e.g. 0,3,7,30")
    parser.add_argument("--dates-file", default="config/dates.json", help="Optional JSON file for dynamic date settings")
    parser.add_argument("--cabin", help="Filter to a single cabin name (e.g., Economy)")
    parser.add_argument("--limit-routes", type=int, help="Process only first N matched routes per airline")
    parser.add_argument("--limit-dates", type=int, help="Process only first N selected dates")
    parser.add_argument("--profile-runtime", action="store_true", help="Write per-search runtime profile")
    parser.add_argument("--profile-output-dir", default="output/reports", help="Runtime profile output directory")
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
    if isinstance(obj, list):
        return _parse_iso_date_list(obj)

    if isinstance(obj, dict):
        if isinstance(obj.get("dates"), list):
            parsed = _parse_iso_date_list(obj["dates"])
            if parsed:
                return parsed
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

def build_current_snapshot(rows):
    snapshot = {}
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
        snapshot[key] = r
    return snapshot


def load_previous_snapshot(session, airline, origin, destination, cabin, departure):
    rows = (
        session.query(FlightOfferORM)
        .filter(
            FlightOfferORM.airline == airline,
            FlightOfferORM.origin == origin,
            FlightOfferORM.destination == destination,
            FlightOfferORM.departure == departure,
            FlightOfferORM.cabin == cabin,
            FlightOfferORM.fare_basis.isnot(None),
        )
        .order_by(
            FlightOfferORM.flight_number,
            FlightOfferORM.scraped_at.desc()
        )
        .all()
    )

    latest = {}

    for r in rows:
        key = (
            r.airline,
            r.origin,
            r.destination,
            r.departure,
            r.flight_number,
            r.cabin,
            r.fare_basis,
            r.brand,
        )

        if key not in latest:
            latest[key] = r

    return latest

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


def _safe_call_fetch(fetch_fn, origin, dest, dt, cabin):
    """Call fetch_fn and guarantee unified contract back; trap exceptions."""
    try:
        resp = fetch_fn(origin, dest, dt, cabin)
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
        "inventory_confidence": meta.get("inventory_confidence"),
        "departure_utc": str(meta.get("departure_utc")) if meta.get("departure_utc") is not None else None,
        "arrival_utc": str(meta.get("arrival_utc")) if meta.get("arrival_utc") is not None else None,
        "fare_ref_num": meta.get("fare_ref_num"),
        "fare_search_reference": meta.get("fare_search_reference"),
        "source_endpoint": meta.get("source_endpoint"),
        "raw_offer": meta.get("raw_offer"),
    }
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()



def main():
    args = parse_args()
    scrape_id = uuid.uuid4()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    scraped_at = now_utc.replace(tzinfo=None)
    init_db(create_tables=True)
    comparison_engine = ComparisonEngine()
    strategy_engine = StrategyEngine()
    airport_offsets = load_airport_offsets()

    LOG.info("Loading configuration...")
    airlines = load_airlines()

    if args.airline:
        selected = args.airline.strip().upper()
        airlines = {k: v for k, v in airlines.items() if k.upper() == selected}
        LOG.info("Airline filter active: %s", selected)

    if not airlines:
        LOG.error("No active airlines. Nothing to do.")
        return

    today = now_utc.date()
    dates = []
    if args.date:
        dates = _parse_iso_date_list([args.date])
    elif args.dates:
        dates = _parse_iso_date_list(args.dates.split(","))
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
        LOG.info("Quick mode enabled: using single-day search window.")
    LOG.info("Searching dates: %s", dates)

    all_rows = []
    runtime_records = []

    for code, cfg in airlines.items():
        LOG.info("\n=== Airline loaded: %s module: %s ===", code, cfg["module"])
        routes = load_routes_for_airline(code)
        if args.origin:
            routes = [r for r in routes if str(r.get("origin", "")).upper() == args.origin.strip().upper()]
        if args.destination:
            routes = [r for r in routes if str(r.get("destination", "")).upper() == args.destination.strip().upper()]
        if args.limit_routes and args.limit_routes > 0:
            routes = routes[: args.limit_routes]

        if not routes:
            LOG.warning("No routes found for airline %s", code)
            continue

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
            cabin_list = r.get("cabins", cfg["cabins"])
            if args.cabin:
                cabin_list = [c for c in cabin_list if str(c).lower() == args.cabin.strip().lower()]
                if not cabin_list:
                    LOG.info("[%s] Skipping %s->%s; cabin filter '%s' not available for this route.", code, origin, dest, args.cabin)
                    continue

            for dt in dates:
                for cabin in cabin_list:
                    LOG.info("[%s] Searching %s -> %s on %s (%s)", code, origin, dest, dt, cabin)
                    query_start = time.perf_counter()

                    # 1) Primary attempt: fetch_flights if provided
                    resp = None
                    if callable(fetch_fn):
                        resp = _safe_call_fetch(fetch_fn, origin, dest, dt, cabin)

                    # 2) If primary failed or not provided, try legacy biman_search fallback
                    if not (resp and resp.get("ok")):
                        if callable(biman_fn):
                            LOG.info("[%s] Primary fetch failed or returned no rows; trying legacy fallback for %s->%s %s (%s).", code, origin, dest, dt, cabin)
                            try:
                                # legacy returns may be (ok, raw_json, status) in some setups
                                result = biman_fn(origin, dest, dt, cabin=cabin)
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
                            LOG.info("[%s] No fetch function or fallback present for module %s; skipping %s->%s %s (%s).", code, cfg["module"], origin, dest, dt, cabin)
                            resp = {"ok": False, "raw": {}, "originalResponse": None, "rows": []}

                    # At this point resp exists and follows unified contract
                    rows = resp.get("rows", [])
                    elapsed = round(time.perf_counter() - query_start, 4)
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
                                core_row = (
                                    session.query(FlightOfferORM)
                                    .filter(
                                        FlightOfferORM.scrape_id == scrape_id,
                                        FlightOfferORM.airline == r.get("airline"),
                                        FlightOfferORM.origin == r.get("origin"),
                                        FlightOfferORM.destination == r.get("destination"),
                                        FlightOfferORM.departure == r.get("departure"),
                                        FlightOfferORM.flight_number == str(r.get("flight_number")),
                                        FlightOfferORM.cabin == r.get("cabin"),
                                        FlightOfferORM.fare_basis == r.get("fare_basis"),
                                        FlightOfferORM.brand == r.get("brand"),
                                    )
                                    .one_or_none()
                                )

                                if not core_row:
                                    continue

                                raw_meta_to_insert.append({
                                    "flight_offer_id": core_row.id,
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
                                    "departure_local": departure_local,
                                    "departure_utc": departure_utc,
                                    "departure_tz_offset": departure_tz_offset,
                                    "arrival_utc": arrival_utc,
                                    "arrival_tz_offset": arrival_tz_offset,
                                    "fare_ref_num": r.get("fare_ref_num") or raw_offer.get("fare_ref_num"),
                                    "fare_search_reference": r.get("fare_search_reference") or raw_offer.get("fare_search_reference"),
                                    "source_endpoint": r.get("source_endpoint"),
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


                        LOG.info("[%s] Persisted %d core rows + %d raw-meta rows",
                                 code, len(core_rows), len(raw_meta_to_insert))

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
                        session_cmp = get_session()
                        departure_dt = datetime.datetime.fromisoformat(dt)
                        try:
                            previous = load_previous_snapshot(
                                session=session_cmp,
                                airline=code,
                                origin=origin,
                                destination=dest,
                                cabin=cabin,
                                departure=departure_dt
                            )
                        finally:
                            session_cmp.close()

                        current = build_current_snapshot(filtered_rows_for_compare)

                        events = comparison_engine.compare(previous, current)
                        if events:
                            save_change_events(events)
                            strategy_engine.process(events)
                        column_events = comparison_engine.compare_column_changes(previous, current)
                        if column_events:
                            saved = save_column_change_events(column_events)
                            LOG.info("[%s] Saved %d column-level change rows", code, saved)



                    else:
                        # Friendly message — we don't error out here.
                        LOG.info("[%s] No rows for %s->%s on %s (%s). This can be normal (none scheduled / sold out / non-operated).", code, origin, dest, dt, cabin)

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

    # Save JSON
    try:
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(all_rows, f, indent=2)
    except Exception as e:
        LOG.error("Failed to write combined results JSON: %s", e)

    # Save CSV
    if all_rows:
        try:
            import csv
            keys = sorted({k for row in all_rows for k in row.keys()})
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for row in all_rows:
                    # ensure fields match header
                    safe_row = {k: row.get(k, "") for k in keys}
                    w.writerow(safe_row)
            LOG.info("Saved CSV: %s (%d rows)", csv_path, len(all_rows))
        except Exception as e:
            LOG.error("Failed to write CSV: %s", e)
    else:
        LOG.warning("No rows to write.")

    LOG.info("Done. Total rows: %d", len(all_rows))

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
    main()
