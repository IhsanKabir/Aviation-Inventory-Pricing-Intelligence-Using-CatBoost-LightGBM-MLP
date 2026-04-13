import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

from db import DATABASE_URL as DEFAULT_DATABASE_URL
from engines.route_scope import (
    load_airport_countries,
    parse_csv_upper_codes,
    route_matches_scope,
)


VIEW_MAP = {
    "price_changes_daily": "airline_intel.vw_price_changes_daily",
    "availability_changes_daily": "airline_intel.vw_availability_changes_daily",
    "route_airline_summary": "airline_intel.vw_route_airline_summary",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Generate reports from reporting views")
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", help="YYYY-MM-DD")
    parser.add_argument("--airline", help="Filter airline code(s), comma-separated (e.g., BG,VQ)")
    parser.add_argument("--origin", help="Filter origin airport")
    parser.add_argument("--destination", help="Filter destination airport")
    parser.add_argument("--cabin", help="Filter cabin")
    parser.add_argument("--trip-type", choices=["OW", "RT"], help="Filter route monitor to one-way or round-trip captures")
    parser.add_argument("--return-date", help="Exact requested return date (YYYY-MM-DD) for RT route monitor")
    parser.add_argument("--return-date-start", help="Requested return date lower bound (YYYY-MM-DD)")
    parser.add_argument("--return-date-end", help="Requested return date upper bound (YYYY-MM-DD)")
    parser.add_argument("--route-scope", choices=["all", "domestic", "international"], default="all")
    parser.add_argument("--market-country", default="BD")
    parser.add_argument(
        "--output-dir",
        default="output/reports",
        help="Output directory for CSV files (default: output/reports)",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL),
        help="Postgres SQLAlchemy URL",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "xlsx", "both"],
        default="csv",
        help="Report output format (default: csv)",
    )
    parser.add_argument(
        "--timestamp-tz",
        choices=["local", "utc"],
        default="local",
        help="Timezone used for report folder/file timestamp labels",
    )
    parser.add_argument(
        "--route-monitor",
        action="store_true",
        help="Also generate route_flight_fare_monitor workbook in same run folder",
    )
    parser.add_argument(
        "--route-monitor-macro-xlsm",
        action="store_true",
        help="When --route-monitor is enabled, also export a macro-enabled .xlsm workbook.",
    )
    parser.add_argument(
        "--route-monitor-macro-xlsm-path",
        help="Optional explicit output path for route monitor macro workbook.",
    )
    parser.add_argument(
        "--style",
        choices=["compact", "presentation"],
        default="compact",
        help="Workbook visual style for route monitor outputs (default: compact)",
    )
    return parser.parse_args()


def _airline_codes(args) -> list[str]:
    return parse_csv_upper_codes(getattr(args, "airline", None))


def _apply_airline_clause(clauses: list[str], params: dict, column_sql: str, raw_airline, prefix: str):
    codes = parse_csv_upper_codes(raw_airline)
    if not codes:
        return
    if len(codes) == 1:
        key = f"{prefix}_airline"
        clauses.append(f"{column_sql} = :{key}")
        params[key] = codes[0]
        return
    placeholders = []
    for i, code in enumerate(codes):
        key = f"{prefix}_airline_{i}"
        placeholders.append(f":{key}")
        params[key] = code
    clauses.append(f"{column_sql} IN ({', '.join(placeholders)})")


def _route_scope_filtered_rows(rows: list[dict], args) -> list[dict]:
    if not rows:
        return rows
    airport_countries = load_airport_countries()
    if not airport_countries:
        return rows

    scope = str(getattr(args, "route_scope", "all") or "all").lower()
    market_country = getattr(args, "market_country", "BD")
    airlines = set(_airline_codes(args))
    out = []
    for r in rows:
        airline = str(r.get("airline") or "").upper()
        if airlines and airline and airline not in airlines:
            continue
        if scope != "all":
            if not route_matches_scope(
                r.get("origin"),
                r.get("destination"),
                scope=scope,
                airport_countries=airport_countries,
                market_country=market_country,
            ):
                continue
        out.append(r)
    return out


def _build_where_clause(args):
    clauses = []
    params = {}

    if args.start_date:
        clauses.append("report_day >= :start_date")
        params["start_date"] = args.start_date
    if args.end_date:
        clauses.append("report_day <= :end_date")
        params["end_date"] = args.end_date
    _apply_airline_clause(clauses, params, "airline", args.airline, "rep")
    if args.origin:
        clauses.append("origin = :origin")
        params["origin"] = args.origin.upper()
    if args.destination:
        clauses.append("destination = :destination")
        params["destination"] = args.destination.upper()
    if args.cabin:
        clauses.append("cabin = :cabin")
        params["cabin"] = args.cabin

    if not clauses:
        clauses.append("report_day = CURRENT_DATE")

    where_sql = " WHERE " + " AND ".join(clauses)
    return where_sql, params


def _write_csv(path: Path, columns: list[str], rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _build_run_stamp(timestamp_tz: str):
    if timestamp_tz == "utc":
        now = datetime.now(timezone.utc)
    else:
        now = datetime.now().astimezone()

    ts = now.strftime("%Y%m%d_%H%M%S_%f")
    tz = now.strftime("%z") or "0000"
    if tz.startswith("+"):
        tz_token = f"UTCp{tz[1:]}"
    elif tz.startswith("-"):
        tz_token = f"UTCm{tz[1:]}"
    else:
        tz_token = f"UTC{tz}"
    return now, ts, tz_token


def _fetch_reports(conn, where_sql: str, params: dict, args=None):
    report_payload = {}
    for report_name, view_name in VIEW_MAP.items():
        sql = text(
            f"""
            SELECT *
            FROM {view_name}
            {where_sql}
            ORDER BY report_day DESC, airline, origin, destination, cabin
            """
        )
        result = conn.execute(sql, params)
        columns = list(result.keys())
        rows = [dict(r) for r in result.mappings().all()]
        if args is not None:
            rows = _route_scope_filtered_rows(rows, args)
        report_payload[report_name] = {"columns": columns, "rows": rows}
    return report_payload


def _build_offer_where_clause(args, alias: str = "fo"):
    clauses = []
    params = {}

    if args.start_date:
        clauses.append(f"DATE({alias}.scraped_at) >= :dq_start_date")
        params["dq_start_date"] = args.start_date
    if args.end_date:
        clauses.append(f"DATE({alias}.scraped_at) <= :dq_end_date")
        params["dq_end_date"] = args.end_date
    _apply_airline_clause(clauses, params, f"{alias}.airline", args.airline, "dq")
    if args.origin:
        clauses.append(f"{alias}.origin = :dq_origin")
        params["dq_origin"] = args.origin.upper()
    if args.destination:
        clauses.append(f"{alias}.destination = :dq_destination")
        params["dq_destination"] = args.destination.upper()
    if args.cabin:
        clauses.append(f"{alias}.cabin = :dq_cabin")
        params["dq_cabin"] = args.cabin

    if not clauses:
        clauses.append(f"DATE({alias}.scraped_at) = CURRENT_DATE")

    return " WHERE " + " AND ".join(clauses), params


def _safe_pct(num, den):
    if den in (0, None):
        return None
    return round((float(num) / float(den)) * 100.0, 4)


def _expected_routes_from_config(args):
    routes_path = Path("config/routes.json")
    if not routes_path.exists():
        return set()

    routes = json.loads(routes_path.read_text(encoding="utf-8"))
    expected = set()
    airline_codes = set(_airline_codes(args))
    airport_countries = load_airport_countries()
    for r in routes:
        airline = str(r.get("airline", "")).upper()
        origin = str(r.get("origin", "")).upper()
        destination = str(r.get("destination", "")).upper()
        cabins = [str(c) for c in (r.get("cabins") or [])]

        if airline_codes and airline not in airline_codes:
            continue
        if args.origin and origin != args.origin.upper():
            continue
        if args.destination and destination != args.destination.upper():
            continue
        if args.cabin and cabins and args.cabin not in cabins:
            continue
        if args.route_scope != "all":
            if not route_matches_scope(
                origin,
                destination,
                scope=args.route_scope,
                airport_countries=airport_countries,
                market_country=args.market_country,
            ):
                continue

        expected.add((airline, origin, destination))
    return expected


def _fetch_data_quality_report(conn, args):
    where_sql, params = _build_offer_where_clause(args, alias="fo")

    summary_sql = text(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT fo.scrape_id) AS scrape_count,
            COUNT(DISTINCT fo.airline || '|' || fo.origin || '|' || fo.destination) AS observed_route_count,
            MIN(fo.scraped_at) AS min_scraped_at,
            MAX(fo.scraped_at) AS max_scraped_at
        FROM flight_offers fo
        {where_sql}
        """
    )
    summary = conn.execute(summary_sql, params).mappings().first() or {}
    total_rows = int(summary.get("total_rows") or 0)

    duplicates_sql = text(
        f"""
        WITH keyed AS (
            SELECT
                fo.scrape_id,
                fo.airline,
                fo.origin,
                fo.destination,
                fo.flight_number,
                fo.departure,
                COALESCE(fo.cabin, '') AS cabin,
                COALESCE(fo.fare_basis, '') AS fare_basis,
                COALESCE(fo.brand, '') AS brand,
                COUNT(*) AS row_count
            FROM flight_offers fo
            {where_sql}
            GROUP BY
                fo.scrape_id,
                fo.airline,
                fo.origin,
                fo.destination,
                fo.flight_number,
                fo.departure,
                COALESCE(fo.cabin, ''),
                COALESCE(fo.fare_basis, ''),
                COALESCE(fo.brand, '')
        )
        SELECT
            COALESCE(SUM(CASE WHEN row_count > 1 THEN row_count - 1 ELSE 0 END), 0) AS duplicate_rows,
            COALESCE(SUM(CASE WHEN row_count > 1 THEN 1 ELSE 0 END), 0) AS duplicate_keys
        FROM keyed
        """
    )
    dup = conn.execute(duplicates_sql, params).mappings().first() or {}

    null_core_sql = text(
        f"""
        SELECT
            SUM(CASE WHEN fo.airline IS NULL THEN 1 ELSE 0 END) AS null_airline,
            SUM(CASE WHEN fo.flight_number IS NULL THEN 1 ELSE 0 END) AS null_flight_number,
            SUM(CASE WHEN fo.origin IS NULL THEN 1 ELSE 0 END) AS null_origin,
            SUM(CASE WHEN fo.destination IS NULL THEN 1 ELSE 0 END) AS null_destination,
            SUM(CASE WHEN fo.departure IS NULL THEN 1 ELSE 0 END) AS null_departure,
            SUM(CASE WHEN fo.cabin IS NULL THEN 1 ELSE 0 END) AS null_cabin,
            SUM(CASE WHEN fo.brand IS NULL THEN 1 ELSE 0 END) AS null_brand,
            SUM(CASE WHEN fo.fare_basis IS NULL THEN 1 ELSE 0 END) AS null_fare_basis,
            SUM(CASE WHEN fo.price_total_bdt IS NULL THEN 1 ELSE 0 END) AS null_price_total_bdt,
            SUM(CASE WHEN fo.seat_available IS NULL THEN 1 ELSE 0 END) AS null_seat_available,
            SUM(CASE WHEN fo.seat_capacity IS NULL THEN 1 ELSE 0 END) AS null_seat_capacity
        FROM flight_offers fo
        {where_sql}
        """
    )
    core_nulls = conn.execute(null_core_sql, params).mappings().first() or {}

    raw_sql = text(
        f"""
        SELECT
            COUNT(*) AS rows_seen,
            SUM(CASE WHEN rm.id IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_raw_meta,
            SUM(CASE WHEN rm.currency IS NULL THEN 1 ELSE 0 END) AS null_currency,
            SUM(CASE WHEN rm.fare_amount IS NULL THEN 1 ELSE 0 END) AS null_fare_amount,
            SUM(CASE WHEN rm.tax_amount IS NULL THEN 1 ELSE 0 END) AS null_tax_amount,
            SUM(CASE WHEN rm.baggage IS NULL THEN 1 ELSE 0 END) AS null_baggage,
            SUM(CASE WHEN rm.aircraft IS NULL THEN 1 ELSE 0 END) AS null_aircraft,
            SUM(CASE WHEN rm.booking_class IS NULL THEN 1 ELSE 0 END) AS null_booking_class,
            SUM(CASE WHEN rm.soldout IS NULL THEN 1 ELSE 0 END) AS null_soldout,
            SUM(CASE WHEN rm.adt_count IS NULL THEN 1 ELSE 0 END) AS null_adt_count,
            SUM(CASE WHEN rm.chd_count IS NULL THEN 1 ELSE 0 END) AS null_chd_count,
            SUM(CASE WHEN rm.inf_count IS NULL THEN 1 ELSE 0 END) AS null_inf_count,
            SUM(CASE WHEN rm.inventory_confidence IS NULL THEN 1 ELSE 0 END) AS null_inventory_confidence,
            SUM(CASE WHEN rm.departure_utc IS NULL THEN 1 ELSE 0 END) AS null_departure_utc,
            SUM(CASE WHEN rm.arrival_utc IS NULL THEN 1 ELSE 0 END) AS null_arrival_utc,
            SUM(CASE WHEN rm.source_endpoint IS NULL THEN 1 ELSE 0 END) AS null_source_endpoint,
            SUM(CASE WHEN rm.inventory_confidence = 'reported' THEN 1 ELSE 0 END) AS inventory_reported,
            SUM(CASE WHEN rm.inventory_confidence = 'unknown' THEN 1 ELSE 0 END) AS inventory_unknown
        FROM flight_offers fo
        LEFT JOIN LATERAL (
            SELECT r.*
            FROM flight_offer_raw_meta r
            WHERE r.flight_offer_id = fo.id
            ORDER BY r.id DESC
            LIMIT 1
        ) rm ON TRUE
        {where_sql}
        """
    )
    try:
        raw_meta = conn.execute(raw_sql, params).mappings().first() or {}
    except Exception:
        # Backward compatibility when newer raw_meta columns are not yet migrated.
        fallback_raw_sql = text(
            f"""
            SELECT
                COUNT(*) AS rows_seen,
                SUM(CASE WHEN rm.id IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_raw_meta,
                SUM(CASE WHEN rm.currency IS NULL THEN 1 ELSE 0 END) AS null_currency,
                SUM(CASE WHEN rm.fare_amount IS NULL THEN 1 ELSE 0 END) AS null_fare_amount,
                SUM(CASE WHEN rm.tax_amount IS NULL THEN 1 ELSE 0 END) AS null_tax_amount,
                SUM(CASE WHEN rm.baggage IS NULL THEN 1 ELSE 0 END) AS null_baggage,
                SUM(CASE WHEN rm.aircraft IS NULL THEN 1 ELSE 0 END) AS null_aircraft,
                SUM(CASE WHEN rm.booking_class IS NULL THEN 1 ELSE 0 END) AS null_booking_class,
                SUM(CASE WHEN rm.soldout IS NULL THEN 1 ELSE 0 END) AS null_soldout
            FROM flight_offers fo
            LEFT JOIN LATERAL (
                SELECT r.*
                FROM flight_offer_raw_meta r
                WHERE r.flight_offer_id = fo.id
                ORDER BY r.id DESC
                LIMIT 1
            ) rm ON TRUE
            {where_sql}
            """
        )
        raw_meta = conn.execute(fallback_raw_sql, params).mappings().first() or {}

    observed_route_sql = text(
        f"""
        SELECT DISTINCT fo.airline, fo.origin, fo.destination
        FROM flight_offers fo
        {where_sql}
        """
    )
    observed_routes = {
        (str(r["airline"]).upper(), str(r["origin"]).upper(), str(r["destination"]).upper())
        for r in conn.execute(observed_route_sql, params).mappings().all()
    }
    expected_routes = _expected_routes_from_config(args)

    rows = []
    def add_row(category, metric, value, notes=""):
        rows.append({"category": category, "metric": metric, "value": value, "notes": notes})

    add_row("scope", "start_date", args.start_date or "today(UTC)")
    add_row("scope", "end_date", args.end_date or "today(UTC)")
    add_row("scope", "airline_filter", args.airline or "ALL")
    add_row("scope", "origin_filter", args.origin or "ALL")
    add_row("scope", "destination_filter", args.destination or "ALL")
    add_row("scope", "cabin_filter", args.cabin or "ALL")

    add_row("summary", "total_rows", total_rows)
    add_row("summary", "scrape_count", int(summary.get("scrape_count") or 0))
    add_row("summary", "observed_route_count", int(summary.get("observed_route_count") or 0))
    add_row("summary", "min_scraped_at", summary.get("min_scraped_at"))
    add_row("summary", "max_scraped_at", summary.get("max_scraped_at"))

    duplicate_rows = int(dup.get("duplicate_rows") or 0)
    add_row("duplicates", "duplicate_rows", duplicate_rows)
    add_row("duplicates", "duplicate_keys", int(dup.get("duplicate_keys") or 0))
    add_row("duplicates", "duplicate_row_rate_pct", _safe_pct(duplicate_rows, total_rows))

    core_fields = [
        "airline",
        "flight_number",
        "origin",
        "destination",
        "departure",
        "cabin",
        "brand",
        "fare_basis",
        "price_total_bdt",
        "seat_available",
        "seat_capacity",
    ]
    for f in core_fields:
        v = int(core_nulls.get(f"null_{f}") or 0)
        add_row("null_core", f"{f}_null_count", v)
        add_row("null_core", f"{f}_null_rate_pct", _safe_pct(v, total_rows))

    rows_seen = int(raw_meta.get("rows_seen") or 0)
    rows_with_raw_meta = int(raw_meta.get("rows_with_raw_meta") or 0)
    add_row("raw_meta", "rows_seen", rows_seen)
    add_row("raw_meta", "rows_with_raw_meta", rows_with_raw_meta)
    add_row("raw_meta", "raw_meta_coverage_pct", _safe_pct(rows_with_raw_meta, rows_seen))

    raw_fields = [
        "currency",
        "fare_amount",
        "tax_amount",
        "baggage",
        "aircraft",
        "booking_class",
        "soldout",
        "adt_count",
        "chd_count",
        "inf_count",
        "inventory_confidence",
        "departure_utc",
        "arrival_utc",
        "source_endpoint",
    ]
    for f in raw_fields:
        v = int(raw_meta.get(f"null_{f}") or 0)
        add_row("null_raw_meta", f"{f}_null_count", v)
        add_row("null_raw_meta", f"{f}_null_rate_pct", _safe_pct(v, rows_seen))

    inventory_reported = int(raw_meta.get("inventory_reported") or 0)
    inventory_unknown = int(raw_meta.get("inventory_unknown") or 0)
    add_row("inventory", "reported_count", inventory_reported)
    add_row("inventory", "unknown_count", inventory_unknown)
    add_row("inventory", "reported_rate_pct", _safe_pct(inventory_reported, rows_seen))
    add_row("inventory", "unknown_rate_pct", _safe_pct(inventory_unknown, rows_seen))

    add_row("route_coverage", "expected_route_count", len(expected_routes), "from config/routes.json under active filters")
    add_row("route_coverage", "observed_route_count", len(observed_routes))
    add_row("route_coverage", "coverage_pct", _safe_pct(len(observed_routes), len(expected_routes)) if expected_routes else None)
    missing = sorted(expected_routes - observed_routes)
    add_row("route_coverage", "missing_routes", "; ".join([f"{a}:{o}-{d}" for a, o, d in missing]) if missing else "")

    return {
        "columns": ["category", "metric", "value", "notes"],
        "rows": rows,
    }


def _format_filters(args):
    parts = []
    if args.start_date:
        parts.append(f"start={args.start_date}")
    if args.end_date:
        parts.append(f"end={args.end_date}")
    airline_codes = _airline_codes(args)
    if airline_codes:
        parts.append(f"airline={','.join(airline_codes)}")
    if args.origin:
        parts.append(f"origin={args.origin.upper()}")
    if args.destination:
        parts.append(f"destination={args.destination.upper()}")
    if getattr(args, "route_scope", "all") != "all":
        parts.append(f"route_scope={args.route_scope}")
        parts.append(f"market_country={str(getattr(args, 'market_country', 'BD')).upper()}")
    if args.cabin:
        parts.append(f"cabin={args.cabin}")
    return ", ".join(parts) if parts else "report_day=today(UTC)"


def _sanitize_excel_value(value):
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.isoformat()
    return value


def _sanitize_rows_for_excel(rows: list[dict]):
    cleaned = []
    for row in rows:
        cleaned.append({k: _sanitize_excel_value(v) for k, v in row.items()})
    return cleaned


def _as_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _autofit_sheet(writer, sheet_name: str, df):
    workbook = writer.book
    worksheet = writer.sheets.get(sheet_name)
    if worksheet is None:
        return

    # Basic UX defaults for operational review sheets.
    if len(df.columns) > 0:
        worksheet.autofilter(0, 0, max(len(df), 1), len(df.columns) - 1)
    worksheet.freeze_panes(1, 0)

    header_fmt = workbook.add_format(
        {
            "bold": True,
            "bg_color": "#D9E1F2",
            "border": 1,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True,
        }
    )
    for col_idx, col_name in enumerate(df.columns):
        col_text = str(col_name)
        max_len = len(col_text)
        if not df.empty:
            max_len = max(
                max_len,
                int(
                    df[col_name]
                    .astype(str)
                    .map(len)
                    .clip(upper=80)
                    .max()
                ),
            )
        width = min(max(max_len + 2, 12), 48)
        worksheet.set_column(col_idx, col_idx, width)
        worksheet.write(0, col_idx, col_text, header_fmt)


def _route_label(row: dict):
    airline = str(row.get("airline") or "")
    origin = str(row.get("origin") or "")
    destination = str(row.get("destination") or "")
    cabin = str(row.get("cabin") or "")
    return f"{airline} {origin}-{destination} {cabin}".strip()


def _normalize_for_score(series):
    import pandas as pd

    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    mx = float(s.max()) if len(s) else 0.0
    if mx <= 0:
        return s * 0.0
    return s / mx


def _risk_level(score):
    if score >= 70:
        return "HIGH"
    if score >= 40:
        return "MEDIUM"
    return "LOW"


def _build_route_features(route_df_full, availability_df_full, price_df_full):
    import pandas as pd

    if route_df_full.empty:
        return pd.DataFrame()

    key_cols = ["report_day", "airline", "origin", "destination", "cabin"]
    route_df = route_df_full.copy()

    for col in [
        "total_change_events",
        "flights_affected",
        "price_events",
        "availability_events",
        "added_events",
        "removed_events",
        "changed_events",
    ]:
        if col in route_df.columns:
            route_df[col] = pd.to_numeric(route_df[col], errors="coerce").fillna(0.0)

    if not availability_df_full.empty:
        av = availability_df_full.copy()
        for col in [
            "availability_change_events",
            "soldout_flag_changes",
            "row_added_events",
            "row_removed_events",
            "avg_abs_magnitude",
        ]:
            if col in av.columns:
                av[col] = pd.to_numeric(av[col], errors="coerce").fillna(0.0)
        av_cols = key_cols + [
            c
            for c in [
                "availability_change_events",
                "soldout_flag_changes",
                "row_added_events",
                "row_removed_events",
                "avg_abs_magnitude",
            ]
            if c in av.columns
        ]
        route_df = route_df.merge(av[av_cols], on=key_cols, how="left")

    if not price_df_full.empty:
        pr = price_df_full.copy()
        for col in ["price_change_events", "avg_abs_magnitude", "avg_abs_percent_change"]:
            if col in pr.columns:
                pr[col] = pd.to_numeric(pr[col], errors="coerce").fillna(0.0)
        pr_cols = key_cols + [
            c
            for c in ["price_change_events", "avg_abs_magnitude", "avg_abs_percent_change"]
            if c in pr.columns
        ]
        route_df = route_df.merge(pr[pr_cols], on=key_cols, how="left", suffixes=("", "_price_sheet"))

    if "availability_change_events" not in route_df.columns:
        route_df["availability_change_events"] = route_df.get("availability_events", 0.0)
    route_df["availability_change_events"] = route_df["availability_change_events"].fillna(
        route_df.get("availability_events", 0.0)
    )

    if "price_change_events" not in route_df.columns:
        route_df["price_change_events"] = route_df.get("price_events", 0.0)
    route_df["price_change_events"] = route_df["price_change_events"].fillna(
        route_df.get("price_events", 0.0)
    )

    for col in [
        "soldout_flag_changes",
        "row_added_events",
        "row_removed_events",
        "avg_abs_magnitude",
        "avg_abs_magnitude_price_sheet",
        "avg_abs_percent_change",
    ]:
        if col not in route_df.columns:
            route_df[col] = 0.0
        route_df[col] = pd.to_numeric(route_df[col], errors="coerce").fillna(0.0)

    route_df["route_label"] = route_df.apply(
        lambda r: f"{r.get('airline', '')} {r.get('origin', '')}-{r.get('destination', '')} {r.get('cabin', '')}".strip(),
        axis=1,
    )
    route_df["net_structure_events"] = (
        pd.to_numeric(route_df.get("added_events", 0.0), errors="coerce").fillna(0.0)
        + pd.to_numeric(route_df.get("removed_events", 0.0), errors="coerce").fillna(0.0)
    )

    n_total = _normalize_for_score(route_df["total_change_events"])
    n_avail = _normalize_for_score(route_df["availability_change_events"])
    n_price = _normalize_for_score(route_df["price_change_events"])
    n_structure = _normalize_for_score(route_df["net_structure_events"])
    n_flights = _normalize_for_score(route_df["flights_affected"])
    soldout_boost = (route_df["soldout_flag_changes"] > 0).astype(float) * 15.0

    route_df["risk_score"] = (
        100.0 * (0.35 * n_total + 0.30 * n_avail + 0.20 * n_price + 0.10 * n_structure + 0.05 * n_flights)
        + soldout_boost
    ).clip(0, 100).round(1)

    route_df["risk_level"] = route_df["risk_score"].map(_risk_level)

    def _flags(row):
        flags = []
        if row.get("soldout_flag_changes", 0) > 0:
            flags.append("SOLDOUT")
        if row.get("availability_change_events", 0) > 0:
            flags.append("AVAIL")
        if row.get("price_change_events", 0) > 0:
            flags.append("PRICE")
        if row.get("net_structure_events", 0) > 0:
            flags.append("STRUCT")
        return ", ".join(flags) if flags else "MONITOR"

    route_df["risk_flags"] = route_df.apply(_flags, axis=1)
    return route_df


def _build_action_queue(route_features_df):
    import pandas as pd

    if route_features_df.empty:
        return pd.DataFrame()

    queue = route_features_df.copy()
    queue = queue.sort_values(
        by=["risk_score", "total_change_events", "availability_change_events", "price_change_events"],
        ascending=[False, False, False, False],
    )
    queue = queue.head(40).copy()

    def _recommendation(row):
        if row.get("soldout_flag_changes", 0) > 0:
            return "Check inventory controls and soldout transitions immediately."
        if row.get("availability_change_events", 0) > row.get("price_change_events", 0):
            return "Validate seat/inventory updates and route capacity assumptions."
        if row.get("price_change_events", 0) > 0:
            return "Review fare ladder, competitor pressure, and pricing rules."
        return "Monitor next scrape cycle for persistence."

    queue["recommended_action"] = queue.apply(_recommendation, axis=1)
    cols = [
        "risk_level",
        "risk_score",
        "risk_flags",
        "route_label",
        "total_change_events",
        "availability_change_events",
        "price_change_events",
        "flights_affected",
        "soldout_flag_changes",
        "recommended_action",
    ]
    present = [c for c in cols if c in queue.columns]
    return queue[present]


def _build_airline_summary(route_features_df):
    import pandas as pd

    if route_features_df.empty or "airline" not in route_features_df.columns:
        return pd.DataFrame()

    s = route_features_df.copy()
    out = (
        s.groupby("airline", as_index=False)
        .agg(
            routes=("route_label", "nunique"),
            cabins=("cabin", "nunique"),
            total_change_events=("total_change_events", "sum"),
            availability_change_events=("availability_change_events", "sum"),
            price_change_events=("price_change_events", "sum"),
            high_risk_routes=("risk_level", lambda x: int((x == "HIGH").sum())),
            medium_risk_routes=("risk_level", lambda x: int((x == "MEDIUM").sum())),
            avg_risk_score=("risk_score", "mean"),
            max_risk_score=("risk_score", "max"),
        )
        .sort_values(by=["total_change_events", "max_risk_score"], ascending=[False, False])
    )
    out["avg_risk_score"] = out["avg_risk_score"].round(1)
    out["max_risk_score"] = out["max_risk_score"].round(1)
    return out


def _build_airline_sections(route_features_df):
    import pandas as pd

    if route_features_df.empty or "airline" not in route_features_df.columns:
        return pd.DataFrame()

    s = route_features_df.copy()
    s = s.sort_values(
        by=["airline", "risk_score", "total_change_events"],
        ascending=[True, False, False],
    )
    s["rank_in_airline"] = s.groupby("airline").cumcount() + 1
    s = s[s["rank_in_airline"] <= 15]
    cols = [
        "airline",
        "rank_in_airline",
        "risk_level",
        "risk_score",
        "route_label",
        "total_change_events",
        "availability_change_events",
        "price_change_events",
        "flights_affected",
        "risk_flags",
    ]
    present = [c for c in cols if c in s.columns]
    return s[present]


def _build_route_airline_pivot(route_features_df):
    import pandas as pd

    if route_features_df.empty:
        return pd.DataFrame()
    p = route_features_df.pivot_table(
        index="route_label",
        columns="airline",
        values="total_change_events",
        aggfunc="sum",
        fill_value=0,
    )
    if p.empty:
        return pd.DataFrame()
    p["all_airlines_total"] = p.sum(axis=1)
    p = p.sort_values("all_airlines_total", ascending=False).head(100).reset_index()
    p.columns = [str(c) for c in p.columns]
    return p


def _apply_risk_formats(writer, sheet_name: str, df):
    worksheet = writer.sheets.get(sheet_name)
    if worksheet is None or df.empty:
        return
    workbook = writer.book
    max_row = len(df)

    def _idx(col):
        if col not in df.columns:
            return None
        return int(df.columns.get_loc(col))

    for col in ["risk_score", "total_change_events", "availability_change_events", "price_change_events"]:
        idx = _idx(col)
        if idx is None:
            continue
        worksheet.conditional_format(
            1,
            idx,
            max_row,
            idx,
            {"type": "3_color_scale"},
        )

    risk_idx = _idx("risk_level")
    if risk_idx is not None:
        fmt_high = workbook.add_format({"bg_color": "#F8CBAD", "font_color": "#9C0006", "bold": True})
        fmt_med = workbook.add_format({"bg_color": "#FFF2CC", "font_color": "#7F6000", "bold": True})
        fmt_low = workbook.add_format({"bg_color": "#E2F0D9", "font_color": "#1F4E28", "bold": True})
        worksheet.conditional_format(1, risk_idx, max_row, risk_idx, {"type": "text", "criteria": "containing", "value": "HIGH", "format": fmt_high})
        worksheet.conditional_format(1, risk_idx, max_row, risk_idx, {"type": "text", "criteria": "containing", "value": "MEDIUM", "format": fmt_med})
        worksheet.conditional_format(1, risk_idx, max_row, risk_idx, {"type": "text", "criteria": "containing", "value": "LOW", "format": fmt_low})


def _query_flight_operations(engine, args, limit=5000):
    """Query detailed flight operations data with flight numbers from flight_offers table."""
    import pandas as pd
    from sqlalchemy import text
    
    clauses = []
    params = {"limit": limit}
    
    # Apply filters
    airline_codes = _airline_codes(args)
    if airline_codes:
        if len(airline_codes) == 1:
            clauses.append("fo.airline = :airline")
            params["airline"] = airline_codes[0]
        else:
            placeholders = ", ".join([f":airline_{i}" for i in range(len(airline_codes))])
            clauses.append(f"fo.airline IN ({placeholders})")
            for i, code in enumerate(airline_codes):
                params[f"airline_{i}"] = code
    
    if args.start_date:
        clauses.append("fo.departure::date >= :start_date")
        params["start_date"] = args.start_date
    
    if args.end_date:
        clauses.append("fo.departure::date <= :end_date")
        params["end_date"] = args.end_date
    
    if args.origin:
        clauses.append("fo.origin = :origin")
        params["origin"] = args.origin.upper()
    
    if args.destination:
        clauses.append("fo.destination = :destination")
        params["destination"] = args.destination.upper()
    
    if args.cabin:
        clauses.append("fo.cabin = :cabin")
        params["cabin"] = args.cabin
    
    where_clause = " AND ".join(clauses) if clauses else "1=1"
    
    query = f"""
    SELECT DISTINCT ON (
        fo.airline,
        fo.origin,
        fo.destination,
        fo.flight_number,
        fo.departure::date,
        TO_CHAR(fo.departure, 'HH24:MI')
    )
        fo.airline,
        fo.origin,
        fo.destination,
        CONCAT(fo.origin, '-', fo.destination) AS route,
        fo.flight_number,
        fo.departure::date AS departure_date,
        TO_CHAR(fo.departure, 'HH24:MI') AS departure_time,
        TO_CHAR(fo.departure, 'Day') AS departure_day,
        fo.cabin,
        fo.brand,
        fo.price_total_bdt AS fare_bdt,
        fo.seat_capacity,
        fo.seat_available,
        frm.aircraft,
        frm.equipment_code,
        frm.duration_min,
        frm.stops,
        frm.via_airports,
        frm.booking_class,
        fo.scraped_at AS last_scraped_utc
    FROM flight_offers fo
    LEFT JOIN flight_offer_raw_meta frm ON frm.flight_offer_id = fo.id
    WHERE {where_clause}
    ORDER BY 
        fo.airline,
        fo.origin,
        fo.destination,
        fo.flight_number,
        fo.departure::date,
        TO_CHAR(fo.departure, 'HH24:MI'),
        fo.scraped_at DESC
    LIMIT :limit
    """
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
        return df
    except Exception as e:
        print(f"Warning: Could not query flight operations data: {e}")
        return pd.DataFrame()


def _write_xlsx(workbook_path: Path, report_payload: dict, args, engine=None):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("XLSX export requires pandas in the active environment.") from exc

    workbook_path.parent.mkdir(parents=True, exist_ok=True)

    route_rows = _sanitize_rows_for_excel(report_payload["route_airline_summary"]["rows"])
    availability_rows = _sanitize_rows_for_excel(report_payload["availability_changes_daily"]["rows"])
    price_rows = _sanitize_rows_for_excel(report_payload["price_changes_daily"]["rows"])
    dq_rows = _sanitize_rows_for_excel(report_payload["data_quality_report"]["rows"])

    total_events = sum(r.get("total_change_events", 0) or 0 for r in route_rows)
    total_flights = sum(r.get("flights_affected", 0) or 0 for r in route_rows)
    total_price_events = sum(r.get("price_change_events", 0) or 0 for r in price_rows)
    total_availability_events = sum(
        r.get("availability_change_events", 0) or 0 for r in availability_rows
    )

    route_df_full = pd.DataFrame(route_rows)
    availability_df_full = pd.DataFrame(availability_rows)
    price_df_full = pd.DataFrame(price_rows)
    dq_df_full = pd.DataFrame(dq_rows)
    route_features_df = _build_route_features(route_df_full, availability_df_full, price_df_full)
    action_queue_df = _build_action_queue(route_features_df)
    airline_summary_df = _build_airline_summary(route_features_df)
    airline_sections_df = _build_airline_sections(route_features_df)
    route_airline_pivot_df = _build_route_airline_pivot(route_features_df)

    dashboard_rows = [
        {"metric": "generated_at_utc", "value": datetime.now(timezone.utc).isoformat()},
        {"metric": "filters", "value": _format_filters(args)},
        {"metric": "total_change_events", "value": total_events},
        {"metric": "total_flights_affected", "value": total_flights},
        {"metric": "total_price_change_events", "value": total_price_events},
        {"metric": "total_availability_change_events", "value": total_availability_events},
        {"metric": "rows_price_changes_daily", "value": len(report_payload["price_changes_daily"]["rows"])},
        {
            "metric": "rows_availability_changes_daily",
            "value": len(report_payload["availability_changes_daily"]["rows"]),
        },
        {"metric": "rows_route_airline_summary", "value": len(route_rows)},
        {"metric": "rows_data_quality_report", "value": len(report_payload["data_quality_report"]["rows"])},
    ]

    dashboard_df = pd.DataFrame(dashboard_rows, columns=["metric", "value"])
    top_routes_df = route_features_df.copy()
    if not top_routes_df.empty and "risk_score" in top_routes_df.columns:
        top_routes_df = top_routes_df.sort_values(
            by=["risk_score", "total_change_events"],
            ascending=[False, False],
        ).head(25)
    else:
        top_routes_df = pd.DataFrame()

    top_avail_df = route_features_df.copy()
    if not top_avail_df.empty and "availability_change_events" in top_avail_df.columns:
        top_avail_df = top_avail_df.sort_values(
            by=["availability_change_events", "risk_score"],
            ascending=[False, False],
        ).head(25)
    else:
        top_avail_df = pd.DataFrame()

    top_route_label = ""
    if not top_routes_df.empty:
        top_route_label = _route_label(top_routes_df.iloc[0].to_dict())

    top_avail_label = ""
    if not top_avail_df.empty:
        top_avail_label = _route_label(top_avail_df.iloc[0].to_dict())

    raw_meta_coverage = None
    if not dq_df_full.empty and {"metric", "value"}.issubset(set(dq_df_full.columns)):
        m = dq_df_full[dq_df_full["metric"] == "raw_meta_coverage_pct"]
        if not m.empty:
            raw_meta_coverage = _as_float(m.iloc[0]["value"])

    executive_rows = [
        {"kpi": "generated_at_utc", "value": datetime.now(timezone.utc).isoformat(), "notes": ""},
        {"kpi": "filters", "value": _format_filters(args), "notes": ""},
        {"kpi": "total_change_events", "value": total_events, "notes": "route_airline_summary aggregate"},
        {"kpi": "total_flights_affected", "value": total_flights, "notes": "route_airline_summary aggregate"},
        {"kpi": "total_price_change_events", "value": total_price_events, "notes": "price_changes_daily aggregate"},
        {
            "kpi": "total_availability_change_events",
            "value": total_availability_events,
            "notes": "availability_changes_daily aggregate",
        },
        {"kpi": "top_route_by_change_events", "value": top_route_label, "notes": ""},
        {"kpi": "top_route_by_availability_changes", "value": top_avail_label, "notes": ""},
        {"kpi": "high_risk_routes", "value": int((route_features_df.get("risk_level") == "HIGH").sum()) if not route_features_df.empty else 0, "notes": "risk_score >= 70"},
        {"kpi": "medium_risk_routes", "value": int((route_features_df.get("risk_level") == "MEDIUM").sum()) if not route_features_df.empty else 0, "notes": "40 <= risk_score < 70"},
        {"kpi": "action_queue_items", "value": len(action_queue_df), "notes": "prioritized review list"},
        {
            "kpi": "raw_meta_coverage_pct",
            "value": raw_meta_coverage,
            "notes": "from data_quality_report; null means unavailable",
        },
    ]
    executive_df = pd.DataFrame(executive_rows, columns=["kpi", "value", "notes"])

    with pd.ExcelWriter(workbook_path, engine="xlsxwriter") as writer:
        executive_df.to_excel(writer, sheet_name="executive_summary", index=False)
        dashboard_df.to_excel(writer, sheet_name="dashboard", index=False)

        for report_name, payload in report_payload.items():
            safe_rows = _sanitize_rows_for_excel(payload["rows"])
            report_df = pd.DataFrame(safe_rows, columns=payload["columns"])
            report_df.to_excel(writer, sheet_name=report_name[:31], index=False)

        top_routes_df.to_excel(writer, sheet_name="top_routes", index=False)
        top_avail_df.to_excel(writer, sheet_name="top_availability", index=False)
        action_queue_df.to_excel(writer, sheet_name="action_queue", index=False)
        airline_summary_df.to_excel(writer, sheet_name="airline_summary", index=False)
        airline_sections_df.to_excel(writer, sheet_name="airline_sections", index=False)
        route_airline_pivot_df.to_excel(writer, sheet_name="pivot_route_airline", index=False)

        _autofit_sheet(writer, "executive_summary", executive_df)
        _autofit_sheet(writer, "dashboard", dashboard_df)
        _autofit_sheet(writer, "top_routes", top_routes_df)
        _autofit_sheet(writer, "top_availability", top_avail_df)
        _autofit_sheet(writer, "action_queue", action_queue_df)
        _autofit_sheet(writer, "airline_summary", airline_summary_df)
        _autofit_sheet(writer, "airline_sections", airline_sections_df)
        _autofit_sheet(writer, "pivot_route_airline", route_airline_pivot_df)
        for report_name, payload in report_payload.items():
            report_df = pd.DataFrame(
                _sanitize_rows_for_excel(payload["rows"]),
                columns=payload["columns"],
            )
            _autofit_sheet(writer, report_name[:31], report_df)

        _apply_risk_formats(writer, "top_routes", top_routes_df)
        _apply_risk_formats(writer, "top_availability", top_avail_df)
        _apply_risk_formats(writer, "action_queue", action_queue_df)
        _apply_risk_formats(writer, "airline_sections", airline_sections_df)

        # Add operations sheet with flight numbers
        if engine is not None:
            ops_df = _query_flight_operations(engine, args, limit=5000)
            if not ops_df.empty:
                ops_df.to_excel(writer, sheet_name="operations_flights", index=False)
                _autofit_sheet(writer, "operations_flights", ops_df)
                print(f"operations_flights: {len(ops_df)} rows added to dashboard workbook")
            else:
                print("operations_flights: No flight operations data available")
        else:
            print("operations_flights: Skipped (no database engine provided)")


def export_reports(args):
    engine = create_engine(args.db_url, pool_pre_ping=True, future=True)
    where_sql, params = _build_where_clause(args)
    _, ts, tz_token = _build_run_stamp(args.timestamp_tz)
    output_dir = Path(args.output_dir)
    run_dir = output_dir / f"run_{ts}_{tz_token}"

    exported = []
    warnings = []
    with engine.connect() as conn:
        report_payload = _fetch_reports(conn, where_sql, params, args=args)
        report_payload["data_quality_report"] = _fetch_data_quality_report(conn, args)

    if args.format in {"csv", "both"}:
        for report_name, payload in report_payload.items():
            columns = payload["columns"]
            rows = payload["rows"]
            output_path = run_dir / f"{report_name}_{ts}_{tz_token}.csv"
            _write_csv(output_path, columns, rows)
            exported.append((report_name, output_path, len(rows)))

    if args.format in {"xlsx", "both"}:
        workbook_path = run_dir / f"airline_intel_dashboard_{ts}_{tz_token}.xlsx"
        _write_xlsx(workbook_path, report_payload, args, engine=engine)
        exported.append(("dashboard_workbook", workbook_path, len(report_payload)))

    if args.route_monitor:
        from generate_route_flight_fare_monitor import (
            export_macro_xlsm,
            generate_route_flight_fare_monitor,
        )

        try:
            rm_path, rm_rows, cur_scrape, prev_scrape = generate_route_flight_fare_monitor(
                output_dir=str(output_dir),
                run_dir=str(run_dir),
                timestamp_tz=args.timestamp_tz,
                db_url=args.db_url,
                style=getattr(args, "style", "compact"),
                airline=args.airline,
                origin=args.origin,
                destination=args.destination,
                cabin=args.cabin,
                trip_type=getattr(args, "trip_type", None),
                return_date=getattr(args, "return_date", None),
                return_date_start=getattr(args, "return_date_start", None),
                return_date_end=getattr(args, "return_date_end", None),
                route_scope=getattr(args, "route_scope", "all"),
                market_country=getattr(args, "market_country", "BD"),
            )
            exported.append(
                ("route_flight_fare_monitor", rm_path, rm_rows)
            )
            if getattr(args, "route_monitor_macro_xlsm", False):
                macro_out = export_macro_xlsm(
                    Path(rm_path),
                    Path(args.route_monitor_macro_xlsm_path)
                    if getattr(args, "route_monitor_macro_xlsm_path", None)
                    else None,
                )
                exported.append(("route_flight_fare_monitor_macro", macro_out, rm_rows))
        except RuntimeError as exc:
            warnings.append(f"route_flight_fare_monitor skipped: {exc}")

    # Keep a stable pointer to the latest report run folder.
    latest_txt = output_dir / "latest_run.txt"
    latest_json = output_dir / "latest_run.json"
    latest_txt.write_text(str(run_dir), encoding="utf-8")
    latest_json.write_text(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "timestamp_tz": args.timestamp_tz,
                "exports": [
                    {"name": name, "path": str(path), "rows": rows}
                    for name, path, rows in exported
                ],
                "warnings": warnings,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return exported


def main():
    args = parse_args()
    exported = export_reports(args)
    for report_name, output_path, row_count in exported:
        print(f"{report_name}: {row_count} rows -> {output_path}")


if __name__ == "__main__":
    main()
