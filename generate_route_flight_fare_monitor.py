import argparse
import json
import logging
import math
import subprocess
from datetime import datetime, timezone
from pathlib import Path
import re

import pandas as pd
from sqlalchemy import create_engine, text

from db import DATABASE_URL as DEFAULT_DATABASE_URL
from engines.comparison_engine import ComparisonEngine
from engines.excel_comparison_adapter import adapt_comparison_for_excel
from engines.output_writer import OutputWriter
from engines.route_scope import (
    load_airport_countries,
    parse_csv_upper_codes,
    route_matches_scope,
)
from engines.scrape_context import ScrapeContext

LOG = logging.getLogger("route_flight_fare_monitor")


def _normalize_airline_codes(codes):
    out = []
    seen = set()
    for code in codes or []:
        c = str(code or "").strip().upper()
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _dominant_scrape_passenger_mix(engine, scrape_id: str):
    q = text(
        """
        SELECT
            COALESCE(frm.adt_count, 1) AS adt_count,
            COALESCE(frm.chd_count, 0) AS chd_count,
            COALESCE(frm.inf_count, 0) AS inf_count,
            COUNT(*) AS row_count
        FROM flight_offers fo
        JOIN flight_offer_raw_meta frm
          ON frm.flight_offer_id = fo.id
        WHERE fo.scrape_id = :scrape_id
        GROUP BY 1,2,3
        ORDER BY row_count DESC, adt_count, chd_count, inf_count
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(q, {"scrape_id": str(scrape_id)}).fetchone()
    if not row:
        return None
    return {
        "adt": int(row[0] or 0),
        "chd": int(row[1] or 0),
        "inf": int(row[2] or 0),
        "rows": int(row[3] or 0),
    }


def _scrape_airline_stats(engine, scrape_id, airline_codes=None):
    airline_codes = _normalize_airline_codes(airline_codes)
    airline_where = ""
    params = {"scrape_id": str(scrape_id)}
    if airline_codes:
        airline_where = " AND fo.airline = ANY(:airline_codes)"
        params["airline_codes"] = airline_codes

    q = text(
        f"""
        SELECT
            fo.airline,
            COUNT(*) AS row_count,
            COUNT(DISTINCT fo.origin || '->' || fo.destination) AS route_count
        FROM flight_offers fo
        WHERE fo.scrape_id = :scrape_id
          {airline_where}
        GROUP BY fo.airline
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, params).mappings().all()
    return {
        str(r["airline"]).upper(): {
            "row_count": int(r["row_count"] or 0),
            "route_count": int(r["route_count"] or 0),
        }
        for r in rows
    }


def _recent_airline_max_stats(engine, lookback=200, airline_codes=None):
    airline_codes = _normalize_airline_codes(airline_codes)
    airline_filter = ""
    params = {"lookback": int(max(2, lookback))}
    if airline_codes:
        airline_filter = "WHERE airline = ANY(:airline_codes)"
        params["airline_codes"] = airline_codes

    q = text(
        f"""
        WITH recent_scrapes AS (
            SELECT scrape_id
            FROM flight_offers
            GROUP BY scrape_id
            ORDER BY MAX(scraped_at) DESC
            LIMIT :lookback
        ),
        per_scrape_airline AS (
            SELECT
                fo.scrape_id,
                fo.airline,
                COUNT(*) AS row_count,
                COUNT(DISTINCT fo.origin || '->' || fo.destination) AS route_count
            FROM flight_offers fo
            JOIN recent_scrapes rs
              ON rs.scrape_id = fo.scrape_id
            {airline_filter}
            GROUP BY fo.scrape_id, fo.airline
        )
        SELECT
            airline,
            MAX(row_count) AS max_row_count,
            MAX(route_count) AS max_route_count
        FROM per_scrape_airline
        GROUP BY airline
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, params).mappings().all()
    return {
        str(r["airline"]).upper(): {
            "max_row_count": int(r["max_row_count"] or 0),
            "max_route_count": int(r["max_route_count"] or 0),
        }
        for r in rows
    }


def _warn_if_partial_scrape_selection(
    engine,
    *,
    current_scrape,
    previous_scrape,
    airline_codes=None,
    scrape_lookback=40,
    min_full_scrape_rows=100,
    min_full_ratio=0.30,
):
    airline_codes = _normalize_airline_codes(airline_codes)
    baseline = _recent_airline_max_stats(
        engine,
        lookback=max(int(scrape_lookback or 0), 200),
        airline_codes=airline_codes,
    )
    if not baseline:
        return

    current_stats = _scrape_airline_stats(engine, current_scrape, airline_codes=airline_codes)
    previous_stats = _scrape_airline_stats(engine, previous_scrape, airline_codes=airline_codes)
    target_airlines = airline_codes or sorted(baseline.keys())

    floor = int(min_full_scrape_rows or 0)
    ratio = float(min_full_ratio or 0.0)

    def _thresholds(max_rows, max_routes):
        row_threshold = 1
        route_threshold = 1
        if max_rows > 0:
            row_threshold = min(max_rows, max(floor, int(max_rows * ratio)))
        if max_routes > 0:
            route_threshold = min(max_routes, max(1, int(math.ceil(max_routes * ratio))))
        return int(row_threshold), int(route_threshold)

    for label, scrape_id, stats in (
        ("current", current_scrape, current_stats),
        ("previous", previous_scrape, previous_stats),
    ):
        for airline_code in target_airlines:
            base = baseline.get(airline_code)
            if not base:
                continue
            max_rows = int(base.get("max_row_count") or 0)
            max_routes = int(base.get("max_route_count") or 0)
            row_threshold, route_threshold = _thresholds(max_rows, max_routes)
            s = stats.get(airline_code, {})
            rows_now = int(s.get("row_count") or 0)
            routes_now = int(s.get("route_count") or 0)
            if rows_now < row_threshold or routes_now < route_threshold:
                LOG.warning(
                    "Selected %s scrape appears partial for airline %s: scrape_id=%s "
                    "rows=%d (threshold=%d, max_recent=%d) routes=%d (threshold=%d, max_recent=%d).",
                    label,
                    airline_code,
                    scrape_id,
                    rows_now,
                    row_threshold,
                    max_rows,
                    routes_now,
                    route_threshold,
                    max_routes,
                )


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
    return ts, tz_token


def _format_capture_label(ts_value):
    if ts_value is None:
        return ""
    try:
        ts = pd.to_datetime(ts_value, utc=True, errors="coerce")
        if pd.isna(ts):
            return str(ts_value)
        local_tz = datetime.now().astimezone().tzinfo
        return ts.tz_convert(local_tz).strftime("%d %b, %H:%M")
    except Exception:
        return str(ts_value)


def _load_execution_plan_payload(output_dir: Path, run_dir: Path):
    """
    Prefer latest computed execution-plan status artifact; fallback to static
    execution_plan in config/schedule.json so the workbook still reflects
    current strategic order when runtime status file is absent.
    """
    candidates = [
        run_dir / "pipeline_execution_plan_latest.json",
        output_dir / "pipeline_execution_plan_latest.json",
        Path("output/reports/pipeline_execution_plan_latest.json"),
    ]
    for p in candidates:
        if not p.exists():
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(obj, dict) and obj:
            payload = dict(obj)
            payload.setdefault("_source", str(p))
            return payload

    schedule_path = Path("config/schedule.json")
    if not schedule_path.exists():
        return None
    try:
        schedule_obj = json.loads(schedule_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(schedule_obj, dict):
        return None
    plan = schedule_obj.get("execution_plan")
    if not isinstance(plan, dict) or not plan:
        return None

    payload = {
        "generated_at_utc": None,
        "ultimate_priority_goal": plan.get("ultimate_priority_goal"),
        "current_phase": plan.get("current_phase"),
        "phase_sequence": plan.get("phase_sequence"),
        "coverage_summary": {},
        "pipeline_rc": None,
        "recommended_next_phase": plan.get("current_phase"),
        "_source": str(schedule_path),
    }
    return payload


def export_macro_xlsm(input_xlsx: Path, output_xlsm: Path | None = None) -> Path:
    script_path = Path(__file__).resolve().parent / "tools" / "export_route_monitor_xlsm.ps1"
    if not script_path.exists():
        raise RuntimeError(f"Macro export script not found: {script_path}")

    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script_path),
        "-InputXlsx",
        str(input_xlsx),
    ]
    if output_xlsm:
        cmd.extend(["-OutputXlsm", str(output_xlsm)])

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        stderr_tail = (proc.stderr or "").strip()[-1000:]
        stdout_tail = (proc.stdout or "").strip()[-1000:]
        raise RuntimeError(
            "Macro export failed. "
            "If VBA injection is blocked, enable: Excel Trust Center > Macro Settings > "
            "Trust access to the VBA project object model.\n"
            f"stdout_tail:\n{stdout_tail}\n"
            f"stderr_tail:\n{stderr_tail}"
        )

    exported_path = None
    for line in (proc.stdout or "").splitlines():
        if line.strip().lower().startswith("xlsm_exported="):
            exported_path = line.split("=", 1)[1].strip()
            break
    if exported_path:
        out = Path(exported_path)
    else:
        out = output_xlsm if output_xlsm else input_xlsx.with_suffix(".xlsm")

    if not out.exists():
        raise RuntimeError(f"Macro export reported success but output file not found: {out}")
    return out


def _filter_df(
    df: pd.DataFrame,
    airline=None,
    origin=None,
    destination=None,
    cabin=None,
    route_scope: str = "all",
    market_country: str = "BD",
):
    out = df.copy()
    airport_countries = load_airport_countries()
    airline_codes = parse_csv_upper_codes(airline)

    if airline_codes and "airline" in out.columns:
        out = out[out["airline"].astype(str).str.upper().isin(set(airline_codes))]
    if origin and "origin" in out.columns:
        out = out[out["origin"].astype(str).str.upper() == str(origin).upper()]
    if destination and "destination" in out.columns:
        out = out[out["destination"].astype(str).str.upper() == str(destination).upper()]
    if cabin and "cabin" in out.columns:
        out = out[out["cabin"].astype(str) == str(cabin)]
    if route_scope != "all" and {"origin", "destination"}.issubset(set(out.columns)):
        out = out[
            out.apply(
                lambda r: route_matches_scope(
                    r.get("origin"),
                    r.get("destination"),
                    scope=route_scope,
                    airport_countries=airport_countries,
                    market_country=market_country,
                ),
                axis=1,
            )
        ]

    return out


def _prepare_for_writer(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Use flight-level aggregated aircraft label when row-level aircraft is missing.
    if "aircraft_label" in out.columns:
        if "aircraft" not in out.columns:
            out["aircraft"] = pd.NA
        aircraft_blank = out["aircraft"].isna() | (out["aircraft"].astype(str).str.strip() == "")
        out.loc[aircraft_blank, "aircraft"] = out.loc[aircraft_blank, "aircraft_label"]

    numeric_defaults = {
        "seat_delta": 0,
        "min_fare_delta": 0,
        "max_fare_delta": 0,
        "tax_delta": 0,
        "load_delta": 0,
    }
    for col, default in numeric_defaults.items():
        if col not in out.columns:
            out[col] = default
        out[col] = out[col].fillna(default)

    nullable_numeric_cols = ["min_seats", "max_seats", "load_pct", "current_tax", "min_rbd_seats", "max_rbd_seats"]
    for col in nullable_numeric_cols:
        if col not in out.columns:
            out[col] = pd.NA

    string_defaults = {
        "min_rbd": "",
        "max_rbd": "",
        "status": "NORMAL",
        "aircraft": "Aircraft NA",
    }
    for col, default in string_defaults.items():
        if col not in out.columns:
            out[col] = default
        out[col] = out[col].fillna(default)

    return out


def _build_full_capture_history_df(
    engine,
    scoped_df: pd.DataFrame,
    *,
    cabin=None,
    route_scope: str = "all",
    market_country: str = "BD",
) -> pd.DataFrame:
    history_cols = [
        "route",
        "airline",
        "flight_number",
        "flight_date",
        "day_name",
        "departure_time",
        "scrape_id",
        "captured_at_utc",
        "capture_label",
        "previous_capture_label",
        "state_changed_flag",
        "status",
        "min_fare",
        "max_fare",
        "tax_amount",
        "min_seats",
        "max_seats",
        "seat_capacity",
        "load_pct",
        "min_fare_delta",
        "max_fare_delta",
        "tax_amount_delta",
        "min_seats_delta",
        "max_seats_delta",
        "load_pct_delta",
        "offer_rows",
    ]
    if scoped_df is None or scoped_df.empty:
        return pd.DataFrame(columns=history_cols)

    required = {"airline", "origin", "destination", "flight_number", "flight_date", "departure_time"}
    if not required.issubset(set(scoped_df.columns)):
        return pd.DataFrame(columns=history_cols)

    work = scoped_df.copy()
    work["airline"] = work["airline"].astype(str).str.upper()
    work["origin"] = work["origin"].astype(str).str.upper()
    work["destination"] = work["destination"].astype(str).str.upper()
    work["flight_number"] = work["flight_number"].astype(str)
    work["flight_date"] = pd.to_datetime(work["flight_date"], errors="coerce").dt.date
    work["departure_time"] = work["departure_time"].astype(str).str.strip().str.slice(0, 5)
    work.loc[work["departure_time"].isin({"", "None", "nan", "NaT"}), "departure_time"] = pd.NA
    work = work.dropna(subset=["flight_date", "departure_time"])
    if work.empty:
        return pd.DataFrame(columns=history_cols)

    airlines = sorted([a for a in work["airline"].dropna().unique() if str(a).strip()])
    route_pairs = sorted(
        {
            (str(r.origin).upper(), str(r.destination).upper())
            for r in work[["origin", "destination"]].itertuples(index=False)
            if str(r.origin).strip() and str(r.destination).strip()
        }
    )
    if not airlines or not route_pairs:
        return pd.DataFrame(columns=history_cols)

    dep_min = work["flight_date"].min()
    dep_max = work["flight_date"].max()
    if pd.isna(dep_min) or pd.isna(dep_max):
        return pd.DataFrame(columns=history_cols)

    params = {
        "airlines": airlines,
        "dep_start": dep_min,
        "dep_end": dep_max,
    }
    route_terms = []
    for idx, (o, d) in enumerate(route_pairs):
        params[f"o{idx}"] = o
        params[f"d{idx}"] = d
        route_terms.append(f"(fo.origin = :o{idx} AND fo.destination = :d{idx})")
    route_clause = f" AND ({' OR '.join(route_terms)})" if route_terms else ""

    cabin_clause = ""
    if cabin:
        params["cabin"] = str(cabin)
        cabin_clause = " AND fo.cabin = :cabin"

    sql = text(
        f"""
        SELECT
            fo.scrape_id::text AS scrape_id,
            MAX(fo.scraped_at) AS captured_at_utc,
            fo.airline,
            fo.origin,
            fo.destination,
            fo.flight_number,
            fo.departure,
            MIN(fo.price_total_bdt) AS min_fare,
            MAX(fo.price_total_bdt) AS max_fare,
            MIN(fo.seat_available) AS min_seats,
            MAX(fo.seat_available) AS max_seats,
            MAX(fo.seat_capacity) AS seat_capacity,
            MIN(frm.tax_amount) AS tax_amount,
            COUNT(*) AS offer_rows
        FROM flight_offers fo
        LEFT JOIN flight_offer_raw_meta frm
          ON frm.flight_offer_id = fo.id
        WHERE fo.airline = ANY(:airlines)
          AND DATE(fo.departure) BETWEEN :dep_start AND :dep_end
          {route_clause}
          {cabin_clause}
        GROUP BY
            fo.scrape_id,
            fo.airline,
            fo.origin,
            fo.destination,
            fo.flight_number,
            fo.departure
        ORDER BY
            MAX(fo.scraped_at) ASC,
            fo.origin,
            fo.destination,
            fo.airline,
            fo.flight_number,
            fo.departure
        """
    )

    with engine.connect() as conn:
        hist = pd.read_sql(sql, conn, params=params)
    if hist.empty:
        return pd.DataFrame(columns=history_cols)

    hist["airline"] = hist["airline"].astype(str).str.upper()
    hist["origin"] = hist["origin"].astype(str).str.upper()
    hist["destination"] = hist["destination"].astype(str).str.upper()
    hist["route"] = hist["origin"] + "-" + hist["destination"]
    hist["flight_number"] = hist["flight_number"].astype(str)
    hist["captured_at_utc"] = pd.to_datetime(hist["captured_at_utc"], errors="coerce", utc=True)
    hist["flight_date"] = pd.to_datetime(hist["departure"], errors="coerce").dt.date
    hist["departure_time"] = pd.to_datetime(hist["departure"], errors="coerce").dt.strftime("%H:%M")
    hist["day_name"] = pd.to_datetime(hist["flight_date"], errors="coerce").dt.day_name()
    hist["capture_label"] = hist["captured_at_utc"].apply(_format_capture_label)

    for c in ["min_fare", "max_fare", "tax_amount", "min_seats", "max_seats", "seat_capacity", "offer_rows"]:
        hist[c] = pd.to_numeric(hist[c], errors="coerce")

    hist["load_pct"] = pd.NA
    valid = hist["seat_capacity"].notna() & (hist["seat_capacity"] > 0) & hist["min_seats"].notna()
    hist.loc[valid, "load_pct"] = (
        100.0 * (1.0 - (hist.loc[valid, "min_seats"] / hist.loc[valid, "seat_capacity"]))
    ).round(1)

    hist["status"] = "AVAILABLE"
    hist.loc[hist["min_seats"].isna() & hist["min_fare"].isna(), "status"] = "UNKNOWN"
    hist.loc[hist["min_seats"] == 0, "status"] = "SOLD OUT"

    if route_scope != "all":
        airport_countries = load_airport_countries()
        hist = hist[
            hist.apply(
                lambda r: route_matches_scope(
                    r.get("origin"),
                    r.get("destination"),
                    scope=route_scope,
                    airport_countries=airport_countries,
                    market_country=market_country,
                ),
                axis=1,
            )
        ]
        if hist.empty:
            return pd.DataFrame(columns=history_cols)

    key_cols = ["route", "airline", "flight_number", "flight_date", "departure_time"]
    scoped_keys = set(
        work.assign(route=work["origin"] + "-" + work["destination"])[key_cols]
        .itertuples(index=False, name=None)
    )
    hist_keys = list(hist[key_cols].itertuples(index=False, name=None))
    hist = hist[[k in scoped_keys for k in hist_keys]].copy()
    if hist.empty:
        return pd.DataFrame(columns=history_cols)

    group_cols = ["route", "airline", "flight_number", "flight_date", "departure_time"]
    hist = hist.sort_values(group_cols + ["captured_at_utc"], na_position="last")
    delta_cols = ["min_fare", "max_fare", "tax_amount", "min_seats", "max_seats", "load_pct"]
    for c in delta_cols:
        hist[f"{c}_delta"] = hist.groupby(group_cols, dropna=False)[c].diff()
    hist["previous_capture_label"] = hist.groupby(group_cols, dropna=False)["capture_label"].shift(1).fillna("")
    state_delta_cols = [f"{c}_delta" for c in delta_cols]
    delta_view = hist[state_delta_cols].apply(pd.to_numeric, errors="coerce")
    hist["state_changed"] = delta_view.ne(0).fillna(False).any(axis=1)
    hist.loc[hist["previous_capture_label"] == "", "state_changed"] = True
    hist["state_changed_flag"] = hist["state_changed"].map({True: "CHANGED/NEW", False: "NO_CHANGE"})

    hist = hist[
        [
            "route",
            "airline",
            "flight_number",
            "flight_date",
            "day_name",
            "departure_time",
            "scrape_id",
            "captured_at_utc",
            "capture_label",
            "previous_capture_label",
            "state_changed_flag",
            "status",
            "min_fare",
            "max_fare",
            "tax_amount",
            "min_seats",
            "max_seats",
            "seat_capacity",
            "load_pct",
            "min_fare_delta",
            "max_fare_delta",
            "tax_amount_delta",
            "min_seats_delta",
            "max_seats_delta",
            "load_pct_delta",
            "offer_rows",
        ]
    ].copy()

    return hist


def generate_route_flight_fare_monitor(
    output_dir="output/reports",
    run_dir=None,
    timestamp_tz="local",
    db_url=DEFAULT_DATABASE_URL,
    style="compact",
    airline=None,
    origin=None,
    destination=None,
    cabin=None,
    current_scrape_id=None,
    previous_scrape_id=None,
    auto_skip_tiny=True,
    scrape_lookback=40,
    min_full_scrape_rows=100,
    min_full_ratio=0.30,
    route_scope="all",
    market_country="BD",
):
    engine = create_engine(db_url, pool_pre_ping=True, future=True)
    scrape_ctx = ScrapeContext(engine)
    selection_airline_codes = parse_csv_upper_codes(airline)

    if current_scrape_id and previous_scrape_id:
        current_scrape = current_scrape_id
        previous_scrape = previous_scrape_id
    else:
        if auto_skip_tiny:
            current_scrape, previous_scrape = scrape_ctx.get_latest_two_full_scrapes(
                lookback=scrape_lookback,
                min_rows_floor=min_full_scrape_rows,
                min_full_ratio=min_full_ratio,
                airline_codes=selection_airline_codes,
            )
        else:
            current_scrape, previous_scrape = scrape_ctx.get_latest_two_scrapes(
                airline_codes=selection_airline_codes,
            )

    current_mix = _dominant_scrape_passenger_mix(engine, current_scrape)
    previous_mix = _dominant_scrape_passenger_mix(engine, previous_scrape)
    if current_mix and previous_mix:
        curr_sig = (current_mix["adt"], current_mix["chd"], current_mix["inf"])
        prev_sig = (previous_mix["adt"], previous_mix["chd"], previous_mix["inf"])
        if curr_sig != prev_sig:
            LOG.warning(
                "Passenger-mix mismatch between compared scrapes: current=%s previous=%s. "
                "Route monitor comparisons should use same ADT/CHD/INF basis.",
                current_mix,
                previous_mix,
            )
    _warn_if_partial_scrape_selection(
        engine,
        current_scrape=current_scrape,
        previous_scrape=previous_scrape,
        airline_codes=selection_airline_codes,
        scrape_lookback=scrape_lookback,
        min_full_scrape_rows=min_full_scrape_rows,
        min_full_ratio=min_full_ratio,
    )

    cmp_engine = ComparisonEngine(engine)
    comparison_df = cmp_engine.compare_scrapes(
        current_scrape=current_scrape,
        previous_scrape=previous_scrape,
    )
    scrape_time_map = scrape_ctx.get_scrape_time_map([current_scrape, previous_scrape])
    current_capture_label = _format_capture_label(scrape_time_map.get(current_scrape))
    previous_capture_label = _format_capture_label(scrape_time_map.get(previous_scrape))
    final_df = adapt_comparison_for_excel(comparison_df)
    final_df = _filter_df(
        final_df,
        airline=airline,
        origin=origin,
        destination=destination,
        cabin=cabin,
        route_scope=route_scope,
        market_country=market_country,
    )
    final_df = _prepare_for_writer(final_df)
    final_df["current_capture_label"] = current_capture_label or "Current snapshot"
    final_df["previous_capture_label"] = previous_capture_label or "Previous snapshot"

    if final_df.empty:
        raise RuntimeError("No rows available for route_flight_fare_monitor after filters.")

    full_capture_history_df = _build_full_capture_history_df(
        engine,
        final_df,
        cabin=cabin,
        route_scope=route_scope,
        market_country=market_country,
    )

    base_output = Path(output_dir)
    if run_dir:
        target_dir = Path(run_dir)
        m = re.match(r"run_(\d{8}_\d{6}(?:_\d{6})?)_(UTC[pm]\d{4}|UTC\d{4})$", target_dir.name)
        if m:
            ts = m.group(1)
            tz_token = m.group(2)
        else:
            ts, tz_token = _build_run_stamp(timestamp_tz)
    else:
        ts, tz_token = _build_run_stamp(timestamp_tz)
        target_dir = base_output / f"run_{ts}_{tz_token}"

    target_dir.mkdir(parents=True, exist_ok=True)
    execution_plan_payload = _load_execution_plan_payload(base_output, target_dir)

    output_path = target_dir / f"route_flight_fare_monitor_{ts}_{tz_token}.xlsx"
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        OutputWriter(style=style).write_route_flight_fare_monitor(
            writer,
            final_df,
            full_capture_history=full_capture_history_df,
            execution_plan_status=execution_plan_payload,
        )

    return output_path, len(final_df), current_scrape, previous_scrape


def parse_args():
    parser = argparse.ArgumentParser(description="Generate route_flight_fare_monitor workbook")
    parser.add_argument("--output-dir", default="output/reports")
    parser.add_argument("--run-dir", help="Optional existing run folder to write into")
    parser.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    parser.add_argument("--style", choices=["compact", "presentation"], default="compact")
    parser.add_argument("--db-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--airline")
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--cabin")
    parser.add_argument("--route-scope", choices=["all", "domestic", "international"], default="all")
    parser.add_argument("--market-country", default="BD")
    parser.add_argument("--current-scrape-id")
    parser.add_argument("--previous-scrape-id")
    parser.add_argument(
        "--no-auto-skip-tiny",
        action="store_true",
        help="Disable auto-skip logic for tiny test scrapes; use raw latest two scrape IDs.",
    )
    parser.add_argument(
        "--scrape-lookback",
        type=int,
        default=40,
        help="How many recent scrapes to inspect when auto-selecting a full pair (default: 40).",
    )
    parser.add_argument(
        "--min-full-scrape-rows",
        type=int,
        default=100,
        help="Minimum rows for a scrape to be considered full in auto-selection (default: 100).",
    )
    parser.add_argument(
        "--min-full-ratio",
        type=float,
        default=0.30,
        help="Adaptive full threshold ratio vs max rows in lookback (default: 0.30).",
    )
    parser.add_argument(
        "--export-macro-xlsm",
        action="store_true",
        help="Also export a macro-enabled .xlsm workbook with airline/signal filter controls.",
    )
    parser.add_argument(
        "--macro-xlsm-path",
        help="Optional explicit output path for the macro-enabled workbook.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_path, row_count, current_scrape, previous_scrape = generate_route_flight_fare_monitor(
        output_dir=args.output_dir,
        run_dir=args.run_dir,
        timestamp_tz=args.timestamp_tz,
        db_url=args.db_url,
        style=args.style,
        airline=args.airline,
        origin=args.origin,
        destination=args.destination,
        cabin=args.cabin,
        route_scope=args.route_scope,
        market_country=args.market_country,
        current_scrape_id=args.current_scrape_id,
        previous_scrape_id=args.previous_scrape_id,
        auto_skip_tiny=not args.no_auto_skip_tiny,
        scrape_lookback=args.scrape_lookback,
        min_full_scrape_rows=args.min_full_scrape_rows,
        min_full_ratio=args.min_full_ratio,
    )
    msg = (
        "route_flight_fare_monitor: "
        f"rows={row_count} current_scrape={current_scrape} previous_scrape={previous_scrape} -> {output_path}"
    )
    if args.export_macro_xlsm:
        macro_path = export_macro_xlsm(
            output_path,
            Path(args.macro_xlsm_path) if args.macro_xlsm_path else None,
        )
        msg = f"{msg}\nroute_flight_fare_monitor_macro: {macro_path}"
    print(msg)


if __name__ == "__main__":
    main()
