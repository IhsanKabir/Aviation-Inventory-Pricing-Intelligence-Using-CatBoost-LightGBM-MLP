import argparse
import logging
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

    if current_scrape_id and previous_scrape_id:
        current_scrape = current_scrape_id
        previous_scrape = previous_scrape_id
    else:
        if auto_skip_tiny:
            current_scrape, previous_scrape = scrape_ctx.get_latest_two_full_scrapes(
                lookback=scrape_lookback,
                min_rows_floor=min_full_scrape_rows,
                min_full_ratio=min_full_ratio,
            )
        else:
            current_scrape, previous_scrape = scrape_ctx.get_latest_two_scrapes()

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

    cmp_engine = ComparisonEngine(engine)
    comparison_df = cmp_engine.compare_scrapes(
        current_scrape=current_scrape,
        previous_scrape=previous_scrape,
    )
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

    if final_df.empty:
        raise RuntimeError("No rows available for route_flight_fare_monitor after filters.")

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

    output_path = target_dir / f"route_flight_fare_monitor_{ts}_{tz_token}.xlsx"
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        OutputWriter(style=style).write_route_flight_fare_monitor(writer, final_df)

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
    print(f"route_flight_fare_monitor: rows={row_count} current_scrape={current_scrape} previous_scrape={previous_scrape} -> {output_path}")


if __name__ == "__main__":
    main()
