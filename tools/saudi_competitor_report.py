"""
Competitor fare report for Saudi Arabia gateway routes (RUH, JED, DMM, MED).

Queries the DB for all airlines operating to/from BD airports to the four
Saudi gateways, filters to itineraries with transit <= MAX_TRANSIT_HOURS (6h
default), then outputs a terminal table + CSV with the standard format:

  Airline | Aircraft Type | Origin | Destination | Transit |
  Departure | Arrival | Starting Fare | Highest Fare | Average |
  Starting Baggage | Mid Baggage | High Baggage

Transit time is estimated as:
  total_duration_min - DIRECT_LEG_TIMES[origin→transit] - DIRECT_LEG_TIMES[transit→dest]

For direct flights (stops=0) transit is shown as "Direct".

Run:
    python tools/saudi_competitor_report.py
    python tools/saudi_competitor_report.py --days-ahead 30 --max-transit-hours 4
    python tools/saudi_competitor_report.py --origin DAC --dest JED
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db import DATABASE_URL as DEFAULT_DATABASE_URL

BD_AIRPORTS   = ["DAC", "CGP", "ZYL", "CXB", "SPD"]
SAUDI_AIRPORTS = ["RUH", "JED", "DMM", "MED"]

# Approximate direct-flight durations in minutes for common pairs used as
# transit legs.  Used to estimate layover time = total_dur - leg1 - leg2.
# Symmetric pairs are listed once; lookup checks both directions.
DIRECT_LEG_MIN: dict[frozenset, int] = {
    # BD airports to common hubs
    frozenset({"DAC", "DXB"}): 330,
    frozenset({"DAC", "AUH"}): 330,
    frozenset({"DAC", "SHJ"}): 330,
    frozenset({"DAC", "MCT"}): 300,
    frozenset({"DAC", "KHI"}): 165,
    frozenset({"DAC", "DEL"}): 110,
    frozenset({"DAC", "CCU"}): 50,
    frozenset({"DAC", "KTM"}): 80,
    frozenset({"DAC", "CMB"}): 170,
    frozenset({"DAC", "KUL"}): 210,
    frozenset({"DAC", "SIN"}): 240,
    frozenset({"DAC", "HKG"}): 210,
    frozenset({"DAC", "BKK"}): 170,
    frozenset({"DAC", "CGK"}): 250,
    frozenset({"DAC", "ADD"}): 390,
    frozenset({"DAC", "MAA"}): 220,
    frozenset({"DAC", "BOM"}): 195,
    frozenset({"DAC", "KMG"}): 170,
    # BD to Saudi direct (for Saudi-hub connections)
    frozenset({"DAC", "JED"}): 430,
    frozenset({"DAC", "RUH"}): 390,
    frozenset({"DAC", "MED"}): 380,
    frozenset({"DAC", "DMM"}): 380,
    # Common hubs to Saudi
    frozenset({"DXB", "JED"}): 90,
    frozenset({"DXB", "RUH"}): 90,
    frozenset({"DXB", "MED"}): 120,
    frozenset({"DXB", "DMM"}): 80,
    frozenset({"AUH", "JED"}): 90,
    frozenset({"AUH", "RUH"}): 90,
    frozenset({"AUH", "MED"}): 120,
    frozenset({"SHJ", "JED"}): 90,
    frozenset({"SHJ", "RUH"}): 90,
    frozenset({"MCT", "JED"}): 120,
    frozenset({"MCT", "RUH"}): 120,
    frozenset({"MCT", "MED"}): 150,
    frozenset({"MCT", "DMM"}): 100,
    frozenset({"KHI", "JED"}): 165,
    frozenset({"KHI", "RUH"}): 165,
    frozenset({"KHI", "MED"}): 180,
    frozenset({"DEL", "JED"}): 210,
    frozenset({"DEL", "RUH"}): 210,
    frozenset({"DEL", "MED"}): 240,
    frozenset({"DEL", "DMM"}): 210,
    frozenset({"MAA", "JED"}): 210,
    frozenset({"MAA", "RUH"}): 210,
    frozenset({"BOM", "JED"}): 210,
    frozenset({"BOM", "RUH"}): 210,
    # Within Saudi
    frozenset({"RUH", "JED"}): 90,
    frozenset({"RUH", "MED"}): 90,
    frozenset({"RUH", "DMM"}): 60,
    frozenset({"JED", "MED"}): 60,
    frozenset({"JED", "DMM"}): 105,
    frozenset({"MED", "DMM"}): 105,
    frozenset({"ADD", "JED"}): 150,
    frozenset({"ADD", "RUH"}): 180,
    frozenset({"SIN", "JED"}): 480,
    frozenset({"SIN", "RUH"}): 450,
    frozenset({"KUL", "JED"}): 510,
    frozenset({"HKG", "JED"}): 540,
}


def _leg_min(a: str, b: str) -> int | None:
    return DIRECT_LEG_MIN.get(frozenset({a, b}))


def _estimate_transit_min(origin: str, via: str, dest: str, total_min: float) -> float | None:
    """Return estimated layover minutes, or None if lookup is incomplete."""
    l1 = _leg_min(origin, via)
    l2 = _leg_min(via, dest)
    if l1 is None or l2 is None:
        return None
    est = total_min - l1 - l2
    return max(est, 0)


def _parse_via_airports(via_raw) -> list[str]:
    if not via_raw or (isinstance(via_raw, float) and math.isnan(via_raw)):
        return []
    return [v.strip().upper() for v in str(via_raw).split("|") if v.strip()]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Saudi Arabia competitor fare report")
    p.add_argument("--origin",  help="Filter to specific BD origin (e.g. DAC)")
    p.add_argument("--dest",    help="Filter to specific Saudi destination (e.g. JED)")
    p.add_argument("--days-ahead", type=int, default=60,
                   help="Number of days ahead for departure window (default: 60)")
    p.add_argument("--lookback-days", type=int, default=90,
                   help="Days of historical departure data to include (default: 90)")
    p.add_argument("--max-transit-hours", type=float, default=6.0,
                   help="Maximum estimated transit hours to include (default: 6)")
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL))
    p.add_argument("--no-csv", action="store_true")
    return p.parse_args()


def _fetch(engine, origins: list[str], dests: list[str],
           start_date: date, end_date: date) -> pd.DataFrame:
    org_list  = ", ".join(f"'{a}'" for a in origins)
    dst_list  = ", ".join(f"'{a}'" for a in dests)
    sql = text(f"""
        SELECT
            fo.airline,
            fo.origin,
            fo.destination,
            fo.flight_number,
            fo.departure,
            frm.arrival,
            fo.cabin,
            fo.brand,
            frm.via_airports,
            frm.stops,
            frm.duration_min,
            frm.aircraft,
            frm.baggage,
            fo.price_total_bdt,
            fo.scraped_at
        FROM flight_offers fo
        JOIN flight_offer_raw_meta frm ON frm.flight_offer_id = fo.id
        WHERE fo.origin IN ({org_list})
          AND fo.destination IN ({dst_list})
          AND DATE(fo.departure) BETWEEN :start_date AND :end_date
        ORDER BY fo.origin, fo.destination, fo.airline, fo.departure, fo.price_total_bdt
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={
            "start_date": str(start_date),
            "end_date":   str(end_date),
        })


def _baggage_tiers(series: pd.Series) -> tuple[str, str, str]:
    """Return (starting, mid, high) baggage from a series of baggage strings."""
    vals = series.dropna().astype(str).str.strip()
    vals = vals[vals.str.len() > 0]
    if vals.empty:
        return ("--", "--", "--")
    unique = vals.unique().tolist()
    if len(unique) == 1:
        return (unique[0], unique[0], unique[0])
    if len(unique) == 2:
        return (unique[0], unique[1], unique[1])
    return (unique[0], unique[len(unique) // 2], unique[-1])


def _build_report(df: pd.DataFrame, max_transit_min: float) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows = []
    group_keys = ["airline", "origin", "destination", "via_airports", "stops"]
    for key, grp in df.groupby(group_keys, dropna=False):
        airline, origin, dest, via_raw, stops = key
        via_list = _parse_via_airports(via_raw)
        stops_int = int(stops) if stops is not None and not (isinstance(stops, float) and math.isnan(stops)) else 0

        # Compute actual total duration from arrival − departure where available.
        # duration_min from some sources stores only a single leg's time, so
        # using the real timestamps is more reliable.
        dep_ts  = pd.to_datetime(grp["departure"],  errors="coerce")
        arr_ts  = pd.to_datetime(grp["arrival"],    errors="coerce")
        actual_mins = ((arr_ts - dep_ts).dt.total_seconds() / 60).dropna()
        # Sanity: clamp to [60, 5000] minutes; negative = overnight/next-day
        actual_mins = actual_mins.where(actual_mins > 60)
        actual_mins = actual_mins.where(actual_mins < 5000)
        med_actual  = actual_mins.median() if not actual_mins.empty else float("nan")

        # Fall back to duration_min if actual is unavailable / absurd
        med_dur_col = grp["duration_min"].median()
        med_dur = med_actual if not math.isnan(med_actual) else med_dur_col

        # Transit filtering
        if stops_int == 0 or not via_list:
            transit_label = "Direct"
            transit_min   = 0
        elif len(via_list) == 1:
            via = via_list[0]
            if not math.isnan(med_dur):
                est = _estimate_transit_min(origin, via, dest, med_dur)
                if est is not None and est > 30:
                    # Reliable estimate (>30 min means data is consistent)
                    if est > max_transit_min:
                        continue  # skip: transit too long
                    transit_min = est
                    h, m = divmod(int(est), 60)
                    transit_label = f"{via} (~{h}h{m:02d}m layover)"
                elif est is not None:
                    # Near-zero: duration_min was a leg time, not total — skip noisy entry
                    continue
                else:
                    # City pair not in lookup — include it but label clearly
                    transit_min = 0
                    total_h, total_m = divmod(int(med_dur), 60)
                    transit_label = f"{via} ({total_h}h{total_m:02d}m total)"
            else:
                transit_label = f"via {via}"
                transit_min   = 0
        else:
            # Multi-stop: use lookup for first pair only as a rough gate
            transit_label = "+".join(via_list)
            if not math.isnan(med_dur) and med_dur > max_transit_min + 700:
                continue  # very long overall — almost certainly > 6h transit
            transit_min = 0

        # Fare stats
        starting = grp["price_total_bdt"].min()
        highest  = grp["price_total_bdt"].max()
        average  = grp["price_total_bdt"].mean()

        # Aircraft (most common)
        aircraft = (
            grp["aircraft"].mode().iloc[0]
            if grp["aircraft"].notna().any()
            else "--"
        )

        # Baggage tiers across distinct brands/cabins
        bag_start, bag_mid, bag_high = _baggage_tiers(grp["baggage"])

        # Departure / arrival window for the group
        dep_sample = grp["departure"].dropna()
        arr_sample = grp["arrival"].dropna()
        dep_str = pd.to_datetime(dep_sample).dt.strftime("%H:%M").mode().iloc[0] if not dep_sample.empty else "--"
        arr_str = pd.to_datetime(arr_sample).dt.strftime("%H:%M").mode().iloc[0] if not arr_sample.empty else "--"

        rows.append({
            "Airline":           airline,
            "Aircraft Type":     aircraft,
            "Origin":            origin,
            "Destination":       dest,
            "Transit":           transit_label,
            "Departure":         dep_str,
            "Arrival":           arr_str,
            "Starting Fare":     int(starting),
            "Highest Fare":      int(highest),
            "Average":           int(round(average)),
            "Starting Baggage":  bag_start,
            "Mid Baggage":       bag_mid,
            "High Baggage":      bag_high,
            "_transit_min":      transit_min,
        })

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows).sort_values(["Origin", "Destination", "Airline", "Transit"])
    return out.drop(columns=["_transit_min"])


def _print_report(df: pd.DataFrame) -> None:
    if df.empty:
        print("\n  No qualifying itineraries found.\n")
        return

    display_cols = [
        "Airline", "Aircraft Type", "Origin", "Destination", "Transit",
        "Departure", "Arrival", "Starting Fare", "Highest Fare", "Average",
        "Starting Baggage", "Mid Baggage", "High Baggage",
    ]

    col_widths = {c: max(len(c), df[c].astype(str).str.len().max()) for c in display_cols}
    header = "  ".join(c.ljust(col_widths[c]) for c in display_cols)
    sep    = "-" * len(header)

    for dest in sorted(df["Destination"].unique()):
        sub = df[df["Destination"] == dest]
        print(f"\n{'='*len(header)}")
        print(f"  Destination: {dest}  ({len(sub)} itineraries)")
        print(f"{'='*len(header)}")
        print(header)
        print(sep)
        for _, row in sub.iterrows():
            line = "  ".join(str(row[c]).ljust(col_widths[c]) for c in display_cols)
            print(line)
        print(sep)


def main() -> int:
    args = parse_args()
    max_transit_min = args.max_transit_hours * 60

    origins = [args.origin.upper()] if args.origin else BD_AIRPORTS
    dests   = [args.dest.upper()]   if args.dest   else SAUDI_AIRPORTS

    today = date.today()
    start = today - timedelta(days=args.lookback_days)
    end   = today + timedelta(days=args.days_ahead)

    print(f"\n[saudi_competitor_report] Origins={origins}  Dests={dests}")
    print(f"  Departure window: {start} -> {end}  Max transit: {args.max_transit_hours}h")

    engine = create_engine(args.db_url)
    df_raw = _fetch(engine, origins, dests, start, end)
    print(f"  Raw rows fetched: {len(df_raw)}")

    df_report = _build_report(df_raw, max_transit_min)
    _print_report(df_report)

    if not args.no_csv and not df_report.empty:
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts      = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        csv_path = out_dir / f"saudi_competitor_report_{ts}.csv"
        df_report.to_csv(csv_path, index=False)
        print(f"\n[saudi_competitor_report] CSV saved -> {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
