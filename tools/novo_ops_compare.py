"""
NOVOAIR 7-day OPS fare comparison.

Queries the DB for the most recent VQ fare snapshot across all routes and
the next 7 departure dates, then outputs:
  1. A terminal price matrix  (always)
  2. A CSV export             (output/reports/novo_ops_compare_<ts>.csv)

Run:
    python tools/novo_ops_compare.py
    python tools/novo_ops_compare.py --days 14
    python tools/novo_ops_compare.py --airline BG,VQ    # multi-airline
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NOVOAIR (VQ) 7-day OPS fare comparison")
    p.add_argument("--airline", default="VQ", help="Comma-separated airline codes (default: VQ)")
    p.add_argument("--days", type=int, default=7, help="Number of future departure days to include")
    p.add_argument("--lookback-hours", type=int, default=24,
                   help="Only include fares scraped within this many hours (default: 24). "
                        "scraped_at is stored as local time so 24h avoids timezone-offset issues.")
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL))
    p.add_argument("--no-csv", action="store_true", help="Skip CSV export")
    return p.parse_args()


def _fetch_fares(
    engine, airlines: list[str], start_date: date, end_date: date, lookback_hours: int = 24
) -> pd.DataFrame:
    airline_list = ", ".join(f"'{a}'" for a in airlines)
    # scraped_at is TIMESTAMP WITHOUT TIME ZONE stored in local (Dhaka UTC+6) time.
    # Comparing against NOW() (UTC) would shift the window by +6h, so we compare against
    # LOCALTIMESTAMP instead, which PostgreSQL resolves using the server's timezone setting.
    sql = text(f"""
        SELECT
            fo.airline,
            fo.origin,
            fo.destination,
            DATE(fo.departure) AS departure_date,
            fo.cabin,
            MIN(fo.price_total_bdt)  AS min_fare_bdt,
            MAX(fo.price_total_bdt)  AS max_fare_bdt,
            AVG(fo.price_total_bdt)  AS avg_fare_bdt,
            MIN(fo.seat_available)   AS min_seats,
            MAX(fo.seat_available)   AS max_seats,
            COUNT(*)                 AS offer_count,
            MAX(fo.scraped_at)       AS last_scraped_at
        FROM flight_offers fo
        WHERE fo.airline IN ({airline_list})
          AND DATE(fo.departure) BETWEEN :start_date AND :end_date
          AND fo.scraped_at >= LOCALTIMESTAMP - INTERVAL '{lookback_hours} hours'
        GROUP BY fo.airline, fo.origin, fo.destination, DATE(fo.departure), fo.cabin
        ORDER BY fo.airline, fo.origin, fo.destination, departure_date, fo.cabin
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"start_date": str(start_date), "end_date": str(end_date)})


def _print_matrix(df: pd.DataFrame) -> None:
    if df.empty:
        print("\n  No fares found in DB for the requested window. Run the accumulation first.\n")
        return

    dates = sorted(df["departure_date"].unique())
    routes = sorted(df[["origin", "destination"]].drop_duplicates().apply(lambda r: f"{r.origin}->{r.destination}", axis=1).tolist())

    col_w = 10
    header = f"{'Route':<16}" + "".join(f"{str(d)[:10]:>{col_w}}" for d in dates)
    sep = "-" * len(header)

    print(f"\n{'='*len(header)}")
    print(f"  NOVOAIR (VQ)  --  Minimum Economy fare (BDT)  --  next {len(dates)} days")
    print(f"{'='*len(header)}")
    print(header)
    print(sep)

    for route in routes:
        origin, dest = route.split("->")
        row_data = df[(df["origin"] == origin) & (df["destination"] == dest)]
        line = f"{route:<16}"
        for d in dates:
            cell = row_data[row_data["departure_date"] == d]
            if cell.empty:
                line += f"{'n/a':>{col_w}}"
            else:
                price = int(cell["min_fare_bdt"].iloc[0])
                line += f"{price:>{col_w},}"
        print(line)

    print(sep)
    print(f"  Prices in BDT.\n")

    # Seat availability summary
    print(f"{'Route':<16}" + "".join(f"{str(d)[:10]:>{col_w}}" for d in dates))
    print(f"  (seats available)")
    print(sep)
    for route in routes:
        origin, dest = route.split("->")
        row_data = df[(df["origin"] == origin) & (df["destination"] == dest)]
        line = f"{route:<16}"
        for d in dates:
            cell = row_data[row_data["departure_date"] == d]
            if cell.empty:
                line += f"{'n/a':>{col_w}}"
            else:
                seats = cell["min_seats"].iloc[0]
                val = int(seats) if (seats is not None and not (isinstance(seats, float) and math.isnan(seats))) else "?"
                line += f"{val:>{col_w}}"
        print(line)
    print(sep + "\n")


def main() -> int:
    args = parse_args()
    airlines = [a.strip().upper() for a in args.airline.split(",") if a.strip()]
    today = date.today()
    start = today + timedelta(days=1)
    end = today + timedelta(days=args.days)

    print(f"\n[novo_ops_compare] Airlines={airlines}  Window={start} -> {end}  Lookback={args.lookback_hours}h")

    engine = create_engine(args.db_url)
    df = _fetch_fares(engine, airlines, start, end, lookback_hours=args.lookback_hours)

    _print_matrix(df)

    if not args.no_csv and not df.empty:
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        csv_path = out_dir / f"novo_ops_compare_{ts}.csv"
        df.to_csv(csv_path, index=False)
        print(f"[novo_ops_compare] CSV saved -> {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
