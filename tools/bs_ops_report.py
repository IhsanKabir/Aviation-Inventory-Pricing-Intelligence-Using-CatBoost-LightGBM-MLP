"""
US-Bangla (BS) international operations report — Aug 1–15.

Queries FirstTrip for all BS international routes for each date in the
window, then outputs:
  1. Flight schedule matrix (route x date showing flight numbers & timings)
  2. Fare summary per route
  3. CSV export

Run:
    python tools/bs_ops_report.py
    python tools/bs_ops_report.py --start 2026-08-01 --end 2026-08-15
    python tools/bs_ops_report.py --airline QR  # any airline
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

from db import DATABASE_URL
from modules.firsttrip import fetch_flights as firsttrip_fetch

BD_AIRPORTS   = {"DAC", "CGP", "ZYL", "CXB", "SPD", "JSR", "BZL", "RJH"}

# All BS international routes to check
BS_INTERNATIONAL = [
    ("DAC","AUH"),("DAC","BKK"),("DAC","CAN"),("DAC","CCU"),
    ("DAC","DOH"),("DAC","DXB"),("DAC","JED"),("DAC","KUL"),
    ("DAC","MAA"),("DAC","MCT"),("DAC","MLE"),("DAC","RUH"),
    ("DAC","SHJ"),("DAC","SIN"),
    # Additional
    ("DAC","SYD"),("DAC","MED"),("DAC","DMM"),
    ("DAC","YYZ"),("DAC","JFK"),("DAC","AMM"),
    # CGP routes
    ("CGP","JED"),("CGP","RUH"),("CGP","MED"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="BS international OPS report via FirstTrip")
    p.add_argument("--airline", default="BS", help="Airline code (default: BS)")
    p.add_argument("--start",   default="2026-08-01", help="Start date YYYY-MM-DD")
    p.add_argument("--end",     default="2026-08-15", help="End date YYYY-MM-DD")
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--no-save", action="store_true", help="Don't save to DB")
    return p.parse_args()


def _date_range(start: str, end: str):
    d = date.fromisoformat(start)
    e = date.fromisoformat(end)
    while d <= e:
        yield d
        d += timedelta(days=1)


def _save_rows(engine, rows: list[dict], scrape_id: str, scraped_at: str) -> int:
    inserted = 0
    for r in rows:
        row_id = None
        try:
            with engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO flight_offers
                        (scrape_id, scraped_at, airline, flight_number,
                         origin, destination, departure, cabin, brand,
                         price_total_bdt, fare_basis, seat_capacity, seat_available)
                    VALUES
                        (:scrape_id, :scraped_at, :airline, :flight_number,
                         :origin, :destination, :departure, :cabin, :brand,
                         :price_total_bdt, :fare_basis, :seat_capacity, :seat_available)
                    ON CONFLICT (scrape_id, airline, origin, destination, flight_number, departure, cabin, fare_basis)
                    DO UPDATE SET scraped_at = EXCLUDED.scraped_at
                    RETURNING id
                """), {
                    "scrape_id":       scrape_id,
                    "scraped_at":      scraped_at,
                    "airline":         r.get("airline", ""),
                    "flight_number":   r.get("flight_number", ""),
                    "origin":          r.get("origin", ""),
                    "destination":     r.get("destination", ""),
                    "departure":       r.get("departure"),
                    "cabin":           r.get("cabin", "Economy"),
                    "brand":           r.get("brand"),
                    "price_total_bdt": r.get("price_total_bdt"),
                    "fare_basis":      r.get("fare_basis"),
                    "seat_capacity":   r.get("seat_capacity"),
                    "seat_available":  r.get("seat_available"),
                })
                row_id = result.scalar()
        except Exception:
            continue

        if not row_id:
            continue

        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO flight_offer_raw_meta
                        (flight_offer_id, scraped_at, currency, fare_amount, tax_amount,
                         baggage, aircraft, equipment_code, duration_min, stops, arrival,
                         via_airports, fare_refundable, raw_offer_storage,
                         adt_count, chd_count, inf_count, source_endpoint)
                    VALUES
                        (:fo_id, :sat, :cur, :fa, :ta, :bag, :ac, :ec, :dur,
                         :stops, :arr, :via, :ref, 'inline', :adt, :chd, :inf, :ep)
                """), {
                    "fo_id": row_id, "sat": scraped_at,
                    "cur":   r.get("currency","BDT"),
                    "fa":    r.get("fare_amount"),
                    "ta":    r.get("tax_amount"),
                    "bag":   r.get("baggage"),
                    "ac":    r.get("aircraft"),
                    "ec":    r.get("equipment_code"),
                    "dur":   r.get("duration_min"),
                    "stops": r.get("stops"),
                    "arr":   r.get("arrival"),
                    "via":   r.get("via_airports"),
                    "ref":   r.get("fare_refundable"),
                    "adt":   r.get("adt_count", 1),
                    "chd":   r.get("chd_count", 0),
                    "inf":   r.get("inf_count", 0),
                    "ep":    r.get("source_endpoint"),
                })
        except Exception:
            pass

        inserted += 1
    return inserted


def main() -> int:
    args   = parse_args()
    airline = args.airline.upper()
    dates  = list(_date_range(args.start, args.end))
    routes = BS_INTERNATIONAL if airline == "BS" else [
        (o, d) for o, d in BS_INTERNATIONAL
    ]

    print(f"\n[bs_ops_report] Airline={airline}  {args.start} -> {args.end}  ({len(dates)} days)")
    print(f"  Routes to check: {len(routes)}")

    engine     = create_engine(DATABASE_URL)
    scrape_id  = str(uuid.uuid4())
    scraped_at = datetime.now(timezone.utc).isoformat()
    total_saved = 0

    # Collect all results: {(origin,dest,dep_date): [rows]}
    schedule: dict[tuple, list] = {}

    for origin, dest in routes:
        print(f"\n  {origin}->{dest}")
        for dep_date in dates:
            result = firsttrip_fetch(
                origin=origin, destination=dest,
                date=str(dep_date), cabin="Economy",
                adt=1, chd=0, inf=0,
                airline_code=airline,
            )
            rows = result.get("rows") or []

            if rows:
                schedule[(origin, dest, dep_date)] = rows
                print(f"    {dep_date}: {len(rows)} offers  "
                      f"min={min(r['price_total_bdt'] for r in rows):,.0f} BDT  "
                      f"flt={rows[0]['flight_number']}  dep={rows[0]['departure'][11:16]}")
                if not args.no_save:
                    n = _save_rows(engine, rows, scrape_id, scraped_at)
                    total_saved += n
            else:
                print(f"    {dep_date}: no service")

    # Build summary DataFrame
    records = []
    for (origin, dest, dep_date), rows in schedule.items():
        for r in rows:
            records.append({
                "Route":        f"{origin}-{dest}",
                "Date":         str(dep_date),
                "Flight":       f"{airline}{r['flight_number']}",
                "Departure":    r["departure"][11:16] if r["departure"] else "--",
                "Arrival":      r["arrival"][11:16] if r.get("arrival") else "--",
                "Aircraft":     r.get("aircraft") or "--",
                "Stops":        r.get("stops", 0),
                "Via":          r.get("via_airports") or "Direct",
                "Duration":     f"{r['duration_min']//60}h{r['duration_min']%60:02d}m" if r.get("duration_min") else "--",
                "Min Fare BDT": int(r["price_total_bdt"]),
                "Baggage":      r.get("baggage") or "--",
                "Seats":        r.get("seat_available") or "--",
            })

    df = pd.DataFrame(records) if records else pd.DataFrame()

    if df.empty:
        print("\n  No flights found for any route in the requested period.")
        return 0

    # Print summary
    print(f"\n\n{'='*100}")
    print(f"  {airline} International Operations  {args.start} -> {args.end}")
    print(f"{'='*100}")
    for route in sorted(df["Route"].unique()):
        sub = df[df["Route"] == route]
        op_days = sub["Date"].nunique()
        min_fare = sub["Min Fare BDT"].min()
        aircraft = sub["Aircraft"].mode().iloc[0] if not sub.empty else "--"
        dep_times = sorted(sub["Departure"].unique())
        print(f"  {route:<12}  {op_days:>2}/{len(dates)} days  "
              f"fare from {min_fare:>8,} BDT  "
              f"aircraft: {aircraft:<25}  dep: {', '.join(dep_times)}")

    print(f"\n  Total: {len(df['Route'].unique())} routes operating  "
          f"{total_saved} rows saved to DB")

    # Save CSV
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"{airline.lower()}_ops_{args.start}_{args.end}_{ts}.csv"
    df.to_csv(csv_path, index=False)
    print(f"\n[bs_ops_report] CSV saved -> {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
