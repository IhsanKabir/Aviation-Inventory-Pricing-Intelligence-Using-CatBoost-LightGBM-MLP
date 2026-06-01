"""
Saudi route scrape — one query per route gets ALL airlines at once.

Searches day 1. If 0 rows, tries days 2-3-4-5 for that route.
Covers all 4 Saudi gateways: JED, RUH, DMM, MED.
Then generates the full competitor report sorted highest to lowest fare.
"""
from __future__ import annotations

import uuid
import sys
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

from db import DATABASE_URL
from modules.sharetrip import fetch_flights

# Search far-out dates for lowest advance fares.
# Start at 30 days out, try 45, 60, 75, 90 if a route returns 0 rows.
SEARCH_OFFSETS = [30, 45, 60, 75, 90]

BD_HUBS = ["DAC", "CGP", "ZYL", "CXB"]
SAUDI   = ["JED", "RUH", "DMM", "MED"]

# All BD hub <-> Saudi gateway combinations, both directions
ROUTES = [
    pair
    for bd in BD_HUBS
    for sa in SAUDI
    for pair in [(bd, sa), (sa, bd)]
]


def _save_rows(engine, rows: list[dict], scrape_id: str, scraped_at: str) -> int:
    if not rows:
        return 0
    inserted = 0
    for r in rows:
        # Insert flight_offers — on duplicate (same scrape session), fetch existing id
        row_id = None
        params = {
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
        }
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
                """), params)
                row_id = result.scalar()
        except Exception as e:
            print(f"    SAVE ERROR (flight_offers): {e}")
            continue

        if not row_id:
            continue

        # Insert raw_meta in its own transaction (failure here doesn't lose the offer)
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO flight_offer_raw_meta
                        (flight_offer_id, scraped_at, currency, fare_amount, tax_amount,
                         baggage, aircraft, equipment_code, duration_min, stops, arrival,
                         estimated_load_factor_pct, booking_class, soldout,
                         inventory_confidence, adt_count, chd_count, inf_count,
                         fare_ref_num, fare_search_reference, source_endpoint,
                         via_airports, fare_refundable, raw_offer_storage)
                    VALUES
                        (:fo_id, :scraped_at, :currency, :fare_amount, :tax_amount,
                         :baggage, :aircraft, :equipment_code, :duration_min, :stops, :arrival,
                         :elp, :booking_class, :soldout,
                         :inv_conf, :adt, :chd, :inf,
                         :fare_ref_num, :fare_search_ref, :source_endpoint,
                         :via_airports, :fare_refundable, 'inline')
                """), {
                    "fo_id":           row_id,
                    "scraped_at":      scraped_at,
                    "currency":        r.get("currency", "BDT"),
                    "fare_amount":     r.get("fare_amount"),
                    "tax_amount":      r.get("tax_amount"),
                    "baggage":         r.get("baggage"),
                    "aircraft":        r.get("aircraft"),
                    "equipment_code":  r.get("equipment_code"),
                    "duration_min":    r.get("duration_min"),
                    "stops":           r.get("stops"),
                    "arrival":         r.get("arrival"),
                    "elp":             r.get("estimated_load_factor_pct"),
                    "booking_class":   r.get("booking_class"),
                    "soldout":         r.get("soldout"),
                    "inv_conf":        r.get("inventory_confidence"),
                    "adt":             r.get("adt_count", 1),
                    "chd":             r.get("chd_count", 0),
                    "inf":             r.get("inf_count", 0),
                    "fare_ref_num":    r.get("fare_ref_num"),
                    "fare_search_ref": r.get("fare_search_reference"),
                    "source_endpoint": r.get("source_endpoint"),
                    "via_airports":    r.get("via_airports"),
                    "fare_refundable": r.get("fare_refundable"),
                })
        except Exception as e:
            print(f"    SAVE ERROR (raw_meta): {e}")

        inserted += 1
    return inserted


def main() -> None:
    engine    = create_engine(DATABASE_URL)
    today     = date.today()
    scrape_id = str(uuid.uuid4())
    scraped_at = datetime.now(timezone.utc).isoformat()
    total     = 0

    for origin, dest in ROUTES:
        got = False
        for day in SEARCH_OFFSETS:
            dep = today + timedelta(days=day)
            print(f"  {origin}->{dest}  {dep} ... ", end="", flush=True)

            result = fetch_flights(
                origin=origin, destination=dest,
                date=str(dep), cabin="Economy",
                adt=1, chd=0, inf=0,
                airline_code=None,       # get ALL airlines on this route
            )
            rows = result.get("rows") or []
            ok   = result.get("ok", False)
            print(f"{len(rows)} rows  ok={ok}")

            if rows:
                n = _save_rows(engine, rows, scrape_id, scraped_at)
                total += n
                got = True
                break

        if not got:
            print(f"  {origin}->{dest}  no data across offsets {SEARCH_OFFSETS}")

    print(f"\nTotal rows saved: {total}")
    print("\n[Generating report — highest to lowest fare]\n")
    subprocess.run([sys.executable,
                    str(REPO_ROOT / "tools" / "saudi_competitor_report.py"),
                    "--max-transit-hours", "6"],
                   cwd=str(REPO_ROOT))


if __name__ == "__main__":
    main()
