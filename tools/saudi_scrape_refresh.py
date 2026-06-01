"""
Post-scrape coverage validator and auto-retry for Saudi Arabia routes.

After any Saudi scrape run, checks which target airlines still have zero rows
in the DB for the next 7 departure dates. For each missing airline it:
  1. Temporarily enables ShareTrip in source_switches.json
  2. Runs run_pipeline.py with explicit --dates for those 7 calendar days
  3. Re-checks coverage
  4. Disables ShareTrip again

Airlines covered by their own dedicated scraper (BG, BS, OV, G9) are retried
without needing ShareTrip.

Run:
    python tools/saudi_scrape_refresh.py
    python tools/saudi_scrape_refresh.py --dry-run      # just show what's missing
    python tools/saudi_scrape_refresh.py --min-rows 5   # require at least 5 rows
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db import DATABASE_URL as DEFAULT_DATABASE_URL

SAUDI_AIRPORTS = ["JED", "RUH", "DMM", "MED"]
BD_AIRPORTS    = ["DAC", "CGP", "ZYL", "CXB"]

SOURCE_SWITCHES_FILE = REPO_ROOT / "config" / "source_switches.json"

# Airlines whose primary scraper module is NOT ShareTrip
DIRECT_SCRAPER_AIRLINES = {"BG", "BS", "OV", "G9", "VQ", "2A", "6E"}

# All Saudi competitor airlines we care about
TARGET_AIRLINES = ["BG", "BS", "OV", "SV", "EK", "QR", "FZ", "WY", "6E",
                   "KU", "AI", "EY", "GF", "PK", "MS", "G9"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Saudi competitor scrape coverage check + auto-retry")
    p.add_argument("--min-rows", type=int, default=1,
                   help="Minimum DB rows required to consider an airline 'covered' (default: 1)")
    p.add_argument("--dry-run", action="store_true",
                   help="Only report gaps, do not trigger re-scrape")
    p.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL))
    p.add_argument("--python-exe", default=sys.executable)
    return p.parse_args()


def _next_7_dates() -> list[date]:
    today = date.today()
    return [today + timedelta(days=i) for i in range(1, 8)]


def _check_coverage(engine, airlines: list[str], dates: list[date]) -> dict[str, int]:
    """Return {airline: row_count} for the given date window."""
    airline_list = ", ".join(f"'{a}'" for a in airlines)
    date_list    = ", ".join(f"'{d}'" for d in dates)
    sql = text(f"""
        SELECT fo.airline, COUNT(*) as rows
        FROM flight_offers fo
        WHERE fo.airline IN ({airline_list})
          AND DATE(fo.departure) IN ({date_list})
          AND (fo.origin IN ('DAC','CGP','ZYL','CXB')
               AND fo.destination IN ('JED','RUH','DMM','MED')
            OR fo.origin IN ('JED','RUH','DMM','MED')
               AND fo.destination IN ('DAC','CGP','ZYL','CXB'))
        GROUP BY fo.airline
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    result = {row.airline: int(row.rows) for row in df.itertuples(index=False)}
    for a in airlines:
        result.setdefault(a, 0)
    return result


def _set_sharetrip(enabled: bool) -> None:
    data = json.loads(SOURCE_SWITCHES_FILE.read_text())
    data["sources"]["sharetrip"]["enabled"] = enabled
    SOURCE_SWITCHES_FILE.write_text(json.dumps(data, indent=2))
    state = "ON" if enabled else "OFF"
    print(f"  [sharetrip] {state}")


def _run_scrape(python_exe: str, airlines: list[str], dates: list[date]) -> int:
    date_str = ",".join(str(d) for d in dates)
    cmd = [
        python_exe,
        str(REPO_ROOT / "run_pipeline.py"),
        "--airline", ",".join(airlines),
        "--dates", date_str,
        "--skip-training", "--skip-prediction", "--skip-reports",
    ]
    print(f"  Running: {' '.join(cmd[-6:])}")
    result = subprocess.run(cmd, cwd=str(REPO_ROOT))
    return result.returncode


def main() -> int:
    args = parse_args()
    dates = _next_7_dates()
    date_str = ", ".join(str(d) for d in dates)

    print(f"\n[saudi_scrape_refresh] Checking coverage for next 7 dates: {date_str}")
    print(f"  Min rows required: {args.min_rows}\n")

    engine = create_engine(args.db_url)
    coverage = _check_coverage(engine, TARGET_AIRLINES, dates)

    missing = [a for a, rows in sorted(coverage.items()) if rows < args.min_rows]
    present = [a for a, rows in sorted(coverage.items()) if rows >= args.min_rows]

    print("Coverage summary:")
    for a, rows in sorted(coverage.items()):
        status = "OK" if rows >= args.min_rows else "MISSING"
        print(f"  {a:4s}  {rows:>6,} rows  [{status}]")

    if not missing:
        print("\n  All airlines covered. Nothing to re-scrape.")
        return 0

    print(f"\nMissing airlines: {missing}")

    if args.dry_run:
        print("  --dry-run: skipping re-scrape.")
        return 0

    # Split into ShareTrip airlines vs direct-scraper airlines
    needs_sharetrip = [a for a in missing if a not in DIRECT_SCRAPER_AIRLINES]
    needs_direct    = [a for a in missing if a in DIRECT_SCRAPER_AIRLINES]

    if needs_direct:
        print(f"\n[Retry] Direct-scraper airlines: {needs_direct}")
        rc = _run_scrape(args.python_exe, needs_direct, dates)
        if rc != 0:
            print(f"  WARNING: direct scrape returned rc={rc}")

    if needs_sharetrip:
        print(f"\n[Retry] ShareTrip airlines: {needs_sharetrip}")
        _set_sharetrip(True)
        try:
            rc = _run_scrape(args.python_exe, needs_sharetrip, dates)
            if rc != 0:
                print(f"  WARNING: ShareTrip scrape returned rc={rc}")
        finally:
            _set_sharetrip(False)

    # Re-check after retry
    print("\nPost-retry coverage:")
    coverage2 = _check_coverage(engine, missing, dates)
    still_missing = []
    for a, rows in sorted(coverage2.items()):
        status = "OK" if rows >= args.min_rows else "STILL MISSING"
        print(f"  {a:4s}  {rows:>6,} rows  [{status}]")
        if rows < args.min_rows:
            still_missing.append(a)

    if still_missing:
        print(f"\n  NOTE: {still_missing} returned no data even after retry.")
        print("  These airlines likely have no service on DAC<->Saudi routes")
        print("  or are not carried by any active scraper/OTA.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
