"""
Post-scrape coverage validator and auto-retry for Saudi Arabia routes.

For every missing airline it enables ShareTrip, retries up to 2 times
using the exact next 7 calendar dates, then generates the competitor
report and disables ShareTrip.

Run:
    python tools/saudi_scrape_refresh.py
    python tools/saudi_scrape_refresh.py --dry-run   # show gaps only
    python tools/saudi_scrape_refresh.py --min-rows 5
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

SOURCE_SWITCHES_FILE = REPO_ROOT / "config" / "source_switches.json"
MAX_RETRIES = 2

TARGET_AIRLINES = [
    "BG", "BS", "OV", "SV",
    "EK", "QR", "FZ", "WY", "6E",
    "KU", "AI", "EY", "GF", "PK", "MS", "G9",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Saudi competitor scrape coverage check + auto-retry")
    p.add_argument("--min-rows", type=int, default=1,
                   help="Minimum DB rows to consider an airline covered (default: 1)")
    p.add_argument("--dry-run", action="store_true",
                   help="Only report gaps, do not scrape or generate report")
    p.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL))
    p.add_argument("--python-exe", default=sys.executable)
    return p.parse_args()


def _next_7_dates() -> list[date]:
    today = date.today()
    return [today + timedelta(days=i) for i in range(1, 8)]


def _check_coverage(engine, airlines: list[str], dates: list[date]) -> dict[str, int]:
    airline_list = ", ".join(f"'{a}'" for a in airlines)
    date_list    = ", ".join(f"'{d}'" for d in dates)
    sql = text(f"""
        SELECT fo.airline, COUNT(*) AS rows
        FROM flight_offers fo
        WHERE fo.airline IN ({airline_list})
          AND DATE(fo.departure) IN ({date_list})
          AND (
              (fo.origin IN ('DAC','CGP','ZYL','CXB')
               AND fo.destination IN ('JED','RUH','DMM','MED'))
           OR (fo.origin IN ('JED','RUH','DMM','MED')
               AND fo.destination IN ('DAC','CGP','ZYL','CXB'))
          )
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
    print(f"  [sharetrip] {'ON' if enabled else 'OFF'}")


def _run_scrape(python_exe: str, airlines: list[str], dates: list[date]) -> int:
    date_str = ",".join(str(d) for d in dates)
    cmd = [
        python_exe,
        str(REPO_ROOT / "run_pipeline.py"),
        "--airline", ",".join(airlines),
        "--dates", date_str,
        "--skip-training", "--skip-prediction", "--skip-reports",
    ]
    print(f"  Scraping: {','.join(airlines)}  dates={date_str}")
    return subprocess.run(cmd, cwd=str(REPO_ROOT)).returncode


def _run_report(python_exe: str) -> None:
    cmd = [python_exe, str(REPO_ROOT / "tools" / "saudi_competitor_report.py"),
           "--max-transit-hours", "6"]
    print("\n  Generating competitor report...")
    subprocess.run(cmd, cwd=str(REPO_ROOT))


def _print_coverage(coverage: dict[str, int], min_rows: int) -> None:
    for airline, rows in sorted(coverage.items()):
        status = "OK     " if rows >= min_rows else "MISSING"
        print(f"    {airline:4s}  {rows:>6,} rows  [{status}]")


def main() -> int:
    args = parse_args()
    dates = _next_7_dates()

    print(f"\n[saudi_scrape_refresh]  Dates: {dates[0]} -> {dates[-1]}")
    print(f"  Min rows: {args.min_rows}  Max retries: {MAX_RETRIES}\n")

    engine = create_engine(args.db_url)

    # Initial coverage check
    coverage = _check_coverage(engine, TARGET_AIRLINES, dates)
    missing = [a for a, n in coverage.items() if n < args.min_rows]

    print("Initial coverage:")
    _print_coverage(coverage, args.min_rows)

    if not missing:
        print("\n  All airlines covered.")
        if not args.dry_run:
            _run_report(args.python_exe)
        return 0

    print(f"\n  Missing: {missing}")

    if args.dry_run:
        print("  --dry-run: stopping here.")
        return 0

    # Retry loop — all missing airlines go through ShareTrip
    _set_sharetrip(True)
    try:
        for attempt in range(1, MAX_RETRIES + 1):
            if not missing:
                break
            print(f"\n[Retry {attempt}/{MAX_RETRIES}]  Airlines: {missing}")
            rc = _run_scrape(args.python_exe, missing, dates)
            if rc != 0:
                print(f"  WARNING: scrape exited rc={rc}")

            coverage = _check_coverage(engine, missing, dates)
            still_missing = [a for a, n in coverage.items() if n < args.min_rows]
            newly_covered = [a for a in missing if a not in still_missing]

            if newly_covered:
                print(f"  Newly covered: {newly_covered}")
            missing = still_missing

            print(f"  After retry {attempt}:")
            _print_coverage(coverage, args.min_rows)

            if not missing:
                print("  All airlines now covered.")
                break

        if missing:
            print(f"\n  NOTE: {missing} returned no data after {MAX_RETRIES} retries.")
            print("  These airlines likely have no BD<->Saudi service or are not")
            print("  carried by any active scraper/OTA.")

    finally:
        _set_sharetrip(False)

    # Generate report regardless of remaining gaps
    _run_report(args.python_exe)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
