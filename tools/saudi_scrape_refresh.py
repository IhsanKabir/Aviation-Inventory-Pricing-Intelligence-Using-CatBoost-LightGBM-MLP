"""
Saudi competitor scrape — single-day probe with up to 5 retries.

For each missing airline:
  - Searches ONE departure date at a time (tomorrow by default)
  - Keeps retrying (up to MAX_RETRIES=5) until at least one row lands
  - All retries go through ShareTrip
  - After all airlines resolved (or retries exhausted), generates the
    competitor report and turns ShareTrip off

Run:
    python tools/saudi_scrape_refresh.py
    python tools/saudi_scrape_refresh.py --dry-run
    python tools/saudi_scrape_refresh.py --date 2026-06-05
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
MAX_RETRIES = 5

TARGET_AIRLINES = [
    "BG", "BS", "OV", "SV",
    "EK", "QR", "FZ", "WY",
    "KU", "AI", "EY", "GF", "PK", "MS", "G9",
]

# Airlines with dedicated scrapers that do NOT need ShareTrip
DIRECT_SCRAPER_AIRLINES = {"BG", "BS", "OV", "G9"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Saudi competitor scrape — single-day probe with retries")
    p.add_argument("--date", help="Departure date to probe (YYYY-MM-DD, default: tomorrow)")
    p.add_argument("--min-rows", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL))
    p.add_argument("--python-exe", default=sys.executable)
    return p.parse_args()


def _probe_date(args) -> date:
    if args.date:
        return date.fromisoformat(args.date)
    return date.today() + timedelta(days=1)


def _check_coverage(engine, airlines: list[str], probe: date) -> dict[str, int]:
    airline_list = ", ".join(f"'{a}'" for a in airlines)
    sql = text(f"""
        SELECT fo.airline, COUNT(*) AS rows
        FROM flight_offers fo
        WHERE fo.airline IN ({airline_list})
          AND DATE(fo.departure) = :probe
          AND (
              (fo.origin IN ('DAC','CGP','ZYL','CXB')
               AND fo.destination IN ('JED','RUH','DMM','MED'))
           OR (fo.origin IN ('JED','RUH','DMM','MED')
               AND fo.destination IN ('DAC','CGP','ZYL','CXB'))
          )
        GROUP BY fo.airline
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"probe": str(probe)})
    result = {row.airline: int(row.rows) for row in df.itertuples(index=False)}
    for a in airlines:
        result.setdefault(a, 0)
    return result


def _set_sharetrip(enabled: bool) -> None:
    data = json.loads(SOURCE_SWITCHES_FILE.read_text())
    data["sources"]["sharetrip"]["enabled"] = enabled
    SOURCE_SWITCHES_FILE.write_text(json.dumps(data, indent=2))
    print(f"  [sharetrip] {'ON' if enabled else 'OFF'}")


def _run_scrape(python_exe: str, airlines: list[str], probe: date) -> int:
    cmd = [
        python_exe,
        str(REPO_ROOT / "run_pipeline.py"),
        "--airline", ",".join(airlines),
        "--dates", str(probe),
        "--skip-training", "--skip-prediction", "--skip-reports",
    ]
    print(f"  Scraping {','.join(airlines)} for {probe} ...")
    return subprocess.run(cmd, cwd=str(REPO_ROOT)).returncode


def _run_report(python_exe: str) -> None:
    cmd = [python_exe, str(REPO_ROOT / "tools" / "saudi_competitor_report.py"),
           "--max-transit-hours", "6"]
    print("\n[Generating competitor report]")
    subprocess.run(cmd, cwd=str(REPO_ROOT))


def _print_coverage(coverage: dict[str, int], min_rows: int) -> None:
    for airline, rows in sorted(coverage.items()):
        tag = "OK     " if rows >= min_rows else "MISSING"
        print(f"    {airline:4s}  {rows:>6,} rows  [{tag}]")


def main() -> int:
    args = parse_args()
    probe = _probe_date(args)

    print(f"\n[saudi_scrape_refresh]  Probe date: {probe}  Max retries: {MAX_RETRIES}\n")

    engine = create_engine(args.db_url)
    coverage = _check_coverage(engine, TARGET_AIRLINES, probe)
    missing  = [a for a, n in coverage.items() if n < args.min_rows]

    print("Initial coverage:")
    _print_coverage(coverage, args.min_rows)

    if not missing:
        print("\n  All airlines have data for this date.")
        if not args.dry_run:
            _run_report(args.python_exe)
        return 0

    print(f"\n  Missing: {missing}")
    if args.dry_run:
        print("  --dry-run: stopping here.")
        return 0

    # Direct-scraper airlines don't need ShareTrip
    direct  = [a for a in missing if a in DIRECT_SCRAPER_AIRLINES]
    via_st  = [a for a in missing if a not in DIRECT_SCRAPER_AIRLINES]

    if direct:
        print(f"\n[Pass 0 — direct scrapers]  {direct}")
        _run_scrape(args.python_exe, direct, probe)
        coverage = _check_coverage(engine, direct, probe)
        missing = [a for a in missing if coverage.get(a, 0) < args.min_rows]

    if not missing:
        print("  All covered after direct-scraper pass.")
    else:
        _set_sharetrip(True)
        try:
            for attempt in range(1, MAX_RETRIES + 1):
                still_missing = [a for a in missing if coverage.get(a, 0) < args.min_rows]
                if not still_missing:
                    break

                print(f"\n[Retry {attempt}/{MAX_RETRIES}]  {still_missing}")
                rc = _run_scrape(args.python_exe, still_missing, probe)
                if rc != 0:
                    print(f"  WARNING: scrape rc={rc}")

                coverage = _check_coverage(engine, still_missing, probe)
                newly_ok  = [a for a in still_missing if coverage.get(a, 0) >= args.min_rows]
                missing   = [a for a in still_missing if coverage.get(a, 0) < args.min_rows]

                print(f"  Covered: {newly_ok or 'none'}  Still missing: {missing or 'none'}")

                if not missing:
                    print("  All airlines covered.")
                    break
        finally:
            _set_sharetrip(False)

        if missing:
            print(f"\n  NOTE: {missing} returned no data after {MAX_RETRIES} retries.")
            print("  These airlines have no BD<->Saudi service or no active scraper.")

    # Always generate the report at the end
    _run_report(args.python_exe)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
