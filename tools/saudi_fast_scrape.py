"""
Fast targeted scrape for EK/QR/FZ/WY on Saudi routes.

Strategy: search day 1 only. For each airline that returns 0 rows,
try day 2. Keep going up to MAX_DAYS (5). Stop per-airline as soon
as ANY data lands. Then generate the report.

26 routes x 1 day = ~26 ShareTrip queries (~5-8 minutes max).
"""
from __future__ import annotations

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

SOURCE_SWITCHES = REPO_ROOT / "config" / "source_switches.json"
TARGET   = ["EK", "QR", "FZ", "WY"]
SAUDI    = {"JED", "RUH", "DMM", "MED"}
MAX_DAYS = 5


def _set_sharetrip(enabled: bool) -> None:
    data = json.loads(SOURCE_SWITCHES.read_text())
    data["sources"]["sharetrip"]["enabled"] = enabled
    SOURCE_SWITCHES.write_text(json.dumps(data, indent=2))
    print(f"  [sharetrip] {'ON' if enabled else 'OFF'}")


def _rows_for(engine, airlines: list[str], dep_date: date) -> dict[str, int]:
    al = ", ".join(f"'{a}'" for a in airlines)
    sql = text(f"""
        SELECT airline, COUNT(*) AS n FROM flight_offers
        WHERE airline IN ({al}) AND DATE(departure) = :d
          AND ((origin IN ('DAC','CGP','ZYL','CXB') AND destination IN ('JED','RUH','DMM','MED'))
            OR (origin IN ('JED','RUH','DMM','MED') AND destination IN ('DAC','CGP','ZYL','CXB')))
        GROUP BY airline
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"d": str(dep_date)})
    result = {r.airline: int(r.n) for r in df.itertuples(index=False)}
    for a in airlines:
        result.setdefault(a, 0)
    return result


def _scrape(airlines: list[str], dep_date: date) -> None:
    cmd = [sys.executable, str(REPO_ROOT / "run_pipeline.py"),
           "--airline", ",".join(airlines),
           "--dates", str(dep_date),
           "--skip-training", "--skip-prediction", "--skip-reports"]
    print(f"  scraping {airlines} for {dep_date} ...")
    subprocess.run(cmd, cwd=str(REPO_ROOT))


def main() -> None:
    engine = create_engine(DEFAULT_DATABASE_URL)
    today  = date.today()

    _set_sharetrip(True)
    try:
        missing = list(TARGET)
        for day_offset in range(1, MAX_DAYS + 1):
            if not missing:
                break
            dep = today + timedelta(days=day_offset)
            print(f"\n[Day {day_offset}] {dep}  Missing: {missing}")
            _scrape(missing, dep)
            coverage = _rows_for(engine, missing, dep)
            covered  = [a for a in missing if coverage[a] > 0]
            missing  = [a for a in missing if coverage[a] == 0]
            print(f"  Got data: {covered or 'none'}  Still missing: {missing or 'none'}")

        if missing:
            print(f"\n  NOTE: {missing} returned no data across {MAX_DAYS} days — no service or not on ShareTrip.")
    finally:
        _set_sharetrip(False)

    # Generate report
    print("\n[Generating report...]")
    subprocess.run([sys.executable, str(REPO_ROOT / "tools" / "saudi_competitor_report.py"),
                    "--max-transit-hours", "6"], cwd=str(REPO_ROOT))


if __name__ == "__main__":
    main()
