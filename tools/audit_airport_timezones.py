"""
Audit airport timezone mapping coverage against stored raw-meta UTC fields.

Outputs:
- timezone_coverage_audit_<timestamp>.csv
- timezone_coverage_gaps_<timestamp>.csv
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import DATABASE_URL as DEFAULT_DATABASE_URL


def parse_args():
    p = argparse.ArgumentParser(description="Audit airport timezone mapping coverage")
    p.add_argument("--airport-timezones", default="config/airport_timezones.json")
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL))
    return p.parse_args()


def _run_stamp(timestamp_tz: str) -> str:
    now = datetime.now(timezone.utc) if timestamp_tz == "utc" else datetime.now().astimezone()
    return now.strftime("%Y%m%d_%H%M%S")


def _load_timezone_map(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    out = set()
    for k in payload.keys():
        out.add(str(k).upper())
    return out


def _query_audit_df(db_url: str) -> pd.DataFrame:
    sql = text(
        """
        WITH latest_rm AS (
          SELECT DISTINCT ON (r.flight_offer_id)
            r.flight_offer_id,
            r.departure_utc,
            r.arrival_utc
          FROM flight_offer_raw_meta r
          ORDER BY r.flight_offer_id, r.id DESC
        ),
        base AS (
          SELECT
            fo.airline,
            UPPER(fo.origin) AS origin,
            UPPER(fo.destination) AS destination,
            lr.departure_utc,
            lr.arrival_utc
          FROM flight_offers fo
          LEFT JOIN latest_rm lr ON lr.flight_offer_id = fo.id
        )
        SELECT
          airport_role,
          airport,
          COUNT(*)::int AS rows_total,
          SUM(CASE WHEN utc_is_null THEN 1 ELSE 0 END)::int AS utc_null_rows,
          COUNT(DISTINCT airline)::int AS airlines_seen
        FROM (
          SELECT airline, origin AS airport, departure_utc IS NULL AS utc_is_null, 'origin'::text AS airport_role
          FROM base
          UNION ALL
          SELECT airline, destination AS airport, arrival_utc IS NULL AS utc_is_null, 'destination'::text AS airport_role
          FROM base
        ) t
        GROUP BY airport_role, airport
        ORDER BY utc_null_rows DESC, rows_total DESC, airport_role, airport
        """
    )
    engine = create_engine(db_url, pool_pre_ping=True, future=True)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def main():
    args = parse_args()
    ts = _run_stamp(args.timestamp_tz)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    known = _load_timezone_map(Path(args.airport_timezones))
    df = _query_audit_df(args.db_url)
    if df.empty:
        print("No rows found for timezone audit scope.")
        return 0

    df["known_timezone"] = df["airport"].astype(str).str.upper().isin(known)
    df["needs_mapping"] = (~df["known_timezone"]) & (df["utc_null_rows"] > 0)

    audit_path = out_dir / f"timezone_coverage_audit_{ts}.csv"
    gaps_path = out_dir / f"timezone_coverage_gaps_{ts}.csv"
    summary_path = out_dir / f"timezone_coverage_summary_{ts}.txt"

    df.to_csv(audit_path, index=False)
    df[df["needs_mapping"]].to_csv(gaps_path, index=False)

    summary = {
        "rows_total": int(len(df)),
        "airports_total": int(df["airport"].nunique()),
        "known_airports": int(df[df["known_timezone"]]["airport"].nunique()),
        "unknown_airports": int(df[~df["known_timezone"]]["airport"].nunique()),
        "needs_mapping_airports": int(df[df["needs_mapping"]]["airport"].nunique()),
        "needs_mapping_codes": sorted(df[df["needs_mapping"]]["airport"].dropna().astype(str).str.upper().unique().tolist()),
    }
    summary_lines = [f"{k}={v}" for k, v in summary.items()]
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")

    print(f"audit_rows={len(df)} -> {audit_path}")
    print(f"gap_rows={len(df[df['needs_mapping']])} -> {gaps_path}")
    print(f"summary -> {summary_path}")
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
