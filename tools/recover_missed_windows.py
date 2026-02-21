"""
Recover stale scrape windows by re-running targeted route/cabin scrapes.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from core.runtime_config import get_database_url


def parse_args():
    p = argparse.ArgumentParser(description="Recover missed/stale route windows")
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--routes-config", default="config/routes.json")
    p.add_argument("--max-age-hours", type=float, default=8.0)
    p.add_argument("--date-offsets", default="0,3,7", help="Offsets to scrape when recovering")
    p.add_argument("--max-routes", type=int, default=8)
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def _load_routes(path: Path):
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def _latest_scrape_for_route(engine, airline: str, origin: str, destination: str, cabin: str):
    q = text(
        """
        SELECT MAX(scraped_at) AS m
        FROM flight_offers
        WHERE airline = :airline
          AND origin = :origin
          AND destination = :destination
          AND cabin = :cabin
        """
    )
    with engine.connect() as conn:
        row = conn.execute(
            q,
            {
                "airline": airline.upper(),
                "origin": origin.upper(),
                "destination": destination.upper(),
                "cabin": cabin,
            },
        ).mappings().first()
    return row["m"] if row else None


def _run(cmd: list[str]):
    started = datetime.now(timezone.utc)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    ended = datetime.now(timezone.utc)
    return {
        "cmd": subprocess.list2cmdline(cmd),
        "rc": int(proc.returncode),
        "duration_sec": (ended - started).total_seconds(),
        "stdout_tail": (proc.stdout or "")[-800:],
        "stderr_tail": (proc.stderr or "")[-800:],
    }


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    cutoff = now - timedelta(hours=max(1.0, args.max_age_hours))
    ts = _stamp(now)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    latest = out_dir / "recover_missed_windows_latest.json"
    run = out_dir / f"recover_missed_windows_{ts}.json"

    routes = _load_routes(Path(args.routes_config))
    engine = create_engine(args.db_url, pool_pre_ping=True, future=True)

    stale_targets = []
    for r in routes:
        airline = str(r.get("airline", "")).upper()
        origin = str(r.get("origin", "")).upper()
        destination = str(r.get("destination", "")).upper()
        cabins = r.get("cabins") or ["Economy"]
        for cabin in cabins:
            latest_scraped = _latest_scrape_for_route(engine, airline, origin, destination, cabin)
            stale = latest_scraped is None or latest_scraped < cutoff.replace(tzinfo=None)
            if stale:
                stale_targets.append(
                    {
                        "airline": airline,
                        "origin": origin,
                        "destination": destination,
                        "cabin": cabin,
                        "latest_scraped_at": latest_scraped.isoformat() if latest_scraped else None,
                    }
                )

    stale_targets = stale_targets[: max(0, args.max_routes)]
    attempts = []
    for t in stale_targets:
        cmd = [
            args.python_exe,
            "run_all.py",
            "--quick",
            "--airline",
            t["airline"],
            "--origin",
            t["origin"],
            "--destination",
            t["destination"],
            "--cabin",
            t["cabin"],
            "--date-offsets",
            args.date_offsets,
        ]
        if args.dry_run:
            attempts.append({"target": t, "planned_cmd": subprocess.list2cmdline(cmd), "rc": None, "dry_run": True})
        else:
            res = _run(cmd)
            attempts.append({"target": t, **res, "dry_run": False})

    failures = [a for a in attempts if a.get("rc") not in (None, 0)]
    payload = {
        "generated_at": now.isoformat(),
        "max_age_hours": args.max_age_hours,
        "stale_target_count": len(stale_targets),
        "attempt_count": len(attempts),
        "dry_run": bool(args.dry_run),
        "failures": len(failures),
        "attempts": attempts,
    }

    latest.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    run.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"stale_targets={len(stale_targets)} attempts={len(attempts)} failures={len(failures)} dry_run={args.dry_run}")
    print(f"latest={latest}")
    print(f"run={run}")
    if args.strict and failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
