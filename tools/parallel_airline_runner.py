"""
Safely parallelize scrapes by running one process per airline.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import time

REPO_ROOT = Path(__file__).resolve().parent.parent
FRAGILE_MODULES = {"sharetrip", "airastra", "bs", "indigo"}


def parse_args():
    p = argparse.ArgumentParser(description="Parallel run_all by airline")
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument("--airlines-config", default="config/airlines.json")
    p.add_argument("--max-workers", type=int, default=2)
    p.add_argument(
        "--trip-plan-mode",
        choices=["operational", "training", "deep"],
        default="operational",
    )
    p.add_argument("--cycle-id", help="Optional shared cycle UUID for all airline worker processes")
    p.add_argument("--origin")
    p.add_argument("--destination")
    p.add_argument("--date")
    p.add_argument("--date-start")
    p.add_argument("--date-end")
    p.add_argument("--dates")
    p.add_argument("--date-offsets")
    p.add_argument("--dates-file")
    p.add_argument("--schedule-file")
    p.add_argument("--cabin")
    p.add_argument("--adt", type=int, default=1)
    p.add_argument("--chd", type=int, default=0)
    p.add_argument("--inf", type=int, default=0)
    p.add_argument("--probe-group-id")
    p.add_argument("--route-scope", choices=["all", "domestic", "international"])
    p.add_argument("--market-country")
    p.add_argument("--strict-route-audit", action="store_true")
    p.add_argument("--query-timeout-seconds", type=float)
    p.add_argument("--quick", action="store_true")
    p.add_argument("--limit-routes", type=int)
    p.add_argument("--limit-dates", type=int)
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument(
        "--fragile-max-workers",
        type=int,
        default=1,
        help="Maximum concurrent workers for fragile connector families such as ShareTrip (default: 1).",
    )
    p.add_argument(
        "--fragile-cooldown-sec",
        type=float,
        default=2.0,
        help="Sleep between fragile-airline launches to reduce upstream burst pressure (default: 2.0s).",
    )
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def _load_enabled_airlines(path: Path):
    if not path.exists():
        return []
    # Use utf-8-sig so Windows-edited JSON files with BOM are accepted.
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    airlines = []
    for row in data:
        if not isinstance(row, dict) or not row.get("enabled"):
            continue
        code = str(row.get("code", "")).upper().strip()
        if not code:
            continue
        airlines.append(
            {
                "code": code,
                "module": str(row.get("module", "")).strip().lower(),
            }
        )
    return airlines


def _build_cmd(args, airline: str, cycle_id: str):
    cmd = [args.python_exe, str(REPO_ROOT / "run_all.py"), "--airline", airline]
    cmd.extend(["--cycle-id", str(cycle_id)])
    if args.quick:
        cmd.append("--quick")
    for flag, value in [
        ("--trip-plan-mode", args.trip_plan_mode),
        ("--origin", args.origin),
        ("--destination", args.destination),
        ("--date", args.date),
        ("--date-start", args.date_start),
        ("--date-end", args.date_end),
        ("--dates", args.dates),
        ("--date-offsets", args.date_offsets),
        ("--dates-file", args.dates_file),
        ("--schedule-file", args.schedule_file),
        ("--cabin", args.cabin),
        ("--adt", args.adt),
        ("--chd", args.chd),
        ("--inf", args.inf),
        ("--probe-group-id", args.probe_group_id),
        ("--route-scope", args.route_scope),
        ("--market-country", args.market_country),
        ("--limit-routes", args.limit_routes),
        ("--limit-dates", args.limit_dates),
        ("--query-timeout-seconds", args.query_timeout_seconds),
    ]:
        if value is not None:
            cmd.extend([flag, str(value)])
    if args.strict_route_audit:
        cmd.append("--strict-route-audit")
    return cmd


def _run_one(cmd: list[str], airline: str):
    started = datetime.now(timezone.utc)
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    ended = datetime.now(timezone.utc)
    return {
        "airline": airline,
        "cmd": subprocess.list2cmdline(cmd),
        "rc": int(proc.returncode),
        "duration_sec": (ended - started).total_seconds(),
        "stdout_tail": (proc.stdout or "")[-1000:],
        "stderr_tail": (proc.stderr or "")[-1000:],
    }


def _run_batch(airlines: list[dict], args, cycle_id: str, *, max_workers: int, cooldown_sec: float = 0.0):
    results = []
    if not airlines:
        return results

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as ex:
        fut_map = {}
        for idx, airline in enumerate(airlines):
            code = airline["code"]
            cmd = _build_cmd(args, code, cycle_id=cycle_id)
            fut_map[ex.submit(_run_one, cmd, code)] = code
            if cooldown_sec > 0 and idx < len(airlines) - 1:
                time.sleep(cooldown_sec)
        for fut in as_completed(fut_map):
            results.append(fut.result())
    return results


def _run_batches_concurrently(robust_airlines: list[dict], fragile_airlines: list[dict], args, cycle_id: str):
    results = []
    batch_futures = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        if robust_airlines:
            batch_futures[
                ex.submit(
                    _run_batch,
                    robust_airlines,
                    args,
                    cycle_id,
                    max_workers=args.max_workers,
                )
            ] = "robust"
        if fragile_airlines:
            batch_futures[
                ex.submit(
                    _run_batch,
                    fragile_airlines,
                    args,
                    cycle_id,
                    max_workers=args.fragile_max_workers,
                    cooldown_sec=max(0.0, float(args.fragile_cooldown_sec or 0.0)),
                )
            ] = "fragile"
        for fut in as_completed(batch_futures):
            results.extend(fut.result())
    return results


def main():
    args = parse_args()
    cycle_id = str(args.cycle_id).strip() if args.cycle_id else str(uuid.uuid4())
    try:
        cycle_id = str(uuid.UUID(cycle_id))
    except Exception:
        raise SystemExit(f"Invalid --cycle-id (must be UUID): {cycle_id}")
    airlines = _load_enabled_airlines(Path(args.airlines_config))
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    started = datetime.now(timezone.utc)

    robust_airlines = [a for a in airlines if a.get("module") not in FRAGILE_MODULES]
    fragile_airlines = [a for a in airlines if a.get("module") in FRAGILE_MODULES]

    results = _run_batches_concurrently(robust_airlines, fragile_airlines, args, cycle_id)

    results = sorted(results, key=lambda x: x["airline"])
    failed = [r for r in results if r["rc"] != 0]
    ended = datetime.now(timezone.utc)
    payload = {
        "generated_at": ended.isoformat(),
        "started_at_utc": started.isoformat(),
        "completed_at_utc": ended.isoformat(),
        "duration_sec": float((ended - started).total_seconds()),
        "cycle_id": cycle_id,
        "airline_count": len(airlines),
        "max_workers": args.max_workers,
        "robust_airline_count": len(robust_airlines),
        "fragile_max_workers": args.fragile_max_workers,
        "fragile_airline_count": len(fragile_airlines),
        "failed_count": len(failed),
        "results": results,
    }

    latest = out_dir / "scrape_parallel_latest.json"
    run = out_dir / f"scrape_parallel_{ts}_{cycle_id}.json"
    latest.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    run.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"parallel_scrape_done airlines={len(airlines)} failed={len(failed)} cycle_id={cycle_id}")
    print(f"latest={latest}")
    print(f"run={run}")
    if args.strict and failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
