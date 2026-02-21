"""
Safely parallelize scrapes by running one process per airline.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Parallel run_all by airline")
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument("--airlines-config", default="config/airlines.json")
    p.add_argument("--max-workers", type=int, default=2)
    p.add_argument("--origin")
    p.add_argument("--destination")
    p.add_argument("--date")
    p.add_argument("--dates")
    p.add_argument("--date-offsets")
    p.add_argument("--dates-file")
    p.add_argument("--cabin")
    p.add_argument("--quick", action="store_true")
    p.add_argument("--limit-routes", type=int)
    p.add_argument("--limit-dates", type=int)
    p.add_argument("--output-dir", default="output/reports")
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def _load_enabled_airlines(path: Path):
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return [str(x.get("code", "")).upper() for x in data if x.get("enabled")]


def _build_cmd(args, airline: str):
    cmd = [args.python_exe, "run_all.py", "--airline", airline]
    if args.quick:
        cmd.append("--quick")
    for flag, value in [
        ("--origin", args.origin),
        ("--destination", args.destination),
        ("--date", args.date),
        ("--dates", args.dates),
        ("--date-offsets", args.date_offsets),
        ("--dates-file", args.dates_file),
        ("--cabin", args.cabin),
        ("--limit-routes", args.limit_routes),
        ("--limit-dates", args.limit_dates),
    ]:
        if value is not None:
            cmd.extend([flag, str(value)])
    return cmd


def _run_one(cmd: list[str], airline: str):
    started = datetime.now(timezone.utc)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    ended = datetime.now(timezone.utc)
    return {
        "airline": airline,
        "cmd": subprocess.list2cmdline(cmd),
        "rc": int(proc.returncode),
        "duration_sec": (ended - started).total_seconds(),
        "stdout_tail": (proc.stdout or "")[-1000:],
        "stderr_tail": (proc.stderr or "")[-1000:],
    }


def main():
    args = parse_args()
    airlines = _load_enabled_airlines(Path(args.airlines_config))
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")

    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
        fut_map = {}
        for a in airlines:
            cmd = _build_cmd(args, a)
            fut_map[ex.submit(_run_one, cmd, a)] = a
        for fut in as_completed(fut_map):
            results.append(fut.result())

    results = sorted(results, key=lambda x: x["airline"])
    failed = [r for r in results if r["rc"] != 0]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "airline_count": len(airlines),
        "max_workers": args.max_workers,
        "failed_count": len(failed),
        "results": results,
    }

    latest = out_dir / "scrape_parallel_latest.json"
    run = out_dir / f"scrape_parallel_{ts}.json"
    latest.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    run.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"parallel_scrape_done airlines={len(airlines)} failed={len(failed)}")
    print(f"latest={latest}")
    print(f"run={run}")
    if args.strict and failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
