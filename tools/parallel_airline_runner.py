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
SHARETRIP_MODULES = {"sharetrip"}
WRAPPER_MODULES = {"airastra", "bs"}
PROTECTED_DIRECT_MODULES = {"indigo"}
GOZAYAAN_MODULES: set[str] = set()  # reserved for future gozayaan direct connector


def parse_args():
    p = argparse.ArgumentParser(description="Parallel run_all by airline")
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument("--airlines-config", default="config/airlines.json")
    p.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum concurrent workers for direct connector families such as BG/VQ (default: 4).",
    )
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
        default=1.0,
        help="Sleep between fragile-airline launches to reduce upstream burst pressure (default: 1.0s).",
    )
    p.add_argument(
        "--sharetrip-max-workers",
        type=int,
        default=1,
        help="Maximum concurrent workers for the shared ShareTrip backend family (default: 1).",
    )
    p.add_argument(
        "--sharetrip-cooldown-sec",
        type=float,
        default=3.0,
        help="Sleep between ShareTrip-backed airline launches to reduce burst pressure (default: 3.0s).",
    )
    p.add_argument(
        "--wrapper-max-workers",
        type=int,
        default=1,
        help="Maximum concurrent workers for wrapper/fallback families such as BS and 2A (default: 1).",
    )
    p.add_argument(
        "--wrapper-cooldown-sec",
        type=float,
        default=1.5,
        help="Sleep between wrapper-family launches to reduce upstream burst pressure (default: 1.5s).",
    )
    p.add_argument(
        "--indigo-max-workers",
        type=int,
        default=2,
        help="Maximum concurrent workers for Indigo direct sessions (default: 2).",
    )
    p.add_argument(
        "--indigo-cooldown-sec",
        type=float,
        default=0.75,
        help="Sleep between Indigo launches to reduce session churn (default: 0.75s).",
    )
    p.add_argument(
        "--gozayaan-max-workers",
        type=int,
        default=1,
        help="Maximum concurrent workers for GoZayaan-backed airlines (default: 1).",
    )
    p.add_argument(
        "--gozayaan-cooldown-sec",
        type=float,
        default=3.0,
        help="Sleep between GoZayaan airline launches (default: 3.0s).",
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


def _module_family(module_name: str) -> str:
    normalized = str(module_name or "").strip().lower()
    if normalized in SHARETRIP_MODULES:
        return "sharetrip"
    if normalized in WRAPPER_MODULES:
        return "wrapper"
    if normalized in PROTECTED_DIRECT_MODULES:
        return "indigo"
    if normalized in GOZAYAAN_MODULES:
        return "gozayaan"
    return "direct"


def _run_batches_concurrently(grouped_airlines: dict[str, list[dict]], args, cycle_id: str):
    results = []
    batch_futures = {}
    non_empty_families = {name: items for name, items in grouped_airlines.items() if items}
    with ThreadPoolExecutor(max_workers=max(1, len(non_empty_families))) as ex:
        if non_empty_families.get("direct"):
            batch_futures[
                ex.submit(
                    _run_batch,
                    non_empty_families["direct"],
                    args,
                    cycle_id,
                    max_workers=args.max_workers,
                )
            ] = "direct"
        if non_empty_families.get("sharetrip"):
            batch_futures[
                ex.submit(
                    _run_batch,
                    non_empty_families["sharetrip"],
                    args,
                    cycle_id,
                    max_workers=args.sharetrip_max_workers,
                    cooldown_sec=max(0.0, float(args.sharetrip_cooldown_sec or 0.0)),
                )
            ] = "sharetrip"
        if non_empty_families.get("wrapper"):
            batch_futures[
                ex.submit(
                    _run_batch,
                    non_empty_families["wrapper"],
                    args,
                    cycle_id,
                    max_workers=args.wrapper_max_workers,
                    cooldown_sec=max(0.0, float(args.wrapper_cooldown_sec or 0.0)),
                )
            ] = "wrapper"
        if non_empty_families.get("indigo"):
            batch_futures[
                ex.submit(
                    _run_batch,
                    non_empty_families["indigo"],
                    args,
                    cycle_id,
                    max_workers=args.indigo_max_workers,
                    cooldown_sec=max(0.0, float(args.indigo_cooldown_sec or 0.0)),
                )
            ] = "indigo"
        if non_empty_families.get("gozayaan"):
            batch_futures[
                ex.submit(
                    _run_batch,
                    non_empty_families["gozayaan"],
                    args,
                    cycle_id,
                    max_workers=args.gozayaan_max_workers,
                    cooldown_sec=max(0.0, float(args.gozayaan_cooldown_sec or 0.0)),
                )
            ] = "gozayaan"
        for fut in as_completed(batch_futures):
            results.extend(fut.result())
    return results


def _read_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_global_parallel_latest_candidate(args, payload: dict) -> bool:
    try:
        airline_count = int(payload.get("airline_count") or 0)
    except (TypeError, ValueError):
        airline_count = 0
    return (
        airline_count >= 5
        and not bool(args.quick)
        and not bool(args.origin)
        and not bool(args.destination)
        and not bool(args.date)
        and not bool(args.date_start)
        and not bool(args.date_end)
        and not bool(args.dates)
        and not bool(args.date_offsets)
        and not bool(args.dates_file)
        and not bool(args.probe_group_id)
        and str(args.trip_plan_mode or "operational").strip().lower() == "operational"
        and str(args.route_scope or "all").strip().lower() == "all"
    )


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

    grouped_airlines = {
        "direct": [],
        "sharetrip": [],
        "wrapper": [],
        "indigo": [],
    }
    for airline in airlines:
        grouped_airlines[_module_family(airline.get("module"))].append(airline)

    results = _run_batches_concurrently(grouped_airlines, args, cycle_id)

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
        "direct_airline_count": len(grouped_airlines["direct"]),
        "sharetrip_airline_count": len(grouped_airlines["sharetrip"]),
        "sharetrip_max_workers": args.sharetrip_max_workers,
        "wrapper_airline_count": len(grouped_airlines["wrapper"]),
        "wrapper_max_workers": args.wrapper_max_workers,
        "indigo_airline_count": len(grouped_airlines["indigo"]),
        "indigo_max_workers": args.indigo_max_workers,
        "failed_count": len(failed),
        "results": results,
    }

    latest = out_dir / "scrape_parallel_latest.json"
    run = out_dir / f"scrape_parallel_{ts}_{cycle_id}.json"
    existing_latest = _read_json(latest)
    existing_cycle = str(existing_latest.get("cycle_id") or "").strip()
    if (
        _is_global_parallel_latest_candidate(args, payload)
        or not existing_latest
        or existing_cycle == cycle_id
    ):
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
