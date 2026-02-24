"""
Always-on maintenance daemon (no-admin friendly).

Runs daily ops snapshot and weekly thesis pack while user session is active.
Intended to be launched at logon/startup.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import subprocess
import sys
import time
from pathlib import Path


LOG = logging.getLogger("always_on_maintenance")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def parse_args():
    p = argparse.ArgumentParser(description="Always-on maintenance loop")
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--state-file", default="output/reports/always_on_maintenance_state.json")
    p.add_argument("--poll-minutes", type=float, default=10.0)
    p.add_argument("--weekly-day", choices=["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"], default="SUN")
    p.add_argument("--run-on-start", action="store_true", help="Evaluate due tasks immediately on daemon start")
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    return p.parse_args()


WEEKDAY_NUM = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path: Path, state: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def run_task(python_exe: str, task: str, reports_dir: str) -> int:
    cmd = [
        python_exe,
        "scheduler/maintenance_tasks.py",
        "--task",
        task,
        "--reports-dir",
        reports_dir,
        "--timestamp-tz",
        "local",
    ]
    LOG.info("Running task=%s cmd=%s", task, subprocess.list2cmdline(cmd))
    rc = subprocess.run(cmd).returncode
    LOG.info("Task=%s rc=%s", task, rc)
    return rc


def current_week_key(now: dt.datetime) -> str:
    iso = now.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def should_run_daily(now: dt.datetime, state: dict) -> bool:
    return state.get("last_daily_date") != now.date().isoformat()


def should_run_weekly(now: dt.datetime, state: dict, weekly_day: str) -> bool:
    if now.weekday() != WEEKDAY_NUM[weekly_day]:
        return False
    return state.get("last_weekly_key") != current_week_key(now)


def cycle(args, state: dict) -> dict:
    now = dt.datetime.now().astimezone()
    if should_run_daily(now, state):
        rc = run_task(args.python_exe, "daily_ops", args.reports_dir)
        if rc == 0:
            state["last_daily_date"] = now.date().isoformat()
            state["last_daily_ok_at"] = now.isoformat()
        else:
            state["last_daily_rc"] = rc

    if should_run_weekly(now, state, args.weekly_day):
        rc = run_task(args.python_exe, "weekly_pack", args.reports_dir)
        if rc == 0:
            state["last_weekly_key"] = current_week_key(now)
            state["last_weekly_ok_at"] = now.isoformat()
        else:
            state["last_weekly_rc"] = rc

    state["last_cycle_at"] = now.isoformat()
    return state


def main():
    args = parse_args()
    poll_seconds = max(60, int(args.poll_minutes * 60))
    state_path = Path(args.state_file)
    state = load_state(state_path)

    LOG.info("Daemon started poll_minutes=%s weekly_day=%s", args.poll_minutes, args.weekly_day)

    if args.run_on_start:
        state = cycle(args, state)
        save_state(state_path, state)
        if args.once:
            return 0

    while True:
        if not args.run_on_start:
            # if not run_on_start, still run on first loop tick
            args.run_on_start = True
            state = cycle(args, state)
            save_state(state_path, state)
            if args.once:
                return 0
        LOG.info("Sleeping %ss", poll_seconds)
        time.sleep(poll_seconds)
        state = cycle(args, state)
        save_state(state_path, state)
        if args.once:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
