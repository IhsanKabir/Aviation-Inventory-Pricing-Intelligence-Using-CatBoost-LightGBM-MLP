from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.source_switches import DEFAULT_SOURCE_SWITCHES_FILE
from core.scheduler_timing import find_timing_entry, load_scheduler_timing_plan, pipeline_filter_args


def parse_args():
    parser = argparse.ArgumentParser(description="Run one configured scheduler timing scope.")
    parser.add_argument("--scope-type", choices=["global", "source", "airline", "route"], required=True)
    parser.add_argument("--scope-id", required=True)
    parser.add_argument("--python-exe", default=sys.executable)
    parser.add_argument("--schedule-file", default=str(REPO_ROOT / "config" / "schedule.json"))
    parser.add_argument("--airlines-file", default=str(REPO_ROOT / "config" / "airlines.json"))
    parser.add_argument("--source-switches-file", default=str(DEFAULT_SOURCE_SWITCHES_FILE))
    parser.add_argument("--report-output-dir", default=str(REPO_ROOT / "output" / "reports"))
    parser.add_argument("--reports-dir", default=str(REPO_ROOT / "output" / "reports"))
    parser.add_argument("--min-completed-gap-minutes", type=int)
    parser.add_argument("--skip-bigquery-sync", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _base_pipeline_cmd(args) -> list[str]:
    cmd = [
        args.python_exe,
        str(REPO_ROOT / "run_pipeline.py"),
        "--python-exe",
        args.python_exe,
        "--schedule-file",
        args.schedule_file,
        "--source-switches-file",
        args.source_switches_file,
        "--report-format",
        "xlsx",
        "--route-monitor",
        "--report-output-dir",
        args.report_output_dir,
        "--report-timestamp-tz",
        "local",
    ]
    if args.skip_bigquery_sync:
        cmd.append("--skip-bigquery-sync")
    return cmd


def main() -> int:
    args = parse_args()
    plan = load_scheduler_timing_plan(
        schedule_file=Path(args.schedule_file),
        airlines_file=Path(args.airlines_file),
        source_switches_file=args.source_switches_file,
    )
    entry = find_timing_entry(plan, scope_type=args.scope_type, scope_id=args.scope_id)
    if entry is None:
        raise SystemExit(f"Scheduler scope not found: {args.scope_type}:{args.scope_id}")
    if not entry.enabled:
        print(f"scheduler_scope_disabled scope={entry.scope_type}:{entry.scope_id}")
        return 0

    filters = pipeline_filter_args(entry)
    if entry.scope_type != "global" and not filters:
        print(f"scheduler_scope_empty scope={entry.scope_type}:{entry.scope_id}")
        return 0

    cmd = _base_pipeline_cmd(args) + filters
    recovery_helper = REPO_ROOT / "tools" / "recover_interrupted_accumulation.py"
    if recovery_helper.exists():
        gap_minutes = args.min_completed_gap_minutes
        if gap_minutes is None:
            gap_minutes = entry.completion_buffer_minutes or 1
        run_cmd = [
            args.python_exe,
            str(recovery_helper),
            "--mode",
            "guarded-run",
            "--python-exe",
            args.python_exe,
            "--root",
            str(REPO_ROOT),
            "--reports-dir",
            args.reports_dir,
            "--min-completed-gap-minutes",
            str(max(1, int(gap_minutes))),
            "--",
            *cmd,
        ]
    else:
        run_cmd = cmd

    print(subprocess.list2cmdline(run_cmd))
    if args.dry_run:
        return 0
    return subprocess.run(run_cmd, cwd=str(REPO_ROOT)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
