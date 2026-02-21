"""
Unified CI/local quality checks:
- py_compile for core scripts
- unit/contract tests
- smoke check
- report generation dry run
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Run CI checks")
    p.add_argument("--python-exe", default=sys.executable)
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--allow-db-skip", action="store_true", help="Do not fail when DB-dependent checks are unavailable")
    p.add_argument("--require-db", action="store_true", help="Fail if DB-dependent checks cannot run")
    p.add_argument("--strict-smoke", action="store_true", help="Enforce smoke_check PASS")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    return p.parse_args()


def _run(cmd: list[str], label: str):
    started = datetime.now(timezone.utc)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    ended = datetime.now(timezone.utc)
    return {
        "label": label,
        "cmd": subprocess.list2cmdline(cmd),
        "rc": int(proc.returncode),
        "stdout": (proc.stdout or "")[-4000:],
        "stderr": (proc.stderr or "")[-4000:],
        "duration_sec": (ended - started).total_seconds(),
    }


def _is_db_skip(step: dict) -> bool:
    text = f"{step.get('stdout', '')}\n{step.get('stderr', '')}".lower()
    db_patterns = [
        "could not connect",
        "connection refused",
        "failed to connect",
        "database",
        "relation",
        "psycopg2",
    ]
    return any(p in text for p in db_patterns)


def main():
    args = parse_args()
    py = args.python_exe
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")

    checks: list[dict] = []

    compile_targets = [
        "run_all.py",
        "run_pipeline.py",
        "predict_next_day.py",
        "generate_reports.py",
        "scheduler/maintenance_tasks.py",
        "tools/smoke_check.py",
        "tools/data_sla_dashboard.py",
        "tools/model_drift_monitor.py",
        "tools/db_restore_drill.py",
    ]
    checks.append(_run([py, "-m", "py_compile", *compile_targets], "py_compile"))
    checks.append(_run([py, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"], "unit_tests"))

    smoke_cmd = [py, "tools/smoke_check.py", "--reports-dir", str(reports_dir), "--timestamp-tz", args.timestamp_tz]
    if args.strict_smoke:
        smoke_cmd.append("--strict")
    checks.append(_run(smoke_cmd, "smoke_check"))

    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=1)).isoformat()
    checks.append(
        _run(
            [
                py,
                "generate_reports.py",
                "--format",
                "csv",
                "--output-dir",
                str(reports_dir),
                "--start-date",
                start_date,
                "--end-date",
                end_date,
                "--timestamp-tz",
                args.timestamp_tz,
            ],
            "report_dry_run",
        )
    )

    failed = []
    skipped = []
    for step in checks:
        if step["rc"] == 0:
            continue
        db_optional = not args.require_db
        if (args.allow_db_skip or db_optional) and step["label"] in {"smoke_check", "report_dry_run"} and _is_db_skip(step):
            skipped.append(step["label"])
            continue
        failed.append(step["label"])

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ok": len(failed) == 0,
        "failed": failed,
        "skipped": skipped,
        "checks": checks,
    }

    latest = reports_dir / "ci_checks_latest.json"
    run_file = reports_dir / f"ci_checks_{ts}.json"
    latest.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    run_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"ci_ok={summary['ok']} failed={failed} skipped={skipped}")
    print(f"latest={latest}")
    print(f"run={run_file}")
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
