"""
Lightweight operational smoke check for daily/weekly automation confidence.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from core.runtime_config import get_database_url



SEVERITY_ORDER = {"PASS": 0, "WARN": 1, "FAIL": 2}


def parse_args():
    p = argparse.ArgumentParser(description="Run smoke checks and emit PASS/WARN/FAIL summary")
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--backups-dir", default="output/backups")
    p.add_argument("--logs-dir", default="logs")
    p.add_argument("--db-url", default=get_database_url())
    p.add_argument("--max-ops-age-hours", type=float, default=30.0)
    p.add_argument("--max-heartbeat-age-hours", type=float, default=6.0)
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def _age_hours(path: Path, now: datetime):
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone(now.tzinfo)
    return (now - modified).total_seconds() / 3600.0, modified


def _file_fresh_check(name: str, path: Path, now: datetime, max_age_hours: float, required: bool = True):
    if not path.exists():
        return {
            "check": name,
            "status": "FAIL" if required else "WARN",
            "detail": f"missing:{path}",
        }

    age_h, modified = _age_hours(path, now)
    if age_h <= max_age_hours:
        status = "PASS"
    else:
        status = "WARN" if not required else "FAIL"
    return {
        "check": name,
        "status": status,
        "detail": f"path={path}; age_hours={age_h:.2f}; modified={modified.isoformat()}",
    }


def _db_check(db_url: str):
    try:
        engine = create_engine(db_url, pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            one = conn.execute(text("SELECT 1")).scalar()
            offers = conn.execute(text("SELECT count(*) FROM flight_offers")).scalar()
        if one == 1:
            return {
                "check": "db_connectivity",
                "status": "PASS",
                "detail": f"select_1_ok; flight_offers_count={offers}",
            }
        return {
            "check": "db_connectivity",
            "status": "FAIL",
            "detail": "select_1_unexpected_result",
        }
    except Exception as exc:  # pragma: no cover
        return {
            "check": "db_connectivity",
            "status": "FAIL",
            "detail": f"db_error:{exc}",
        }


def _deps_check():
    missing = []
    for pkg in ["sqlalchemy", "pandas", "openpyxl", "requests", "alembic", "networkx"]:
        if importlib.util.find_spec(pkg) is None:
            missing.append(pkg)
    if missing:
        return {
            "check": "python_dependencies",
            "status": "FAIL",
            "detail": f"missing={','.join(missing)}",
        }
    return {
        "check": "python_dependencies",
        "status": "PASS",
        "detail": "core_packages_available",
    }


def _latest_heartbeat_check(now: datetime, reports_dir: Path, logs_dir: Path, max_age_hours: float):
    candidates = [
        reports_dir / "always_on_maintenance.log",
        reports_dir / "maintenance_pulse.log",
        logs_dir / "always_on_maintenance.log",
        logs_dir / "maintenance_pulse.log",
    ]
    existing = [p for p in candidates if p.exists()]
    if not existing:
        return {
            "check": "maintenance_heartbeat",
            "status": "WARN",
            "detail": "no_heartbeat_log_found",
        }

    newest = max(existing, key=lambda p: p.stat().st_mtime)
    age_h, modified = _age_hours(newest, now)
    status = "PASS" if age_h <= max_age_hours else "WARN"
    return {
        "check": "maintenance_heartbeat",
        "status": status,
        "detail": f"latest={newest}; age_hours={age_h:.2f}; modified={modified.isoformat()}",
    }


def _backup_ok_check(backups_dir: Path):
    latest = backups_dir / "db_backup_latest.json"
    if not latest.exists():
        return {
            "check": "db_backup_latest",
            "status": "WARN",
            "detail": "missing:db_backup_latest.json",
        }
    try:
        meta = json.loads(latest.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "check": "db_backup_latest",
            "status": "WARN",
            "detail": "invalid_json:db_backup_latest.json",
        }
    if meta.get("ok"):
        return {
            "check": "db_backup_latest",
            "status": "PASS",
            "detail": f"ok:true; backup_file={meta.get('backup_file')}",
        }
    return {
        "check": "db_backup_latest",
        "status": "WARN",
        "detail": f"ok:false; detail={meta.get('detail', '')}",
    }


def _restore_ok_check(backups_dir: Path, now: datetime):
    latest = backups_dir / "db_restore_test_latest.json"
    if not latest.exists():
        return {
            "check": "db_restore_test_latest",
            "status": "WARN",
            "detail": "missing:db_restore_test_latest.json",
        }
    try:
        meta = json.loads(latest.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "check": "db_restore_test_latest",
            "status": "WARN",
            "detail": "invalid_json:db_restore_test_latest.json",
        }

    ts = meta.get("generated_at")
    stale_note = ""
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if now - dt.astimezone(now.tzinfo) > timedelta(days=8):
                stale_note = " (stale>8d)"
        except ValueError:
            pass

    if meta.get("ok"):
        return {
            "check": "db_restore_test_latest",
            "status": "PASS",
            "detail": f"ok:true; mode={meta.get('mode')}; toc_entries={meta.get('toc_entries')}{stale_note}",
        }
    return {
        "check": "db_restore_test_latest",
        "status": "WARN",
        "detail": f"ok:false; detail={meta.get('detail', '')}{stale_note}",
    }


def _overall_status(checks: list[dict]):
    max_level = max(SEVERITY_ORDER[c["status"]] for c in checks) if checks else 2
    for status, level in SEVERITY_ORDER.items():
        if level == max_level:
            return status
    return "FAIL"


def _render_md(summary: dict):
    lines = [
        "# Smoke Check",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Overall status: **{summary['status']}**",
        "",
        "## Checks",
        "",
    ]
    for check in summary["checks"]:
        lines.append(f"- `{check['check']}`: **{check['status']}** - {check['detail']}")
    lines.append("")
    return "\n".join(lines)


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    ts = _stamp(now)

    reports_dir = Path(args.reports_dir)
    backups_dir = Path(args.backups_dir)
    logs_dir = Path(args.logs_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    checks = []
    checks.append(_deps_check())
    checks.append(_db_check(args.db_url))
    checks.append(_file_fresh_check("ops_health_latest", reports_dir / "ops_health_latest.md", now, args.max_ops_age_hours, required=True))
    checks.append(_file_fresh_check("system_status_latest", reports_dir / "system_status_latest.json", now, args.max_ops_age_hours, required=False))
    checks.append(_backup_ok_check(backups_dir))
    checks.append(_restore_ok_check(backups_dir, now))
    checks.append(_latest_heartbeat_check(now, reports_dir, logs_dir, args.max_heartbeat_age_hours))

    status = _overall_status(checks)
    summary = {
        "generated_at": now.isoformat(),
        "status": status,
        "checks": checks,
    }

    latest_json = reports_dir / "smoke_check_latest.json"
    archive_json = reports_dir / f"smoke_check_{ts}.json"
    latest_md = reports_dir / "smoke_check_latest.md"
    archive_md = reports_dir / f"smoke_check_{ts}.md"

    latest_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    archive_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    md_text = _render_md(summary)
    latest_md.write_text(md_text, encoding="utf-8")
    archive_md.write_text(md_text, encoding="utf-8")

    print(f"smoke_status={status}")
    print(f"latest_json={latest_json}")
    print(f"latest_md={latest_md}")

    if args.strict and status != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
