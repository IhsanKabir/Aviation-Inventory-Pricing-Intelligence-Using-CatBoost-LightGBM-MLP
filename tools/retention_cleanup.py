"""
Retention cleanup for logs and report artifacts.

Keeps critical pointers and only removes files/dirs older than configured age.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


KEEP_REPORT_FILES = {
    "ops_health_latest.md",
    "latest_run.json",
    "latest_run.txt",
    "always_on_maintenance_state.json",
    "system_status_latest.md",
    "system_status_latest.json",
    "smoke_check_latest.md",
    "smoke_check_latest.json",
    "data_sla_latest.md",
    "data_sla_latest.json",
    "model_drift_latest.md",
    "model_drift_latest.json",
    "operator_dashboard_latest.md",
    "operator_dashboard_latest.html",
    "operator_dashboard_latest.json",
}

KEEP_LOG_FILES = {
    "run_all.log",
    "always_on_maintenance.log",
    "maintenance_daily_ops.log",
    "maintenance_weekly_pack.log",
    "maintenance_pulse.log",
    "scheduler_bg_live.err.log",
    "scheduler_vq_live.err.log",
}


def parse_args():
    p = argparse.ArgumentParser(description="Cleanup stale logs/report artifacts")
    p.add_argument("--logs-dir", default="logs")
    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--archive-dir", default="output/archive")
    p.add_argument("--log-retention-days", type=int, default=30)
    p.add_argument("--report-retention-days", type=int, default=60)
    p.add_argument("--raw-retention-days", type=int, default=14, help="Retention for raw payload/history artifacts")
    p.add_argument("--aggregate-retention-days", type=int, default=180, help="Retention for aggregate reports")
    p.add_argument("--thesis-retention-days", type=int, default=365, help="Retention for thesis packs")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    return p.parse_args()


def _now(tz_mode: str):
    return datetime.now(timezone.utc) if tz_mode == "utc" else datetime.now().astimezone()


def _stamp(now: datetime):
    return now.strftime("%Y%m%d_%H%M%S")


def old_enough(path: Path, cutoff: datetime) -> bool:
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=cutoff.tzinfo)
    return mtime < cutoff


def cleanup_logs(logs_dir: Path, cutoff: datetime, dry_run: bool):
    removed = []
    kept = []
    if not logs_dir.exists():
        return removed, kept
    for p in logs_dir.glob("*"):
        if not p.is_file():
            continue
        if p.name in KEEP_LOG_FILES:
            kept.append(str(p))
            continue
        if old_enough(p, cutoff):
            if not dry_run:
                p.unlink(missing_ok=True)
            removed.append(str(p))
        else:
            kept.append(str(p))
    return removed, kept


def _classify_report_path(path: Path) -> str:
    name = path.name.lower()
    if "thesis_pack_" in name:
        return "thesis"
    raw_tokens = ["combined_results", "raw_response", "debug_response", "inspect_payload", "legacy_migration"]
    if any(t in name for t in raw_tokens):
        return "raw"
    return "aggregate"


def _cleanup_dir_recursive(path: Path, dry_run: bool):
    if dry_run:
        return
    for c in sorted(path.rglob("*"), reverse=True):
        if c.is_file():
            c.unlink(missing_ok=True)
        elif c.is_dir():
            c.rmdir()
    path.rmdir()


def cleanup_reports(
    reports_dir: Path,
    raw_cutoff: datetime,
    aggregate_cutoff: datetime,
    thesis_cutoff: datetime,
    dry_run: bool,
):
    removed = []
    kept = []
    removed_by_tier = {"raw": 0, "aggregate": 0, "thesis": 0}
    kept_by_tier = {"raw": 0, "aggregate": 0, "thesis": 0}
    if not reports_dir.exists():
        return removed, kept, removed_by_tier, kept_by_tier

    for p in reports_dir.iterdir():
        tier = _classify_report_path(p)
        cutoff = aggregate_cutoff
        if tier == "raw":
            cutoff = raw_cutoff
        elif tier == "thesis":
            cutoff = thesis_cutoff

        if p.is_file():
            if p.name in KEEP_REPORT_FILES:
                kept.append(str(p))
                kept_by_tier[tier] += 1
                continue
            if old_enough(p, cutoff):
                if not dry_run:
                    p.unlink(missing_ok=True)
                removed.append(str(p))
                removed_by_tier[tier] += 1
            else:
                kept.append(str(p))
                kept_by_tier[tier] += 1
            continue

        if p.is_dir():
            if old_enough(p, cutoff):
                _cleanup_dir_recursive(p, dry_run)
                removed.append(str(p))
                removed_by_tier[tier] += 1
            else:
                kept.append(str(p))
                kept_by_tier[tier] += 1
    return removed, kept, removed_by_tier, kept_by_tier


def cleanup_archive(archive_dir: Path, cutoff: datetime, dry_run: bool):
    removed = []
    kept = []
    if not archive_dir.exists():
        return removed, kept
    for p in archive_dir.iterdir():
        if old_enough(p, cutoff):
            if p.is_file():
                if not dry_run:
                    p.unlink(missing_ok=True)
            elif p.is_dir():
                _cleanup_dir_recursive(p, dry_run)
            removed.append(str(p))
        else:
            kept.append(str(p))
    return removed, kept


def main():
    args = parse_args()
    now = _now(args.timestamp_tz)
    stamp = _stamp(now)

    logs_dir = Path(args.logs_dir)
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    log_cutoff = now - timedelta(days=max(1, args.log_retention_days))
    report_cutoff = now - timedelta(days=max(1, args.report_retention_days))
    raw_cutoff = now - timedelta(days=max(1, args.raw_retention_days))
    aggregate_cutoff = now - timedelta(days=max(1, args.aggregate_retention_days))
    thesis_cutoff = now - timedelta(days=max(1, args.thesis_retention_days))

    log_removed, log_kept = cleanup_logs(logs_dir, log_cutoff, args.dry_run)
    rep_removed, rep_kept, rep_removed_tier, rep_kept_tier = cleanup_reports(
        reports_dir,
        raw_cutoff=raw_cutoff,
        aggregate_cutoff=aggregate_cutoff,
        thesis_cutoff=thesis_cutoff,
        dry_run=args.dry_run,
    )
    arch_removed, arch_kept = cleanup_archive(Path(args.archive_dir), raw_cutoff, args.dry_run)

    summary = {
        "generated_at": now.isoformat(),
        "dry_run": bool(args.dry_run),
        "log_retention_days": args.log_retention_days,
        "report_retention_days": args.report_retention_days,
        "raw_retention_days": args.raw_retention_days,
        "aggregate_retention_days": args.aggregate_retention_days,
        "thesis_retention_days": args.thesis_retention_days,
        "removed_log_count": len(log_removed),
        "removed_report_count": len(rep_removed),
        "removed_archive_count": len(arch_removed),
        "removed_logs": log_removed,
        "removed_reports": rep_removed,
        "removed_archive": arch_removed,
        "kept_log_count": len(log_kept),
        "kept_report_count": len(rep_kept),
        "kept_archive_count": len(arch_kept),
        "report_removed_by_tier": rep_removed_tier,
        "report_kept_by_tier": rep_kept_tier,
    }

    latest_path = reports_dir / "retention_cleanup_latest.json"
    run_path = reports_dir / f"retention_cleanup_{stamp}.json"
    latest_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    run_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"removed_logs={len(log_removed)} removed_reports={len(rep_removed)} dry_run={args.dry_run}")
    print(f"latest={latest_path}")
    print(f"run={run_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
