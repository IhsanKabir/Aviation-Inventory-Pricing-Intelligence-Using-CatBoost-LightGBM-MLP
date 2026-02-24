import argparse
import datetime
import shutil
import subprocess
import sys
from pathlib import Path


DEFAULT_OPS_LOGS = [
    "logs/scheduler_bg_live.err.log",
    "logs/scheduler_vq_live.err.log",
]


def parse_args():
    p = argparse.ArgumentParser(description="Run recurring maintenance tasks")
    p.add_argument(
        "--task",
        choices=["daily_ops", "weekly_pack", "both"],
        default="both",
        help="Which maintenance task to execute",
    )
    p.add_argument("--python-exe", default=sys.executable, help="Python executable for child scripts")

    p.add_argument("--reports-dir", default="output/reports")
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")

    p.add_argument("--ops-hours", type=float, default=24.0)
    p.add_argument("--ops-log", action="append", dest="ops_logs", help="Additional ops log paths")

    p.add_argument("--pack-prefix", default="thesis_pack")
    p.add_argument("--no-zip", action="store_true", help="Disable zip generation for thesis pack")
    p.add_argument("--disable-alert-notify", action="store_true", help="Skip ops WARN/FAIL notifier")
    p.add_argument("--notify-webhook-url", default="")
    p.add_argument("--notify-channel", default="ops-alerts")
    p.add_argument("--notify-strict", action="store_true")

    p.add_argument("--disable-cleanup", action="store_true", help="Skip retention cleanup")
    p.add_argument("--retention-log-days", type=int, default=30)
    p.add_argument("--retention-report-days", type=int, default=60)
    p.add_argument("--retention-dry-run", action="store_true")

    p.add_argument("--disable-status-snapshot", action="store_true", help="Skip status snapshot generation")
    p.add_argument("--logs-dir", default="logs")
    p.add_argument("--disable-db-storage-health", action="store_true", help="Skip DB storage health check")
    p.add_argument("--db-storage-health-strict", action="store_true")
    p.add_argument("--db-storage-health-warn-runway-days", type=float, default=60.0)
    p.add_argument("--db-storage-health-fail-runway-days", type=float, default=21.0)
    p.add_argument("--disable-db-backup", action="store_true", help="Skip daily DB backup")
    p.add_argument("--db-backup-output-dir", default="output/backups")
    p.add_argument("--db-backup-strict", action="store_true")
    p.add_argument("--disable-db-restore-test", action="store_true", help="Skip weekly DB restore validation")
    p.add_argument("--db-restore-mode", choices=["toc", "schema_sql"], default="toc")
    p.add_argument("--db-restore-strict", action="store_true")
    p.add_argument("--disable-smoke-check", action="store_true", help="Skip smoke check generation")
    p.add_argument("--smoke-strict", action="store_true")
    p.add_argument("--smoke-max-ops-age-hours", type=float, default=30.0)
    p.add_argument("--smoke-max-heartbeat-age-hours", type=float, default=6.0)
    p.add_argument("--disable-sla-dashboard", action="store_true", help="Skip data SLA dashboard")
    p.add_argument("--sla-strict", action="store_true")
    p.add_argument("--disable-drift-monitor", action="store_true", help="Skip model drift monitor")
    p.add_argument("--drift-strict", action="store_true")
    p.add_argument("--disable-recovery-check", action="store_true", help="Skip missed-window recovery scan")
    p.add_argument("--recovery-active", action="store_true", help="Enable active missed-window recovery (default is dry-run scan)")
    p.add_argument("--disable-operator-dashboard", action="store_true", help="Skip operator dashboard build")
    p.add_argument("--disable-restore-drill", action="store_true", help="Skip weekly full restore drill")
    p.add_argument("--restore-drill-strict", action="store_true")
    p.add_argument("--enable-db-compact-raw-meta", action="store_true", help="Run raw-meta compaction in weekly task if within maintenance window")
    p.add_argument("--db-compact-mode", choices=["analyze", "vacuum", "vacuum_full", "reindex", "vacuum_full_reindex"], default="vacuum")
    p.add_argument("--db-compact-window-weekday", type=int, default=6, help="0=Mon ... 6=Sun")
    p.add_argument("--db-compact-window-hour", type=int, default=3)
    p.add_argument("--db-compact-window-minute", type=int, default=30)
    p.add_argument("--db-compact-window-span-minutes", type=int, default=120)
    p.add_argument("--db-compact-dry-run", action="store_true", help="Build and run compaction script in dry-run mode only")
    p.add_argument("--db-compact-strict", action="store_true")
    return p.parse_args()


def _run(cmd):
    print("RUN:", subprocess.list2cmdline(cmd))
    rc = subprocess.run(cmd).returncode
    print("RC:", rc)
    return rc


def _run_soft(cmd, label: str):
    rc = _run(cmd)
    if rc != 0:
        print(f"WARNING: soft step failed ({label}) rc={rc}")
    return rc


def _now_local():
    return datetime.datetime.now().astimezone()


def _within_maintenance_window(now_dt: datetime.datetime, weekday: int, hour: int, minute: int, span_minutes: int) -> bool:
    if now_dt.weekday() != int(weekday):
        return False
    start_min = int(hour) * 60 + int(minute)
    now_min = now_dt.hour * 60 + now_dt.minute
    return start_min <= now_min < (start_min + max(int(span_minutes), 1))


def run_daily_ops(args) -> int:
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    latest = reports_dir / "ops_health_latest.md"
    ts = _now_local().strftime("%Y%m%d_%H%M%S")
    archive = reports_dir / f"ops_health_{ts}.md"

    logs = list(DEFAULT_OPS_LOGS)
    if args.ops_logs:
        logs.extend(args.ops_logs)

    cmd = [
        args.python_exe,
        "tools/ops_health_check.py",
        "--hours",
        str(args.ops_hours),
        "--output",
        str(latest),
    ]
    for log in logs:
        cmd.extend(["--log", log])

    rc = _run(cmd)
    if rc != 0:
        return rc
    if latest.exists():
        shutil.copy2(latest, archive)
        print(f"Saved daily archive: {archive}")

    if not args.disable_alert_notify:
        notify_cmd = [
            args.python_exe,
            "tools/notify_ops_health.py",
            "--ops-health-path",
            str(latest),
            "--output-dir",
            str(reports_dir),
            "--channel",
            args.notify_channel,
        ]
        if args.notify_webhook_url:
            notify_cmd.extend(["--webhook-url", args.notify_webhook_url])
        if args.notify_strict:
            notify_cmd.append("--strict")
            rc_n = _run(notify_cmd)
            if rc_n != 0:
                return rc_n
        else:
            _run_soft(notify_cmd, "notify_ops_health")

    if not args.disable_db_backup:
        backup_cmd = [
            args.python_exe,
            "tools/db_backup.py",
            "--output-dir",
            args.db_backup_output_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.db_backup_strict:
            backup_cmd.append("--strict")
            rc_b = _run(backup_cmd)
            if rc_b != 0:
                return rc_b
        else:
            _run_soft(backup_cmd, "db_backup")

    if not args.disable_cleanup:
        cleanup_cmd = [
            args.python_exe,
            "tools/retention_cleanup.py",
            "--logs-dir",
            args.logs_dir,
            "--reports-dir",
            str(reports_dir),
            "--log-retention-days",
            str(args.retention_log_days),
            "--report-retention-days",
            str(args.retention_report_days),
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.retention_dry_run:
            cleanup_cmd.append("--dry-run")
        _run_soft(cleanup_cmd, "retention_cleanup")

    if not args.disable_status_snapshot:
        status_cmd = [
            args.python_exe,
            "tools/system_status_snapshot.py",
            "--reports-dir",
            str(reports_dir),
            "--logs-dir",
            args.logs_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        _run_soft(status_cmd, "system_status_snapshot")

    if not args.disable_db_storage_health:
        storage_cmd = [
            args.python_exe,
            "tools/db_storage_health_check.py",
            "--output-dir",
            str(reports_dir),
            "--timestamp-tz",
            args.timestamp_tz,
            "--warn-runway-days",
            str(args.db_storage_health_warn_runway_days),
            "--fail-runway-days",
            str(args.db_storage_health_fail_runway_days),
        ]
        if args.db_storage_health_strict:
            storage_cmd.append("--strict")
            rc_sh = _run(storage_cmd)
            if rc_sh != 0:
                return rc_sh
        else:
            _run_soft(storage_cmd, "db_storage_health_check")

    if not args.disable_smoke_check:
        smoke_cmd = [
            args.python_exe,
            "tools/smoke_check.py",
            "--reports-dir",
            str(reports_dir),
            "--backups-dir",
            args.db_backup_output_dir,
            "--logs-dir",
            args.logs_dir,
            "--max-ops-age-hours",
            str(args.smoke_max_ops_age_hours),
            "--max-heartbeat-age-hours",
            str(args.smoke_max_heartbeat_age_hours),
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.smoke_strict:
            smoke_cmd.append("--strict")
            rc_sm = _run(smoke_cmd)
            if rc_sm != 0:
                return rc_sm
        else:
            _run_soft(smoke_cmd, "smoke_check")

    if not args.disable_sla_dashboard:
        sla_cmd = [
            args.python_exe,
            "tools/data_sla_dashboard.py",
            "--reports-dir",
            str(reports_dir),
            "--logs-dir",
            args.logs_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.notify_webhook_url:
            sla_cmd.extend(["--webhook-url", args.notify_webhook_url, "--channel", args.notify_channel])
        if args.sla_strict:
            sla_cmd.append("--strict")
            rc_sla = _run(sla_cmd)
            if rc_sla != 0:
                return rc_sla
        else:
            _run_soft(sla_cmd, "data_sla_dashboard")

    if not args.disable_drift_monitor:
        drift_cmd = [
            args.python_exe,
            "tools/model_drift_monitor.py",
            "--reports-dir",
            str(reports_dir),
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.drift_strict:
            drift_cmd.append("--strict")
            rc_drift = _run(drift_cmd)
            if rc_drift != 0:
                return rc_drift
        else:
            _run_soft(drift_cmd, "model_drift_monitor")

    if not args.disable_recovery_check:
        recover_cmd = [
            args.python_exe,
            "tools/recover_missed_windows.py",
            "--python-exe",
            args.python_exe,
            "--output-dir",
            str(reports_dir),
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if not args.recovery_active:
            recover_cmd.append("--dry-run")
        _run_soft(recover_cmd, "recover_missed_windows")

    if not args.disable_operator_dashboard:
        dash_cmd = [
            args.python_exe,
            "tools/build_operator_dashboard.py",
            "--reports-dir",
            str(reports_dir),
            "--backups-dir",
            args.db_backup_output_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        _run_soft(dash_cmd, "build_operator_dashboard")

    if args.enable_db_compact_raw_meta:
        now_local = _now_local()
        if _within_maintenance_window(
            now_local,
            args.db_compact_window_weekday,
            args.db_compact_window_hour,
            args.db_compact_window_minute,
            args.db_compact_window_span_minutes,
        ):
            compact_cmd = [
                args.python_exe,
                "tools/db_compact_raw_meta.py",
                "--mode",
                args.db_compact_mode,
                "--output-dir",
                args.reports_dir,
                "--timestamp-tz",
                args.timestamp_tz,
            ]
            if args.db_compact_dry_run:
                compact_cmd.append("--dry-run")
            if args.db_compact_strict:
                compact_cmd.append("--strict")
                rc_c = _run(compact_cmd)
                if rc_c != 0:
                    return rc_c
            else:
                _run_soft(compact_cmd, "db_compact_raw_meta")
        else:
            print(
                "SKIP: db_compact_raw_meta outside maintenance window "
                f"(weekday={now_local.weekday()} time={now_local.strftime('%H:%M')})"
            )
    return 0


def run_weekly_pack(args) -> int:
    cmd = [
        args.python_exe,
        "tools/build_thesis_pack.py",
        "--reports-dir",
        args.reports_dir,
        "--output-dir",
        args.reports_dir,
        "--pack-prefix",
        args.pack_prefix,
        "--timestamp-tz",
        args.timestamp_tz,
    ]
    if not args.no_zip:
        cmd.append("--zip")
    rc = _run(cmd)
    if rc != 0:
        return rc

    if not args.disable_status_snapshot:
        status_cmd = [
            args.python_exe,
            "tools/system_status_snapshot.py",
            "--reports-dir",
            args.reports_dir,
            "--logs-dir",
            args.logs_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        _run_soft(status_cmd, "system_status_snapshot")

    if not args.disable_db_restore_test:
        restore_cmd = [
            args.python_exe,
            "tools/db_restore_test.py",
            "--backup-meta",
            str(Path(args.db_backup_output_dir) / "db_backup_latest.json"),
            "--output-dir",
            args.db_backup_output_dir,
            "--mode",
            args.db_restore_mode,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.db_restore_strict:
            restore_cmd.append("--strict")
            rc_r = _run(restore_cmd)
            if rc_r != 0:
                return rc_r
        else:
            _run_soft(restore_cmd, "db_restore_test")

    if not args.disable_restore_drill:
        drill_cmd = [
            args.python_exe,
            "tools/db_restore_drill.py",
            "--output-dir",
            args.db_backup_output_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        if args.db_backup_output_dir:
            drill_cmd.extend(["--backup-meta", str(Path(args.db_backup_output_dir) / "db_backup_latest.json")])
        if args.restore_drill_strict:
            drill_cmd.append("--strict")
            rc_d = _run(drill_cmd)
            if rc_d != 0:
                return rc_d
        else:
            _run_soft(drill_cmd, "db_restore_drill")

    if not args.disable_operator_dashboard:
        dash_cmd = [
            args.python_exe,
            "tools/build_operator_dashboard.py",
            "--reports-dir",
            args.reports_dir,
            "--backups-dir",
            args.db_backup_output_dir,
            "--timestamp-tz",
            args.timestamp_tz,
        ]
        _run_soft(dash_cmd, "build_operator_dashboard")
    return 0


def main():
    args = parse_args()
    rc = 0

    if args.task in ("daily_ops", "both"):
        rc = run_daily_ops(args)
        if rc != 0:
            return rc

    if args.task in ("weekly_pack", "both"):
        rc = run_weekly_pack(args)
        if rc != 0:
            return rc

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
