import argparse
import datetime
import logging
import subprocess
import sys
import time
from pathlib import Path


LOG = logging.getLogger("run_every_4_hours")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_args():
    parser = argparse.ArgumentParser(description="Run run_pipeline.py every N hours")
    parser.add_argument("--interval-hours", type=float, default=4.0, help="Scheduler interval in hours")
    parser.add_argument("--python-exe", default=sys.executable, help="Python executable")
    parser.add_argument("--once", action="store_true", help="Run pipeline only once and exit")

    # pass-through filters to pipeline
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--airline")
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date")
    parser.add_argument("--date-start")
    parser.add_argument("--date-end")
    parser.add_argument("--dates")
    parser.add_argument("--date-offsets")
    parser.add_argument("--dates-file")
    parser.add_argument("--schedule-file")
    parser.add_argument("--cabin")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--probe-group-id")
    parser.add_argument("--route-scope", choices=["all", "domestic", "international"])
    parser.add_argument("--market-country")
    parser.add_argument("--strict-route-audit", action="store_true")
    parser.add_argument("--limit-routes", type=int)
    parser.add_argument("--limit-dates", type=int)
    parser.add_argument("--report-start-date")
    parser.add_argument("--report-end-date")
    parser.add_argument("--report-format", choices=["csv", "xlsx", "both"], default="both")
    parser.add_argument("--report-timestamp-tz", choices=["local", "utc"], default="local")
    parser.add_argument("--report-output-dir", default="output/reports")
    parser.add_argument("--route-monitor", action="store_true")
    parser.add_argument("--run-prediction", action="store_true")
    return parser.parse_args()


def _add_arg(cmd: list[str], flag: str, value):
    if value is None:
        return
    cmd.extend([flag, str(value)])


def build_pipeline_cmd(args):
    cmd = [args.python_exe, str(REPO_ROOT / "run_pipeline.py")]
    if args.quick:
        cmd.append("--quick")
    if args.run_prediction:
        cmd.append("--run-prediction")
    if args.route_monitor:
        cmd.append("--route-monitor")
    cmd.extend(["--report-format", args.report_format])
    cmd.extend(["--report-timestamp-tz", args.report_timestamp_tz])
    cmd.extend(["--report-output-dir", args.report_output_dir])

    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--date", args.date)
    _add_arg(cmd, "--date-start", args.date_start)
    _add_arg(cmd, "--date-end", args.date_end)
    _add_arg(cmd, "--dates", args.dates)
    _add_arg(cmd, "--date-offsets", args.date_offsets)
    _add_arg(cmd, "--dates-file", args.dates_file)
    _add_arg(cmd, "--schedule-file", args.schedule_file)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--adt", args.adt)
    _add_arg(cmd, "--chd", args.chd)
    _add_arg(cmd, "--inf", args.inf)
    _add_arg(cmd, "--probe-group-id", args.probe_group_id)
    _add_arg(cmd, "--route-scope", args.route_scope)
    _add_arg(cmd, "--market-country", args.market_country)
    if args.strict_route_audit:
        cmd.append("--strict-route-audit")
    _add_arg(cmd, "--limit-routes", args.limit_routes)
    _add_arg(cmd, "--limit-dates", args.limit_dates)
    _add_arg(cmd, "--report-start-date", args.report_start_date)
    _add_arg(cmd, "--report-end-date", args.report_end_date)
    return cmd


def run_once(args):
    cmd = build_pipeline_cmd(args)
    LOG.info("Pipeline command: %s", subprocess.list2cmdline(cmd))
    started = datetime.datetime.now()
    rc = subprocess.run(cmd, cwd=str(REPO_ROOT)).returncode
    duration = (datetime.datetime.now() - started).total_seconds()
    LOG.info("Pipeline finished rc=%s in %.1fs", rc, duration)
    return rc


def main():
    args = parse_args()
    interval_sec = int(args.interval_hours * 3600)
    if interval_sec <= 0:
        raise SystemExit("--interval-hours must be greater than 0")

    LOG.info("Scheduler started interval=%s hours", args.interval_hours)

    while True:
        run_once(args)
        if args.once:
            break
        LOG.info("Sleeping %s seconds", interval_sec)
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()
