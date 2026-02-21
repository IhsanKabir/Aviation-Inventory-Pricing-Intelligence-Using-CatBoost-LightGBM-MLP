import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from db import DATABASE_URL as DEFAULT_DATABASE_URL


LOG = logging.getLogger("run_pipeline")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def parse_args():
    parser = argparse.ArgumentParser(description="Run scrape + reports as one pipeline")
    parser.add_argument("--python-exe", default=sys.executable, help="Python executable to run child scripts")
    parser.add_argument("--db-url", default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL), help="Postgres URL")

    # scrape filters
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--airline")
    parser.add_argument("--origin")
    parser.add_argument("--destination")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--dates", help="Comma-separated YYYY-MM-DD values for dynamic search windows")
    parser.add_argument("--date-offsets", help="Comma-separated day offsets from today")
    parser.add_argument("--dates-file", help="Optional dates config file path")
    parser.add_argument("--cabin")
    parser.add_argument("--limit-routes", type=int)
    parser.add_argument("--limit-dates", type=int)
    parser.add_argument("--parallel-airlines", type=int, default=1, help="Run one run_all process per airline in parallel when airline filter is not set")
    parser.add_argument("--profile-runtime", action="store_true", help="Enable runtime profiling output from run_all")
    parser.add_argument("--skip-scrape", action="store_true")

    # reports
    parser.add_argument("--skip-reports", action="store_true")
    parser.add_argument("--report-start-date", help="YYYY-MM-DD")
    parser.add_argument("--report-end-date", help="YYYY-MM-DD")
    parser.add_argument("--report-format", choices=["csv", "xlsx", "both"], default="both")
    parser.add_argument("--report-output-dir", default="output/reports")
    parser.add_argument("--report-timestamp-tz", choices=["local", "utc"], default="local")
    parser.add_argument("--route-monitor", action="store_true", help="Also generate route_flight_fare_monitor workbook")

    # prediction
    parser.add_argument("--run-prediction", action="store_true")
    parser.add_argument(
        "--prediction-target",
        choices=[
            "total_change_events",
            "price_events",
            "availability_events",
            "min_price_bdt",
            "avg_seat_available",
            "offers_count",
            "soldout_rate",
        ],
        default="total_change_events",
    )
    parser.add_argument(
        "--prediction-series-mode",
        choices=["event_daily", "search_dynamic"],
        default="event_daily",
    )
    parser.add_argument("--prediction-departure-start-date", help="YYYY-MM-DD departure lower bound for search_dynamic")
    parser.add_argument("--prediction-departure-end-date", help="YYYY-MM-DD departure upper bound for search_dynamic")
    parser.add_argument("--prediction-disable-backtest", action="store_true")

    # alert quality
    parser.add_argument("--run-alert-eval", action="store_true")
    parser.add_argument("--alert-lookback-days", type=int, default=7)
    parser.add_argument("--alert-spike-threshold", type=float, default=250.0)
    parser.add_argument("--alert-sellout-threshold", type=float, default=1.0)
    parser.add_argument("--alert-spike-false-alarm-cost", type=float, default=1.0)
    parser.add_argument("--alert-spike-missed-cost", type=float, default=3.0)
    parser.add_argument("--alert-sellout-false-alarm-cost", type=float, default=2.0)
    parser.add_argument("--alert-sellout-missed-cost", type=float, default=8.0)

    parser.add_argument("--fail-fast", action="store_true", help="Stop immediately on first step failure")
    return parser.parse_args()


def _count_column_events(db_url: str):
    try:
        engine = create_engine(db_url, pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            return conn.execute(text("SELECT count(*) FROM airline_intel.column_change_events")).scalar()
    except Exception as exc:
        LOG.warning("Could not read column_change_events count: %s", exc)
        return None


def _add_arg(cmd: list[str], flag: str, value):
    if value is None:
        return
    cmd.extend([flag, str(value)])


def _run_step(name: str, cmd: list[str]):
    LOG.info("%s command: %s", name, subprocess.list2cmdline(cmd))
    started = datetime.now(timezone.utc)
    result = subprocess.run(cmd)
    ended = datetime.now(timezone.utc)
    duration_sec = (ended - started).total_seconds()
    LOG.info("%s finished with rc=%s in %.1fs", name, result.returncode, duration_sec)
    return result.returncode


def build_scrape_cmd(args):
    if (args.parallel_airlines or 1) > 1 and not args.airline:
        cmd = [
            args.python_exe,
            "tools/parallel_airline_runner.py",
            "--python-exe",
            args.python_exe,
            "--max-workers",
            str(args.parallel_airlines),
            "--output-dir",
            args.report_output_dir,
        ]
        if args.quick:
            cmd.append("--quick")
        _add_arg(cmd, "--origin", args.origin)
        _add_arg(cmd, "--destination", args.destination)
        _add_arg(cmd, "--date", args.date)
        _add_arg(cmd, "--dates", args.dates)
        _add_arg(cmd, "--date-offsets", args.date_offsets)
        _add_arg(cmd, "--dates-file", args.dates_file)
        _add_arg(cmd, "--cabin", args.cabin)
        _add_arg(cmd, "--limit-routes", args.limit_routes)
        _add_arg(cmd, "--limit-dates", args.limit_dates)
        return cmd

    cmd = [args.python_exe, "run_all.py"]
    if args.quick:
        cmd.append("--quick")
    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--date", args.date)
    _add_arg(cmd, "--dates", args.dates)
    _add_arg(cmd, "--date-offsets", args.date_offsets)
    _add_arg(cmd, "--dates-file", args.dates_file)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--limit-routes", args.limit_routes)
    _add_arg(cmd, "--limit-dates", args.limit_dates)
    if args.profile_runtime:
        cmd.append("--profile-runtime")
        _add_arg(cmd, "--profile-output-dir", args.report_output_dir)
    return cmd


def build_report_cmd(args):
    cmd = [
        args.python_exe,
        "generate_reports.py",
        "--format",
        args.report_format,
        "--output-dir",
        args.report_output_dir,
        "--timestamp-tz",
        args.report_timestamp_tz,
    ]

    _add_arg(cmd, "--start-date", args.report_start_date)
    _add_arg(cmd, "--end-date", args.report_end_date)
    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    if args.route_monitor:
        cmd.append("--route-monitor")
    return cmd


def build_prediction_cmd(args):
    cmd = [
        args.python_exe,
        "predict_next_day.py",
        "--series-mode",
        args.prediction_series_mode,
        "--target-column",
        args.prediction_target,
        "--output-dir",
        args.report_output_dir,
    ]
    _add_arg(cmd, "--start-date", args.report_start_date)
    _add_arg(cmd, "--end-date", args.report_end_date)
    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    _add_arg(cmd, "--departure-start-date", args.prediction_departure_start_date)
    _add_arg(cmd, "--departure-end-date", args.prediction_departure_end_date)
    if args.prediction_disable_backtest:
        cmd.append("--disable-backtest")
    return cmd


def build_alert_eval_cmd(args):
    cmd = [
        args.python_exe,
        "tools/evaluate_alert_quality.py",
        "--output-dir",
        args.report_output_dir,
        "--timestamp-tz",
        args.report_timestamp_tz,
        "--lookback-days",
        str(args.alert_lookback_days),
        "--spike-threshold",
        str(args.alert_spike_threshold),
        "--sellout-threshold",
        str(args.alert_sellout_threshold),
        "--spike-false-alarm-cost",
        str(args.alert_spike_false_alarm_cost),
        "--spike-missed-cost",
        str(args.alert_spike_missed_cost),
        "--sellout-false-alarm-cost",
        str(args.alert_sellout_false_alarm_cost),
        "--sellout-missed-cost",
        str(args.alert_sellout_missed_cost),
    ]
    _add_arg(cmd, "--start-date", args.report_start_date)
    _add_arg(cmd, "--end-date", args.report_end_date)
    _add_arg(cmd, "--airline", args.airline)
    _add_arg(cmd, "--origin", args.origin)
    _add_arg(cmd, "--destination", args.destination)
    _add_arg(cmd, "--cabin", args.cabin)
    return cmd


def main():
    args = parse_args()
    before_count = _count_column_events(args.db_url)

    pipeline_rc = 0

    if not args.skip_scrape:
        rc = _run_step("scrape", build_scrape_cmd(args))
        if rc != 0:
            pipeline_rc = rc
            if args.fail_fast:
                return pipeline_rc
    else:
        LOG.info("Skipping scrape step.")

    if not args.skip_reports:
        rc = _run_step("reports", build_report_cmd(args))
        if rc != 0:
            pipeline_rc = rc or pipeline_rc
            if args.fail_fast:
                return pipeline_rc
    else:
        LOG.info("Skipping reports step.")

    if args.run_prediction:
        rc = _run_step("prediction", build_prediction_cmd(args))
        if rc != 0:
            pipeline_rc = rc or pipeline_rc
            if args.fail_fast:
                return pipeline_rc

    if args.run_alert_eval:
        rc = _run_step("alert_eval", build_alert_eval_cmd(args))
        if rc != 0:
            pipeline_rc = rc or pipeline_rc
            if args.fail_fast:
                return pipeline_rc

    after_count = _count_column_events(args.db_url)
    if before_count is not None and after_count is not None:
        LOG.info("column_change_events before=%s after=%s delta=%s", before_count, after_count, after_count - before_count)

    return pipeline_rc


if __name__ == "__main__":
    raise SystemExit(main())
