import argparse
import os
from pathlib import Path

from db import DATABASE_URL as DEFAULT_DATABASE_URL
from generate_reports import export_reports


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Compatibility entrypoint for hourly report generation. "
            "Defaults match the modern pipeline style."
        )
    )
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", help="YYYY-MM-DD")
    parser.add_argument("--airline", help="Filter airline code")
    parser.add_argument("--origin", help="Filter origin airport")
    parser.add_argument("--destination", help="Filter destination airport")
    parser.add_argument("--cabin", help="Filter cabin")
    parser.add_argument(
        "--output-dir",
        default="output/reports",
        help="Output directory for report run folders",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("AIRLINE_DB_URL", DEFAULT_DATABASE_URL),
        help="Postgres SQLAlchemy URL",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "xlsx", "both"],
        default="both",
        help="Report output format (default: both)",
    )
    parser.add_argument(
        "--timestamp-tz",
        choices=["local", "utc"],
        default="local",
        help="Timezone used for report timestamp labels",
    )
    parser.add_argument(
        "--route-monitor",
        dest="route_monitor",
        action="store_true",
        default=True,
        help="Include route_flight_fare_monitor workbook (default: enabled)",
    )
    parser.add_argument(
        "--style",
        choices=["compact", "presentation"],
        default="compact",
        help="Workbook visual style (default: compact)",
    )
    parser.add_argument(
        "--no-route-monitor",
        dest="route_monitor",
        action="store_false",
        help="Disable route_flight_fare_monitor workbook",
    )
    return parser.parse_args()


def generate_hourly_change_report():
    args = parse_args()
    exported = export_reports(args)

    for report_name, output_path, row_count in exported:
        print(f"{report_name}: {row_count} rows -> {output_path}")

    latest_run_txt = Path(args.output_dir) / "latest_run.txt"
    if latest_run_txt.exists():
        latest_run = latest_run_txt.read_text(encoding="utf-8").strip()
        if latest_run:
            print(f"latest_run: {latest_run}")


if __name__ == "__main__":
    generate_hourly_change_report()
