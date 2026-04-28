from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Iterable


PROJECT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
DATASET_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

HIGH_VOLUME_FACT_TABLES = [
    "fact_cycle_run",
    "fact_offer_snapshot",
    "fact_change_event",
    "fact_penalty_snapshot",
    "fact_tax_snapshot",
]

FORECAST_FACT_TABLES = [
    "fact_forecast_bundle",
    "fact_forecast_model_eval",
    "fact_forecast_route_eval",
    "fact_forecast_next_day",
    "fact_backtest_eval",
    "fact_backtest_split",
]


def _validate_project_dataset(project_id: str, dataset: str) -> None:
    if not PROJECT_RE.match(project_id or ""):
        raise ValueError(f"Invalid BigQuery project id: {project_id!r}")
    if not DATASET_RE.match(dataset or ""):
        raise ValueError(f"Invalid BigQuery dataset: {dataset!r}")


def _validate_retention_args(hot_days: int, forecast_days: int, time_travel_hours: int) -> None:
    if hot_days < 1:
        raise ValueError("--hot-days must be >= 1")
    if forecast_days < hot_days:
        raise ValueError("--forecast-days must be >= --hot-days")
    if time_travel_hours not in {48, 72, 96, 120, 144, 168}:
        raise ValueError("--time-travel-hours must be one of 48, 72, 96, 120, 144, 168")


def build_retention_statements(
    project_id: str,
    dataset: str,
    hot_days: int = 35,
    forecast_days: int = 90,
    time_travel_hours: int = 48,
) -> list[str]:
    _validate_project_dataset(project_id, dataset)
    _validate_retention_args(hot_days, forecast_days, time_travel_hours)

    dataset_ref = f"`{project_id}.{dataset}`"
    statements = [
        f"""ALTER SCHEMA {dataset_ref}
SET OPTIONS (
  default_partition_expiration_days = {int(hot_days)},
  max_time_travel_hours = {int(time_travel_hours)}
)"""
    ]

    for table_name in HIGH_VOLUME_FACT_TABLES:
        statements.append(
            f"""ALTER TABLE `{project_id}.{dataset}.{table_name}`
SET OPTIONS (
  partition_expiration_days = {int(hot_days)}
)"""
        )

    for table_name in FORECAST_FACT_TABLES:
        statements.append(
            f"""ALTER TABLE `{project_id}.{dataset}.{table_name}`
SET OPTIONS (
  partition_expiration_days = {int(forecast_days)}
)"""
        )

    return statements


def _print_statements(statements: Iterable[str]) -> None:
    for idx, statement in enumerate(statements, start=1):
        print(f"-- statement {idx}")
        print(statement.rstrip() + ";")


def _resolve_dataset_location(client, project_id: str, dataset: str, requested_location: str | None) -> str | None:
    if requested_location:
        return requested_location
    dataset_ref = f"{project_id}.{dataset}"
    dataset_obj = client.get_dataset(dataset_ref)
    return dataset_obj.location


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply Aero Pulse BigQuery hot-cache retention settings.")
    parser.add_argument("--project-id", default=os.getenv("BIGQUERY_PROJECT_ID", "").strip())
    parser.add_argument("--dataset", default=os.getenv("BIGQUERY_DATASET", "").strip())
    parser.add_argument("--hot-days", type=int, default=int(os.getenv("BIGQUERY_HOT_RETENTION_DAYS", "35")))
    parser.add_argument(
        "--forecast-days",
        type=int,
        default=int(os.getenv("BIGQUERY_FORECAST_RETENTION_DAYS", "90")),
    )
    parser.add_argument("--time-travel-hours", type=int, default=48)
    parser.add_argument("--location", help="Optional BigQuery job location, e.g. US or asia-south1.")
    parser.add_argument("--apply", action="store_true", help="Execute the generated DDL. Without this, print a dry run.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.project_id or not args.dataset:
        raise SystemExit("--project-id and --dataset are required, or set BIGQUERY_PROJECT_ID/BIGQUERY_DATASET.")

    try:
        statements = build_retention_statements(
            project_id=args.project_id,
            dataset=args.dataset,
            hot_days=args.hot_days,
            forecast_days=args.forecast_days,
            time_travel_hours=args.time_travel_hours,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    if not args.apply:
        print("dry_run=true")
        _print_statements(statements)
        return 0

    try:
        from google.cloud import bigquery
    except Exception as exc:
        raise SystemExit(f"google-cloud-bigquery is required: {exc}") from exc

    client = bigquery.Client(project=args.project_id)
    location = _resolve_dataset_location(client, args.project_id, args.dataset, args.location)
    for idx, statement in enumerate(statements, start=1):
        job = client.query(statement, location=location)
        job.result()
        print(f"applied_statement={idx}")
    print(
        f"retention_applied project={args.project_id} dataset={args.dataset} "
        f"location={location or ''} "
        f"hot_days={args.hot_days} forecast_days={args.forecast_days} "
        f"time_travel_hours={args.time_travel_hours}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
