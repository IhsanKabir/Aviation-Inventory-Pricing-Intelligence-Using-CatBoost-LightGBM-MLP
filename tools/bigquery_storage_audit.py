from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, timedelta
from typing import Any


PROJECT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
DATASET_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

HIGH_VOLUME_FACT_TABLES = [
    "fact_cycle_run",
    "fact_offer_snapshot",
    "fact_change_event",
    "fact_penalty_snapshot",
    "fact_tax_snapshot",
]


def _validate_project_dataset(project_id: str, dataset: str) -> None:
    if not PROJECT_RE.match(project_id or ""):
        raise ValueError(f"Invalid BigQuery project id: {project_id!r}")
    if not DATASET_RE.match(dataset or ""):
        raise ValueError(f"Invalid BigQuery dataset: {dataset!r}")


def _region_qualifier(location: str) -> str:
    value = str(location or "").strip().lower()
    if not value:
        raise ValueError("BigQuery dataset location could not be resolved.")
    return f"region-{value}"


def build_storage_query(project_id: str, dataset: str, location: str) -> str:
    _validate_project_dataset(project_id, dataset)
    region = _region_qualifier(location)
    return f"""
        SELECT
          table_name,
          total_rows,
          total_partitions,
          total_logical_bytes,
          active_logical_bytes,
          long_term_logical_bytes,
          current_physical_bytes,
          total_physical_bytes,
          active_physical_bytes,
          long_term_physical_bytes,
          time_travel_physical_bytes,
          fail_safe_physical_bytes,
          storage_last_modified_time
        FROM `{project_id}.{region}.INFORMATION_SCHEMA.TABLE_STORAGE`
        WHERE table_schema = @dataset
          AND deleted = FALSE
        ORDER BY total_logical_bytes DESC
    """


def build_partitions_query(project_id: str, dataset: str) -> str:
    _validate_project_dataset(project_id, dataset)
    return f"""
        SELECT
          table_name,
          MIN(partition_id) AS oldest_partition_id,
          MAX(partition_id) AS newest_partition_id,
          COUNT(*) AS partition_count
        FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name IN UNNEST(@table_names)
          AND partition_id IS NOT NULL
          AND partition_id NOT IN ("__NULL__", "__UNPARTITIONED__")
        GROUP BY table_name
        ORDER BY table_name
    """


def build_enable_table_storage_statement(project_id: str, location: str) -> str:
    _validate_project_dataset(project_id, "placeholder_dataset")
    region = _region_qualifier(location)
    return f"ALTER PROJECT `{project_id}`\nSET OPTIONS (`{region}.enable_info_schema_storage` = TRUE)"


def _partition_id_to_date(partition_id: str | None) -> date | None:
    value = str(partition_id or "").strip()
    if not re.fullmatch(r"\d{8}", value):
        return None
    return date(int(value[0:4]), int(value[4:6]), int(value[6:8]))


def build_retention_warnings(
    partition_rows: list[dict[str, Any]],
    hot_retention_days: int,
    today: date | None = None,
) -> list[str]:
    today = today or date.today()
    cutoff = today - timedelta(days=max(1, int(hot_retention_days)))
    warnings: list[str] = []
    for row in partition_rows:
        table_name = str(row.get("table_name") or "")
        if table_name not in HIGH_VOLUME_FACT_TABLES:
            continue
        oldest_partition = _partition_id_to_date(row.get("oldest_partition_id"))
        if oldest_partition and oldest_partition < cutoff:
            warnings.append(
                f"{table_name} oldest_partition={oldest_partition.isoformat()} "
                f"is older than hot_retention_days={hot_retention_days}"
            )
    return warnings


def _gib(value: Any) -> float:
    return float(value or 0) / (1024**3)


def _resolve_dataset_location(client: Any, project_id: str, dataset: str, requested_location: str | None) -> str | None:
    if requested_location:
        return requested_location
    dataset_ref = f"{project_id}.{dataset}"
    dataset_obj = client.get_dataset(dataset_ref)
    return dataset_obj.location


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Report BigQuery storage usage for Aero Pulse hot-cache tables.")
    parser.add_argument("--project-id", default=os.getenv("BIGQUERY_PROJECT_ID", "").strip())
    parser.add_argument("--dataset", default=os.getenv("BIGQUERY_DATASET", "").strip())
    parser.add_argument("--location", help="Optional BigQuery job location, e.g. US or asia-south1.")
    parser.add_argument("--hot-days", type=int, default=int(os.getenv("BIGQUERY_HOT_RETENTION_DAYS", "35")))
    parser.add_argument("--fail-on-warning", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.project_id or not args.dataset:
        raise SystemExit("--project-id and --dataset are required, or set BIGQUERY_PROJECT_ID/BIGQUERY_DATASET.")
    try:
        _validate_project_dataset(args.project_id, args.dataset)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    try:
        from google.cloud import bigquery
        from google.api_core.exceptions import BadRequest
    except Exception as exc:
        raise SystemExit(f"google-cloud-bigquery is required: {exc}") from exc

    client = bigquery.Client(project=args.project_id)
    location = _resolve_dataset_location(client, args.project_id, args.dataset, args.location)
    storage_rows: list[dict[str, Any]] = []
    storage_metrics_available = True
    storage_enable_statement = ""
    try:
        storage_job = client.query(
            build_storage_query(args.project_id, args.dataset, location),
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("dataset", "STRING", args.dataset)]
            ),
            location=location,
        )
        storage_rows = [dict(row.items()) for row in storage_job.result()]
    except BadRequest as exc:
        message = str(exc)
        if "enable_info_schema_storage" not in message and "TABLE_STORAGE hasn't been enabled" not in message:
            raise
        storage_metrics_available = False
        storage_enable_statement = build_enable_table_storage_statement(args.project_id, location)

    partitions_job = client.query(
        build_partitions_query(args.project_id, args.dataset),
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("table_names", "STRING", HIGH_VOLUME_FACT_TABLES)]
        ),
        location=location,
    )
    partition_rows = [dict(row.items()) for row in partitions_job.result()]
    warnings = build_retention_warnings(partition_rows, hot_retention_days=args.hot_days)

    print(f"bigquery_storage_audit project={args.project_id} dataset={args.dataset} location={location or ''}")
    print(f"storage_metrics_available={str(storage_metrics_available).lower()}")
    if not storage_metrics_available:
        print("WARNING: INFORMATION_SCHEMA.TABLE_STORAGE is not enabled for this project/location.")
        print("Run this SQL in BigQuery, then wait up to about 1 day for full historical storage metrics:")
        print(storage_enable_statement + ";")
    print("table,total_rows,partitions,total_logical_gib,active_logical_gib,long_term_logical_gib,total_physical_gib,time_travel_physical_gib,fail_safe_physical_gib,last_modified")
    for row in storage_rows:
        print(
            ",".join(
                [
                    str(row.get("table_name") or ""),
                    str(row.get("total_rows") or 0),
                    str(row.get("total_partitions") or 0),
                    f"{_gib(row.get('total_logical_bytes')):.6f}",
                    f"{_gib(row.get('active_logical_bytes')):.6f}",
                    f"{_gib(row.get('long_term_logical_bytes')):.6f}",
                    f"{_gib(row.get('total_physical_bytes')):.6f}",
                    f"{_gib(row.get('time_travel_physical_bytes')):.6f}",
                    f"{_gib(row.get('fail_safe_physical_bytes')):.6f}",
                    str(row.get("storage_last_modified_time") or ""),
                ]
            )
        )

    print("partition_retention_warnings=" + str(len(warnings)))
    for warning in warnings:
        print("WARNING: " + warning)
    return 1 if warnings and args.fail_on_warning else 0


if __name__ == "__main__":
    sys.exit(main())
