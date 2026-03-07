from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.runtime_config import get_database_url


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _stage_dir(base_dir: Path) -> Path:
    out = base_dir / f"run_{_utc_stamp()}_UTC"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _parse_tables(raw: str) -> list[str]:
    if not raw or raw.strip().lower() == "all":
        return [
            "dim_airline",
            "dim_route",
            "fact_cycle_run",
            "fact_offer_snapshot",
            "fact_change_event",
            "fact_penalty_snapshot",
            "fact_tax_snapshot",
        ]
    return [part.strip() for part in raw.split(",") if part.strip()]


def _query_fact_offer_snapshot() -> str:
    return """
        SELECT
            fo.scrape_id::text AS cycle_id,
            fo.scraped_at AS captured_at_utc,
            fo.airline,
            fo.origin,
            fo.destination,
            (fo.origin || '-' || fo.destination) AS route_key,
            fo.flight_number,
            fo.departure AS departure_utc,
            DATE(fo.departure) AS departure_date,
            fo.cabin,
            fo.brand,
            fo.fare_basis,
            CAST(fo.price_total_bdt AS NUMERIC(12, 2)) AS total_price_bdt,
            CAST(frm.fare_amount AS NUMERIC(12, 2)) AS base_fare_amount,
            CAST(frm.tax_amount AS NUMERIC(12, 2)) AS tax_amount,
            frm.currency,
            fo.seat_available,
            fo.seat_capacity,
            CAST(frm.estimated_load_factor_pct AS NUMERIC(6, 2)) AS load_factor_pct,
            frm.booking_class,
            frm.baggage,
            frm.aircraft,
            frm.duration_min,
            frm.stops,
            frm.soldout,
            frm.penalty_source
        FROM flight_offers fo
        LEFT JOIN flight_offer_raw_meta frm
            ON frm.flight_offer_id = fo.id
        WHERE fo.scraped_at >= :start_ts
          AND fo.scraped_at < :end_ts
    """


def _query_fact_change_event() -> str:
    return """
        SELECT
            cce.scrape_id::text AS cycle_id,
            cce.previous_scrape_id::text AS previous_cycle_id,
            cce.detected_at AS detected_at_utc,
            cce.detected_at::date AS report_day,
            cce.airline,
            cce.origin,
            cce.destination,
            (cce.origin || '-' || cce.destination) AS route_key,
            cce.flight_number,
            cce.departure_day,
            cce.departure_time,
            cce.cabin,
            cce.fare_basis,
            cce.brand,
            cce.domain,
            cce.change_type,
            cce.direction,
            cce.field_name,
            cce.old_value,
            cce.new_value,
            cce.magnitude,
            cce.percent_change,
            cce.event_meta
        FROM airline_intel.column_change_events cce
        WHERE cce.detected_at >= :start_ts
          AND cce.detected_at < :end_ts
    """


def _query_fact_penalty_snapshot() -> str:
    return """
        SELECT
            fo.scrape_id::text AS cycle_id,
            fo.scraped_at AS captured_at_utc,
            fo.airline,
            fo.origin,
            fo.destination,
            (fo.origin || '-' || fo.destination) AS route_key,
            fo.flight_number,
            fo.departure AS departure_utc,
            fo.cabin,
            fo.fare_basis,
            frm.penalty_source,
            frm.penalty_currency,
            CAST(frm.fare_change_fee_before_24h AS NUMERIC(12, 2)) AS fare_change_fee_before_24h,
            CAST(frm.fare_change_fee_within_24h AS NUMERIC(12, 2)) AS fare_change_fee_within_24h,
            CAST(frm.fare_change_fee_no_show AS NUMERIC(12, 2)) AS fare_change_fee_no_show,
            CAST(frm.fare_cancel_fee_before_24h AS NUMERIC(12, 2)) AS fare_cancel_fee_before_24h,
            CAST(frm.fare_cancel_fee_within_24h AS NUMERIC(12, 2)) AS fare_cancel_fee_within_24h,
            CAST(frm.fare_cancel_fee_no_show AS NUMERIC(12, 2)) AS fare_cancel_fee_no_show,
            frm.fare_changeable,
            frm.fare_refundable,
            frm.penalty_rule_text
        FROM flight_offers fo
        JOIN flight_offer_raw_meta frm
            ON frm.flight_offer_id = fo.id
        WHERE fo.scraped_at >= :start_ts
          AND fo.scraped_at < :end_ts
          AND (
                frm.penalty_rule_text IS NOT NULL
                OR frm.fare_change_fee_before_24h IS NOT NULL
                OR frm.fare_change_fee_within_24h IS NOT NULL
                OR frm.fare_change_fee_no_show IS NOT NULL
                OR frm.fare_cancel_fee_before_24h IS NOT NULL
                OR frm.fare_cancel_fee_within_24h IS NOT NULL
                OR frm.fare_cancel_fee_no_show IS NOT NULL
          )
    """


def _query_fact_tax_snapshot() -> str:
    return """
        SELECT
            fo.scrape_id::text AS cycle_id,
            fo.scraped_at AS captured_at_utc,
            fo.airline,
            fo.origin,
            fo.destination,
            (fo.origin || '-' || fo.destination) AS route_key,
            fo.flight_number,
            fo.departure AS departure_utc,
            fo.cabin,
            fo.fare_basis,
            CAST(frm.tax_amount AS NUMERIC(12, 2)) AS tax_amount,
            frm.currency
        FROM flight_offers fo
        JOIN flight_offer_raw_meta frm
            ON frm.flight_offer_id = fo.id
        WHERE fo.scraped_at >= :start_ts
          AND fo.scraped_at < :end_ts
          AND frm.tax_amount IS NOT NULL
    """


EXPORT_QUERIES = {
    "dim_airline": """
        SELECT
            fo.airline,
            MIN(fo.scraped_at) AS first_seen_at_utc,
            MAX(fo.scraped_at) AS last_seen_at_utc,
            COUNT(*) AS offer_rows,
            (
                SELECT x.scrape_id::text
                FROM flight_offers x
                WHERE x.airline = fo.airline
                ORDER BY x.scraped_at DESC
                LIMIT 1
            ) AS latest_cycle_id
        FROM flight_offers fo
        GROUP BY fo.airline
        ORDER BY fo.airline
    """,
    "dim_route": """
        SELECT
            (fo.origin || '-' || fo.destination) AS route_key,
            fo.origin,
            fo.destination,
            MIN(fo.scraped_at) AS first_seen_at_utc,
            MAX(fo.scraped_at) AS last_seen_at_utc,
            COUNT(*) AS offer_rows,
            COUNT(DISTINCT fo.airline) AS airlines_present
        FROM flight_offers fo
        GROUP BY fo.origin, fo.destination
        ORDER BY fo.origin, fo.destination
    """,
    "fact_cycle_run": """
        SELECT
            fo.scrape_id::text AS cycle_id,
            MIN(fo.scraped_at) AS cycle_started_at_utc,
            MAX(fo.scraped_at) AS cycle_completed_at_utc,
            COUNT(*) AS offer_rows,
            COUNT(DISTINCT fo.airline) AS airline_count,
            COUNT(DISTINCT (fo.origin || '-' || fo.destination)) AS route_count
        FROM flight_offers fo
        WHERE fo.scraped_at >= :start_ts
          AND fo.scraped_at < :end_ts
        GROUP BY fo.scrape_id
        ORDER BY cycle_completed_at_utc DESC
    """,
    "fact_offer_snapshot": _query_fact_offer_snapshot(),
    "fact_change_event": _query_fact_change_event(),
    "fact_penalty_snapshot": _query_fact_penalty_snapshot(),
    "fact_tax_snapshot": _query_fact_tax_snapshot(),
}


def _read_query(engine, sql: str, params: dict[str, Any]) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def _normalize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    for column in out.columns:
        if out[column].dtype != "object":
            continue
        non_null = [value for value in out[column].tolist() if not pd.isna(value)]
        if not non_null:
            continue

        if all(isinstance(value, bool) for value in non_null):
            out[column] = out[column].astype("boolean")
            continue

        if all(isinstance(value, datetime) for value in non_null):
            out[column] = pd.to_datetime(out[column], errors="coerce")
            continue

        if all(isinstance(value, date) and not isinstance(value, datetime) for value in non_null):
            continue

        if all(isinstance(value, time) for value in non_null):
            continue

        if all(isinstance(value, str) for value in non_null):
            out[column] = out[column].astype("string")
            continue

        out[column] = out[column].map(
            lambda value: None
            if pd.isna(value)
            else (
                json.dumps(value, ensure_ascii=False, default=str)
                if isinstance(value, (dict, list, tuple, set))
                else str(value)
            )
        ).astype("string")
    return out


def _load_bigquery(file_path: Path, table_name: str, project_id: str, dataset: str, replace: bool) -> dict[str, Any]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if replace
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
    )

    with file_path.open("rb") as handle:
        job = client.load_table_from_file(handle, table_id, job_config=job_config)
        job.result()

    table = client.get_table(table_id)
    return {"table_id": table_id, "rows": table.num_rows}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage curated PostgreSQL exports for BigQuery and optionally load them.")
    parser.add_argument("--output-dir", default="output/warehouse/bigquery", help="Base output directory for staged parquet files.")
    parser.add_argument("--tables", default="all", help="Comma-separated table list or 'all'.")
    parser.add_argument("--start-date", required=True, help="Inclusive UTC date, e.g. 2026-03-01")
    parser.add_argument("--end-date", required=True, help="Exclusive UTC date, e.g. 2026-03-08")
    parser.add_argument("--load-bigquery", action="store_true", help="Load parquet outputs directly into BigQuery after staging.")
    parser.add_argument("--project-id", help="BigQuery project id. Fallback: BIGQUERY_PROJECT_ID")
    parser.add_argument("--dataset", help="BigQuery dataset. Fallback: BIGQUERY_DATASET")
    parser.add_argument("--replace", action="store_true", help="Replace destination tables instead of append.")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    engine = create_engine(get_database_url(), pool_pre_ping=True, future=True)
    stage_root = _stage_dir(Path(args.output_dir))
    tables = _parse_tables(args.tables)
    start_ts = f"{args.start_date}T00:00:00+00:00"
    end_ts = f"{args.end_date}T00:00:00+00:00"
    params = {"start_ts": start_ts, "end_ts": end_ts}

    exports: list[dict[str, Any]] = []
    bq_results: list[dict[str, Any]] = []

    for table_name in tables:
        sql = EXPORT_QUERIES.get(table_name)
        if not sql:
            raise SystemExit(f"Unknown table export: {table_name}")

        df = _normalize_for_parquet(_read_query(engine, sql, params=params))
        file_path = stage_root / f"{table_name}.parquet"
        df.to_parquet(file_path, index=False)
        exports.append({"table": table_name, "rows": int(len(df)), "file": str(file_path)})

    if args.load_bigquery:
        project_id = args.project_id or os.getenv("BIGQUERY_PROJECT_ID", "").strip()
        dataset = args.dataset or os.getenv("BIGQUERY_DATASET", "").strip()
        if not project_id or not dataset:
            raise SystemExit("BigQuery load requested but project/dataset not provided.")
        for exported in exports:
            result = _load_bigquery(
                Path(exported["file"]),
                exported["table"],
                project_id=project_id,
                dataset=dataset,
                replace=args.replace,
            )
            bq_results.append(result)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "tables": exports,
        "bigquery_loads": bq_results,
    }
    manifest_path = stage_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
