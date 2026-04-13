from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from pandas.errors import EmptyDataError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.runtime_config import get_database_url


REPORTS_ROOT = REPO_ROOT / "output" / "reports"
PREDICTION_EVAL_RE = re.compile(r"^prediction_eval_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_NEXT_RE = re.compile(r"^prediction_next_day_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_ROUTE_EVAL_RE = re.compile(r"^prediction_eval_by_route_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_ROUTE_WINNER_RE = re.compile(r"^prediction_route_winners_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_BACKTEST_META_RE = re.compile(r"^prediction_backtest_meta_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.json$")
PREDICTION_BACKTEST_EVAL_RE = re.compile(r"^prediction_backtest_eval_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_BACKTEST_ROUTE_EVAL_RE = re.compile(r"^prediction_backtest_eval_by_route_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_BACKTEST_ROUTE_WINNER_RE = re.compile(r"^prediction_backtest_route_winners_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_BACKTEST_SPLITS_RE = re.compile(r"^prediction_backtest_splits_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")


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
            "fact_forecast_bundle",
            "fact_forecast_model_eval",
            "fact_forecast_route_eval",
            "fact_forecast_route_winner",
            "fact_forecast_next_day",
            "fact_backtest_eval",
            "fact_backtest_route_winner",
            "fact_backtest_split",
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
            CAST(COALESCE(frm.tax_amount, GREATEST(fo.price_total_bdt - frm.fare_amount, 0)) AS NUMERIC(12, 2)) AS tax_amount,
            frm.currency,
            fo.seat_available,
            fo.seat_capacity,
            CAST(frm.estimated_load_factor_pct AS NUMERIC(6, 2)) AS load_factor_pct,
            frm.booking_class,
            frm.baggage,
            frm.aircraft,
            frm.duration_min,
            frm.stops,
            frm.via_airports,
            frm.soldout,
            frm.penalty_source,
            COALESCE(frm.search_trip_type, 'OW') AS search_trip_type,
            frm.trip_request_id,
            frm.requested_outbound_date,
            frm.requested_return_date,
            frm.trip_duration_days,
            frm.trip_origin,
            frm.trip_destination,
            COALESCE(frm.trip_origin, fo.origin) || '-' || COALESCE(frm.trip_destination, fo.destination) AS trip_pair_key,
            COALESCE(frm.leg_direction, 'outbound') AS leg_direction,
            frm.leg_sequence,
            frm.itinerary_leg_count
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
            CAST(COALESCE(frm.tax_amount, GREATEST(fo.price_total_bdt - frm.fare_amount, 0)) AS NUMERIC(12, 2)) AS tax_amount,
            frm.currency
        FROM flight_offers fo
        JOIN flight_offer_raw_meta frm
            ON frm.flight_offer_id = fo.id
        WHERE fo.scraped_at >= :start_ts
          AND fo.scraped_at < :end_ts
          AND COALESCE(frm.tax_amount, GREATEST(fo.price_total_bdt - frm.fare_amount, 0)) IS NOT NULL
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


def _stamp_to_dt_utc(stamp: str) -> datetime:
    return datetime.strptime(stamp, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)


def _bundle_id(bundle_name: str, target: str, stamp: str) -> str:
    return f"{bundle_name}|{target}|{stamp}"


def _find_prediction_bundles(start_dt: datetime, end_dt: datetime) -> list[dict[str, Any]]:
    bundles: dict[tuple[str, str, str], dict[str, Any]] = {}
    for path in REPORTS_ROOT.rglob("prediction_*"):
        if not path.is_file():
            continue

        file_name = path.name
        match = (
            PREDICTION_ROUTE_WINNER_RE.match(file_name)
            or PREDICTION_ROUTE_EVAL_RE.match(file_name)
            or PREDICTION_BACKTEST_ROUTE_EVAL_RE.match(file_name)
            or PREDICTION_BACKTEST_ROUTE_WINNER_RE.match(file_name)
            or PREDICTION_BACKTEST_META_RE.match(file_name)
            or PREDICTION_BACKTEST_EVAL_RE.match(file_name)
            or PREDICTION_BACKTEST_SPLITS_RE.match(file_name)
            or PREDICTION_NEXT_RE.match(file_name)
            or PREDICTION_EVAL_RE.match(file_name)
        )
        if not match:
            continue

        target = match.group("target")
        stamp = match.group("stamp")
        bundle_created_at_utc = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if not (start_dt <= bundle_created_at_utc < end_dt):
            continue

        bundle_name = path.parent.name
        key = (bundle_name, target, stamp)
        bundle = bundles.setdefault(
            key,
            {
                "bundle_id": _bundle_id(bundle_name, target, stamp),
                "bundle_name": bundle_name,
                "bundle_dir": str(path.parent),
                "target": target,
                "stamp": stamp,
                "bundle_created_at_utc": bundle_created_at_utc.isoformat(),
                "eval_path": None,
                "route_eval_path": None,
                "route_winner_path": None,
                "next_day_path": None,
                "backtest_eval_path": None,
                "backtest_route_eval_path": None,
                "backtest_route_winner_path": None,
                "backtest_splits_path": None,
                "backtest_meta_path": None,
            },
        )
        latest_mtime = max(
            datetime.fromisoformat(bundle["bundle_created_at_utc"]),
            bundle_created_at_utc,
        )
        bundle["bundle_created_at_utc"] = latest_mtime.isoformat()

        if file_name.startswith("prediction_eval_by_route_"):
            bundle["route_eval_path"] = str(path)
        elif file_name.startswith("prediction_route_winners_"):
            bundle["route_winner_path"] = str(path)
        elif file_name.startswith("prediction_eval_"):
            bundle["eval_path"] = str(path)
        elif file_name.startswith("prediction_next_day_"):
            bundle["next_day_path"] = str(path)
        elif file_name.startswith("prediction_backtest_eval_by_route_"):
            bundle["backtest_route_eval_path"] = str(path)
        elif file_name.startswith("prediction_backtest_route_winners_"):
            bundle["backtest_route_winner_path"] = str(path)
        elif file_name.startswith("prediction_backtest_eval_"):
            bundle["backtest_eval_path"] = str(path)
        elif file_name.startswith("prediction_backtest_splits_"):
            bundle["backtest_splits_path"] = str(path)
        elif file_name.startswith("prediction_backtest_meta_"):
            bundle["backtest_meta_path"] = str(path)

    return sorted(
        bundles.values(),
        key=lambda item: (item["bundle_created_at_utc"], item["bundle_name"], item["target"], item["stamp"]),
    )


def _read_csv_if_exists(path_value: str | None) -> pd.DataFrame:
    if not path_value:
        return pd.DataFrame()
    path = Path(path_value)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _read_json_if_exists(path_value: str | None) -> dict[str, Any] | None:
    if not path_value:
        return None
    path = Path(path_value)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _bundle_base_record(bundle: dict[str, Any]) -> dict[str, Any]:
    return {
        "bundle_id": bundle["bundle_id"],
        "bundle_name": bundle["bundle_name"],
        "target": bundle["target"],
        "stamp": bundle["stamp"],
        "bundle_created_at_utc": bundle["bundle_created_at_utc"],
    }


def _export_fact_forecast_bundle(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bundle in _find_prediction_bundles(start_dt, end_dt):
        meta = _read_json_if_exists(bundle.get("backtest_meta_path")) or {}
        backtest = meta.get("backtest") if isinstance(meta.get("backtest"), dict) else {}
        rows.append(
            {
                **_bundle_base_record(bundle),
                "bundle_dir": bundle["bundle_dir"],
                "has_overall_eval": bool(bundle.get("eval_path")),
                "has_route_eval": bool(bundle.get("route_eval_path")),
                "has_route_winner": bool(bundle.get("route_winner_path")),
                "has_next_day": bool(bundle.get("next_day_path")),
                "has_backtest_eval": bool(bundle.get("backtest_eval_path")),
                "has_backtest_route_eval": bool(bundle.get("backtest_route_eval_path")),
                "has_backtest_route_winner": bool(bundle.get("backtest_route_winner_path")),
                "has_backtest_splits": bool(bundle.get("backtest_splits_path")),
                "has_backtest_meta": bool(bundle.get("backtest_meta_path")),
                "target_column": meta.get("target_column"),
                "backtest_status": backtest.get("status"),
                "backtest_split_count": backtest.get("split_count"),
                "backtest_selection_metric": meta.get("backtest_selection_metric"),
            }
        )
    return pd.DataFrame(rows)


def _export_fact_forecast_model_eval(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bundle in _find_prediction_bundles(start_dt, end_dt):
        df = _read_csv_if_exists(bundle.get("eval_path"))
        if df.empty:
            continue
        for record in df.where(pd.notnull(df), None).to_dict(orient="records"):
            rows.append({**_bundle_base_record(bundle), **record})
    return pd.DataFrame(rows)


def _export_fact_forecast_route_eval(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bundle in _find_prediction_bundles(start_dt, end_dt):
        df = _read_csv_if_exists(bundle.get("route_eval_path"))
        if df.empty:
            continue
        df = df.where(pd.notnull(df), None)
        if {"origin", "destination"}.issubset(df.columns):
            df["route_key"] = df["origin"].astype(str) + "-" + df["destination"].astype(str)
        for record in df.to_dict(orient="records"):
            rows.append({**_bundle_base_record(bundle), **record})
    return pd.DataFrame(rows)


def _export_fact_forecast_route_winner(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bundle in _find_prediction_bundles(start_dt, end_dt):
        df = _read_csv_if_exists(bundle.get("route_winner_path"))
        if df.empty:
            continue
        df = df.where(pd.notnull(df), None)
        if {"origin", "destination"}.issubset(df.columns):
            df["route_key"] = df["origin"].astype(str) + "-" + df["destination"].astype(str)
        for record in df.to_dict(orient="records"):
            rows.append({**_bundle_base_record(bundle), **record})
    return pd.DataFrame(rows)


def _export_fact_forecast_next_day(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bundle in _find_prediction_bundles(start_dt, end_dt):
        df = _read_csv_if_exists(bundle.get("next_day_path"))
        if df.empty:
            continue
        df = df.where(pd.notnull(df), None)
        if {"origin", "destination"}.issubset(df.columns):
            df["route_key"] = df["origin"].astype(str) + "-" + df["destination"].astype(str)
        if "pred_ewm_alpha_0.30" in df.columns:
            df = df.rename(columns={"pred_ewm_alpha_0.30": "pred_ewm_alpha_0_30"})
        for record in df.to_dict(orient="records"):
            rows.append({**_bundle_base_record(bundle), **record})
    return pd.DataFrame(rows)


def _export_fact_backtest_eval(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bundle in _find_prediction_bundles(start_dt, end_dt):
        df = _read_csv_if_exists(bundle.get("backtest_eval_path"))
        if df.empty:
            continue
        for record in df.where(pd.notnull(df), None).to_dict(orient="records"):
            rows.append({**_bundle_base_record(bundle), **record})
    return pd.DataFrame(rows)


def _export_fact_backtest_route_winner(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bundle in _find_prediction_bundles(start_dt, end_dt):
        df = _read_csv_if_exists(bundle.get("backtest_route_winner_path"))
        if df.empty:
            continue
        df = df.where(pd.notnull(df), None)
        if {"origin", "destination"}.issubset(df.columns):
            df["route_key"] = df["origin"].astype(str) + "-" + df["destination"].astype(str)
        for record in df.to_dict(orient="records"):
            rows.append({**_bundle_base_record(bundle), **record})
    return pd.DataFrame(rows)


def _export_fact_backtest_split(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for bundle in _find_prediction_bundles(start_dt, end_dt):
        df = _read_csv_if_exists(bundle.get("backtest_splits_path"))
        if df.empty:
            continue
        for record in df.where(pd.notnull(df), None).to_dict(orient="records"):
            rows.append({**_bundle_base_record(bundle), **record})
    return pd.DataFrame(rows)


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


FACT_OFFER_SNAPSHOT_STRING_COLUMNS = {
    "cycle_id",
    "airline",
    "origin",
    "destination",
    "route_key",
    "flight_number",
    "cabin",
    "brand",
    "fare_basis",
    "currency",
    "booking_class",
    "baggage",
    "aircraft",
    "via_airports",
    "penalty_source",
    "search_trip_type",
    "trip_request_id",
    "trip_origin",
    "trip_destination",
    "trip_pair_key",
    "leg_direction",
}

FACT_OFFER_SNAPSHOT_DATE_COLUMNS = {
    "departure_date",
    "requested_outbound_date",
    "requested_return_date",
}

FACT_OFFER_SNAPSHOT_INT_COLUMNS = {
    "trip_duration_days",
    "leg_sequence",
    "itinerary_leg_count",
}

FACT_OFFER_SNAPSHOT_FLOAT_COLUMNS = {
    "total_price_bdt",
    "base_fare_amount",
    "tax_amount",
    "seat_available",
    "seat_capacity",
    "load_factor_pct",
    "duration_min",
    "stops",
}


def _normalize_fact_offer_snapshot_types(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    for column in FACT_OFFER_SNAPSHOT_STRING_COLUMNS:
        if column in out.columns:
            out[column] = out[column].astype("string")
    for column in FACT_OFFER_SNAPSHOT_DATE_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], errors="coerce").dt.date.astype("object")
    for column in FACT_OFFER_SNAPSHOT_INT_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").astype("Int64")
    for column in FACT_OFFER_SNAPSHOT_FLOAT_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").astype("Float64")
    return out


FILE_EXPORTERS = {
    "fact_forecast_bundle": _export_fact_forecast_bundle,
    "fact_forecast_model_eval": _export_fact_forecast_model_eval,
    "fact_forecast_route_eval": _export_fact_forecast_route_eval,
    "fact_forecast_route_winner": _export_fact_forecast_route_winner,
    "fact_forecast_next_day": _export_fact_forecast_next_day,
    "fact_backtest_eval": _export_fact_backtest_eval,
    "fact_backtest_route_winner": _export_fact_backtest_route_winner,
    "fact_backtest_split": _export_fact_backtest_split,
}


FORECAST_TIMESTAMP_COLUMNS = {"bundle_created_at_utc"}
FORECAST_DATE_COLUMNS = {
    "latest_report_day",
    "predicted_for_day",
    "train_start",
    "train_end",
    "val_start",
    "val_end",
    "test_start",
    "test_end",
}
FORECAST_BOOL_COLUMNS = {
    "has_overall_eval",
    "has_route_eval",
    "has_route_winner",
    "has_next_day",
    "has_backtest_eval",
    "has_backtest_route_eval",
    "has_backtest_route_winner",
    "has_backtest_splits",
    "has_backtest_meta",
    "selected_on_val",
}

FORECAST_EXPORT_COLUMNS: dict[str, list[str]] = {
    "fact_forecast_bundle": [
        "bundle_id",
        "bundle_name",
        "bundle_dir",
        "target",
        "stamp",
        "bundle_created_at_utc",
        "has_overall_eval",
        "has_route_eval",
        "has_route_winner",
        "has_next_day",
        "has_backtest_eval",
        "has_backtest_route_eval",
        "has_backtest_route_winner",
        "has_backtest_splits",
        "has_backtest_meta",
        "target_column",
        "backtest_status",
        "backtest_split_count",
        "backtest_selection_metric",
    ],
    "fact_forecast_model_eval": [
        "bundle_id",
        "bundle_name",
        "target",
        "stamp",
        "bundle_created_at_utc",
        "model",
        "n",
        "mae",
        "rmse",
        "mape_pct",
        "smape_pct",
        "n_directional",
        "directional_accuracy_pct",
        "f1_up",
        "f1_down",
        "f1_macro",
    ],
    "fact_forecast_route_eval": [
        "bundle_id",
        "bundle_name",
        "target",
        "stamp",
        "bundle_created_at_utc",
        "airline",
        "origin",
        "destination",
        "route_key",
        "cabin",
        "model",
        "n",
        "mae",
        "rmse",
        "mape_pct",
        "smape_pct",
        "n_directional",
        "directional_accuracy_pct",
        "f1_up",
        "f1_down",
        "f1_macro",
    ],
    "fact_forecast_route_winner": [
        "bundle_id",
        "bundle_name",
        "target",
        "stamp",
        "bundle_created_at_utc",
        "airline",
        "origin",
        "destination",
        "route_key",
        "cabin",
        "winner_model",
        "winner_metric",
        "winner_n",
        "winner_mae",
        "winner_rmse",
        "winner_directional_accuracy_pct",
        "winner_f1_macro",
        "max_candidate_n",
        "coverage_threshold_n",
        "candidate_models",
    ],
    "fact_forecast_next_day": [
        "bundle_id",
        "bundle_name",
        "target",
        "stamp",
        "bundle_created_at_utc",
        "latest_report_day",
        "predicted_for_day",
        "history_days",
        "airline",
        "origin",
        "destination",
        "route_key",
        "cabin",
        "latest_actual_value",
        "pred_last_value",
        "pred_rolling_mean_3",
        "pred_rolling_mean_7",
        "pred_seasonal_naive_7",
        "pred_ewm_alpha_0_30",
        "pred_dl_mlp_q10",
        "pred_dl_mlp_q50",
        "pred_dl_mlp_q90",
        "pred_ml_catboost_q10",
        "pred_ml_catboost_q50",
        "pred_ml_catboost_q90",
        "pred_ml_lightgbm_q10",
        "pred_ml_lightgbm_q50",
        "pred_ml_lightgbm_q90",
    ],
    "fact_backtest_eval": [
        "bundle_id",
        "bundle_name",
        "target",
        "stamp",
        "bundle_created_at_utc",
        "split_id",
        "dataset",
        "model",
        "selected_on_val",
        "n",
        "mae",
        "rmse",
        "mape_pct",
        "smape_pct",
        "n_directional",
        "directional_accuracy_pct",
        "f1_up",
        "f1_down",
        "f1_macro",
        "train_start",
        "train_end",
        "val_start",
        "val_end",
        "test_start",
        "test_end",
    ],
    "fact_backtest_route_winner": [
        "bundle_id",
        "bundle_name",
        "target",
        "stamp",
        "bundle_created_at_utc",
        "dataset",
        "airline",
        "origin",
        "destination",
        "route_key",
        "cabin",
        "winner_model",
        "winner_metric",
        "winner_n",
        "winner_mae",
        "winner_rmse",
        "winner_directional_accuracy_pct",
        "winner_f1_macro",
        "max_candidate_n",
        "coverage_threshold_n",
        "candidate_models",
    ],
    "fact_backtest_split": [
        "bundle_id",
        "bundle_name",
        "target",
        "stamp",
        "bundle_created_at_utc",
        "split_id",
        "train_start",
        "train_end",
        "val_start",
        "val_end",
        "test_start",
        "test_end",
        "train_rows",
        "val_rows",
        "test_rows",
        "selected_model",
    ],
}


def _select_export_columns(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    desired = FORECAST_EXPORT_COLUMNS.get(table_name)
    if not desired:
        return df
    out = df.copy()
    for column in desired:
        if column not in out.columns:
            out[column] = None
    return out[desired]


def _normalize_forecast_table_types(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    for column in FORECAST_TIMESTAMP_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], errors="coerce", utc=True)
    for column in FORECAST_DATE_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], errors="coerce").dt.date
    for column in FORECAST_BOOL_COLUMNS:
        if column in out.columns:
            out[column] = out[column].astype("boolean")
    return out


def _load_bigquery(
    file_path: Path,
    table_name: str,
    project_id: str,
    dataset: str,
    replace: bool,
    df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if replace
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
    )
    existing_schema = None
    try:
        existing_schema = client.get_table(table_id).schema
    except Exception:
        existing_schema = None
    if existing_schema:
        job_config.schema = existing_schema

    if df is not None:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
    else:
        job_config.source_format = bigquery.SourceFormat.PARQUET
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
    from datetime import timedelta
    start_ts = f"{args.start_date}T00:00:00+00:00"
    # end_ts is the start of the day *after* end_date so that all data scraped on
    # end_date (00:00 – 23:59 UTC) is included. The window is [start_ts, end_ts).
    _end_date_next = (date.fromisoformat(args.end_date) + timedelta(days=1)).isoformat()
    end_ts = f"{_end_date_next}T00:00:00+00:00"
    params = {"start_ts": start_ts, "end_ts": end_ts}
    start_dt = datetime.fromisoformat(start_ts)
    end_dt = datetime.fromisoformat(end_ts)

    exports: list[dict[str, Any]] = []
    export_frames: dict[str, pd.DataFrame] = {}
    bq_results: list[dict[str, Any]] = []

    for table_name in tables:
        if table_name in FILE_EXPORTERS:
            df = _normalize_for_parquet(
                _normalize_forecast_table_types(
                    _select_export_columns(table_name, FILE_EXPORTERS[table_name](start_dt, end_dt))
                )
            )
        else:
            sql = EXPORT_QUERIES.get(table_name)
            if not sql:
                raise SystemExit(f"Unknown table export: {table_name}")
            df = _read_query(engine, sql, params=params)
            if table_name == "fact_offer_snapshot":
                df = _normalize_fact_offer_snapshot_types(df)
            df = _normalize_for_parquet(df)
        file_path = stage_root / f"{table_name}.parquet"
        df.to_parquet(file_path, index=False)
        exports.append({"table": table_name, "rows": int(len(df)), "file": str(file_path)})
        export_frames[table_name] = df

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
                df=export_frames.get(exported["table"]),
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
