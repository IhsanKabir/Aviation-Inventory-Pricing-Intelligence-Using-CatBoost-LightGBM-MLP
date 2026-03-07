from __future__ import annotations

import json
import re
from collections.abc import Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

REPO_ROOT = Path(__file__).resolve().parents[4]
ROUTES_CONFIG_PATH = REPO_ROOT / "config" / "routes.json"
RUN_STATUS_LATEST_PATH = REPO_ROOT / "output" / "reports" / "run_all_status_latest.json"
REPORTS_ROOT = REPO_ROOT / "output" / "reports"
PREDICTION_EVAL_RE = re.compile(r"^prediction_eval_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_NEXT_RE = re.compile(r"^prediction_next_day_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_ROUTE_EVAL_RE = re.compile(r"^prediction_eval_by_route_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_BACKTEST_META_RE = re.compile(r"^prediction_backtest_meta_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.json$")
PREDICTION_BACKTEST_EVAL_RE = re.compile(r"^prediction_backtest_eval_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")


def _normalize_codes(values: Sequence[str] | None, uppercase: bool = True) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    for value in values:
        if value is None:
            continue
        cleaned = value.strip()
        if not cleaned:
            continue
        out.append(cleaned.upper() if uppercase else cleaned)
    return out


def _apply_in_filter(
    clauses: list[str],
    params: dict[str, Any],
    column: str,
    values: Sequence[str] | None,
    prefix: str,
    uppercase: bool = True,
) -> None:
    normalized = _normalize_codes(values, uppercase=uppercase)
    if not normalized:
        return
    placeholders: list[str] = []
    for idx, value in enumerate(normalized):
        key = f"{prefix}_{idx}"
        params[key] = value
        placeholders.append(f":{key}")
    clauses.append(f"{column} IN ({', '.join(placeholders)})")


def _rows_to_dicts(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        clean: dict[str, Any] = {}
        for key, value in row.items():
            clean[key] = float(value) if isinstance(value, Decimal) else value
        payload.append(clean)
    return payload


def get_latest_cycle(session: Session) -> dict[str, Any] | None:
    row = session.execute(
        text(
            """
            SELECT
                fo.scrape_id::text AS cycle_id,
                MIN(fo.scraped_at) AS cycle_started_at_utc,
                MAX(fo.scraped_at) AS cycle_completed_at_utc,
                COUNT(*) AS offer_rows,
                COUNT(DISTINCT fo.airline) AS airline_count,
                COUNT(DISTINCT (fo.origin || '-' || fo.destination)) AS route_count
            FROM flight_offers fo
            GROUP BY fo.scrape_id
            ORDER BY MAX(fo.scraped_at) DESC
            LIMIT 1
            """
        )
    ).mappings().first()
    return dict(row) if row else None


def get_recent_cycles(session: Session, limit: int = 10) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                fo.scrape_id::text AS cycle_id,
                MIN(fo.scraped_at) AS cycle_started_at_utc,
                MAX(fo.scraped_at) AS cycle_completed_at_utc,
                COUNT(*) AS offer_rows,
                COUNT(DISTINCT fo.airline) AS airline_count,
                COUNT(DISTINCT (fo.origin || '-' || fo.destination)) AS route_count
            FROM flight_offers fo
            GROUP BY fo.scrape_id
            ORDER BY MAX(fo.scraped_at) DESC
            LIMIT :limit
            """
        ),
        {"limit": limit},
    ).mappings().all()
    return _rows_to_dicts([dict(row) for row in rows])


def get_health(session: Session) -> dict[str, Any]:
    session.execute(text("SELECT 1"))
    latest_cycle = get_latest_cycle(session)
    return {
        "database_ok": True,
        "latest_cycle_id": latest_cycle["cycle_id"] if latest_cycle else None,
        "latest_cycle_completed_at_utc": latest_cycle["cycle_completed_at_utc"] if latest_cycle else None,
    }


def _load_configured_route_pairs() -> list[str]:
    try:
        payload = json.loads(ROUTES_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    route_pairs: list[str] = []
    for row in payload:
        airline = str(row.get("airline", "")).strip().upper()
        origin = str(row.get("origin", "")).strip().upper()
        destination = str(row.get("destination", "")).strip().upper()
        if airline and origin and destination:
            route_pairs.append(f"{airline}:{origin}-{destination}")
    return route_pairs


def _load_latest_run_status() -> dict[str, Any] | None:
    try:
        return json.loads(RUN_STATUS_LATEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def get_cycle_health(session: Session) -> dict[str, Any]:
    latest_cycle = get_latest_cycle(session)
    if not latest_cycle:
        return {
            "database_ok": True,
            "cycle_id": None,
            "cycle_completed_at_utc": None,
            "cycle_age_minutes": None,
            "stale": True,
            "configured_route_pair_count": 0,
            "observed_route_pair_count": 0,
            "route_pair_coverage_pct": 0.0,
            "missing_route_pairs": [],
            "latest_run_status": None,
        }

    cycle_id = str(latest_cycle["cycle_id"])
    observed_rows = session.execute(
        text(
            """
            SELECT DISTINCT (fo.airline || ':' || fo.origin || '-' || fo.destination) AS route_pair
            FROM flight_offers fo
            WHERE fo.scrape_id = CAST(:cycle_id AS uuid)
            ORDER BY route_pair
            """
        ),
        {"cycle_id": cycle_id},
    ).scalars().all()
    observed_route_pairs = [str(item) for item in observed_rows if item]
    configured_route_pairs = _load_configured_route_pairs()
    configured_set = set(configured_route_pairs)
    observed_set = set(observed_route_pairs)
    missing_route_pairs = sorted(configured_set - observed_set)

    cycle_completed = latest_cycle.get("cycle_completed_at_utc")
    cycle_age_minutes: float | None = None
    stale = True
    if isinstance(cycle_completed, datetime):
        age = datetime.now(timezone.utc) - cycle_completed.replace(tzinfo=timezone.utc)
        cycle_age_minutes = round(age.total_seconds() / 60.0, 2)
        stale = cycle_age_minutes > 180

    coverage_pct = round((len(observed_set) / len(configured_set) * 100.0), 2) if configured_set else 0.0
    run_status = _load_latest_run_status()

    return {
        "database_ok": True,
        "cycle_id": cycle_id,
        "cycle_started_at_utc": latest_cycle.get("cycle_started_at_utc"),
        "cycle_completed_at_utc": cycle_completed,
        "cycle_age_minutes": cycle_age_minutes,
        "stale": stale,
        "offer_rows": latest_cycle.get("offer_rows"),
        "airline_count": latest_cycle.get("airline_count"),
        "route_count": latest_cycle.get("route_count"),
        "configured_route_pair_count": len(configured_set),
        "observed_route_pair_count": len(observed_set),
        "route_pair_coverage_pct": coverage_pct,
        "missing_route_pairs": missing_route_pairs[:60],
        "latest_run_status": {
            "state": run_status.get("state"),
            "phase": run_status.get("phase"),
            "overall_query_total": run_status.get("overall_query_total"),
            "overall_query_completed": run_status.get("overall_query_completed"),
            "total_rows_accumulated": run_status.get("total_rows_accumulated"),
            "completed_at_utc": run_status.get("completed_at_utc"),
            "selected_dates": run_status.get("selected_dates"),
        } if run_status else None,
    }


def _read_prediction_csv(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    df = pd.read_csv(path)
    if limit is not None and limit >= 0:
        df = df.head(limit)
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def _read_prediction_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _find_prediction_bundles() -> list[dict[str, Any]]:
    bundles: dict[tuple[str, str, str], dict[str, Any]] = {}
    for path in REPORTS_ROOT.rglob("prediction_*"):
        if not path.is_file():
            continue

        file_name = path.name
        match = (
            PREDICTION_EVAL_RE.match(file_name)
            or PREDICTION_NEXT_RE.match(file_name)
            or PREDICTION_ROUTE_EVAL_RE.match(file_name)
            or PREDICTION_BACKTEST_META_RE.match(file_name)
            or PREDICTION_BACKTEST_EVAL_RE.match(file_name)
        )
        if not match:
            continue

        target = match.group("target")
        stamp = match.group("stamp")
        key = (str(path.parent), target, stamp)
        bundle = bundles.setdefault(
            key,
            {
                "bundle_dir": str(path.parent),
                "bundle_name": path.parent.name,
                "target": target,
                "stamp": stamp,
                "modified_at_utc": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(),
                "eval_path": None,
                "route_eval_path": None,
                "next_day_path": None,
                "backtest_eval_path": None,
                "backtest_meta_path": None,
            },
        )
        bundle["modified_at_utc"] = datetime.fromtimestamp(
            max(path.stat().st_mtime, Path(bundle["bundle_dir"]).stat().st_mtime),
            tz=timezone.utc,
        ).isoformat()
        if file_name.startswith("prediction_eval_by_route_"):
            bundle["route_eval_path"] = str(path)
        elif file_name.startswith("prediction_eval_"):
            bundle["eval_path"] = str(path)
        elif file_name.startswith("prediction_next_day_"):
            bundle["next_day_path"] = str(path)
        elif file_name.startswith("prediction_backtest_eval_"):
            bundle["backtest_eval_path"] = str(path)
        elif file_name.startswith("prediction_backtest_meta_"):
            bundle["backtest_meta_path"] = str(path)

    return sorted(
        bundles.values(),
        key=lambda item: (
            item["stamp"],
            item["modified_at_utc"],
            item["bundle_name"],
        ),
        reverse=True,
    )


def get_forecasting_payload(limit_routes: int = 25, limit_next_day: int = 40) -> dict[str, Any]:
    bundles = _find_prediction_bundles()
    latest_bundle = bundles[0] if bundles else None
    latest_backtest_bundle = next(
        (bundle for bundle in bundles if bundle.get("backtest_meta_path") and bundle.get("backtest_eval_path")),
        None,
    )

    def materialize(bundle: dict[str, Any] | None) -> dict[str, Any] | None:
        if not bundle:
            return None
        eval_rows = _read_prediction_csv(Path(bundle["eval_path"]), limit=50) if bundle.get("eval_path") else []
        route_eval_rows = _read_prediction_csv(Path(bundle["route_eval_path"]), limit=limit_routes) if bundle.get("route_eval_path") else []
        next_day_rows = _read_prediction_csv(Path(bundle["next_day_path"]), limit=limit_next_day) if bundle.get("next_day_path") else []
        backtest_eval_rows = _read_prediction_csv(Path(bundle["backtest_eval_path"]), limit=40) if bundle.get("backtest_eval_path") else []
        backtest_meta = _read_prediction_json(Path(bundle["backtest_meta_path"])) if bundle.get("backtest_meta_path") else None
        return {
            "bundle_dir": bundle["bundle_dir"],
            "bundle_name": bundle["bundle_name"],
            "target": bundle["target"],
            "stamp": bundle["stamp"],
            "modified_at_utc": bundle["modified_at_utc"],
            "overall_eval": eval_rows,
            "route_eval": route_eval_rows,
            "next_day": next_day_rows,
            "backtest_eval": backtest_eval_rows,
            "backtest_meta": backtest_meta,
        }

    return {
        "latest_prediction_bundle": materialize(latest_bundle),
        "latest_backtest_bundle": materialize(latest_backtest_bundle),
        "bundle_count": len(bundles),
    }


def list_airlines(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                fo.airline,
                MIN(fo.scraped_at) AS first_seen_at_utc,
                MAX(fo.scraped_at) AS last_seen_at_utc,
                COUNT(*) AS offer_rows
            FROM flight_offers fo
            GROUP BY fo.airline
            ORDER BY fo.airline
            """
        )
    ).mappings().all()
    return _rows_to_dicts([dict(row) for row in rows])


def list_routes(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                COUNT(*) AS offer_rows,
                COUNT(DISTINCT fo.airline) AS airlines_present,
                MIN(fo.scraped_at) AS first_seen_at_utc,
                MAX(fo.scraped_at) AS last_seen_at_utc
            FROM flight_offers fo
            GROUP BY fo.origin, fo.destination
            ORDER BY fo.origin, fo.destination
            """
        )
    ).mappings().all()
    return _rows_to_dicts([dict(row) for row in rows])


def _resolve_cycle_id(session: Session, cycle_id: str | None) -> str | None:
    if cycle_id:
        return cycle_id.strip()
    latest_cycle = get_latest_cycle(session)
    return str(latest_cycle["cycle_id"]) if latest_cycle else None


def get_current_snapshot(
    session: Session,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    limit: int = 250,
) -> dict[str, Any]:
    resolved_cycle_id = _resolve_cycle_id(session, cycle_id)
    if not resolved_cycle_id:
        return {"cycle_id": None, "rows": []}

    clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)"]
    params: dict[str, Any] = {"cycle_id": resolved_cycle_id, "limit": limit}
    _apply_in_filter(clauses, params, "fo.airline", airlines, "airline")
    _apply_in_filter(clauses, params, "fo.origin", origins, "origin")
    _apply_in_filter(clauses, params, "fo.destination", destinations, "destination")
    _apply_in_filter(clauses, params, "fo.cabin", cabins, "cabin", uppercase=False)

    rows = session.execute(
        text(
            f"""
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
            WHERE {' AND '.join(clauses)}
            ORDER BY fo.origin, fo.destination, fo.departure, fo.airline, fo.flight_number
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()

    return {"cycle_id": resolved_cycle_id, "rows": _rows_to_dicts([dict(row) for row in rows])}


def get_route_summary(
    session: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    clauses = ["1=1"]
    params: dict[str, Any] = {"limit": limit}
    if start_date:
        clauses.append("cce.detected_at::date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        clauses.append("cce.detected_at::date <= :end_date")
        params["end_date"] = end_date
    _apply_in_filter(clauses, params, "cce.airline", airlines, "airline")
    _apply_in_filter(clauses, params, "cce.origin", origins, "origin")
    _apply_in_filter(clauses, params, "cce.destination", destinations, "destination")
    _apply_in_filter(clauses, params, "cce.cabin", cabins, "cabin", uppercase=False)

    rows = session.execute(
        text(
            f"""
            SELECT
                cce.detected_at::date AS report_day,
                cce.airline,
                cce.origin,
                cce.destination,
                (cce.origin || '-' || cce.destination) AS route_key,
                cce.cabin,
                COUNT(*) AS total_change_events,
                COUNT(DISTINCT cce.flight_number) AS flights_affected,
                COUNT(DISTINCT cce.fare_basis) AS fare_bases_affected,
                COUNT(DISTINCT cce.field_name) AS unique_fields_changed,
                COUNT(*) FILTER (WHERE cce.domain = 'price') AS price_events,
                COUNT(*) FILTER (WHERE cce.domain = 'availability') AS availability_events,
                COUNT(*) FILTER (WHERE cce.domain = 'metadata') AS metadata_events,
                COUNT(*) FILTER (WHERE cce.direction = 'up') AS up_events,
                COUNT(*) FILTER (WHERE cce.direction = 'down') AS down_events,
                MIN(cce.detected_at) AS first_event_at_utc,
                MAX(cce.detected_at) AS last_event_at_utc
            FROM airline_intel.column_change_events cce
            WHERE {' AND '.join(clauses)}
            GROUP BY
                cce.detected_at::date,
                cce.airline,
                cce.origin,
                cce.destination,
                cce.cabin
            ORDER BY report_day DESC, cce.origin, cce.destination, cce.airline
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return _rows_to_dicts([dict(row) for row in rows])


def get_change_events(
    session: Session,
    start_date: date | None = None,
    end_date: date | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    domains: Sequence[str] | None = None,
    change_types: Sequence[str] | None = None,
    directions: Sequence[str] | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    clauses = ["1=1"]
    params: dict[str, Any] = {"limit": limit}
    if start_date:
        clauses.append("cce.detected_at::date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        clauses.append("cce.detected_at::date <= :end_date")
        params["end_date"] = end_date
    _apply_in_filter(clauses, params, "cce.airline", airlines, "airline")
    _apply_in_filter(clauses, params, "cce.origin", origins, "origin")
    _apply_in_filter(clauses, params, "cce.destination", destinations, "destination")
    _apply_in_filter(clauses, params, "cce.domain", domains, "domain", uppercase=False)
    _apply_in_filter(clauses, params, "cce.change_type", change_types, "change_type", uppercase=False)
    _apply_in_filter(clauses, params, "cce.direction", directions, "direction", uppercase=False)

    rows = session.execute(
        text(
            f"""
            SELECT
                cce.id,
                cce.scrape_id::text AS cycle_id,
                cce.previous_scrape_id::text AS previous_cycle_id,
                cce.detected_at AS detected_at_utc,
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
            WHERE {' AND '.join(clauses)}
            ORDER BY cce.detected_at DESC, cce.id DESC
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return _rows_to_dicts([dict(row) for row in rows])


def get_penalties(
    session: Session,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    resolved_cycle_id = _resolve_cycle_id(session, cycle_id)
    if not resolved_cycle_id:
        return {"cycle_id": None, "rows": []}

    clauses = [
        "fo.scrape_id = CAST(:cycle_id AS uuid)",
        """(
            frm.penalty_rule_text IS NOT NULL
            OR frm.fare_change_fee_before_24h IS NOT NULL
            OR frm.fare_change_fee_within_24h IS NOT NULL
            OR frm.fare_change_fee_no_show IS NOT NULL
            OR frm.fare_cancel_fee_before_24h IS NOT NULL
            OR frm.fare_cancel_fee_within_24h IS NOT NULL
            OR frm.fare_cancel_fee_no_show IS NOT NULL
        )""",
    ]
    params: dict[str, Any] = {"cycle_id": resolved_cycle_id, "limit": limit}
    _apply_in_filter(clauses, params, "fo.airline", airlines, "airline")
    _apply_in_filter(clauses, params, "fo.origin", origins, "origin")
    _apply_in_filter(clauses, params, "fo.destination", destinations, "destination")

    rows = session.execute(
        text(
            f"""
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
            LEFT JOIN flight_offer_raw_meta frm
                ON frm.flight_offer_id = fo.id
            WHERE {' AND '.join(clauses)}
            ORDER BY fo.origin, fo.destination, fo.departure, fo.airline, fo.flight_number
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return {"cycle_id": resolved_cycle_id, "rows": _rows_to_dicts([dict(row) for row in rows])}


def get_taxes(
    session: Session,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    resolved_cycle_id = _resolve_cycle_id(session, cycle_id)
    if not resolved_cycle_id:
        return {"cycle_id": None, "rows": []}

    clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)", "frm.tax_amount IS NOT NULL"]
    params: dict[str, Any] = {"cycle_id": resolved_cycle_id, "limit": limit}
    _apply_in_filter(clauses, params, "fo.airline", airlines, "airline")
    _apply_in_filter(clauses, params, "fo.origin", origins, "origin")
    _apply_in_filter(clauses, params, "fo.destination", destinations, "destination")

    rows = session.execute(
        text(
            f"""
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
            LEFT JOIN flight_offer_raw_meta frm
                ON frm.flight_offer_id = fo.id
            WHERE {' AND '.join(clauses)}
            ORDER BY fo.origin, fo.destination, fo.departure, fo.airline, fo.flight_number
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return {"cycle_id": resolved_cycle_id, "rows": _rows_to_dicts([dict(row) for row in rows])}
