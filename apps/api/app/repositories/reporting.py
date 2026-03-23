from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import TimeoutError as FuturesTimeoutError
from copy import deepcopy
from contextvars import ContextVar
from functools import lru_cache
from threading import Lock
from collections import defaultdict
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..config import settings

REPO_ROOT = Path(__file__).resolve().parents[4]
ROUTES_CONFIG_PATH = REPO_ROOT / "config" / "routes.json"
AIRPORT_COUNTRIES_CONFIG_PATH = REPO_ROOT / "config" / "airport_countries.json"
RUN_STATUS_LATEST_PATH = REPO_ROOT / "output" / "reports" / "run_all_status_latest.json"
SCRAPE_PARALLEL_LATEST_PATH = REPO_ROOT / "output" / "reports" / "scrape_parallel_latest.json"
ACCUMULATION_CYCLE_LATEST_PATH = REPO_ROOT / "output" / "reports" / "accumulation_cycle_latest.json"
REPORTS_ROOT = REPO_ROOT / "output" / "reports"
PREDICTION_EVAL_RE = re.compile(r"^prediction_eval_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_NEXT_RE = re.compile(r"^prediction_next_day_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_ROUTE_EVAL_RE = re.compile(r"^prediction_eval_by_route_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
PREDICTION_BACKTEST_META_RE = re.compile(r"^prediction_backtest_meta_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.json$")
PREDICTION_BACKTEST_EVAL_RE = re.compile(r"^prediction_backtest_eval_(?P<target>.+)_(?P<stamp>\d{8}_\d{6})\.csv$")
ROUTE_TYPE_DOM = "DOM"
ROUTE_TYPE_INT = "INT"
ROUTE_TYPE_UNK = "UNK"
AIRPORT_CODE_RE = re.compile(r"^[A-Z]{3}$")
WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
COMPARABLE_CYCLE_MIN_OFFER_ROWS = 500
COMPARABLE_CYCLE_MIN_AIRLINES = 5
COMPARABLE_CYCLE_MIN_ROUTES = 10
ROUTE_LIST_CACHE_TTL_SEC = 60
ROUTE_MATRIX_CACHE_TTL_SEC = 60
ROUTE_DATE_AVAILABILITY_CACHE_TTL_SEC = 60
ROUTE_MATRIX_MIN_HISTORY_CYCLES = 12

_EPHEMERAL_RESPONSE_CACHE: dict[tuple[Any, ...], tuple[float, Any]] = {}
_EPHEMERAL_RESPONSE_CACHE_LOCK = Lock()
_REQUEST_METRICS: ContextVar[dict[str, Any] | None] = ContextVar("reporting_request_metrics", default=None)
LOG = logging.getLogger("api.reporting")


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


def _normalize_code_prefix(value: str | None, uppercase: bool = True) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return cleaned.upper() if uppercase else cleaned


def _normalize_scalar_cache_value(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _normalize_sequence_cache_value(values: Sequence[str] | None, uppercase: bool = True) -> tuple[str, ...]:
    return tuple(_normalize_codes(values, uppercase=uppercase))


def _cache_key(prefix: str, **parts: Any) -> tuple[Any, ...]:
    return tuple(
        [prefix]
        + [
            (name, _normalize_scalar_cache_value(value))
            for name, value in sorted(parts.items())
        ]
    )


def _get_cached_response(key: tuple[Any, ...], ttl_seconds: int) -> Any | None:
    now = time.monotonic()
    with _EPHEMERAL_RESPONSE_CACHE_LOCK:
        cached = _EPHEMERAL_RESPONSE_CACHE.get(key)
        if not cached:
            return None
        stored_at, payload = cached
        if now - stored_at > ttl_seconds:
            _EPHEMERAL_RESPONSE_CACHE.pop(key, None)
            return None
    return deepcopy(payload)


def _set_cached_response(key: tuple[Any, ...], payload: Any) -> Any:
    cached_payload = deepcopy(payload)
    with _EPHEMERAL_RESPONSE_CACHE_LOCK:
        _EPHEMERAL_RESPONSE_CACHE[key] = (time.monotonic(), cached_payload)
    return deepcopy(cached_payload)


def _route_matrix_history_cycle_limit(history_limit: int) -> int:
    return max(int(history_limit or 0) + 2, ROUTE_MATRIX_MIN_HISTORY_CYCLES)


def _route_matrix_capture_display_limit(history_limit: int, compact_history: bool) -> int:
    normalized_limit = max(int(history_limit or 0), 1)
    if not compact_history:
        return normalized_limit
    return min(normalized_limit, 2)


def _route_matrix_cycle_fetch_limit(history_limit: int, compact_history: bool) -> int:
    if not compact_history:
        return _route_matrix_history_cycle_limit(history_limit)
    # The collapsed matrix only shows the latest two capture slices, so we can
    # keep the cycle window tighter on first load and defer the deeper scan to
    # the row-level drilldown request.
    return max(_route_matrix_capture_display_limit(history_limit, compact_history) + 2, 6)


def clear_request_metrics() -> None:
    _REQUEST_METRICS.set({})


def get_request_metrics() -> dict[str, Any]:
    return dict(_REQUEST_METRICS.get() or {})


def _set_request_metrics(**entries: Any) -> None:
    metrics = _REQUEST_METRICS.get()
    if metrics is None:
        return
    for key, value in entries.items():
        metrics[key] = value


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


def _apply_route_pair_filter(
    clauses: list[str],
    params: dict[str, Any],
    origin_column: str,
    destination_column: str,
    route_pairs: Sequence[tuple[str, str]],
    prefix: str,
) -> None:
    normalized_pairs = [
        (str(origin or "").strip().upper(), str(destination or "").strip().upper())
        for origin, destination in route_pairs
        if str(origin or "").strip() and str(destination or "").strip()
    ]
    if not normalized_pairs:
        return
    pair_clauses: list[str] = []
    for idx, (origin, destination) in enumerate(normalized_pairs):
        origin_key = f"{prefix}_origin_{idx}"
        destination_key = f"{prefix}_destination_{idx}"
        params[origin_key] = origin
        params[destination_key] = destination
        pair_clauses.append(f"({origin_column} = :{origin_key} AND {destination_column} = :{destination_key})")
    clauses.append(f"({' OR '.join(pair_clauses)})")


def _rows_to_dicts(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        clean: dict[str, Any] = {}
        for key, value in row.items():
            clean[key] = float(value) if isinstance(value, Decimal) else value
        payload.append(clean)
    return payload


def _is_cycle_comparable(row: Mapping[str, Any] | None) -> bool:
    if not row:
        return False
    try:
        offer_rows = int(row.get("offer_rows") or 0)
    except (TypeError, ValueError):
        offer_rows = 0
    try:
        airline_count = int(row.get("airline_count") or 0)
    except (TypeError, ValueError):
        airline_count = 0
    try:
        route_count = int(row.get("route_count") or 0)
    except (TypeError, ValueError):
        route_count = 0
    return (
        offer_rows >= COMPARABLE_CYCLE_MIN_OFFER_ROWS
        and airline_count >= COMPARABLE_CYCLE_MIN_AIRLINES
        and route_count >= COMPARABLE_CYCLE_MIN_ROUTES
    )


@lru_cache(maxsize=1)
def _load_airport_country_map() -> dict[str, str]:
    try:
        payload = json.loads(AIRPORT_COUNTRIES_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    mapping: dict[str, str] = {}
    if not isinstance(payload, dict):
        return mapping

    for airport_code, country_code in payload.items():
        normalized_airport = str(airport_code or "").strip().upper()
        normalized_country = str(country_code or "").strip().upper()
        if normalized_airport and normalized_country:
            mapping[normalized_airport] = normalized_country
    return mapping


def _classify_route(origin: Any, destination: Any) -> dict[str, Any]:
    origin_code = str(origin or "").strip().upper()
    destination_code = str(destination or "").strip().upper()
    country_map = _load_airport_country_map()
    origin_country = country_map.get(origin_code)
    destination_country = country_map.get(destination_code)

    route_type = ROUTE_TYPE_UNK
    is_cross_border = False
    domestic_country_code: str | None = None
    if origin_country and destination_country:
        if origin_country == destination_country:
            route_type = ROUTE_TYPE_DOM
            domestic_country_code = origin_country
        else:
            route_type = ROUTE_TYPE_INT
            is_cross_border = True

    country_pair = f"{origin_country}-{destination_country}" if origin_country and destination_country else None
    return {
        "origin_country_code": origin_country,
        "destination_country_code": destination_country,
        "country_pair": country_pair,
        "route_type": route_type,
        "domestic_country_code": domestic_country_code,
        "is_cross_border": is_cross_border,
    }


def _annotate_route_record(row: dict[str, Any]) -> dict[str, Any]:
    annotated = dict(row)
    annotated.update(_classify_route(annotated.get("origin"), annotated.get("destination")))
    return annotated


def _annotate_route_records(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_annotate_route_record(row) for row in rows]


def _normalize_route_types(values: Sequence[str] | None) -> list[str]:
    normalized = _normalize_codes(values)
    return [value for value in normalized if value in {ROUTE_TYPE_DOM, ROUTE_TYPE_INT, ROUTE_TYPE_UNK}]


def _filter_route_type_records(rows: Sequence[dict[str, Any]], route_types: Sequence[str] | None) -> list[dict[str, Any]]:
    normalized = set(_normalize_route_types(route_types))
    if not normalized:
        return list(rows)
    return [row for row in rows if str(row.get("route_type") or ROUTE_TYPE_UNK) in normalized]


def _time_sort_key(value: Any) -> tuple[int, str]:
    cleaned = str(value or "").strip()
    if not cleaned:
        return (1, "99:99")
    return (0, cleaned)


def _weekday_sort_key(label: str) -> tuple[int, str]:
    try:
        return (WEEKDAY_ORDER.index(label), label)
    except ValueError:
        return (len(WEEKDAY_ORDER), label)


def _stops_label(value: Any) -> str:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return "Unknown"
    if normalized <= 0:
        return "Direct"
    return f"{normalized} stop" if normalized == 1 else f"{normalized} stops"


def _split_via_airports(value: Any) -> list[str]:
    if not value:
        return []
    parts = [str(part or "").strip().upper() for part in str(value).replace(",", "|").split("|")]
    return [part for part in dict.fromkeys(parts) if AIRPORT_CODE_RE.fullmatch(part)]


def _display_change_field_name(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return "-"
    explicit_labels = {
        "tax_amount": "Tax amount",
        "total_price_bdt": "Total price",
        "base_fare_amount": "Base fare",
        "ota_gross_fare": "Channel gross fare",
        "ota_discount_amount": "Channel discount amount",
        "ota_discount_pct": "Channel discount percent",
        "seat_available": "Seat available",
        "seat_capacity": "Seat capacity",
        "load_factor_pct": "Load factor",
        "booking_class": "Booking class",
        "penalty_rule_text": "Penalty text",
        "operating_airline": "Operating airline",
    }
    if normalized in explicit_labels:
        return explicit_labels[normalized]
    return " ".join(token.capitalize() for token in normalized.split("_") if token)


def _build_change_bigquery_filter_state(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    domains: Sequence[str] | None = None,
    change_types: Sequence[str] | None = None,
    directions: Sequence[str] | None = None,
) -> tuple[list[str], list[bigquery.ScalarQueryParameter]]:
    filters = ["1=1"]
    params: list[bigquery.ScalarQueryParameter] = []
    if start_date:
        filters.append("report_day >= @start_date")
        params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date:
        filters.append("report_day <= @end_date")
        params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))
    if airlines:
        filters.append("airline IN UNNEST(@airlines)")
        params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
    if origins:
        filters.append("origin IN UNNEST(@origins)")
        params.append(bigquery.ArrayQueryParameter("origins", "STRING", _normalize_codes(origins)))
    if destinations:
        filters.append("destination IN UNNEST(@destinations)")
        params.append(bigquery.ArrayQueryParameter("destinations", "STRING", _normalize_codes(destinations)))
    if domains:
        filters.append("domain IN UNNEST(@domains)")
        params.append(bigquery.ArrayQueryParameter("domains", "STRING", _normalize_codes(domains, uppercase=False)))
    if change_types:
        filters.append("change_type IN UNNEST(@change_types)")
        params.append(bigquery.ArrayQueryParameter("change_types", "STRING", _normalize_codes(change_types, uppercase=False)))
    if directions:
        filters.append("direction IN UNNEST(@directions)")
        params.append(bigquery.ArrayQueryParameter("directions", "STRING", _normalize_codes(directions, uppercase=False)))
    return filters, params


def _build_change_sql_filter_state(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    domains: Sequence[str] | None = None,
    change_types: Sequence[str] | None = None,
    directions: Sequence[str] | None = None,
    alias: str = "cce",
) -> tuple[list[str], dict[str, Any]]:
    clauses = ["1=1"]
    params: dict[str, Any] = {}
    if start_date:
        clauses.append(f"{alias}.detected_at::date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        clauses.append(f"{alias}.detected_at::date <= :end_date")
        params["end_date"] = end_date
    _apply_in_filter(clauses, params, f"{alias}.airline", airlines, "airline")
    _apply_in_filter(clauses, params, f"{alias}.origin", origins, "origin")
    _apply_in_filter(clauses, params, f"{alias}.destination", destinations, "destination")
    _apply_in_filter(clauses, params, f"{alias}.domain", domains, "domain", uppercase=False)
    _apply_in_filter(clauses, params, f"{alias}.change_type", change_types, "change_type", uppercase=False)
    _apply_in_filter(clauses, params, f"{alias}.direction", directions, "direction", uppercase=False)
    return clauses, params


def _build_change_dashboard_payload(
    *,
    summary_row: dict[str, Any] | None,
    daily_rows: Sequence[dict[str, Any]],
    route_rows: Sequence[dict[str, Any]],
    airline_rows: Sequence[dict[str, Any]],
    domain_rows: Sequence[dict[str, Any]],
    field_rows: Sequence[dict[str, Any]],
    largest_moves: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    summary = dict(summary_row or {})
    summary.setdefault("event_count", 0)
    summary.setdefault("route_count", 0)
    summary.setdefault("airline_count", 0)
    summary.setdefault("up_count", 0)
    summary.setdefault("down_count", 0)
    summary.setdefault("added_count", 0)
    summary.setdefault("removed_count", 0)
    summary.setdefault("price_event_count", 0)
    summary.setdefault("availability_event_count", 0)
    summary.setdefault("schedule_event_count", 0)
    summary.setdefault("tax_event_count", 0)
    summary.setdefault("penalty_event_count", 0)

    field_mix = []
    for row in field_rows:
        normalized = dict(row)
        normalized["display_name"] = _display_change_field_name(normalized.get("field_name"))
        field_mix.append(normalized)

    return {
        "summary": summary,
        "daily_series": list(daily_rows),
        "top_routes": _annotate_route_records(route_rows),
        "top_airlines": list(airline_rows),
        "domain_mix": list(domain_rows),
        "field_mix": field_mix,
        "largest_moves": _annotate_route_records(largest_moves),
    }


def _get_latest_cycle_from_bigquery(comparable_only: bool = True) -> dict[str, Any] | None:
    comparable_filter = ""
    if comparable_only:
        comparable_filter = (
            f"WHERE offer_rows >= {COMPARABLE_CYCLE_MIN_OFFER_ROWS} "
            f"AND airline_count >= {COMPARABLE_CYCLE_MIN_AIRLINES} "
            f"AND route_count >= {COMPARABLE_CYCLE_MIN_ROUTES}"
        )
    rows = _run_bigquery_query(
        f"""
        SELECT
          cycle_id,
          cycle_started_at_utc,
          cycle_completed_at_utc,
          offer_rows,
          airline_count,
          route_count
        FROM {_bq_table("fact_cycle_run")}
        {comparable_filter}
        QUALIFY ROW_NUMBER() OVER (ORDER BY cycle_completed_at_utc DESC, cycle_id DESC) = 1
        """
    )
    clean_rows = _serialize_warehouse_rows(rows)
    return clean_rows[0] if clean_rows else None


def _get_latest_cycle_from_sql(session: Session, comparable_only: bool = True) -> dict[str, Any] | None:
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
            HAVING (
                :comparable_only = FALSE
                OR (
                    COUNT(*) >= :min_offer_rows
                    AND COUNT(DISTINCT fo.airline) >= :min_airlines
                    AND COUNT(DISTINCT (fo.origin || '-' || fo.destination)) >= :min_routes
                )
            )
            ORDER BY MAX(fo.scraped_at) DESC
            LIMIT 1
            """
        ),
        {
            "comparable_only": comparable_only,
            "min_offer_rows": COMPARABLE_CYCLE_MIN_OFFER_ROWS,
            "min_airlines": COMPARABLE_CYCLE_MIN_AIRLINES,
            "min_routes": COMPARABLE_CYCLE_MIN_ROUTES,
        },
    ).mappings().first()
    return dict(row) if row else None


def get_latest_cycle(session: Session | None, comparable_only: bool = True) -> dict[str, Any] | None:
    if _bigquery_ready():
        try:
            return _get_latest_cycle_from_bigquery(comparable_only=comparable_only)
        except (GoogleAPIError, RuntimeError, ValueError):
            pass
    if session is None:
        return None
    return _get_latest_cycle_from_sql(session, comparable_only=comparable_only)


def _get_recent_cycles_from_sql(
    session: Session,
    limit: int = 10,
    comparable_only: bool = True,
) -> list[dict[str, Any]]:
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
            HAVING (
                :comparable_only = FALSE
                OR (
                    COUNT(*) >= :min_offer_rows
                    AND COUNT(DISTINCT fo.airline) >= :min_airlines
                    AND COUNT(DISTINCT (fo.origin || '-' || fo.destination)) >= :min_routes
                )
            )
            ORDER BY MAX(fo.scraped_at) DESC
            LIMIT :limit
            """
        ),
        {
            "limit": limit,
            "comparable_only": comparable_only,
            "min_offer_rows": COMPARABLE_CYCLE_MIN_OFFER_ROWS,
            "min_airlines": COMPARABLE_CYCLE_MIN_AIRLINES,
            "min_routes": COMPARABLE_CYCLE_MIN_ROUTES,
        },
    ).mappings().all()
    return _rows_to_dicts([dict(row) for row in rows])


def get_recent_cycles(session: Session | None, limit: int = 10, comparable_only: bool = True) -> list[dict[str, Any]]:
    prefer_sql = session is not None
    if not prefer_sql and _bigquery_ready():
        try:
            comparable_filter = ""
            if comparable_only:
                comparable_filter = (
                    f"WHERE offer_rows >= {COMPARABLE_CYCLE_MIN_OFFER_ROWS} "
                    f"AND airline_count >= {COMPARABLE_CYCLE_MIN_AIRLINES} "
                    f"AND route_count >= {COMPARABLE_CYCLE_MIN_ROUTES}"
                )
            rows = _run_bigquery_query(
                f"""
                WITH ranked_cycles AS (
                  SELECT
                    cycle_id,
                    cycle_started_at_utc,
                    cycle_completed_at_utc,
                    offer_rows,
                    airline_count,
                    route_count,
                    ROW_NUMBER() OVER (
                      PARTITION BY cycle_id
                      ORDER BY cycle_completed_at_utc DESC, cycle_started_at_utc DESC, offer_rows DESC
                    ) AS row_rank
                  FROM {_bq_table("fact_cycle_run")}
                  {comparable_filter}
                )
                SELECT
                  cycle_id,
                  cycle_started_at_utc,
                  cycle_completed_at_utc,
                  offer_rows,
                  airline_count,
                  route_count
                FROM ranked_cycles
                WHERE row_rank = 1
                ORDER BY cycle_completed_at_utc DESC, cycle_id DESC
                LIMIT @row_limit
                """,
                [bigquery.ScalarQueryParameter("row_limit", "INT64", limit)],
            )
            return _serialize_warehouse_rows(rows)
        except (GoogleAPIError, RuntimeError, ValueError):
            pass
    if session is None:
        return []
    return _get_recent_cycles_from_sql(session, limit=limit, comparable_only=comparable_only)


def _serialize_date_count_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        normalized_date = _iso_date(row.get("date"))
        if not normalized_date:
            continue
        try:
            row_count = int(row.get("row_count") or 0)
        except (TypeError, ValueError):
            row_count = 0
        payload.append({"date": normalized_date, "row_count": row_count})
    return payload


def _get_route_date_availability_from_bigquery(
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    trip_types: Sequence[str] | None = None,
) -> dict[str, Any]:
    resolved_cycle_id = _resolve_cycle_id_bigquery(cycle_id)
    if not resolved_cycle_id:
        return {"cycle_id": None, "departure_dates": [], "return_dates": []}

    filters = ["cycle_id = @cycle_id"]
    params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id)
    ]
    if airlines:
        filters.append("airline IN UNNEST(@airlines)")
        params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
    if origins:
        filters.append("origin IN UNNEST(@origins)")
        params.append(bigquery.ArrayQueryParameter("origins", "STRING", _normalize_codes(origins)))
    if destinations:
        filters.append("destination IN UNNEST(@destinations)")
        params.append(bigquery.ArrayQueryParameter("destinations", "STRING", _normalize_codes(destinations)))
    if cabins:
        filters.append("cabin IN UNNEST(@cabins)")
        params.append(bigquery.ArrayQueryParameter("cabins", "STRING", _normalize_codes(cabins, uppercase=False)))
    normalized_trip_types = [value for value in _normalize_codes(trip_types) if value in {"OW", "RT"}]
    if normalized_trip_types:
        filters.append("search_trip_type IN UNNEST(@trip_types)")
        params.append(bigquery.ArrayQueryParameter("trip_types", "STRING", normalized_trip_types))

    where_clause = " AND ".join(filters)
    departure_rows = _serialize_warehouse_rows(
        _run_bigquery_query(
            f"""
            SELECT
              departure_date AS date,
              COUNT(*) AS row_count
            FROM {_bq_table("fact_offer_snapshot")}
            WHERE {where_clause}
              AND departure_date IS NOT NULL
            GROUP BY departure_date
            ORDER BY departure_date
            """,
            params,
        )
    )
    return_rows = _serialize_warehouse_rows(
        _run_bigquery_query(
            f"""
            SELECT
              requested_return_date AS date,
              COUNT(*) AS row_count
            FROM {_bq_table("fact_offer_snapshot")}
            WHERE {where_clause}
              AND requested_return_date IS NOT NULL
            GROUP BY requested_return_date
            ORDER BY requested_return_date
            """,
            params,
        )
    )
    return {
        "cycle_id": resolved_cycle_id,
        "departure_dates": _serialize_date_count_rows(departure_rows),
        "return_dates": _serialize_date_count_rows(return_rows),
    }


def get_route_date_availability(
    session: Session | None,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    trip_types: Sequence[str] | None = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_trip_types = [value for value in _normalize_codes(trip_types) if value in {"OW", "RT"}]
    prefer_sql = session is not None
    if not prefer_sql and _bigquery_ready():
        try:
            resolved_cycle_id = _resolve_cycle_id_bigquery(cycle_id)
            cache_key = _cache_key(
                "route_date_availability",
                backend="bigquery",
                cycle_id=resolved_cycle_id,
                airlines=_normalize_sequence_cache_value(airlines),
                origins=_normalize_sequence_cache_value(origins),
                destinations=_normalize_sequence_cache_value(destinations),
                cabins=_normalize_sequence_cache_value(cabins, uppercase=False),
                trip_types=tuple(normalized_trip_types),
            )
            cached = _get_cached_response(cache_key, ROUTE_DATE_AVAILABILITY_CACHE_TTL_SEC)
            if cached is not None:
                _set_request_metrics(
                    route_date_availability_backend="bigquery",
                    route_date_availability_cache="hit",
                    route_date_availability_departure_dates=len(cached.get("departure_dates") or []),
                    route_date_availability_return_dates=len(cached.get("return_dates") or []),
                    route_date_availability_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
                )
                LOG.info(
                    "route_date_availability cache_hit backend=bigquery cycle_id=%s airlines=%d origins=%d destinations=%d cabins=%d trip_types=%s total_ms=%.1f",
                    resolved_cycle_id,
                    len(_normalize_sequence_cache_value(airlines)),
                    len(_normalize_sequence_cache_value(origins)),
                    len(_normalize_sequence_cache_value(destinations)),
                    len(_normalize_sequence_cache_value(cabins, uppercase=False)),
                    ",".join(normalized_trip_types) or "-",
                    (time.perf_counter() - started_at) * 1000,
                )
                return cached
            payload = _get_route_date_availability_from_bigquery(
                cycle_id=resolved_cycle_id,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                cabins=cabins,
                trip_types=normalized_trip_types,
            )
            _set_request_metrics(
                route_date_availability_backend="bigquery",
                route_date_availability_cache="miss",
                route_date_availability_departure_dates=len(payload.get("departure_dates") or []),
                route_date_availability_return_dates=len(payload.get("return_dates") or []),
                route_date_availability_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
            )
            LOG.info(
                "route_date_availability backend=bigquery cycle_id=%s departure_dates=%d return_dates=%d total_ms=%.1f",
                resolved_cycle_id,
                len(payload.get("departure_dates") or []),
                len(payload.get("return_dates") or []),
                (time.perf_counter() - started_at) * 1000,
            )
            return _set_cached_response(cache_key, payload)
        except (GoogleAPIError, RuntimeError, ValueError):
            pass
    if session is None:
        return {"cycle_id": None, "departure_dates": [], "return_dates": []}

    resolved_cycle_id = _resolve_cycle_id(session, cycle_id)
    if not resolved_cycle_id:
        _set_request_metrics(
            route_date_availability_backend="sql",
            route_date_availability_cache="miss",
            route_date_availability_departure_dates=0,
            route_date_availability_return_dates=0,
            route_date_availability_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        return {"cycle_id": None, "departure_dates": [], "return_dates": []}

    cache_key = _cache_key(
        "route_date_availability",
        backend="sql",
        cycle_id=resolved_cycle_id,
        airlines=_normalize_sequence_cache_value(airlines),
        origins=_normalize_sequence_cache_value(origins),
        destinations=_normalize_sequence_cache_value(destinations),
        cabins=_normalize_sequence_cache_value(cabins, uppercase=False),
        trip_types=tuple(normalized_trip_types),
    )
    cached = _get_cached_response(cache_key, ROUTE_DATE_AVAILABILITY_CACHE_TTL_SEC)
    if cached is not None:
        _set_request_metrics(
            route_date_availability_backend="sql",
            route_date_availability_cache="hit",
            route_date_availability_departure_dates=len(cached.get("departure_dates") or []),
            route_date_availability_return_dates=len(cached.get("return_dates") or []),
            route_date_availability_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        LOG.info(
            "route_date_availability cache_hit backend=sql cycle_id=%s airlines=%d origins=%d destinations=%d cabins=%d trip_types=%s total_ms=%.1f",
            resolved_cycle_id,
            len(_normalize_sequence_cache_value(airlines)),
            len(_normalize_sequence_cache_value(origins)),
            len(_normalize_sequence_cache_value(destinations)),
            len(_normalize_sequence_cache_value(cabins, uppercase=False)),
            ",".join(normalized_trip_types) or "-",
            (time.perf_counter() - started_at) * 1000,
        )
        return cached

    clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)"]
    params: dict[str, Any] = {"cycle_id": resolved_cycle_id}
    _apply_in_filter(clauses, params, "fo.airline", airlines, "availability_airline")
    _apply_in_filter(clauses, params, "fo.origin", origins, "availability_origin")
    _apply_in_filter(clauses, params, "fo.destination", destinations, "availability_destination")
    _apply_in_filter(clauses, params, "fo.cabin", cabins, "availability_cabin", uppercase=False)
    if normalized_trip_types:
        _apply_in_filter(
            clauses,
            params,
            "COALESCE(frm.search_trip_type, 'OW')",
            normalized_trip_types,
            "availability_trip_type",
        )

    where_clause = " AND ".join(clauses)
    departure_rows = session.execute(
        text(
            f"""
            SELECT
              fo.departure::date AS date,
              COUNT(*) AS row_count
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta frm
              ON frm.flight_offer_id = fo.id
            WHERE {where_clause}
            GROUP BY fo.departure::date
            ORDER BY fo.departure::date
            """
        ),
        params,
    ).mappings().all()
    return_rows = session.execute(
        text(
            f"""
            SELECT
              frm.requested_return_date AS date,
              COUNT(*) AS row_count
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta frm
              ON frm.flight_offer_id = fo.id
            WHERE {where_clause}
              AND frm.requested_return_date IS NOT NULL
            GROUP BY frm.requested_return_date
            ORDER BY frm.requested_return_date
            """
        ),
        params,
    ).mappings().all()
    payload = {
        "cycle_id": resolved_cycle_id,
        "departure_dates": _serialize_date_count_rows(_rows_to_dicts([dict(row) for row in departure_rows])),
        "return_dates": _serialize_date_count_rows(_rows_to_dicts([dict(row) for row in return_rows])),
    }
    _set_request_metrics(
        route_date_availability_backend="sql",
        route_date_availability_cache="miss",
        route_date_availability_departure_dates=len(payload.get("departure_dates") or []),
        route_date_availability_return_dates=len(payload.get("return_dates") or []),
        route_date_availability_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
    )
    LOG.info(
        "route_date_availability backend=sql cycle_id=%s departure_dates=%d return_dates=%d total_ms=%.1f",
        resolved_cycle_id,
        len(payload.get("departure_dates") or []),
        len(payload.get("return_dates") or []),
        (time.perf_counter() - started_at) * 1000,
    )
    return _set_cached_response(cache_key, payload)


def get_health(session: Session | None) -> dict[str, Any]:
    if session is not None:
        session.execute(text("SELECT 1"))
    latest_cycle = get_latest_cycle(session)
    run_status = None
    if latest_cycle:
        run_status = _build_cycle_run_status(
            cycle_id=str(latest_cycle["cycle_id"]),
            latest_cycle=latest_cycle,
            wrapper_status=_load_latest_cycle_wrapper_status(),
            worker_status=_load_latest_run_status(),
            parallel_status=_load_latest_parallel_status(),
        )
    return {
        "database_ok": session is not None or _bigquery_ready(),
        "latest_cycle_id": latest_cycle["cycle_id"] if latest_cycle else None,
        "latest_cycle_started_at_utc": latest_cycle.get("cycle_started_at_utc") if latest_cycle else None,
        "latest_cycle_completed_at_utc": latest_cycle["cycle_completed_at_utc"] if latest_cycle else None,
        "latest_run_status": run_status,
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


@lru_cache(maxsize=1)
def _load_configured_route_entries() -> list[dict[str, Any]]:
    try:
        payload = json.loads(ROUTES_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []

    entries: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        airline = str(row.get("airline") or "").strip().upper()
        origin = str(row.get("origin") or "").strip().upper()
        destination = str(row.get("destination") or "").strip().upper()
        cabins = [
            str(item or "").strip()
            for item in row.get("cabins", [])
            if str(item or "").strip()
        ] if isinstance(row.get("cabins"), list) else []
        if not airline or not origin or not destination:
            continue
        entries.append(
            {
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "route_key": f"{origin}-{destination}",
                "cabins": cabins,
            }
        )
    return entries


def _list_configured_routes(
    *,
    airlines: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    origin_prefix: str | None = None,
    destination_prefix: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    normalized_airlines = set(_normalize_codes(airlines))
    normalized_cabins = {value.strip().lower() for value in _normalize_codes(cabins, uppercase=False) if value.strip()}
    normalized_origin_prefix = _normalize_code_prefix(origin_prefix)
    normalized_destination_prefix = _normalize_code_prefix(destination_prefix)

    aggregated: dict[str, dict[str, Any]] = {}
    for entry in _load_configured_route_entries():
        origin = str(entry.get("origin") or "")
        destination = str(entry.get("destination") or "")
        route_key = str(entry.get("route_key") or "")
        airline = str(entry.get("airline") or "")
        entry_cabins = [str(item or "").strip().lower() for item in entry.get("cabins", [])]
        if normalized_airlines and airline not in normalized_airlines:
            continue
        if normalized_cabins and entry_cabins and not normalized_cabins.intersection(entry_cabins):
            continue
        if normalized_origin_prefix and not origin.startswith(normalized_origin_prefix):
            continue
        if normalized_destination_prefix and not destination.startswith(normalized_destination_prefix):
            continue

        current = aggregated.setdefault(
            route_key,
            {
                "route_key": route_key,
                "origin": origin,
                "destination": destination,
                "offer_rows": None,
                "first_seen_at_utc": None,
                "last_seen_at_utc": None,
                "_airlines": set(),
            },
        )
        current["_airlines"].add(airline)

    rows: list[dict[str, Any]] = []
    for route_key, row in aggregated.items():
        clean = {key: value for key, value in row.items() if key != "_airlines"}
        clean["airlines_present"] = len(row.get("_airlines") or [])
        rows.append(clean)

    rows.sort(
        key=lambda item: (
            -int(item.get("airlines_present") or 0),
            str(item.get("route_key") or ""),
        )
    )
    if limit:
        rows = rows[: int(limit)]
    return _annotate_route_records(rows)


def _load_latest_run_status() -> dict[str, Any] | None:
    try:
        return json.loads(RUN_STATUS_LATEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _load_latest_parallel_status() -> dict[str, Any] | None:
    try:
        payload = json.loads(SCRAPE_PARALLEL_LATEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _load_latest_cycle_wrapper_status() -> dict[str, Any] | None:
    try:
        payload = json.loads(ACCUMULATION_CYCLE_LATEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _build_cycle_run_status(
    *,
    cycle_id: str,
    latest_cycle: Mapping[str, Any],
    wrapper_status: dict[str, Any] | None,
    worker_status: dict[str, Any] | None,
    parallel_status: dict[str, Any] | None,
) -> dict[str, Any] | None:
    wrapper_cycle_id = str(wrapper_status.get("cycle_id") or wrapper_status.get("accumulation_run_id") or "") if wrapper_status else ""
    wrapper_matches_latest = wrapper_cycle_id == cycle_id
    wrapper_state = str(wrapper_status.get("state") or "").strip().lower() if wrapper_status else ""

    worker_cycle_id = str(worker_status.get("cycle_id") or worker_status.get("scrape_id") or "") if worker_status else ""
    worker_matches_latest = worker_cycle_id == cycle_id

    parallel_cycle_id = str(parallel_status.get("cycle_id") or "") if parallel_status else ""
    parallel_matches_latest = parallel_cycle_id == cycle_id

    if wrapper_matches_latest and wrapper_state in {"completed", "failed"}:
        return {
            "cycle_id": cycle_id,
            "state": wrapper_status.get("state"),
            "phase": wrapper_status.get("phase") or "aggregate_wrapper",
            "overall_query_total": wrapper_status.get("overall_query_total"),
            "overall_query_completed": wrapper_status.get("overall_query_completed"),
            "total_rows_accumulated": wrapper_status.get("total_rows_accumulated") or latest_cycle.get("offer_rows"),
            "completed_at_utc": wrapper_status.get("completed_at_utc") or latest_cycle.get("cycle_completed_at_utc"),
            "selected_dates": wrapper_status.get("selected_dates"),
            "matches_latest_cycle": True,
            "status_source": "wrapper_cycle_state",
            "aggregate_airline_count": wrapper_status.get("aggregate_airline_count"),
            "aggregate_failed_count": wrapper_status.get("aggregate_failed_count"),
            "duration_sec": wrapper_status.get("duration_sec"),
        }

    if parallel_matches_latest:
        selected_dates = worker_status.get("selected_dates") if worker_matches_latest and worker_status else None
        return {
            "cycle_id": cycle_id,
            "state": "completed",
            "phase": "aggregate_parallel",
            "overall_query_total": None,
            "overall_query_completed": None,
            "total_rows_accumulated": latest_cycle.get("offer_rows"),
            "completed_at_utc": parallel_status.get("completed_at_utc") or latest_cycle.get("cycle_completed_at_utc"),
            "selected_dates": selected_dates,
            "matches_latest_cycle": True,
            "status_source": "parallel_aggregate",
            "aggregate_airline_count": parallel_status.get("airline_count"),
            "aggregate_failed_count": parallel_status.get("failed_count"),
            "duration_sec": parallel_status.get("duration_sec"),
        }

    if worker_status:
        return {
            "cycle_id": worker_status.get("cycle_id") or worker_status.get("scrape_id"),
            "state": worker_status.get("state"),
            "phase": worker_status.get("phase"),
            "overall_query_total": worker_status.get("overall_query_total"),
            "overall_query_completed": worker_status.get("overall_query_completed"),
            "total_rows_accumulated": worker_status.get("total_rows_accumulated"),
            "completed_at_utc": worker_status.get("completed_at_utc"),
            "selected_dates": worker_status.get("selected_dates"),
            "matches_latest_cycle": worker_matches_latest,
            "status_source": "worker_heartbeat",
            "aggregate_airline_count": None,
            "aggregate_failed_count": None,
            "duration_sec": None,
        }

    return None


def get_cycle_health(session: Session | None) -> dict[str, Any]:
    latest_cycle = get_latest_cycle(session)
    if not latest_cycle:
        return {
            "database_ok": session is not None or _bigquery_ready(),
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
    if _bigquery_ready():
        try:
            observed_route_pairs = [
                str(item["route_pair"])
                for item in _run_bigquery_query(
                    f"""
                    SELECT DISTINCT CONCAT(airline, ':', route_key) AS route_pair
                    FROM {_bq_table("fact_offer_snapshot")}
                    WHERE cycle_id = @cycle_id
                    ORDER BY route_pair
                    """,
                    [bigquery.ScalarQueryParameter("cycle_id", "STRING", cycle_id)],
                )
                if item.get("route_pair")
            ]
        except (GoogleAPIError, RuntimeError, ValueError):
            observed_route_pairs = []
    elif session is not None:
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
    else:
        observed_route_pairs = []
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
    wrapper_status = _load_latest_cycle_wrapper_status()
    worker_status = _load_latest_run_status()
    parallel_status = _load_latest_parallel_status()
    run_status = _build_cycle_run_status(
        cycle_id=cycle_id,
        latest_cycle=latest_cycle,
        wrapper_status=wrapper_status,
        worker_status=worker_status,
        parallel_status=parallel_status,
    )

    return {
        "database_ok": session is not None or _bigquery_ready(),
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
        "latest_run_status": run_status,
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


@lru_cache(maxsize=1)
def _get_bigquery_client() -> bigquery.Client | None:
    if not settings.bigquery_project_id:
        return None
    try:
        return bigquery.Client(project=settings.bigquery_project_id)
    except Exception:
        return None


def _bigquery_ready() -> bool:
    return bool(settings.bigquery_project_id and settings.bigquery_dataset and _get_bigquery_client())


def _run_bigquery_query(query: str, parameters: Sequence[bigquery.ScalarQueryParameter] | None = None) -> list[dict[str, Any]]:
    client = _get_bigquery_client()
    if client is None:
        raise RuntimeError("BigQuery client is not configured")

    job_config = bigquery.QueryJobConfig(query_parameters=list(parameters or ()))
    job = client.query(query, job_config=job_config)
    started_at = time.perf_counter()
    try:
        rows = job.result(timeout=settings.bigquery_query_timeout_sec)
    except (GoogleAPIError, FuturesTimeoutError, TimeoutError) as exc:
        try:
            job.cancel()
        except Exception:
            pass
        LOG.warning(
            "bigquery query_timeout timeout_sec=%.1f elapsed_ms=%.1f",
            settings.bigquery_query_timeout_sec,
            (time.perf_counter() - started_at) * 1000,
        )
        raise RuntimeError(
            f"BigQuery query timed out after {settings.bigquery_query_timeout_sec:.1f}s"
        ) from exc
    return [dict(row.items()) for row in rows]


def _bq_table(table_name: str) -> str:
    return f"`{settings.bigquery_project_id}.{settings.bigquery_dataset}.{table_name}`"


def _serialize_warehouse_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _serialize_warehouse_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    clean_rows: list[dict[str, Any]] = []
    for row in rows:
        clean: dict[str, Any] = {}
        for key, value in row.items():
            normalized_key = "pred_ewm_alpha_0.30" if key == "pred_ewm_alpha_0_30" else key
            clean[normalized_key] = _serialize_warehouse_value(value)
        clean_rows.append(clean)
    return clean_rows


def _iso_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")


def _iso_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "")


def _get_latest_bundle_row(backtest_only: bool = False) -> dict[str, Any] | None:
    where_clause = "WHERE has_backtest_eval" if backtest_only else ""
    rows = _run_bigquery_query(
        f"""
        SELECT
          bundle_id,
          bundle_name,
          target,
          stamp,
          bundle_created_at_utc,
          has_backtest_eval,
          has_backtest_splits,
          has_backtest_meta,
          backtest_status,
          backtest_split_count,
          backtest_selection_metric
        FROM {_bq_table("fact_forecast_bundle")}
        {where_clause}
        QUALIFY ROW_NUMBER() OVER (
          ORDER BY bundle_created_at_utc DESC, stamp DESC, bundle_name DESC
        ) = 1
        """
    )
    return _serialize_warehouse_rows(rows)[0] if rows else None


def _get_bundle_count() -> int:
    rows = _run_bigquery_query(
        f"""
        SELECT COUNT(*) AS bundle_count
        FROM {_bq_table("fact_forecast_bundle")}
        """
    )
    if not rows:
        return 0
    return int(rows[0]["bundle_count"])


def _get_bundle_model_eval(bundle_id: str) -> list[dict[str, Any]]:
    rows = _run_bigquery_query(
        f"""
        SELECT
          model,
          n,
          mae,
          rmse,
          mape_pct,
          smape_pct,
          directional_accuracy_pct,
          f1_macro
        FROM {_bq_table("fact_forecast_model_eval")}
        WHERE bundle_id = @bundle_id
        ORDER BY mae ASC NULLS LAST, rmse ASC NULLS LAST, model
        LIMIT 50
        """,
        [bigquery.ScalarQueryParameter("bundle_id", "STRING", bundle_id)],
    )
    return _serialize_warehouse_rows(rows)


def _get_bundle_route_eval(bundle_id: str, limit_routes: int) -> list[dict[str, Any]]:
    rows = _run_bigquery_query(
        f"""
        SELECT
          airline,
          origin,
          destination,
          route_key,
          cabin,
          model,
          n,
          mae,
          rmse,
          mape_pct,
          smape_pct,
          directional_accuracy_pct,
          f1_macro
        FROM {_bq_table("fact_forecast_route_eval")}
        WHERE bundle_id = @bundle_id
        ORDER BY mae ASC NULLS LAST, airline, route_key, model
        LIMIT @row_limit
        """,
        [
            bigquery.ScalarQueryParameter("bundle_id", "STRING", bundle_id),
            bigquery.ScalarQueryParameter("row_limit", "INT64", limit_routes),
        ],
    )
    return _serialize_warehouse_rows(rows)


def _get_bundle_route_winners(bundle_id: str, limit_routes: int) -> list[dict[str, Any]]:
    rows = _run_bigquery_query(
        f"""
        SELECT
          airline,
          origin,
          destination,
          route_key,
          cabin,
          winner_model,
          winner_metric,
          winner_n,
          winner_mae,
          winner_rmse,
          winner_directional_accuracy_pct,
          winner_f1_macro,
          max_candidate_n,
          coverage_threshold_n,
          candidate_models
        FROM {_bq_table("fact_forecast_route_winner")}
        WHERE bundle_id = @bundle_id
        ORDER BY winner_mae ASC NULLS LAST, airline, route_key, winner_model
        LIMIT @row_limit
        """,
        [
            bigquery.ScalarQueryParameter("bundle_id", "STRING", bundle_id),
            bigquery.ScalarQueryParameter("row_limit", "INT64", limit_routes),
        ],
    )
    return _serialize_warehouse_rows(rows)


def _get_bundle_next_day(bundle_id: str, limit_next_day: int) -> list[dict[str, Any]]:
    rows = _run_bigquery_query(
        f"""
        SELECT
          latest_report_day,
          predicted_for_day,
          history_days,
          airline,
          origin,
          destination,
          route_key,
          cabin,
          latest_actual_value,
          pred_last_value,
          pred_rolling_mean_3,
          pred_rolling_mean_7,
          pred_seasonal_naive_7,
          pred_ewm_alpha_0_30,
          pred_dl_mlp_q10,
          pred_dl_mlp_q50,
          pred_dl_mlp_q90,
          pred_ml_catboost_q10,
          pred_ml_catboost_q50,
          pred_ml_catboost_q90,
          pred_ml_lightgbm_q10,
          pred_ml_lightgbm_q50,
          pred_ml_lightgbm_q90
        FROM {_bq_table("fact_forecast_next_day")}
        WHERE bundle_id = @bundle_id
        ORDER BY predicted_for_day, airline, route_key
        LIMIT @row_limit
        """,
        [
            bigquery.ScalarQueryParameter("bundle_id", "STRING", bundle_id),
            bigquery.ScalarQueryParameter("row_limit", "INT64", limit_next_day),
        ],
    )
    return _serialize_warehouse_rows(rows)


def _get_bundle_backtest_eval(bundle_id: str) -> list[dict[str, Any]]:
    rows = _run_bigquery_query(
        f"""
        SELECT
          split_id,
          dataset,
          model,
          selected_on_val,
          n,
          mae,
          rmse,
          mape_pct,
          smape_pct,
          directional_accuracy_pct,
          f1_macro,
          train_start,
          train_end,
          val_start,
          val_end,
          test_start,
          test_end
        FROM {_bq_table("fact_backtest_eval")}
        WHERE bundle_id = @bundle_id
        ORDER BY split_id DESC, dataset, mae ASC NULLS LAST, model
        LIMIT 40
        """,
        [bigquery.ScalarQueryParameter("bundle_id", "STRING", bundle_id)],
    )
    return _serialize_warehouse_rows(rows)


def _get_bundle_backtest_route_winners(bundle_id: str, limit_routes: int) -> list[dict[str, Any]]:
    rows = _run_bigquery_query(
        f"""
        SELECT
          split_id,
          airline,
          origin,
          destination,
          route_key,
          cabin,
          winner_model,
          winner_metric,
          winner_n,
          winner_mae,
          winner_rmse,
          winner_directional_accuracy_pct,
          winner_f1_macro,
          dataset,
          selected_on_val
        FROM {_bq_table("fact_backtest_route_winner")}
        WHERE bundle_id = @bundle_id
        ORDER BY split_id DESC, winner_mae ASC NULLS LAST, airline, route_key
        LIMIT @row_limit
        """,
        [
            bigquery.ScalarQueryParameter("bundle_id", "STRING", bundle_id),
            bigquery.ScalarQueryParameter("row_limit", "INT64", limit_routes),
        ],
    )
    return _serialize_warehouse_rows(rows)


def _build_backtest_meta(bundle: dict[str, Any]) -> dict[str, Any]:
    return {
        "target_column": bundle.get("target"),
        "backtest_selection_metric": bundle.get("backtest_selection_metric"),
        "backtest": {
            "status": bundle.get("backtest_status"),
            "split_count": bundle.get("backtest_split_count"),
        },
    }


def _materialize_bigquery_bundle(
    bundle: dict[str, Any] | None,
    *,
    limit_routes: int,
    limit_next_day: int,
) -> dict[str, Any] | None:
    if not bundle:
        return None
    bundle_id = str(bundle["bundle_id"])
    return {
        "bundle_dir": f"bigquery://{settings.bigquery_project_id}.{settings.bigquery_dataset}.fact_forecast_bundle/{bundle_id}",
        "bundle_name": bundle["bundle_name"],
        "target": bundle["target"],
        "stamp": bundle["stamp"],
        "modified_at_utc": bundle.get("bundle_created_at_utc"),
        "overall_eval": _get_bundle_model_eval(bundle_id),
        "route_eval": _get_bundle_route_eval(bundle_id, limit_routes),
        "route_winners": _get_bundle_route_winners(bundle_id, limit_routes),
        "next_day": _get_bundle_next_day(bundle_id, limit_next_day),
        "backtest_eval": _get_bundle_backtest_eval(bundle_id) if bundle.get("has_backtest_eval") else [],
        "backtest_route_winners": _get_bundle_backtest_route_winners(bundle_id, limit_routes)
        if bundle.get("has_backtest_route_winner")
        else [],
        "backtest_meta": _build_backtest_meta(bundle) if bundle.get("has_backtest_eval") else None,
    }


def _get_forecasting_payload_from_bigquery(limit_routes: int = 25, limit_next_day: int = 40) -> dict[str, Any]:
    latest_bundle = _get_latest_bundle_row(backtest_only=False)
    latest_backtest_bundle = _get_latest_bundle_row(backtest_only=True)
    return {
        "latest_prediction_bundle": _materialize_bigquery_bundle(
            latest_bundle,
            limit_routes=limit_routes,
            limit_next_day=limit_next_day,
        ),
        "latest_backtest_bundle": _materialize_bigquery_bundle(
            latest_backtest_bundle,
            limit_routes=limit_routes,
            limit_next_day=limit_next_day,
        ),
        "bundle_count": _get_bundle_count(),
        "source": "bigquery",
    }


def _get_forecasting_payload_from_files(limit_routes: int = 25, limit_next_day: int = 40) -> dict[str, Any]:
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
            "route_winners": [],
            "next_day": next_day_rows,
            "backtest_eval": backtest_eval_rows,
            "backtest_route_winners": [],
            "backtest_meta": backtest_meta,
        }

    return {
        "latest_prediction_bundle": materialize(latest_bundle),
        "latest_backtest_bundle": materialize(latest_backtest_bundle),
        "bundle_count": len(bundles),
        "source": "filesystem",
    }


def get_forecasting_payload(limit_routes: int = 25, limit_next_day: int = 40) -> dict[str, Any]:
    source = settings.forecasting_source
    if source in {"bigquery", "warehouse"}:
        if not _bigquery_ready():
            return {
                "latest_prediction_bundle": None,
                "latest_backtest_bundle": None,
                "bundle_count": 0,
                "source": "bigquery",
                "warning": "BigQuery forecasting source is configured but unavailable.",
            }
        try:
            return _get_forecasting_payload_from_bigquery(limit_routes=limit_routes, limit_next_day=limit_next_day)
        except (GoogleAPIError, RuntimeError, ValueError) as exc:
            return {
                "latest_prediction_bundle": None,
                "latest_backtest_bundle": None,
                "bundle_count": 0,
                "source": "bigquery",
                "warning": f"BigQuery forecasting query failed: {exc}",
            }
    if source in {"auto", "hybrid"}:
        if _bigquery_ready():
            try:
                return _get_forecasting_payload_from_bigquery(limit_routes=limit_routes, limit_next_day=limit_next_day)
            except (GoogleAPIError, RuntimeError, ValueError):
                pass
        return _get_forecasting_payload_from_files(limit_routes=limit_routes, limit_next_day=limit_next_day)
    return _get_forecasting_payload_from_files(limit_routes=limit_routes, limit_next_day=limit_next_day)


def list_airlines(session: Session | None) -> list[dict[str, Any]]:
    if _bigquery_ready():
        try:
            rows = _run_bigquery_query(
                f"""
                WITH ranked_airlines AS (
                  SELECT
                    airline,
                    first_seen_at_utc,
                    last_seen_at_utc,
                    offer_rows,
                    ROW_NUMBER() OVER (
                      PARTITION BY airline
                      ORDER BY last_seen_at_utc DESC, first_seen_at_utc DESC, offer_rows DESC
                    ) AS row_rank
                  FROM {_bq_table("dim_airline")}
                )
                SELECT
                  airline,
                  first_seen_at_utc,
                  last_seen_at_utc,
                  offer_rows
                FROM ranked_airlines
                WHERE row_rank = 1
                ORDER BY offer_rows DESC NULLS LAST, airline
                """
            )
            return _serialize_warehouse_rows(rows)
        except (GoogleAPIError, RuntimeError, ValueError):
            pass
    if session is None:
        return []
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
            ORDER BY offer_rows DESC, fo.airline
            """
        )
    ).mappings().all()
    return _rows_to_dicts([dict(row) for row in rows])


def list_routes(
    session: Session | None,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    trip_types: Sequence[str] | None = None,
    origin_prefix: str | None = None,
    destination_prefix: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    started_at = time.perf_counter()
    normalized_trip_types = [value for value in _normalize_codes(trip_types) if value in {"OW", "RT"}]
    normalized_origin_prefix = _normalize_code_prefix(origin_prefix)
    normalized_destination_prefix = _normalize_code_prefix(destination_prefix)
    use_filtered_scope = bool(
        cycle_id or airlines or cabins or normalized_trip_types or normalized_origin_prefix or normalized_destination_prefix
    )
    if not cycle_id:
        cache_key = _cache_key(
            "route_list",
            backend="config",
            airlines=_normalize_sequence_cache_value(airlines),
            cabins=_normalize_sequence_cache_value(cabins, uppercase=False),
            trip_types=tuple(normalized_trip_types),
            origin_prefix=normalized_origin_prefix,
            destination_prefix=normalized_destination_prefix,
            limit=limit,
        )
        cached = _get_cached_response(cache_key, ROUTE_LIST_CACHE_TTL_SEC)
        if cached is not None:
            _set_request_metrics(
                route_list_backend="config",
                route_list_cache="hit",
                route_list_rows=len(cached),
                route_list_filtered=use_filtered_scope,
                route_list_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
            )
            LOG.info(
                "route_list cache_hit backend=config rows=%d filtered=%s total_ms=%.1f",
                len(cached),
                use_filtered_scope,
                (time.perf_counter() - started_at) * 1000,
            )
            return cached
        configured_payload = _list_configured_routes(
            airlines=airlines,
            cabins=cabins,
            origin_prefix=normalized_origin_prefix,
            destination_prefix=normalized_destination_prefix,
            limit=limit,
        )
        _set_request_metrics(
            route_list_backend="config",
            route_list_cache="miss",
            route_list_rows=len(configured_payload),
            route_list_filtered=use_filtered_scope,
            route_list_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        LOG.info(
            "route_list backend=config rows=%d filtered=%s total_ms=%.1f",
            len(configured_payload),
            use_filtered_scope,
            (time.perf_counter() - started_at) * 1000,
        )
        return _set_cached_response(cache_key, configured_payload)

    prefer_sql = session is not None
    if not prefer_sql and _bigquery_ready():
        try:
            resolved_cycle_id = _resolve_cycle_id_bigquery(cycle_id) if use_filtered_scope else None
            cache_key = _cache_key(
                "route_list",
                backend="bigquery",
                cycle_id=resolved_cycle_id,
                airlines=_normalize_sequence_cache_value(airlines),
                cabins=_normalize_sequence_cache_value(cabins, uppercase=False),
                trip_types=tuple(normalized_trip_types),
                origin_prefix=normalized_origin_prefix,
                destination_prefix=normalized_destination_prefix,
                limit=limit,
            )
            cached = _get_cached_response(cache_key, ROUTE_LIST_CACHE_TTL_SEC)
            if cached is not None:
                _set_request_metrics(
                    route_list_backend="bigquery",
                    route_list_cache="hit",
                    route_list_rows=len(cached),
                    route_list_filtered=use_filtered_scope,
                    route_list_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
                )
                LOG.info(
                    "route_list cache_hit backend=bigquery cycle_id=%s rows=%d filtered=%s total_ms=%.1f",
                    resolved_cycle_id,
                    len(cached),
                    use_filtered_scope,
                    (time.perf_counter() - started_at) * 1000,
                )
                return cached
            if use_filtered_scope:
                if not resolved_cycle_id:
                    _set_request_metrics(
                        route_list_backend="bigquery",
                        route_list_cache="miss",
                        route_list_rows=0,
                        route_list_filtered=use_filtered_scope,
                        route_list_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
                    )
                    return []
                filters = ["cycle_id = @cycle_id"]
                params: list[bigquery.ScalarQueryParameter] = [
                    bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id)
                ]
                if airlines:
                    filters.append("airline IN UNNEST(@airlines)")
                    params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
                if cabins:
                    filters.append("cabin IN UNNEST(@cabins)")
                    params.append(bigquery.ArrayQueryParameter("cabins", "STRING", _normalize_codes(cabins, uppercase=False)))
                if normalized_trip_types:
                    filters.append("search_trip_type IN UNNEST(@trip_types)")
                    params.append(bigquery.ArrayQueryParameter("trip_types", "STRING", normalized_trip_types))
                if normalized_origin_prefix:
                    filters.append("STARTS_WITH(origin, @origin_prefix)")
                    params.append(bigquery.ScalarQueryParameter("origin_prefix", "STRING", normalized_origin_prefix))
                if normalized_destination_prefix:
                    filters.append("STARTS_WITH(destination, @destination_prefix)")
                    params.append(bigquery.ScalarQueryParameter("destination_prefix", "STRING", normalized_destination_prefix))
                limit_clause = f"LIMIT {int(limit)}" if limit else ""
                rows = _run_bigquery_query(
                    f"""
                    SELECT
                      origin,
                      destination,
                      route_key,
                      COUNT(*) AS offer_rows,
                      COUNT(DISTINCT airline) AS airlines_present,
                      MIN(captured_at_utc) AS first_seen_at_utc,
                      MAX(captured_at_utc) AS last_seen_at_utc
                    FROM {_bq_table("fact_offer_snapshot")}
                    WHERE {' AND '.join(filters)}
                    GROUP BY origin, destination, route_key
                    ORDER BY offer_rows DESC NULLS LAST, route_key
                    {limit_clause}
                    """,
                    params,
                )
            else:
                where_prefix = []
                params: list[bigquery.ScalarQueryParameter] = []
                if normalized_origin_prefix:
                    where_prefix.append("STARTS_WITH(origin, @origin_prefix)")
                    params.append(bigquery.ScalarQueryParameter("origin_prefix", "STRING", normalized_origin_prefix))
                if normalized_destination_prefix:
                    where_prefix.append("STARTS_WITH(destination, @destination_prefix)")
                    params.append(bigquery.ScalarQueryParameter("destination_prefix", "STRING", normalized_destination_prefix))
                where_clause = f"WHERE {' AND '.join(where_prefix)}" if where_prefix else ""
                limit_clause = f"LIMIT {int(limit)}" if limit else ""
                rows = _run_bigquery_query(
                    f"""
                    WITH ranked_routes AS (
                      SELECT
                        origin,
                        destination,
                        route_key,
                        offer_rows,
                        airlines_present,
                        first_seen_at_utc,
                        last_seen_at_utc,
                        ROW_NUMBER() OVER (
                          PARTITION BY route_key
                          ORDER BY last_seen_at_utc DESC, offer_rows DESC, airlines_present DESC
                        ) AS row_rank
                      FROM {_bq_table("dim_route")}
                    )
                    SELECT
                      origin,
                      destination,
                      route_key,
                      offer_rows,
                      airlines_present,
                      first_seen_at_utc,
                      last_seen_at_utc
                    FROM ranked_routes
                    WHERE row_rank = 1
                    {"AND " + " AND ".join(where_prefix) if where_prefix else ""}
                    ORDER BY offer_rows DESC NULLS LAST, route_key
                    {limit_clause}
                    """,
                    params if params else None,
                )
            payload = _annotate_route_records(_serialize_warehouse_rows(rows))
            _set_request_metrics(
                route_list_backend="bigquery",
                route_list_cache="miss",
                route_list_rows=len(payload),
                route_list_filtered=use_filtered_scope,
                route_list_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
            )
            LOG.info(
                "route_list backend=bigquery cycle_id=%s rows=%d filtered=%s total_ms=%.1f",
                resolved_cycle_id,
                len(payload),
                use_filtered_scope,
                (time.perf_counter() - started_at) * 1000,
            )
            return _set_cached_response(cache_key, payload)
        except (GoogleAPIError, RuntimeError, ValueError) as exc:
            LOG.warning("route_date_availability bigquery_fallback reason=%s", exc)
    if session is None:
        return []

    resolved_cycle_id = _resolve_cycle_id(session, cycle_id) if use_filtered_scope else None
    cache_key = _cache_key(
        "route_list",
        backend="sql",
        cycle_id=resolved_cycle_id,
        airlines=_normalize_sequence_cache_value(airlines),
        cabins=_normalize_sequence_cache_value(cabins, uppercase=False),
        trip_types=tuple(normalized_trip_types),
        origin_prefix=normalized_origin_prefix,
        destination_prefix=normalized_destination_prefix,
        limit=limit,
    )
    cached = _get_cached_response(cache_key, ROUTE_LIST_CACHE_TTL_SEC)
    if cached is not None:
        _set_request_metrics(
            route_list_backend="sql",
            route_list_cache="hit",
            route_list_rows=len(cached),
            route_list_filtered=use_filtered_scope,
            route_list_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        LOG.info(
            "route_list cache_hit backend=sql cycle_id=%s rows=%d filtered=%s total_ms=%.1f",
            resolved_cycle_id,
            len(cached),
            use_filtered_scope,
            (time.perf_counter() - started_at) * 1000,
        )
        return cached
    clauses = ["1=1"]
    params: dict[str, Any] = {}
    if resolved_cycle_id:
        clauses.append("fo.scrape_id = CAST(:cycle_id AS uuid)")
        params["cycle_id"] = resolved_cycle_id
    _apply_in_filter(clauses, params, "fo.airline", airlines, "route_airline")
    _apply_in_filter(clauses, params, "fo.cabin", cabins, "route_cabin", uppercase=False)
    if normalized_trip_types:
        _apply_in_filter(clauses, params, "COALESCE(frm.search_trip_type, 'OW')", normalized_trip_types, "route_trip_type")
    if normalized_origin_prefix:
        clauses.append("fo.origin LIKE :route_origin_prefix")
        params["route_origin_prefix"] = f"{normalized_origin_prefix}%"
    if normalized_destination_prefix:
        clauses.append("fo.destination LIKE :route_destination_prefix")
        params["route_destination_prefix"] = f"{normalized_destination_prefix}%"
    limit_clause = "LIMIT :route_limit" if limit else ""
    if limit:
        params["route_limit"] = int(limit)

    rows = session.execute(
        text(
            f"""
            SELECT
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                COUNT(*) AS offer_rows,
                COUNT(DISTINCT fo.airline) AS airlines_present,
                MIN(fo.scraped_at) AS first_seen_at_utc,
                MAX(fo.scraped_at) AS last_seen_at_utc
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta frm
              ON frm.flight_offer_id = fo.id
            WHERE {' AND '.join(clauses)}
            GROUP BY fo.origin, fo.destination
            ORDER BY offer_rows DESC, route_key
            {limit_clause}
            """
        ),
        params,
    ).mappings().all()
    payload = _annotate_route_records(_rows_to_dicts([dict(row) for row in rows]))
    _set_request_metrics(
        route_list_backend="sql",
        route_list_cache="miss",
        route_list_rows=len(payload),
        route_list_filtered=use_filtered_scope,
        route_list_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
    )
    LOG.info(
        "route_list backend=sql cycle_id=%s rows=%d filtered=%s total_ms=%.1f",
        resolved_cycle_id,
        len(payload),
        use_filtered_scope,
        (time.perf_counter() - started_at) * 1000,
    )
    return _set_cached_response(cache_key, payload)


def _resolve_cycle_id(session: Session | None, cycle_id: str | None) -> str | None:
    if cycle_id:
        return cycle_id.strip()
    if session is None:
        return None
    latest_cycle = _get_latest_cycle_from_sql(session, comparable_only=True)
    return str(latest_cycle["cycle_id"]) if latest_cycle else None


def get_current_snapshot(
    session: Session | None,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    limit: int = 250,
) -> dict[str, Any]:
    resolved_cycle_id = _resolve_cycle_id(session, cycle_id)
    if not resolved_cycle_id or session is None:
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

    return {"cycle_id": resolved_cycle_id, "rows": _annotate_route_records(_rows_to_dicts([dict(row) for row in rows]))}


def _departure_day_label(value: date | datetime | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%A")
    if isinstance(value, date):
        return value.strftime("%A")
    try:
        return datetime.fromisoformat(str(value)).strftime("%A")
    except Exception:
        return ""


def _build_route_monitor_matrix_from_aggregates(
    resolved_cycle_id: str,
    selected_routes: Sequence[dict[str, Any]],
    current_rows: Sequence[dict[str, Any]],
    history_rows: Sequence[dict[str, Any]],
    history_limit: int,
    compact_history: bool = True,
) -> dict[str, Any]:
    if not current_rows:
        return {"cycle_id": resolved_cycle_id, "routes": []}

    started_at = time.perf_counter()
    route_date_map: dict[str, set[str]] = defaultdict(set)
    flight_groups_by_route: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    route_trip_meta: dict[str, dict[str, Any]] = {}
    for row in current_rows:
        route_key = str(row["route_key"])
        dep_date_iso = _iso_date(row.get("departure_date"))
        if not dep_date_iso:
            continue
        route_date_map[route_key].add(dep_date_iso)
        normalized_row = dict(row)
        normalized_row["departure_date"] = dep_date_iso
        if route_key not in route_trip_meta:
            route_trip_meta[route_key] = {
                "search_trip_type": normalized_row.get("search_trip_type"),
                "trip_pair_key": normalized_row.get("trip_pair_key"),
                "requested_outbound_date": normalized_row.get("requested_outbound_date"),
                "requested_return_date": normalized_row.get("requested_return_date"),
                "trip_duration_days": normalized_row.get("trip_duration_days"),
                "trip_origin": normalized_row.get("trip_origin"),
                "trip_destination": normalized_row.get("trip_destination"),
            }
        flight_group_id = _matrix_flight_group_id(normalized_row)
        if flight_group_id not in flight_groups_by_route[route_key]:
            flight_groups_by_route[route_key][flight_group_id] = {
                "flight_group_id": flight_group_id,
                "airline": normalized_row.get("airline"),
                "flight_number": normalized_row.get("flight_number"),
                "departure_time": normalized_row.get("departure_time"),
                "cabin": normalized_row.get("cabin"),
                "aircraft": normalized_row.get("aircraft"),
                "search_trip_type": normalized_row.get("search_trip_type"),
                "requested_return_date": normalized_row.get("requested_return_date"),
                "leg_direction": normalized_row.get("leg_direction"),
                "leg_sequence": normalized_row.get("leg_sequence"),
                "itinerary_leg_count": normalized_row.get("itinerary_leg_count"),
            }

    current_scan_ms = (time.perf_counter() - started_at) * 1000
    grouped_cell_history: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    captures_by_route_date: dict[tuple[str, str], set[str]] = defaultdict(set)

    for row in history_rows:
        route_key = str(row["route_key"])
        dep_date_iso = _iso_date(row.get("departure_date"))
        if dep_date_iso not in route_date_map.get(route_key, set()):
            continue
        normalized_row = dict(row)
        normalized_row["departure_date"] = dep_date_iso
        flight_group_id = _matrix_flight_group_id(normalized_row)
        if flight_group_id not in flight_groups_by_route.get(route_key, {}):
            continue
        captured_at_iso = _iso_timestamp(row.get("captured_at_utc"))
        normalized_row["flight_group_id"] = flight_group_id
        normalized_row["captured_at_utc"] = captured_at_iso
        captures_by_route_date[(route_key, dep_date_iso)].add(captured_at_iso)
        grouped_cell_history[(route_key, dep_date_iso, flight_group_id)].append(normalized_row)

    history_scan_ms = (time.perf_counter() - started_at) * 1000 - current_scan_ms
    preprocess_started_at = time.perf_counter()
    signal_counts: dict[str, int] = defaultdict(int)
    cells_by_route_date_capture: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)

    for (route_key, dep_date_iso, flight_group_id), cell_history in grouped_cell_history.items():
        ordered_history = sorted(cell_history, key=lambda item: item.get("captured_at_utc") or "")
        previous_cell: dict[str, Any] | None = None
        latest_captured_at = ordered_history[-1].get("captured_at_utc") if ordered_history else None
        for current_cell in ordered_history:
            captured_at_iso = current_cell.get("captured_at_utc")
            if not captured_at_iso:
                previous_cell = current_cell
                continue
            signal = _cell_signal(previous_cell, current_cell)
            if captured_at_iso == latest_captured_at:
                signal_counts[signal] += 1
            cells_by_route_date_capture[(route_key, dep_date_iso, captured_at_iso)].append(
                {
                    "flight_group_id": flight_group_id,
                    "min_total_price_bdt": current_cell.get("min_total_price_bdt"),
                    "max_total_price_bdt": current_cell.get("max_total_price_bdt"),
                    "tax_amount": current_cell.get("tax_amount"),
                    "booking_class": current_cell.get("booking_class"),
                    "min_booking_class": current_cell.get("min_booking_class") or current_cell.get("booking_class"),
                    "max_booking_class": current_cell.get("max_booking_class"),
                    "seat_available": current_cell.get("seat_available"),
                    "min_seat_available": current_cell.get("min_seat_available")
                    if current_cell.get("min_seat_available") is not None
                    else current_cell.get("seat_available"),
                    "max_seat_available": current_cell.get("max_seat_available"),
                    "seat_capacity": current_cell.get("seat_capacity"),
                    "load_factor_pct": current_cell.get("load_factor_pct"),
                    "soldout": current_cell.get("soldout"),
                    "signal": signal,
                }
            )
            previous_cell = current_cell

    preprocess_ms = (time.perf_counter() - preprocess_started_at) * 1000
    build_started_at = time.perf_counter()
    route_payloads: list[dict[str, Any]] = []

    for route_row in selected_routes:
        route_key = str(route_row["route_key"])
        flight_groups = sorted(
            flight_groups_by_route.get(route_key, {}).values(),
            key=lambda item: (
                str(item.get("departure_time") or ""),
                str(item.get("airline") or ""),
                str(item.get("flight_number") or ""),
            ),
        )
        flight_group_order = {
            str(item.get("flight_group_id") or ""): idx
            for idx, item in enumerate(flight_groups)
        }
        date_groups: list[dict[str, Any]] = []
        for dep_date in sorted(route_date_map.get(route_key, set())):
            all_capture_times = sorted(captures_by_route_date.get((route_key, dep_date), set()), reverse=True)
            capture_times = all_capture_times[: _route_matrix_capture_display_limit(history_limit, compact_history)]
            captures: list[dict[str, Any]] = []
            for captured_at_iso in capture_times:
                cells = sorted(
                    cells_by_route_date_capture.get((route_key, dep_date, captured_at_iso), []),
                    key=lambda item: flight_group_order.get(str(item.get("flight_group_id") or ""), 10**9),
                )
                captures.append(
                    {
                        "captured_at_utc": captured_at_iso,
                        "cells": cells,
                    }
                )
            date_groups.append(
                {
                    "departure_date": dep_date,
                    "day_label": _departure_day_label(dep_date),
                    "captures": captures,
                    "capture_count": len(all_capture_times),
                    "captures_loaded": len(capture_times),
                    "history_complete": len(capture_times) >= len(all_capture_times),
                }
            )

        route_payloads.append(
            {
                "route_key": route_key,
                "origin": route_row.get("origin"),
                "destination": route_row.get("destination"),
                **_classify_route(route_row.get("origin"), route_row.get("destination")),
                **route_trip_meta.get(route_key, {}),
                "flight_groups": flight_groups,
                "date_groups": date_groups,
            }
        )

    build_ms = (time.perf_counter() - build_started_at) * 1000
    total_ms = (time.perf_counter() - started_at) * 1000
    LOG.info(
        "route_monitor_matrix_builder cycle_id=%s routes=%d current_rows=%d history_rows=%d current_scan_ms=%.1f history_scan_ms=%.1f preprocess_ms=%.1f build_ms=%.1f total_ms=%.1f",
        resolved_cycle_id,
        len(route_payloads),
        len(current_rows),
        len(history_rows),
        current_scan_ms,
        history_scan_ms,
        preprocess_ms,
        build_ms,
        total_ms,
    )
    return {
        "cycle_id": resolved_cycle_id,
        "routes": route_payloads,
        "signal_counts": {key: signal_counts.get(key, 0) for key in sorted(signal_counts, key=_signal_sort_key)},
    }


def _resolve_cycle_id_bigquery(cycle_id: str | None) -> str | None:
    if cycle_id:
        return cycle_id.strip()
    latest_cycle = _get_latest_cycle_from_bigquery(comparable_only=True)
    return str(latest_cycle["cycle_id"]) if latest_cycle else None


def _get_route_monitor_matrix_from_bigquery(
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    trip_types: Sequence[str] | None = None,
    return_date: date | None = None,
    return_date_start: date | None = None,
    return_date_end: date | None = None,
    departure_date: date | None = None,
    route_limit: int = 8,
    history_limit: int = 12,
    compact_history: bool = True,
) -> dict[str, Any]:
    resolved_cycle_id = _resolve_cycle_id_bigquery(cycle_id)
    if not resolved_cycle_id:
        return {"cycle_id": None, "routes": []}

    route_filters = ["cycle_id = @cycle_id"]
    route_params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id),
        bigquery.ScalarQueryParameter("route_limit", "INT64", route_limit),
    ]
    if airlines:
        route_filters.append("airline IN UNNEST(@airlines)")
        route_params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
    if origins:
        route_filters.append("origin IN UNNEST(@origins)")
        route_params.append(bigquery.ArrayQueryParameter("origins", "STRING", _normalize_codes(origins)))
    if destinations:
        route_filters.append("destination IN UNNEST(@destinations)")
        route_params.append(bigquery.ArrayQueryParameter("destinations", "STRING", _normalize_codes(destinations)))
    if cabins:
        route_filters.append("cabin IN UNNEST(@cabins)")
        route_params.append(bigquery.ArrayQueryParameter("cabins", "STRING", _normalize_codes(cabins, uppercase=False)))
    normalized_trip_types = [value for value in _normalize_codes(trip_types) if value in {"OW", "RT"}]
    if normalized_trip_types:
        route_filters.append("search_trip_type IN UNNEST(@trip_types)")
        route_params.append(bigquery.ArrayQueryParameter("trip_types", "STRING", normalized_trip_types))
    if return_date:
        route_filters.append("requested_return_date = @return_date")
        route_params.append(bigquery.ScalarQueryParameter("return_date", "DATE", return_date))
    else:
        if return_date_start:
            route_filters.append("requested_return_date >= @return_date_start")
            route_params.append(bigquery.ScalarQueryParameter("return_date_start", "DATE", return_date_start))
        if return_date_end:
            route_filters.append("requested_return_date <= @return_date_end")
            route_params.append(bigquery.ScalarQueryParameter("return_date_end", "DATE", return_date_end))
    if normalized_trip_types:
        if origins:
            route_filters.append("trip_origin IN UNNEST(@trip_origins)")
            route_params.append(bigquery.ArrayQueryParameter("trip_origins", "STRING", _normalize_codes(origins)))
        if destinations:
            route_filters.append("trip_destination IN UNNEST(@trip_destinations)")
            route_params.append(bigquery.ArrayQueryParameter("trip_destinations", "STRING", _normalize_codes(destinations)))
    if departure_date:
        route_filters.append("departure_date = @departure_date")
        route_params.append(bigquery.ScalarQueryParameter("departure_date", "DATE", departure_date))

    selected_routes = _serialize_warehouse_rows(
        _run_bigquery_query(
            f"""
            SELECT
              origin,
              destination,
              route_key,
              COUNT(*) AS row_count
            FROM {_bq_table("fact_offer_snapshot")}
            WHERE {' AND '.join(route_filters)}
            GROUP BY origin, destination, route_key
            ORDER BY row_count DESC, route_key
            LIMIT @route_limit
            """,
            route_params,
        )
    )
    if not selected_routes:
        return {"cycle_id": resolved_cycle_id, "routes": []}

    route_keys = [str(row["route_key"]) for row in selected_routes]
    current_params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id),
        bigquery.ArrayQueryParameter("route_keys", "STRING", route_keys),
    ]
    current_filters = ["cycle_id = @cycle_id", "route_key IN UNNEST(@route_keys)"]
    if airlines:
        current_filters.append("airline IN UNNEST(@airlines)")
        current_params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
    if cabins:
        current_filters.append("cabin IN UNNEST(@cabins)")
        current_params.append(bigquery.ArrayQueryParameter("cabins", "STRING", _normalize_codes(cabins, uppercase=False)))
    if normalized_trip_types:
        current_filters.append("search_trip_type IN UNNEST(@trip_types)")
        current_params.append(bigquery.ArrayQueryParameter("trip_types", "STRING", normalized_trip_types))
    if return_date:
        current_filters.append("requested_return_date = @return_date")
        current_params.append(bigquery.ScalarQueryParameter("return_date", "DATE", return_date))
    else:
        if return_date_start:
            current_filters.append("requested_return_date >= @return_date_start")
            current_params.append(bigquery.ScalarQueryParameter("return_date_start", "DATE", return_date_start))
        if return_date_end:
            current_filters.append("requested_return_date <= @return_date_end")
            current_params.append(bigquery.ScalarQueryParameter("return_date_end", "DATE", return_date_end))
    if departure_date:
        current_filters.append("departure_date = @departure_date")
        current_params.append(bigquery.ScalarQueryParameter("departure_date", "DATE", departure_date))

    current_rows = _serialize_warehouse_rows(
        _run_bigquery_query(
            f"""
            WITH current_offers AS (
              SELECT *
              FROM {_bq_table("fact_offer_snapshot")}
              WHERE {' AND '.join(current_filters)}
            ),
            tax_lookup AS (
              SELECT
                cycle_id,
                captured_at_utc,
                airline,
                origin,
                destination,
                flight_number,
                departure_utc,
                cabin,
                MAX(tax_amount) AS tax_amount
              FROM {_bq_table("fact_tax_snapshot")}
              WHERE cycle_id = @cycle_id
                AND route_key IN UNNEST(@route_keys)
                {"AND airline IN UNNEST(@airlines)" if airlines else ""}
                {"AND cabin IN UNNEST(@cabins)" if cabins else ""}
              GROUP BY
                cycle_id, captured_at_utc, airline, origin, destination,
                flight_number, departure_utc, cabin
            )
            SELECT
              co.cycle_id,
              co.captured_at_utc,
              co.airline,
              co.origin,
              co.destination,
              co.route_key,
              co.flight_number,
              co.departure_utc,
              co.departure_date,
              FORMAT_TIMESTAMP('%H:%M', co.departure_utc) AS departure_time,
              co.cabin,
              COALESCE(co.aircraft, '') AS aircraft,
              co.search_trip_type,
              co.requested_outbound_date,
              co.requested_return_date,
              co.trip_duration_days,
              co.trip_origin,
              co.trip_destination,
              co.trip_pair_key,
              co.leg_direction,
              co.leg_sequence,
              co.itinerary_leg_count,
              MIN(co.total_price_bdt) AS min_total_price_bdt,
              MAX(co.total_price_bdt) AS max_total_price_bdt,
              MAX(
                COALESCE(
                  CASE
                    WHEN co.tax_amount IS NOT NULL AND co.tax_amount > 0 THEN co.tax_amount
                  END,
                  CASE
                    WHEN tl.tax_amount IS NOT NULL AND tl.tax_amount > 0 THEN tl.tax_amount
                  END,
                  CASE
                    WHEN co.base_fare_amount IS NOT NULL
                      AND co.total_price_bdt IS NOT NULL
                      AND co.total_price_bdt > co.base_fare_amount
                    THEN co.total_price_bdt - co.base_fare_amount
                  END,
                  co.tax_amount,
                  tl.tax_amount,
                  GREATEST(co.total_price_bdt - co.base_fare_amount, 0)
                )
              ) AS tax_amount,
              ARRAY_AGG(COALESCE(co.booking_class, co.fare_basis) IGNORE NULLS ORDER BY co.total_price_bdt ASC, co.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS booking_class,
              ARRAY_AGG(COALESCE(co.booking_class, co.fare_basis) IGNORE NULLS ORDER BY co.total_price_bdt ASC, co.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS min_booking_class,
              ARRAY_AGG(COALESCE(co.booking_class, co.fare_basis) IGNORE NULLS ORDER BY co.total_price_bdt DESC, co.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS max_booking_class,
              ARRAY_AGG(co.seat_available IGNORE NULLS ORDER BY co.total_price_bdt ASC, co.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS seat_available,
              ARRAY_AGG(co.seat_available IGNORE NULLS ORDER BY co.total_price_bdt ASC, co.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS min_seat_available,
              ARRAY_AGG(co.seat_available IGNORE NULLS ORDER BY co.total_price_bdt DESC, co.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS max_seat_available,
              MAX(co.seat_capacity) AS seat_capacity,
              MAX(co.load_factor_pct) AS load_factor_pct,
              LOGICAL_OR(IFNULL(co.soldout, FALSE)) AS soldout
            FROM current_offers co
            LEFT JOIN tax_lookup tl
              ON tl.cycle_id = co.cycle_id
             AND tl.captured_at_utc = co.captured_at_utc
             AND tl.airline = co.airline
             AND tl.origin = co.origin
             AND tl.destination = co.destination
             AND tl.flight_number = co.flight_number
             AND tl.departure_utc = co.departure_utc
             AND tl.cabin = co.cabin
            GROUP BY
              co.cycle_id, co.captured_at_utc, co.airline, co.origin, co.destination, co.route_key,
              co.flight_number, co.departure_utc, co.departure_date, departure_time, co.cabin, aircraft,
              co.search_trip_type, co.requested_outbound_date, co.requested_return_date,
              co.trip_duration_days, co.trip_origin, co.trip_destination, co.trip_pair_key, co.leg_direction,
              co.leg_sequence, co.itinerary_leg_count
            ORDER BY co.route_key, co.departure_date, departure_time, co.airline, co.flight_number
            """,
            current_params,
        )
    )
    if not current_rows:
        return {"cycle_id": resolved_cycle_id, "routes": []}

    dep_dates = [_iso_date(row.get("departure_date")) for row in current_rows if _iso_date(row.get("departure_date"))]
    min_date = min(dep_dates)
    max_date = max(dep_dates)

    history_params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ArrayQueryParameter("route_keys", "STRING", route_keys),
        bigquery.ScalarQueryParameter("min_date", "DATE", min_date),
        bigquery.ScalarQueryParameter("max_date", "DATE", max_date),
        bigquery.ScalarQueryParameter(
            "history_capture_limit",
            "INT64",
            _route_matrix_capture_display_limit(history_limit, compact_history),
        ),
    ]
    history_filters = [
        "route_key IN UNNEST(@route_keys)",
        "departure_date >= @min_date",
        "departure_date <= @max_date",
    ]
    if airlines:
        history_filters.append("airline IN UNNEST(@airlines)")
        history_params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
    if cabins:
        history_filters.append("cabin IN UNNEST(@cabins)")
        history_params.append(bigquery.ArrayQueryParameter("cabins", "STRING", _normalize_codes(cabins, uppercase=False)))
    if normalized_trip_types:
        history_filters.append("search_trip_type IN UNNEST(@trip_types)")
        history_params.append(bigquery.ArrayQueryParameter("trip_types", "STRING", normalized_trip_types))
    if return_date:
        history_filters.append("requested_return_date = @return_date")
        history_params.append(bigquery.ScalarQueryParameter("return_date", "DATE", return_date))
    else:
        if return_date_start:
            history_filters.append("requested_return_date >= @return_date_start")
            history_params.append(bigquery.ScalarQueryParameter("return_date_start", "DATE", return_date_start))
        if return_date_end:
            history_filters.append("requested_return_date <= @return_date_end")
            history_params.append(bigquery.ScalarQueryParameter("return_date_end", "DATE", return_date_end))
    if departure_date:
        history_filters.append("departure_date = @departure_date")
        history_params.append(bigquery.ScalarQueryParameter("departure_date", "DATE", departure_date))

    history_rows = _serialize_warehouse_rows(
        _run_bigquery_query(
            f"""
            WITH history_offers AS (
              SELECT *
              FROM {_bq_table("fact_offer_snapshot")}
              WHERE {' AND '.join(history_filters)}
            ),
            tax_lookup AS (
              SELECT
                cycle_id,
                captured_at_utc,
                airline,
                origin,
                destination,
                flight_number,
                departure_utc,
                cabin,
                MAX(tax_amount) AS tax_amount
              FROM {_bq_table("fact_tax_snapshot")}
              WHERE route_key IN UNNEST(@route_keys)
                AND DATE(departure_utc) >= @min_date
                AND DATE(departure_utc) <= @max_date
                {"AND airline IN UNNEST(@airlines)" if airlines else ""}
                {"AND cabin IN UNNEST(@cabins)" if cabins else ""}
              GROUP BY
                cycle_id, captured_at_utc, airline, origin, destination,
                flight_number, departure_utc, cabin
            ),
            aggregated_history AS (
              SELECT
                ho.cycle_id,
                ho.captured_at_utc,
                ho.airline,
                ho.origin,
                ho.destination,
                ho.route_key,
                ho.flight_number,
                ho.departure_utc,
                ho.departure_date,
                FORMAT_TIMESTAMP('%H:%M', ho.departure_utc) AS departure_time,
                ho.cabin,
                COALESCE(ho.aircraft, '') AS aircraft,
                ho.search_trip_type,
                ho.requested_outbound_date,
                ho.requested_return_date,
                ho.trip_duration_days,
                ho.trip_origin,
                ho.trip_destination,
                ho.trip_pair_key,
                ho.leg_direction,
                ho.leg_sequence,
                ho.itinerary_leg_count,
                MIN(ho.total_price_bdt) AS min_total_price_bdt,
                MAX(ho.total_price_bdt) AS max_total_price_bdt,
                MAX(
                  COALESCE(
                    CASE
                      WHEN ho.tax_amount IS NOT NULL AND ho.tax_amount > 0 THEN ho.tax_amount
                    END,
                    CASE
                      WHEN tl.tax_amount IS NOT NULL AND tl.tax_amount > 0 THEN tl.tax_amount
                    END,
                    CASE
                      WHEN ho.base_fare_amount IS NOT NULL
                        AND ho.total_price_bdt IS NOT NULL
                        AND ho.total_price_bdt > ho.base_fare_amount
                      THEN ho.total_price_bdt - ho.base_fare_amount
                    END,
                    ho.tax_amount,
                    tl.tax_amount,
                    GREATEST(ho.total_price_bdt - ho.base_fare_amount, 0)
                  )
                ) AS tax_amount,
                ARRAY_AGG(COALESCE(ho.booking_class, ho.fare_basis) IGNORE NULLS ORDER BY ho.total_price_bdt ASC, ho.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS booking_class,
                ARRAY_AGG(COALESCE(ho.booking_class, ho.fare_basis) IGNORE NULLS ORDER BY ho.total_price_bdt ASC, ho.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS min_booking_class,
                ARRAY_AGG(COALESCE(ho.booking_class, ho.fare_basis) IGNORE NULLS ORDER BY ho.total_price_bdt DESC, ho.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS max_booking_class,
                ARRAY_AGG(ho.seat_available IGNORE NULLS ORDER BY ho.total_price_bdt ASC, ho.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS seat_available,
                ARRAY_AGG(ho.seat_available IGNORE NULLS ORDER BY ho.total_price_bdt ASC, ho.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS min_seat_available,
                ARRAY_AGG(ho.seat_available IGNORE NULLS ORDER BY ho.total_price_bdt DESC, ho.seat_available DESC LIMIT 1)[SAFE_OFFSET(0)] AS max_seat_available,
                MAX(ho.seat_capacity) AS seat_capacity,
                MAX(ho.load_factor_pct) AS load_factor_pct,
                LOGICAL_OR(IFNULL(ho.soldout, FALSE)) AS soldout
              FROM history_offers ho
              LEFT JOIN tax_lookup tl
                ON tl.cycle_id = ho.cycle_id
               AND tl.captured_at_utc = ho.captured_at_utc
               AND tl.airline = ho.airline
               AND tl.origin = ho.origin
               AND tl.destination = ho.destination
               AND tl.flight_number = ho.flight_number
               AND tl.departure_utc = ho.departure_utc
               AND tl.cabin = ho.cabin
              GROUP BY
                ho.cycle_id, ho.captured_at_utc, ho.airline, ho.origin, ho.destination, ho.route_key,
                ho.flight_number, ho.departure_utc, ho.departure_date, departure_time, ho.cabin, aircraft,
                ho.search_trip_type, ho.requested_outbound_date, ho.requested_return_date,
                ho.trip_duration_days, ho.trip_origin, ho.trip_destination, ho.trip_pair_key, ho.leg_direction,
                ho.leg_sequence, ho.itinerary_leg_count
            ),
            ranked_history AS (
              SELECT
                aggregated_history.*,
                DENSE_RANK() OVER (
                  PARTITION BY route_key, departure_date
                  ORDER BY captured_at_utc DESC
                ) AS capture_rank
              FROM aggregated_history
            )
            SELECT
              cycle_id,
              captured_at_utc,
              airline,
              origin,
              destination,
              route_key,
              flight_number,
              departure_utc,
              departure_date,
              departure_time,
              cabin,
              aircraft,
              search_trip_type,
              requested_outbound_date,
              requested_return_date,
              trip_duration_days,
              trip_origin,
              trip_destination,
              trip_pair_key,
              leg_direction,
              leg_sequence,
              itinerary_leg_count,
              min_total_price_bdt,
              max_total_price_bdt,
              tax_amount,
              booking_class,
              min_booking_class,
              max_booking_class,
              seat_available,
              min_seat_available,
              max_seat_available,
              seat_capacity,
              load_factor_pct,
              soldout
            FROM ranked_history
            WHERE capture_rank <= @history_capture_limit
            ORDER BY captured_at_utc DESC, route_key, departure_date, departure_time, airline, flight_number
            """,
            history_params,
        )
    )

    return _build_route_monitor_matrix_from_aggregates(
        resolved_cycle_id=resolved_cycle_id,
        selected_routes=selected_routes,
        current_rows=current_rows,
        history_rows=history_rows,
        history_limit=history_limit,
        compact_history=compact_history,
    )


def _matrix_flight_group_id(row: dict[str, Any]) -> str:
    departure_time = str(row.get("departure_time") or "").strip()
    aircraft = str(row.get("aircraft") or "").strip().upper()
    cabin = str(row.get("cabin") or "").strip()
    search_trip_type = str(row.get("search_trip_type") or "OW").strip().upper()
    requested_return_date = str(row.get("requested_return_date") or "").strip()
    leg_direction = str(row.get("leg_direction") or "").strip().lower()
    return "|".join(
        [
            str(row.get("route_key") or ""),
            str(row.get("airline") or ""),
            str(row.get("flight_number") or ""),
            departure_time,
            cabin,
            aircraft,
            search_trip_type,
            requested_return_date,
            leg_direction,
        ]
    )


def _cell_signal(previous: dict[str, Any] | None, current: dict[str, Any]) -> str:
    if current.get("soldout"):
        return "sold_out"
    if not previous:
        return "new"

    current_price = current.get("min_total_price_bdt")
    previous_price = previous.get("min_total_price_bdt")
    if current_price is None or previous_price is None:
        return "unknown"

    try:
        if float(current_price) > float(previous_price):
            return "increase"
        if float(current_price) < float(previous_price):
            return "decrease"
    except Exception:
        return "unknown"
    return "unknown"


def _signal_sort_key(value: str) -> tuple[int, str]:
    order = {
        "increase": 0,
        "decrease": 1,
        "new": 2,
        "sold_out": 3,
        "unknown": 4,
    }
    return (order.get(value, 99), value)


def get_route_monitor_matrix(
    session: Session,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    cabins: Sequence[str] | None = None,
    trip_types: Sequence[str] | None = None,
    return_date: date | None = None,
    return_date_start: date | None = None,
    return_date_end: date | None = None,
    departure_date: date | None = None,
    route_limit: int = 8,
    history_limit: int = 12,
    compact_history: bool = True,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_trip_types = [value for value in _normalize_codes(trip_types) if value in {"OW", "RT"}]
    prefer_sql = session is not None
    if not prefer_sql and _bigquery_ready():
        try:
            resolved_cycle_id = _resolve_cycle_id_bigquery(cycle_id)
            cache_key = _cache_key(
                "route_monitor_matrix",
                backend="bigquery",
                cycle_id=resolved_cycle_id,
                airlines=_normalize_sequence_cache_value(airlines),
                origins=_normalize_sequence_cache_value(origins),
                destinations=_normalize_sequence_cache_value(destinations),
                cabins=_normalize_sequence_cache_value(cabins, uppercase=False),
                trip_types=tuple(normalized_trip_types),
                return_date=return_date,
                return_date_start=return_date_start,
                return_date_end=return_date_end,
                departure_date=departure_date,
                route_limit=route_limit,
                history_limit=history_limit,
                compact_history=compact_history,
            )
            cached = _get_cached_response(cache_key, ROUTE_MATRIX_CACHE_TTL_SEC)
            if cached is not None:
                _set_request_metrics(
                    route_matrix_backend="bigquery",
                    route_matrix_cache="hit",
                    route_matrix_routes=len(cached.get("routes") or []),
                    route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
                )
                LOG.info(
                    "route_monitor_matrix cache_hit backend=bigquery cycle_id=%s routes=%d total_ms=%.1f",
                    resolved_cycle_id,
                    len(cached.get("routes") or []),
                    (time.perf_counter() - started_at) * 1000,
                )
                return cached
            payload = _get_route_monitor_matrix_from_bigquery(
                cycle_id=resolved_cycle_id,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                cabins=cabins,
                trip_types=normalized_trip_types,
                return_date=return_date,
                return_date_start=return_date_start,
                return_date_end=return_date_end,
                departure_date=departure_date,
                route_limit=route_limit,
                history_limit=history_limit,
                compact_history=compact_history,
            )
            _set_request_metrics(
                route_matrix_backend="bigquery",
                route_matrix_cache="miss",
                route_matrix_routes=len(payload.get("routes") or []),
                route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
            )
            LOG.info(
                "route_monitor_matrix backend=bigquery cycle_id=%s routes=%d total_ms=%.1f",
                resolved_cycle_id,
                len(payload.get("routes") or []),
                (time.perf_counter() - started_at) * 1000,
            )
            return _set_cached_response(cache_key, payload)
        except (GoogleAPIError, RuntimeError, ValueError) as exc:
            LOG.warning("route_list bigquery_fallback reason=%s", exc)
    if session is None:
        _set_request_metrics(
            route_matrix_backend="sql",
            route_matrix_cache="miss",
            route_matrix_routes=0,
            route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        return {"cycle_id": None, "routes": []}

    resolved_cycle_id = _resolve_cycle_id(session, cycle_id)
    if not resolved_cycle_id:
        _set_request_metrics(
            route_matrix_backend="sql",
            route_matrix_cache="miss",
            route_matrix_routes=0,
            route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        return {"cycle_id": None, "routes": []}

    cache_key = _cache_key(
        "route_monitor_matrix",
        backend="sql",
        cycle_id=resolved_cycle_id,
        airlines=_normalize_sequence_cache_value(airlines),
        origins=_normalize_sequence_cache_value(origins),
        destinations=_normalize_sequence_cache_value(destinations),
        cabins=_normalize_sequence_cache_value(cabins, uppercase=False),
        trip_types=tuple(normalized_trip_types),
        return_date=return_date,
        return_date_start=return_date_start,
        return_date_end=return_date_end,
        departure_date=departure_date,
        route_limit=route_limit,
        history_limit=history_limit,
        compact_history=compact_history,
    )
    cached = _get_cached_response(cache_key, ROUTE_MATRIX_CACHE_TTL_SEC)
    if cached is not None:
        _set_request_metrics(
            route_matrix_backend="sql",
            route_matrix_cache="hit",
            route_matrix_routes=len(cached.get("routes") or []),
            route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        LOG.info(
            "route_monitor_matrix cache_hit backend=sql cycle_id=%s routes=%d total_ms=%.1f",
            resolved_cycle_id,
            len(cached.get("routes") or []),
            (time.perf_counter() - started_at) * 1000,
        )
        return cached

    route_started_at = time.perf_counter()
    route_clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)"]
    route_params: dict[str, Any] = {"cycle_id": resolved_cycle_id, "route_limit": route_limit}
    _apply_in_filter(route_clauses, route_params, "fo.airline", airlines, "route_airline")
    _apply_in_filter(route_clauses, route_params, "fo.cabin", cabins, "route_cabin", uppercase=False)
    if normalized_trip_types:
        _apply_in_filter(route_clauses, route_params, "COALESCE(frm.search_trip_type, 'OW')", normalized_trip_types, "route_trip_type")
    if return_date:
        route_clauses.append("frm.requested_return_date = :route_return_date")
        route_params["route_return_date"] = return_date.isoformat()
    else:
        if return_date_start:
            route_clauses.append("frm.requested_return_date >= :route_return_date_start")
            route_params["route_return_date_start"] = return_date_start.isoformat()
        if return_date_end:
            route_clauses.append("frm.requested_return_date <= :route_return_date_end")
            route_params["route_return_date_end"] = return_date_end.isoformat()
    if departure_date:
        route_clauses.append("fo.departure::date = :route_departure_date")
        route_params["route_departure_date"] = departure_date.isoformat()
    if normalized_trip_types:
        _apply_in_filter(route_clauses, route_params, "COALESCE(frm.trip_origin, fo.origin)", origins, "route_trip_origin")
        _apply_in_filter(route_clauses, route_params, "COALESCE(frm.trip_destination, fo.destination)", destinations, "route_trip_destination")
    else:
        _apply_in_filter(route_clauses, route_params, "fo.origin", origins, "route_origin")
        _apply_in_filter(route_clauses, route_params, "fo.destination", destinations, "route_destination")

    route_rows = session.execute(
        text(
            f"""
            SELECT
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                COUNT(*) AS row_count
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta frm
              ON frm.flight_offer_id = fo.id
            WHERE {' AND '.join(route_clauses)}
            GROUP BY fo.origin, fo.destination
            ORDER BY row_count DESC, route_key
            LIMIT :route_limit
            """
        ),
        route_params,
    ).mappings().all()
    selected_routes = [dict(row) for row in route_rows]
    if not selected_routes:
        _set_request_metrics(
            route_matrix_backend="sql",
            route_matrix_cache="miss",
            route_matrix_routes=0,
            route_matrix_selected_routes=0,
            route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        return _set_cached_response(cache_key, {"cycle_id": resolved_cycle_id, "routes": []})
    route_selection_ms = (time.perf_counter() - route_started_at) * 1000

    route_pairs = [(str(row.get("origin") or ""), str(row.get("destination") or "")) for row in selected_routes]
    min_date: date | None = None
    max_date: date | None = None

    current_started_at = time.perf_counter()
    current_clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)"]
    current_params: dict[str, Any] = {"cycle_id": resolved_cycle_id}
    _apply_route_pair_filter(current_clauses, current_params, "fo.origin", "fo.destination", route_pairs, "matrix_route")
    _apply_in_filter(current_clauses, current_params, "fo.airline", airlines, "matrix_airline")
    _apply_in_filter(current_clauses, current_params, "fo.cabin", cabins, "matrix_cabin", uppercase=False)
    if normalized_trip_types:
        _apply_in_filter(current_clauses, current_params, "COALESCE(frm.search_trip_type, 'OW')", normalized_trip_types, "matrix_trip_type")
    if return_date:
        current_clauses.append("frm.requested_return_date = :matrix_return_date")
        current_params["matrix_return_date"] = return_date.isoformat()
    else:
        if return_date_start:
            current_clauses.append("frm.requested_return_date >= :matrix_return_date_start")
            current_params["matrix_return_date_start"] = return_date_start.isoformat()
        if return_date_end:
            current_clauses.append("frm.requested_return_date <= :matrix_return_date_end")
            current_params["matrix_return_date_end"] = return_date_end.isoformat()
    if departure_date:
        current_clauses.append("fo.departure::date = :matrix_departure_date")
        current_params["matrix_departure_date"] = departure_date.isoformat()

    current_rows = session.execute(
        text(
            f"""
            SELECT DISTINCT
                fo.airline,
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                fo.flight_number,
                fo.departure::date AS departure_date,
                TO_CHAR(fo.departure, 'HH24:MI') AS departure_time,
                fo.cabin,
                COALESCE(frm.aircraft, '') AS aircraft,
                COALESCE(frm.search_trip_type, 'OW') AS search_trip_type,
                frm.requested_outbound_date,
                frm.requested_return_date,
                frm.trip_duration_days,
                COALESCE(frm.trip_origin, fo.origin) AS trip_origin,
                COALESCE(frm.trip_destination, fo.destination) AS trip_destination,
                COALESCE(frm.trip_origin, fo.origin) || '-' || COALESCE(frm.trip_destination, fo.destination) AS trip_pair_key,
                COALESCE(frm.leg_direction, 'outbound') AS leg_direction,
                frm.leg_sequence,
                frm.itinerary_leg_count
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta frm
              ON frm.flight_offer_id = fo.id
            WHERE {' AND '.join(current_clauses)}
            ORDER BY route_key, departure_date, departure_time, fo.airline, fo.flight_number
            """
        ),
        current_params,
    ).mappings().all()
    current_dicts = _rows_to_dicts([dict(row) for row in current_rows])
    if not current_dicts:
        _set_request_metrics(
            route_matrix_backend="sql",
            route_matrix_cache="miss",
            route_matrix_routes=0,
            route_matrix_selected_routes=len(selected_routes),
            route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        return _set_cached_response(cache_key, {"cycle_id": resolved_cycle_id, "routes": []})
    current_rows_ms = (time.perf_counter() - current_started_at) * 1000

    for row in current_dicts:
        dep_date = row.get("departure_date")
        if isinstance(dep_date, date):
            min_date = dep_date if min_date is None or dep_date < min_date else min_date
            max_date = dep_date if max_date is None or dep_date > max_date else max_date

    if min_date is None or max_date is None:
        _set_request_metrics(
            route_matrix_backend="sql",
            route_matrix_cache="miss",
            route_matrix_routes=0,
            route_matrix_selected_routes=len(selected_routes),
            route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
        )
        return _set_cached_response(cache_key, {"cycle_id": resolved_cycle_id, "routes": []})

    history_cycle_ids = [
        str(item.get("cycle_id") or "").strip()
        for item in get_recent_cycles(
            session,
            limit=_route_matrix_cycle_fetch_limit(history_limit, compact_history),
            comparable_only=True,
        )
        if str(item.get("cycle_id") or "").strip() and str(item.get("cycle_id") or "").strip() != resolved_cycle_id
    ]
    history_dicts: list[dict[str, Any]] = []
    history_rows_ms = 0.0
    if history_cycle_ids:
        history_started_at = time.perf_counter()
        history_clauses = [
            "fo.departure >= :min_departure_ts",
            "fo.departure < :max_departure_exclusive_ts",
        ]
        history_params: dict[str, Any] = {
            "min_departure_ts": datetime.combine(min_date, datetime.min.time()),
            "max_departure_exclusive_ts": datetime.combine(max_date, datetime.min.time()) + timedelta(days=1),
            "history_capture_limit": _route_matrix_capture_display_limit(history_limit, compact_history),
        }
        _apply_route_pair_filter(history_clauses, history_params, "fo.origin", "fo.destination", route_pairs, "history_route")
        _apply_in_filter(history_clauses, history_params, "fo.scrape_id::text", history_cycle_ids, "history_cycle", uppercase=False)
        _apply_in_filter(history_clauses, history_params, "fo.airline", airlines, "history_airline")
        _apply_in_filter(history_clauses, history_params, "fo.cabin", cabins, "history_cabin", uppercase=False)
        if normalized_trip_types:
            _apply_in_filter(history_clauses, history_params, "COALESCE(frm.search_trip_type, 'OW')", normalized_trip_types, "history_trip_type")
        if return_date:
            history_clauses.append("frm.requested_return_date = :history_return_date")
            history_params["history_return_date"] = return_date.isoformat()
        else:
            if return_date_start:
                history_clauses.append("frm.requested_return_date >= :history_return_date_start")
                history_params["history_return_date_start"] = return_date_start.isoformat()
            if return_date_end:
                history_clauses.append("frm.requested_return_date <= :history_return_date_end")
                history_params["history_return_date_end"] = return_date_end.isoformat()
        if departure_date:
            history_clauses.append("fo.departure::date = :history_departure_date")
            history_params["history_departure_date"] = departure_date.isoformat()

        history_rows = session.execute(
            text(
                f"""
                WITH aggregated_history AS (
                    SELECT
                        fo.scrape_id::text AS cycle_id,
                        fo.scraped_at AS captured_at_utc,
                        fo.airline,
                        fo.origin,
                        fo.destination,
                        (fo.origin || '-' || fo.destination) AS route_key,
                        fo.flight_number,
                        fo.departure AS departure_utc,
                        fo.departure::date AS departure_date,
                        TO_CHAR(fo.departure, 'HH24:MI') AS departure_time,
                        fo.cabin,
                        COALESCE(frm.aircraft, '') AS aircraft,
                        COALESCE(frm.search_trip_type, 'OW') AS search_trip_type,
                        frm.requested_outbound_date,
                        frm.requested_return_date,
                        frm.trip_duration_days,
                        COALESCE(frm.trip_origin, fo.origin) AS trip_origin,
                        COALESCE(frm.trip_destination, fo.destination) AS trip_destination,
                        COALESCE(frm.trip_origin, fo.origin) || '-' || COALESCE(frm.trip_destination, fo.destination) AS trip_pair_key,
                        COALESCE(frm.leg_direction, 'outbound') AS leg_direction,
                        frm.leg_sequence,
                        frm.itinerary_leg_count,
                        CAST(MIN(fo.price_total_bdt) AS NUMERIC(12, 2)) AS min_total_price_bdt,
                        CAST(MAX(fo.price_total_bdt) AS NUMERIC(12, 2)) AS max_total_price_bdt,
                        CAST(MAX(COALESCE(frm.tax_amount, GREATEST(fo.price_total_bdt - frm.fare_amount, 0))) AS NUMERIC(12, 2)) AS tax_amount,
                        (ARRAY_AGG(COALESCE(frm.booking_class, fo.fare_basis) ORDER BY fo.price_total_bdt ASC NULLS LAST, fo.seat_available DESC NULLS LAST)
                          FILTER (WHERE COALESCE(frm.booking_class, fo.fare_basis) IS NOT NULL))[1] AS booking_class,
                        (ARRAY_AGG(COALESCE(frm.booking_class, fo.fare_basis) ORDER BY fo.price_total_bdt ASC NULLS LAST, fo.seat_available DESC NULLS LAST)
                          FILTER (WHERE COALESCE(frm.booking_class, fo.fare_basis) IS NOT NULL))[1] AS min_booking_class,
                        (ARRAY_AGG(COALESCE(frm.booking_class, fo.fare_basis) ORDER BY fo.price_total_bdt DESC NULLS LAST, fo.seat_available DESC NULLS LAST)
                          FILTER (WHERE COALESCE(frm.booking_class, fo.fare_basis) IS NOT NULL))[1] AS max_booking_class,
                        (ARRAY_AGG(fo.seat_available ORDER BY fo.price_total_bdt ASC NULLS LAST, fo.seat_available DESC NULLS LAST)
                          FILTER (WHERE fo.seat_available IS NOT NULL))[1] AS seat_available,
                        (ARRAY_AGG(fo.seat_available ORDER BY fo.price_total_bdt ASC NULLS LAST, fo.seat_available DESC NULLS LAST)
                          FILTER (WHERE fo.seat_available IS NOT NULL))[1] AS min_seat_available,
                        (ARRAY_AGG(fo.seat_available ORDER BY fo.price_total_bdt DESC NULLS LAST, fo.seat_available DESC NULLS LAST)
                          FILTER (WHERE fo.seat_available IS NOT NULL))[1] AS max_seat_available,
                        MAX(fo.seat_capacity) AS seat_capacity,
                        CAST(MAX(frm.estimated_load_factor_pct) AS NUMERIC(6, 2)) AS load_factor_pct,
                        BOOL_OR(COALESCE(frm.soldout, FALSE)) AS soldout
                    FROM flight_offers fo
                    LEFT JOIN flight_offer_raw_meta frm
                      ON frm.flight_offer_id = fo.id
                    WHERE {' AND '.join(history_clauses)}
                    GROUP BY
                        fo.scrape_id,
                        fo.scraped_at,
                        fo.airline,
                        fo.origin,
                        fo.destination,
                        fo.flight_number,
                        fo.departure,
                        fo.cabin,
                        COALESCE(frm.aircraft, ''),
                        COALESCE(frm.search_trip_type, 'OW'),
                        frm.requested_outbound_date,
                        frm.requested_return_date,
                        frm.trip_duration_days,
                        COALESCE(frm.trip_origin, fo.origin),
                        COALESCE(frm.trip_destination, fo.destination),
                        COALESCE(frm.leg_direction, 'outbound'),
                        frm.leg_sequence,
                        frm.itinerary_leg_count
                ),
                ranked_history AS (
                    SELECT
                        aggregated_history.*,
                        DENSE_RANK() OVER (
                            PARTITION BY route_key, departure_date
                            ORDER BY captured_at_utc DESC
                        ) AS capture_rank
                    FROM aggregated_history
                )
                SELECT
                    cycle_id,
                    captured_at_utc,
                    airline,
                    origin,
                    destination,
                    route_key,
                    flight_number,
                    departure_utc,
                    departure_date,
                    departure_time,
                    cabin,
                    aircraft,
                    search_trip_type,
                    requested_outbound_date,
                    requested_return_date,
                    trip_duration_days,
                    trip_origin,
                    trip_destination,
                    trip_pair_key,
                    leg_direction,
                    leg_sequence,
                    itinerary_leg_count,
                    min_total_price_bdt,
                    max_total_price_bdt,
                    tax_amount,
                    booking_class,
                    min_booking_class,
                    max_booking_class,
                    seat_available,
                    min_seat_available,
                    max_seat_available,
                    seat_capacity,
                    load_factor_pct,
                    soldout
                FROM ranked_history
                WHERE capture_rank <= :history_capture_limit
                ORDER BY captured_at_utc DESC, route_key, departure_date, departure_time, airline, flight_number
                """
            ),
            history_params,
        ).mappings().all()
        history_dicts = _rows_to_dicts([dict(row) for row in history_rows])
        history_rows_ms = (time.perf_counter() - history_started_at) * 1000

    payload = _build_route_monitor_matrix_from_aggregates(
        resolved_cycle_id=resolved_cycle_id,
        selected_routes=selected_routes,
        current_rows=current_dicts,
        history_rows=history_dicts,
        history_limit=history_limit,
        compact_history=compact_history,
    )
    _set_request_metrics(
        route_matrix_backend="sql",
        route_matrix_cache="miss",
        route_matrix_routes=len(payload.get("routes") or []),
        route_matrix_selected_routes=len(selected_routes),
        route_matrix_current_rows=len(current_dicts),
        route_matrix_history_rows=len(history_dicts),
        route_matrix_history_cycles=len(history_cycle_ids),
        route_matrix_ms=f"{(time.perf_counter() - started_at) * 1000:.1f}",
    )
    LOG.info(
        "route_monitor_matrix backend=sql cycle_id=%s selected_routes=%d current_rows=%d history_rows=%d history_cycles=%d route_ms=%.1f current_ms=%.1f history_ms=%.1f total_ms=%.1f",
        resolved_cycle_id,
        len(selected_routes),
        len(current_dicts),
        len(history_dicts),
        len(history_cycle_ids),
        route_selection_ms,
        current_rows_ms,
        history_rows_ms,
        (time.perf_counter() - started_at) * 1000,
    )
    return _set_cached_response(cache_key, payload)


def _build_airline_operations_payload(
    *,
    resolved_cycle_id: str,
    selected_routes: Sequence[dict[str, Any]],
    current_rows: Sequence[dict[str, Any]],
    trend_route_rows: Sequence[dict[str, Any]],
    trend_airline_rows: Sequence[dict[str, Any]],
    recent_cycles: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    if not selected_routes:
        return {"cycle_id": resolved_cycle_id, "routes": []}

    current_by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in current_rows:
        current_by_route[str(row.get("route_key") or "")].append(dict(row))

    trend_route_by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trend_route_rows:
        trend_route_by_key[str(row.get("route_key") or "")].append(dict(row))

    trend_airline_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in trend_airline_rows:
        trend_airline_by_key[(str(row.get("route_key") or ""), str(row.get("airline") or ""))].append(dict(row))

    cycle_lookup = {
        str(item.get("cycle_id") or ""): item
        for item in recent_cycles
        if item.get("cycle_id")
    }

    route_payloads: list[dict[str, Any]] = []
    for route in selected_routes:
        route_key = str(route.get("route_key") or "")
        route_rows = sorted(
            current_by_route.get(route_key, []),
            key=lambda row: (
                _iso_date(row.get("departure_date")),
                _time_sort_key(row.get("departure_time")),
                str(row.get("airline") or ""),
                str(row.get("flight_number") or ""),
            ),
        )

        weekday_map: dict[str, dict[str, Any]] = {}
        departure_days_map: dict[str, dict[str, Any]] = {}
        airline_map: dict[str, dict[str, Any]] = {}
        route_stop_counts: dict[str, int] = defaultdict(int)
        route_via_airports: set[str] = set()
        for row in route_rows:
            dep_date = _iso_date(row.get("departure_date"))
            dep_time = str(row.get("departure_time") or "").strip()
            airline = str(row.get("airline") or "").strip()
            flight_number = str(row.get("flight_number") or "").strip()
            day_label = _departure_day_label(dep_date)
            stop_label = _stops_label(row.get("stops"))
            route_stop_counts[stop_label] += 1
            for via_airport in _split_via_airports(row.get("via_airports")):
                route_via_airports.add(via_airport)

            departure_day = departure_days_map.setdefault(
                dep_date,
                {
                    "departure_date": dep_date,
                    "day_label": day_label,
                    "flight_instance_count": 0,
                    "airlines": set(),
                    "departure_times": set(),
                },
            )
            departure_day["flight_instance_count"] += 1
            if airline:
                departure_day["airlines"].add(airline)
            if dep_time:
                departure_day["departure_times"].add(dep_time)

            weekday = weekday_map.setdefault(
                day_label,
                {
                    "day_label": day_label,
                    "flight_instance_count": 0,
                    "active_dates": set(),
                    "airlines": set(),
                },
            )
            weekday["flight_instance_count"] += 1
            if dep_date:
                weekday["active_dates"].add(dep_date)
            if airline:
                weekday["airlines"].add(airline)

            airline_entry = airline_map.setdefault(
                airline,
                {
                    "airline": airline,
                    "flight_numbers": set(),
                    "departure_times": set(),
                    "active_dates": set(),
                    "stop_counts": defaultdict(int),
                    "via_airports": set(),
                    "weekday_counts": defaultdict(int),
                    "weekday_dates": defaultdict(set),
                },
            )
            if flight_number:
                airline_entry["flight_numbers"].add(flight_number)
            if dep_time:
                airline_entry["departure_times"].add(dep_time)
            if dep_date:
                airline_entry["active_dates"].add(dep_date)
            airline_entry["stop_counts"][stop_label] += 1
            for via_airport in _split_via_airports(row.get("via_airports")):
                airline_entry["via_airports"].add(via_airport)
            airline_entry["weekday_counts"][day_label] += 1
            if dep_date:
                airline_entry["weekday_dates"][day_label].add(dep_date)

        airlines_payload: list[dict[str, Any]] = []
        for airline, entry in sorted(
            airline_map.items(),
            key=lambda item: (_time_sort_key(min(item[1]["departure_times"]) if item[1]["departure_times"] else None), item[0]),
        ):
            departure_times = sorted(entry["departure_times"], key=_time_sort_key)
            service_patterns = [
                label
                for label, _count in sorted(
                    entry["stop_counts"].items(),
                    key=lambda item: (0 if item[0] == "Direct" else 1, item[0]),
                )
            ]
            weekday_profile = [
                {
                    "day_label": day_label,
                    "flight_instance_count": int(entry["weekday_counts"].get(day_label, 0)),
                    "active_date_count": len(entry["weekday_dates"].get(day_label, set())),
                }
                for day_label in WEEKDAY_ORDER
                if entry["weekday_counts"].get(day_label, 0) > 0
            ]
            timeline = sorted(
                [
                    {
                        "cycle_id": str(item.get("cycle_id") or ""),
                        "cycle_completed_at_utc": cycle_lookup.get(str(item.get("cycle_id") or ""), {}).get("cycle_completed_at_utc"),
                        "flight_instance_count": int(item.get("flight_instance_count") or 0),
                        "active_date_count": int(item.get("active_date_count") or 0),
                        "first_departure_time": item.get("first_departure_time"),
                        "last_departure_time": item.get("last_departure_time"),
                    }
                    for item in trend_airline_by_key.get((route_key, airline), [])
                ],
                key=lambda item: str(item.get("cycle_completed_at_utc") or item.get("cycle_id") or ""),
            )
            airlines_payload.append(
                {
                    "airline": airline,
                    "flight_instance_count": len(
                        [
                            row
                            for row in route_rows
                            if str(row.get("airline") or "").strip() == airline
                        ]
                    ),
                    "active_date_count": len(entry["active_dates"]),
                    "first_departure_time": departure_times[0] if departure_times else None,
                    "last_departure_time": departure_times[-1] if departure_times else None,
                    "departure_times": departure_times,
                    "flight_numbers": sorted(entry["flight_numbers"]),
                    "service_patterns": service_patterns,
                    "via_airports": sorted(entry["via_airports"]),
                    "weekday_profile": weekday_profile,
                    "timeline": timeline,
                }
            )

        weekday_profile = [
            {
                "day_label": item["day_label"],
                "flight_instance_count": int(item["flight_instance_count"]),
                "active_date_count": len(item["active_dates"]),
                "airline_count": len(item["airlines"]),
                "airlines": sorted(item["airlines"]),
            }
            for item in sorted(weekday_map.values(), key=lambda item: _weekday_sort_key(str(item["day_label"])))
        ]
        departure_days = [
            {
                "departure_date": item["departure_date"],
                "day_label": item["day_label"],
                "flight_instance_count": int(item["flight_instance_count"]),
                "airline_count": len(item["airlines"]),
                "first_departure_time": min(item["departure_times"], key=_time_sort_key) if item["departure_times"] else None,
                "last_departure_time": max(item["departure_times"], key=_time_sort_key) if item["departure_times"] else None,
            }
            for item in sorted(departure_days_map.values(), key=lambda item: item["departure_date"])
        ]
        route_timeline = sorted(
            [
                {
                    "cycle_id": str(item.get("cycle_id") or ""),
                    "cycle_completed_at_utc": cycle_lookup.get(str(item.get("cycle_id") or ""), {}).get("cycle_completed_at_utc"),
                    "flight_instance_count": int(item.get("flight_instance_count") or 0),
                    "active_date_count": int(item.get("active_date_count") or 0),
                    "airline_count": int(item.get("airline_count") or 0),
                    "first_departure_time": item.get("first_departure_time"),
                    "last_departure_time": item.get("last_departure_time"),
                }
                for item in trend_route_by_key.get(route_key, [])
            ],
            key=lambda item: str(item.get("cycle_completed_at_utc") or item.get("cycle_id") or ""),
        )

        route_times = sorted(
            {
                str(row.get("departure_time") or "").strip()
                for row in route_rows
                if str(row.get("departure_time") or "").strip()
            },
            key=_time_sort_key,
        )
        route_service_patterns = [
            label
            for label, _count in sorted(
                route_stop_counts.items(),
                key=lambda item: (0 if item[0] == "Direct" else 1, item[0]),
            )
        ]
        route_payloads.append(
            {
                **dict(route),
                "airline_count": len(airline_map),
                "flight_instance_count": len(route_rows),
                "active_date_count": len(departure_days_map),
                "first_departure_time": route_times[0] if route_times else None,
                "last_departure_time": route_times[-1] if route_times else None,
                "departure_times": route_times,
                "service_patterns": route_service_patterns,
                "via_airports": sorted(route_via_airports),
                "departure_days": departure_days,
                "weekday_profile": weekday_profile,
                "airlines": airlines_payload,
                "timeline": route_timeline,
            }
        )

    return {"cycle_id": resolved_cycle_id, "routes": route_payloads}


def _get_airline_operations_from_bigquery(
    *,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    via_airports: Sequence[str] | None = None,
    route_types: Sequence[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    route_limit: int = 4,
    trend_limit: int = 8,
) -> dict[str, Any]:
    resolved_cycle_id = _resolve_cycle_id_bigquery(cycle_id)
    if not resolved_cycle_id:
        return {"cycle_id": None, "routes": []}

    route_filters = ["cycle_id = @cycle_id"]
    route_params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id),
    ]
    if airlines:
        route_filters.append("airline IN UNNEST(@airlines)")
        route_params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
    if origins:
        route_filters.append("origin IN UNNEST(@origins)")
        route_params.append(bigquery.ArrayQueryParameter("origins", "STRING", _normalize_codes(origins)))
    if destinations:
        route_filters.append("destination IN UNNEST(@destinations)")
        route_params.append(bigquery.ArrayQueryParameter("destinations", "STRING", _normalize_codes(destinations)))
    if via_airports:
        via_codes = _normalize_codes(via_airports)
        route_filters.append(
            "(" + " OR ".join(
                [
                    f"REGEXP_CONTAINS(CONCAT('|', IFNULL(via_airports, ''), '|'), r'\\|{code}\\|')"
                    for code in via_codes
                ]
            ) + ")"
        )
    if start_date:
        route_filters.append("departure_date >= @start_date")
        route_params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date:
        route_filters.append("departure_date <= @end_date")
        route_params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))

    route_rows = _serialize_warehouse_rows(
        _run_bigquery_query(
            f"""
            SELECT
              origin,
              destination,
              route_key,
              COUNT(*) AS row_count
            FROM {_bq_table("fact_offer_snapshot")}
            WHERE {' AND '.join(route_filters)}
            GROUP BY origin, destination, route_key
            ORDER BY row_count DESC, route_key
            LIMIT @route_scan_limit
            """,
            route_params + [bigquery.ScalarQueryParameter("route_scan_limit", "INT64", max(route_limit * 4, route_limit))],
        )
    )
    selected_routes = _filter_route_type_records(_annotate_route_records(route_rows), route_types)[:route_limit]
    if not selected_routes:
        return {"cycle_id": resolved_cycle_id, "routes": []}

    route_keys = [str(row["route_key"]) for row in selected_routes]
    current_filters = ["cycle_id = @cycle_id", "route_key IN UNNEST(@route_keys)"]
    current_params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id),
        bigquery.ArrayQueryParameter("route_keys", "STRING", route_keys),
    ]
    if airlines:
        current_filters.append("airline IN UNNEST(@airlines)")
        current_params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
    if via_airports:
        via_codes = _normalize_codes(via_airports)
        current_filters.append(
            "(" + " OR ".join(
                [
                    f"REGEXP_CONTAINS(CONCAT('|', IFNULL(via_airports, ''), '|'), r'\\|{code}\\|')"
                    for code in via_codes
                ]
            ) + ")"
        )
    if start_date:
        current_filters.append("departure_date >= @start_date")
        current_params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date:
        current_filters.append("departure_date <= @end_date")
        current_params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))

    current_rows = _serialize_warehouse_rows(
        _run_bigquery_query(
            f"""
            WITH ranked_ops AS (
              SELECT
                cycle_id,
                captured_at_utc,
                airline,
                origin,
                destination,
                route_key,
                flight_number,
                departure_date,
                FORMAT_TIMESTAMP('%H:%M', departure_utc) AS departure_time,
                via_airports,
                stops,
                ROW_NUMBER() OVER (
                  PARTITION BY route_key, airline, flight_number, departure_date, FORMAT_TIMESTAMP('%H:%M', departure_utc), IFNULL(via_airports, '')
                  ORDER BY captured_at_utc DESC
                ) AS row_num
              FROM {_bq_table("fact_offer_snapshot")}
              WHERE {' AND '.join(current_filters)}
            )
            SELECT
              cycle_id,
              captured_at_utc,
              airline,
              origin,
              destination,
              route_key,
              flight_number,
              departure_date,
              departure_time,
              via_airports,
              stops
            FROM ranked_ops
            WHERE row_num = 1
            ORDER BY route_key, departure_date, departure_time, airline, flight_number
            """,
            current_params,
        )
    )

    recent_cycles = get_recent_cycles(None, limit=trend_limit)
    trend_cycle_ids = [str(item.get("cycle_id") or "") for item in recent_cycles if item.get("cycle_id")]
    if not trend_cycle_ids:
        return _build_airline_operations_payload(
            resolved_cycle_id=resolved_cycle_id,
            selected_routes=selected_routes,
            current_rows=current_rows,
            trend_route_rows=[],
            trend_airline_rows=[],
            recent_cycles=[],
        )

    trend_base_filters = ["cycle_id IN UNNEST(@cycle_ids)", "route_key IN UNNEST(@route_keys)"]
    trend_base_params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ArrayQueryParameter("cycle_ids", "STRING", trend_cycle_ids),
        bigquery.ArrayQueryParameter("route_keys", "STRING", route_keys),
    ]
    if airlines:
        trend_base_filters.append("airline IN UNNEST(@airlines)")
        trend_base_params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
    if via_airports:
        via_codes = _normalize_codes(via_airports)
        trend_base_filters.append(
            "(" + " OR ".join(
                [
                    f"REGEXP_CONTAINS(CONCAT('|', IFNULL(via_airports, ''), '|'), r'\\|{code}\\|')"
                    for code in via_codes
                ]
            ) + ")"
        )
    if start_date:
        trend_base_filters.append("departure_date >= @start_date")
        trend_base_params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date:
        trend_base_filters.append("departure_date <= @end_date")
        trend_base_params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))

    trend_dedup = f"""
      WITH dedup_ops AS (
        SELECT DISTINCT
          cycle_id,
          airline,
          origin,
          destination,
          route_key,
          flight_number,
          departure_date,
          FORMAT_TIMESTAMP('%H:%M', departure_utc) AS departure_time
        FROM {_bq_table("fact_offer_snapshot")}
        WHERE {' AND '.join(trend_base_filters)}
      )
    """
    trend_route_rows = _serialize_warehouse_rows(
        _run_bigquery_query(
            trend_dedup
            + """
            SELECT
              cycle_id,
              origin,
              destination,
              route_key,
              COUNT(*) AS flight_instance_count,
              COUNT(DISTINCT departure_date) AS active_date_count,
              COUNT(DISTINCT airline) AS airline_count,
              MIN(departure_time) AS first_departure_time,
              MAX(departure_time) AS last_departure_time
            FROM dedup_ops
            GROUP BY cycle_id, origin, destination, route_key
            ORDER BY route_key, cycle_id
            """,
            trend_base_params,
        )
    )
    trend_airline_rows = _serialize_warehouse_rows(
        _run_bigquery_query(
            trend_dedup
            + """
            SELECT
              cycle_id,
              airline,
              origin,
              destination,
              route_key,
              COUNT(*) AS flight_instance_count,
              COUNT(DISTINCT departure_date) AS active_date_count,
              MIN(departure_time) AS first_departure_time,
              MAX(departure_time) AS last_departure_time
            FROM dedup_ops
            GROUP BY cycle_id, airline, origin, destination, route_key
            ORDER BY route_key, cycle_id, airline
            """,
            trend_base_params,
        )
    )

    return _build_airline_operations_payload(
        resolved_cycle_id=resolved_cycle_id,
        selected_routes=selected_routes,
        current_rows=current_rows,
        trend_route_rows=trend_route_rows,
        trend_airline_rows=trend_airline_rows,
        recent_cycles=recent_cycles,
    )


def get_airline_operations(
    session: Session | None,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    via_airports: Sequence[str] | None = None,
    route_types: Sequence[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    route_limit: int = 4,
    trend_limit: int = 8,
) -> dict[str, Any]:
    if _bigquery_ready():
        try:
            return _get_airline_operations_from_bigquery(
                cycle_id=cycle_id,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                via_airports=via_airports,
                route_types=route_types,
                start_date=start_date,
                end_date=end_date,
                route_limit=route_limit,
                trend_limit=trend_limit,
            )
        except (GoogleAPIError, RuntimeError, ValueError) as exc:
            LOG.warning("route_monitor_matrix bigquery_fallback reason=%s", exc)

    resolved_cycle_id = _resolve_cycle_id(session, cycle_id) if session is not None else None
    if not resolved_cycle_id or session is None:
        return {"cycle_id": None, "routes": []}

    route_clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)"]
    route_params: dict[str, Any] = {"cycle_id": resolved_cycle_id, "route_scan_limit": max(route_limit * 4, route_limit)}
    _apply_in_filter(route_clauses, route_params, "fo.airline", airlines, "ops_route_airline")
    _apply_in_filter(route_clauses, route_params, "fo.origin", origins, "ops_route_origin")
    _apply_in_filter(route_clauses, route_params, "fo.destination", destinations, "ops_route_destination")
    if via_airports:
        via_codes = _normalize_codes(via_airports)
        route_clauses.append(
            "(" + " OR ".join(
                [f"('|' || COALESCE(frm.via_airports, '') || '|') LIKE :ops_route_via_{idx}" for idx, _code in enumerate(via_codes)]
            ) + ")"
        )
        for idx, code in enumerate(via_codes):
            route_params[f"ops_route_via_{idx}"] = f"%|{code}|%"
    if start_date:
        route_clauses.append("fo.departure::date >= :start_date")
        route_params["start_date"] = start_date
    if end_date:
        route_clauses.append("fo.departure::date <= :end_date")
        route_params["end_date"] = end_date

    route_rows = session.execute(
        text(
            f"""
            SELECT
              fo.origin,
              fo.destination,
              (fo.origin || '-' || fo.destination) AS route_key,
              COUNT(*) AS row_count
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta frm
              ON frm.flight_offer_id = fo.id
            WHERE {' AND '.join(route_clauses)}
            GROUP BY fo.origin, fo.destination
            ORDER BY row_count DESC, route_key
            LIMIT :route_scan_limit
            """
        ),
        route_params,
    ).mappings().all()
    selected_routes = _filter_route_type_records(_annotate_route_records(_rows_to_dicts([dict(row) for row in route_rows])), route_types)[:route_limit]
    if not selected_routes:
        return {"cycle_id": resolved_cycle_id, "routes": []}

    route_keys = [str(row["route_key"]) for row in selected_routes]
    current_clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)"]
    current_params: dict[str, Any] = {"cycle_id": resolved_cycle_id}
    _apply_in_filter(current_clauses, current_params, "(fo.origin || '-' || fo.destination)", route_keys, "ops_current_route")
    _apply_in_filter(current_clauses, current_params, "fo.airline", airlines, "ops_current_airline")
    if via_airports:
        via_codes = _normalize_codes(via_airports)
        current_clauses.append(
            "(" + " OR ".join(
                [f"('|' || COALESCE(frm.via_airports, '') || '|') LIKE :ops_current_via_{idx}" for idx, _code in enumerate(via_codes)]
            ) + ")"
        )
        for idx, code in enumerate(via_codes):
            current_params[f"ops_current_via_{idx}"] = f"%|{code}|%"
    if start_date:
        current_clauses.append("fo.departure::date >= :start_date")
        current_params["start_date"] = start_date
    if end_date:
        current_clauses.append("fo.departure::date <= :end_date")
        current_params["end_date"] = end_date

    current_rows = session.execute(
        text(
            f"""
            SELECT
              current_ops.cycle_id,
              current_ops.captured_at_utc,
              current_ops.airline,
              current_ops.origin,
              current_ops.destination,
              current_ops.route_key,
              current_ops.flight_number,
              current_ops.departure_date,
              current_ops.departure_time,
              current_ops.via_airports,
              current_ops.stops
            FROM (
              SELECT DISTINCT ON (
                (fo.origin || '-' || fo.destination),
                fo.airline,
                fo.flight_number,
                fo.departure::date,
                TO_CHAR(fo.departure, 'HH24:MI'),
                COALESCE(frm.via_airports, '')
              )
                fo.scrape_id::text AS cycle_id,
                fo.scraped_at AS captured_at_utc,
                fo.airline,
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                fo.flight_number,
                fo.departure::date AS departure_date,
                TO_CHAR(fo.departure, 'HH24:MI') AS departure_time,
                frm.via_airports,
                COALESCE(frm.stops, 0) AS stops
              FROM flight_offers fo
              LEFT JOIN flight_offer_raw_meta frm
                ON frm.flight_offer_id = fo.id
              WHERE {' AND '.join(current_clauses)}
              ORDER BY
                (fo.origin || '-' || fo.destination),
                fo.airline,
                fo.flight_number,
                fo.departure::date,
                TO_CHAR(fo.departure, 'HH24:MI'),
                COALESCE(frm.via_airports, ''),
                fo.scraped_at DESC
            ) current_ops
            ORDER BY current_ops.route_key, current_ops.departure_date, current_ops.departure_time, current_ops.airline, current_ops.flight_number
            """
        ),
        current_params,
    ).mappings().all()
    current_dicts = _rows_to_dicts([dict(row) for row in current_rows])

    recent_cycles = get_recent_cycles(session, limit=trend_limit)
    trend_cycle_ids = [str(item.get("cycle_id") or "") for item in recent_cycles if item.get("cycle_id")]
    if not trend_cycle_ids:
        return _build_airline_operations_payload(
            resolved_cycle_id=resolved_cycle_id,
            selected_routes=selected_routes,
            current_rows=current_dicts,
            trend_route_rows=[],
            trend_airline_rows=[],
            recent_cycles=[],
        )

    trend_clauses = []
    trend_params: dict[str, Any] = {}
    _apply_in_filter(trend_clauses, trend_params, "trend_ops.cycle_id", trend_cycle_ids, "ops_trend_cycle", uppercase=False)
    _apply_in_filter(trend_clauses, trend_params, "trend_ops.route_key", route_keys, "ops_trend_route")
    _apply_in_filter(trend_clauses, trend_params, "trend_ops.airline", airlines, "ops_trend_airline")
    if via_airports:
        via_codes = _normalize_codes(via_airports)
        trend_clauses.append(
            "(" + " OR ".join(
                [f"('|' || COALESCE(trend_ops.via_airports, '') || '|') LIKE :ops_trend_via_{idx}" for idx, _code in enumerate(via_codes)]
            ) + ")"
        )
        for idx, code in enumerate(via_codes):
            trend_params[f"ops_trend_via_{idx}"] = f"%|{code}|%"
    if start_date:
        trend_clauses.append("trend_ops.departure_date >= :start_date")
        trend_params["start_date"] = start_date
    if end_date:
        trend_clauses.append("trend_ops.departure_date <= :end_date")
        trend_params["end_date"] = end_date

    trend_where = " AND ".join(trend_clauses) if trend_clauses else "1=1"
    trend_route_rows = session.execute(
        text(
            f"""
            WITH trend_ops AS (
              SELECT DISTINCT
                fo.scrape_id::text AS cycle_id,
                fo.airline,
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                fo.flight_number,
                fo.departure::date AS departure_date,
                TO_CHAR(fo.departure, 'HH24:MI') AS departure_time,
                frm.via_airports
              FROM flight_offers fo
              LEFT JOIN flight_offer_raw_meta frm
                ON frm.flight_offer_id = fo.id
            )
            SELECT
              trend_ops.cycle_id,
              trend_ops.origin,
              trend_ops.destination,
              trend_ops.route_key,
              COUNT(*) AS flight_instance_count,
              COUNT(DISTINCT trend_ops.departure_date) AS active_date_count,
              COUNT(DISTINCT trend_ops.airline) AS airline_count,
              MIN(trend_ops.departure_time) AS first_departure_time,
              MAX(trend_ops.departure_time) AS last_departure_time
            FROM trend_ops
            WHERE {trend_where}
            GROUP BY trend_ops.cycle_id, trend_ops.origin, trend_ops.destination, trend_ops.route_key
            ORDER BY trend_ops.route_key, trend_ops.cycle_id
            """
        ),
        trend_params,
    ).mappings().all()
    trend_airline_rows = session.execute(
        text(
            f"""
            WITH trend_ops AS (
              SELECT DISTINCT
                fo.scrape_id::text AS cycle_id,
                fo.airline,
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                fo.flight_number,
                fo.departure::date AS departure_date,
                TO_CHAR(fo.departure, 'HH24:MI') AS departure_time,
                frm.via_airports
              FROM flight_offers fo
              LEFT JOIN flight_offer_raw_meta frm
                ON frm.flight_offer_id = fo.id
            )
            SELECT
              trend_ops.cycle_id,
              trend_ops.airline,
              trend_ops.origin,
              trend_ops.destination,
              trend_ops.route_key,
              COUNT(*) AS flight_instance_count,
              COUNT(DISTINCT trend_ops.departure_date) AS active_date_count,
              MIN(trend_ops.departure_time) AS first_departure_time,
              MAX(trend_ops.departure_time) AS last_departure_time
            FROM trend_ops
            WHERE {trend_where}
            GROUP BY trend_ops.cycle_id, trend_ops.airline, trend_ops.origin, trend_ops.destination, trend_ops.route_key
            ORDER BY trend_ops.route_key, trend_ops.cycle_id, trend_ops.airline
            """
        ),
        trend_params,
    ).mappings().all()

    return _build_airline_operations_payload(
        resolved_cycle_id=resolved_cycle_id,
        selected_routes=selected_routes,
        current_rows=current_dicts,
        trend_route_rows=_rows_to_dicts([dict(row) for row in trend_route_rows]),
        trend_airline_rows=_rows_to_dicts([dict(row) for row in trend_airline_rows]),
        recent_cycles=recent_cycles,
    )


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
    return _annotate_route_records(_rows_to_dicts([dict(row) for row in rows]))


def get_change_events(
    session: Session | None,
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
    if _bigquery_ready():
        try:
            filters, params = _build_change_bigquery_filter_state(
                start_date=start_date,
                end_date=end_date,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                domains=domains,
                change_types=change_types,
                directions=directions,
            )
            params = params + [bigquery.ScalarQueryParameter("row_limit", "INT64", limit)]

            rows = _run_bigquery_query(
                f"""
                SELECT
                  ROW_NUMBER() OVER (ORDER BY detected_at_utc DESC, airline, route_key, field_name) AS id,
                  cycle_id,
                  previous_cycle_id,
                  detected_at_utc,
                  airline,
                  origin,
                  destination,
                  route_key,
                  flight_number,
                  departure_day,
                  departure_time,
                  cabin,
                  fare_basis,
                  brand,
                  domain,
                  change_type,
                  direction,
                  field_name,
                  old_value,
                  new_value,
                  magnitude,
                  percent_change,
                  event_meta
                FROM {_bq_table("fact_change_event")}
                WHERE {' AND '.join(filters)}
                ORDER BY detected_at_utc DESC, airline, route_key, field_name
                LIMIT @row_limit
                """,
                params,
            )
            return _annotate_route_records(_serialize_warehouse_rows(rows))
        except (GoogleAPIError, RuntimeError, ValueError):
            pass
    if session is None:
        return []
    clauses, params = _build_change_sql_filter_state(
        start_date=start_date,
        end_date=end_date,
        airlines=airlines,
        origins=origins,
        destinations=destinations,
        domains=domains,
        change_types=change_types,
        directions=directions,
    )
    params["limit"] = limit

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
    return _annotate_route_records(_rows_to_dicts([dict(row) for row in rows]))


def get_change_dashboard(
    session: Session | None,
    start_date: date | None = None,
    end_date: date | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    domains: Sequence[str] | None = None,
    change_types: Sequence[str] | None = None,
    directions: Sequence[str] | None = None,
    top_n: int = 8,
) -> dict[str, Any]:
    if _bigquery_ready():
        try:
            filters, base_params = _build_change_bigquery_filter_state(
                start_date=start_date,
                end_date=end_date,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                domains=domains,
                change_types=change_types,
                directions=directions,
            )
            top_params = base_params + [bigquery.ScalarQueryParameter("top_n", "INT64", top_n)]

            summary_rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    SELECT
                      COUNT(*) AS event_count,
                      COUNT(DISTINCT route_key) AS route_count,
                      COUNT(DISTINCT airline) AS airline_count,
                      MAX(detected_at_utc) AS latest_event_at_utc,
                      COUNTIF(direction = 'up') AS up_count,
                      COUNTIF(direction = 'down') AS down_count,
                      COUNTIF(change_type = 'added') AS added_count,
                      COUNTIF(change_type = 'removed') AS removed_count,
                      COUNTIF(domain = 'price') AS price_event_count,
                      COUNTIF(domain = 'availability') AS availability_event_count,
                      COUNTIF(domain = 'schedule') AS schedule_event_count,
                      COUNTIF(domain = 'tax') AS tax_event_count,
                      COUNTIF(domain = 'penalty') AS penalty_event_count
                    FROM {_bq_table("fact_change_event")}
                    WHERE {' AND '.join(filters)}
                    """,
                    base_params,
                )
            )
            daily_rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    SELECT
                      report_day,
                      COUNT(*) AS event_count,
                      COUNT(DISTINCT route_key) AS route_count,
                      COUNT(DISTINCT airline) AS airline_count,
                      COUNTIF(direction = 'up') AS up_count,
                      COUNTIF(direction = 'down') AS down_count,
                      COUNTIF(change_type = 'added') AS added_count,
                      COUNTIF(change_type = 'removed') AS removed_count
                    FROM {_bq_table("fact_change_event")}
                    WHERE {' AND '.join(filters)}
                    GROUP BY report_day
                    ORDER BY report_day
                    """,
                    base_params,
                )
            )
            route_rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    SELECT
                      origin,
                      destination,
                      route_key,
                      COUNT(*) AS event_count,
                      COUNT(DISTINCT airline) AS airline_count,
                      MAX(detected_at_utc) AS latest_event_at_utc
                    FROM {_bq_table("fact_change_event")}
                    WHERE {' AND '.join(filters)}
                    GROUP BY origin, destination, route_key
                    ORDER BY event_count DESC, route_key
                    LIMIT @top_n
                    """,
                    top_params,
                )
            )
            airline_rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    SELECT
                      airline,
                      COUNT(*) AS event_count,
                      COUNT(DISTINCT route_key) AS route_count,
                      MAX(detected_at_utc) AS latest_event_at_utc
                    FROM {_bq_table("fact_change_event")}
                    WHERE {' AND '.join(filters)}
                    GROUP BY airline
                    ORDER BY event_count DESC, airline
                    LIMIT @top_n
                    """,
                    top_params,
                )
            )
            domain_rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    SELECT
                      COALESCE(domain, 'unclassified') AS domain,
                      COUNT(*) AS event_count
                    FROM {_bq_table("fact_change_event")}
                    WHERE {' AND '.join(filters)}
                    GROUP BY domain
                    ORDER BY event_count DESC, domain
                    LIMIT @top_n
                    """,
                    top_params,
                )
            )
            field_rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    SELECT
                      field_name,
                      COUNT(*) AS event_count
                    FROM {_bq_table("fact_change_event")}
                    WHERE {' AND '.join(filters)}
                    GROUP BY field_name
                    ORDER BY event_count DESC, field_name
                    LIMIT @top_n
                    """,
                    top_params,
                )
            )
            largest_moves = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    SELECT
                      ROW_NUMBER() OVER (ORDER BY ABS(magnitude) DESC NULLS LAST, detected_at_utc DESC) AS id,
                      cycle_id,
                      previous_cycle_id,
                      detected_at_utc,
                      airline,
                      origin,
                      destination,
                      route_key,
                      flight_number,
                      departure_day,
                      departure_time,
                      cabin,
                      fare_basis,
                      brand,
                      domain,
                      change_type,
                      direction,
                      field_name,
                      old_value,
                      new_value,
                      magnitude,
                      percent_change,
                      event_meta
                    FROM {_bq_table("fact_change_event")}
                    WHERE {' AND '.join(filters)}
                    ORDER BY ABS(magnitude) DESC NULLS LAST, detected_at_utc DESC, airline, route_key
                    LIMIT @top_n
                    """,
                    top_params,
                )
            )
            return _build_change_dashboard_payload(
                summary_row=summary_rows[0] if summary_rows else None,
                daily_rows=daily_rows,
                route_rows=route_rows,
                airline_rows=airline_rows,
                domain_rows=domain_rows,
                field_rows=field_rows,
                largest_moves=largest_moves,
            )
        except (GoogleAPIError, RuntimeError, ValueError):
            pass

    if session is None:
        return _build_change_dashboard_payload(
            summary_row=None,
            daily_rows=[],
            route_rows=[],
            airline_rows=[],
            domain_rows=[],
            field_rows=[],
            largest_moves=[],
        )

    clauses, params = _build_change_sql_filter_state(
        start_date=start_date,
        end_date=end_date,
        airlines=airlines,
        origins=origins,
        destinations=destinations,
        domains=domains,
        change_types=change_types,
        directions=directions,
    )
    params["top_n"] = top_n

    summary_row = session.execute(
        text(
            f"""
            SELECT
                COUNT(*) AS event_count,
                COUNT(DISTINCT (cce.origin || '-' || cce.destination)) AS route_count,
                COUNT(DISTINCT cce.airline) AS airline_count,
                MAX(cce.detected_at) AS latest_event_at_utc,
                COUNT(*) FILTER (WHERE cce.direction = 'up') AS up_count,
                COUNT(*) FILTER (WHERE cce.direction = 'down') AS down_count,
                COUNT(*) FILTER (WHERE cce.change_type = 'added') AS added_count,
                COUNT(*) FILTER (WHERE cce.change_type = 'removed') AS removed_count,
                COUNT(*) FILTER (WHERE cce.domain = 'price') AS price_event_count,
                COUNT(*) FILTER (WHERE cce.domain = 'availability') AS availability_event_count,
                COUNT(*) FILTER (WHERE cce.domain = 'schedule') AS schedule_event_count,
                COUNT(*) FILTER (WHERE cce.domain = 'tax') AS tax_event_count,
                COUNT(*) FILTER (WHERE cce.domain = 'penalty') AS penalty_event_count
            FROM airline_intel.column_change_events cce
            WHERE {' AND '.join(clauses)}
            """
        ),
        params,
    ).mappings().first()
    daily_rows = session.execute(
        text(
            f"""
            SELECT
                cce.detected_at::date AS report_day,
                COUNT(*) AS event_count,
                COUNT(DISTINCT (cce.origin || '-' || cce.destination)) AS route_count,
                COUNT(DISTINCT cce.airline) AS airline_count,
                COUNT(*) FILTER (WHERE cce.direction = 'up') AS up_count,
                COUNT(*) FILTER (WHERE cce.direction = 'down') AS down_count,
                COUNT(*) FILTER (WHERE cce.change_type = 'added') AS added_count,
                COUNT(*) FILTER (WHERE cce.change_type = 'removed') AS removed_count
            FROM airline_intel.column_change_events cce
            WHERE {' AND '.join(clauses)}
            GROUP BY cce.detected_at::date
            ORDER BY report_day
            """
        ),
        params,
    ).mappings().all()
    route_rows = session.execute(
        text(
            f"""
            SELECT
                cce.origin,
                cce.destination,
                (cce.origin || '-' || cce.destination) AS route_key,
                COUNT(*) AS event_count,
                COUNT(DISTINCT cce.airline) AS airline_count,
                MAX(cce.detected_at) AS latest_event_at_utc
            FROM airline_intel.column_change_events cce
            WHERE {' AND '.join(clauses)}
            GROUP BY cce.origin, cce.destination
            ORDER BY event_count DESC, route_key
            LIMIT :top_n
            """
        ),
        params,
    ).mappings().all()
    airline_rows = session.execute(
        text(
            f"""
            SELECT
                cce.airline,
                COUNT(*) AS event_count,
                COUNT(DISTINCT (cce.origin || '-' || cce.destination)) AS route_count,
                MAX(cce.detected_at) AS latest_event_at_utc
            FROM airline_intel.column_change_events cce
            WHERE {' AND '.join(clauses)}
            GROUP BY cce.airline
            ORDER BY event_count DESC, cce.airline
            LIMIT :top_n
            """
        ),
        params,
    ).mappings().all()
    domain_rows = session.execute(
        text(
            f"""
            SELECT
                COALESCE(cce.domain, 'unclassified') AS domain,
                COUNT(*) AS event_count
            FROM airline_intel.column_change_events cce
            WHERE {' AND '.join(clauses)}
            GROUP BY COALESCE(cce.domain, 'unclassified')
            ORDER BY event_count DESC, domain
            LIMIT :top_n
            """
        ),
        params,
    ).mappings().all()
    field_rows = session.execute(
        text(
            f"""
            SELECT
                cce.field_name,
                COUNT(*) AS event_count
            FROM airline_intel.column_change_events cce
            WHERE {' AND '.join(clauses)}
            GROUP BY cce.field_name
            ORDER BY event_count DESC, cce.field_name
            LIMIT :top_n
            """
        ),
        params,
    ).mappings().all()
    largest_moves = session.execute(
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
            ORDER BY ABS(cce.magnitude) DESC NULLS LAST, cce.detected_at DESC, cce.id DESC
            LIMIT :top_n
            """
        ),
        params,
    ).mappings().all()

    return _build_change_dashboard_payload(
        summary_row=dict(summary_row) if summary_row else None,
        daily_rows=_rows_to_dicts([dict(row) for row in daily_rows]),
        route_rows=_rows_to_dicts([dict(row) for row in route_rows]),
        airline_rows=_rows_to_dicts([dict(row) for row in airline_rows]),
        domain_rows=_rows_to_dicts([dict(row) for row in domain_rows]),
        field_rows=_rows_to_dicts([dict(row) for row in field_rows]),
        largest_moves=_rows_to_dicts([dict(row) for row in largest_moves]),
    )


def get_penalties(
    session: Session | None,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    if _bigquery_ready():
        try:
            resolved_cycle_id = _resolve_cycle_id_bigquery(cycle_id)
            if not resolved_cycle_id:
                return {"cycle_id": None, "rows": []}
            filters = [
                "cycle_id = @cycle_id",
                """(
                  penalty_rule_text IS NOT NULL
                  OR fare_change_fee_before_24h IS NOT NULL
                  OR fare_change_fee_within_24h IS NOT NULL
                  OR fare_change_fee_no_show IS NOT NULL
                  OR fare_cancel_fee_before_24h IS NOT NULL
                  OR fare_cancel_fee_within_24h IS NOT NULL
                  OR fare_cancel_fee_no_show IS NOT NULL
                )""",
            ]
            params: list[bigquery.ScalarQueryParameter] = [
                bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id),
                bigquery.ScalarQueryParameter("row_limit", "INT64", limit),
            ]
            if airlines:
                filters.append("airline IN UNNEST(@airlines)")
                params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
            if origins:
                filters.append("origin IN UNNEST(@origins)")
                params.append(bigquery.ArrayQueryParameter("origins", "STRING", _normalize_codes(origins)))
            if destinations:
                filters.append("destination IN UNNEST(@destinations)")
                params.append(bigquery.ArrayQueryParameter("destinations", "STRING", _normalize_codes(destinations)))

            rows = _run_bigquery_query(
                f"""
                SELECT
                  cycle_id,
                  captured_at_utc,
                  airline,
                  origin,
                  destination,
                  route_key,
                  flight_number,
                  departure_utc,
                  cabin,
                  fare_basis,
                  penalty_source,
                  penalty_currency,
                  fare_change_fee_before_24h,
                  fare_change_fee_within_24h,
                  fare_change_fee_no_show,
                  fare_cancel_fee_before_24h,
                  fare_cancel_fee_within_24h,
                  fare_cancel_fee_no_show,
                  fare_changeable,
                  fare_refundable,
                  penalty_rule_text
                FROM {_bq_table("fact_penalty_snapshot")}
                WHERE {' AND '.join(filters)}
                ORDER BY origin, destination, departure_utc, airline, flight_number
                LIMIT @row_limit
                """,
                params,
            )
            return {"cycle_id": resolved_cycle_id, "rows": _annotate_route_records(_serialize_warehouse_rows(rows))}
        except (GoogleAPIError, RuntimeError, ValueError):
            pass
    resolved_cycle_id = _resolve_cycle_id(session, cycle_id)
    if not resolved_cycle_id:
        return {"cycle_id": None, "rows": []}
    if session is None:
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
    return {"cycle_id": resolved_cycle_id, "rows": _annotate_route_records(_rows_to_dicts([dict(row) for row in rows]))}


def _build_tax_monitor_payload(
    *,
    resolved_cycle_id: str,
    detail_rows: Sequence[dict[str, Any]],
    route_summaries: Sequence[dict[str, Any]],
    airline_summaries: Sequence[dict[str, Any]],
    route_trend_rows: Sequence[dict[str, Any]],
    airline_trend_rows: Sequence[dict[str, Any]],
    recent_cycles: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    cycle_lookup = {
        str(item.get("cycle_id") or ""): item
        for item in recent_cycles
        if item.get("cycle_id")
    }

    route_trend_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in route_trend_rows:
        route_trend_map[str(row.get("route_key") or "")].append(dict(row))

    airline_trend_map: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in airline_trend_rows:
        airline_trend_map[(str(row.get("route_key") or ""), str(row.get("airline") or ""))].append(dict(row))

    annotated_routes = _annotate_route_records(route_summaries)
    annotated_airlines = _annotate_route_records(airline_summaries)
    annotated_rows = _annotate_route_records(detail_rows)

    for item in annotated_routes:
        timeline = sorted(
            [
                {
                    **trend_item,
                    "cycle_completed_at_utc": cycle_lookup.get(str(trend_item.get("cycle_id") or ""), {}).get("cycle_completed_at_utc"),
                }
                for trend_item in route_trend_map.get(str(item.get("route_key") or ""), [])
            ],
            key=lambda trend_item: str(trend_item.get("cycle_completed_at_utc") or trend_item.get("cycle_id") or ""),
        )
        item["timeline"] = timeline
        if len(timeline) >= 2:
            item["avg_tax_change_amount"] = float(timeline[-1].get("avg_tax_amount") or 0) - float(timeline[-2].get("avg_tax_amount") or 0)
        else:
            item["avg_tax_change_amount"] = None

    for item in annotated_airlines:
        timeline = sorted(
            [
                {
                    **trend_item,
                    "cycle_completed_at_utc": cycle_lookup.get(str(trend_item.get("cycle_id") or ""), {}).get("cycle_completed_at_utc"),
                }
                for trend_item in airline_trend_map.get((str(item.get("route_key") or ""), str(item.get("airline") or "")), [])
            ],
            key=lambda trend_item: str(trend_item.get("cycle_completed_at_utc") or trend_item.get("cycle_id") or ""),
        )
        item["timeline"] = timeline
        if len(timeline) >= 2:
            item["avg_tax_change_amount"] = float(timeline[-1].get("avg_tax_amount") or 0) - float(timeline[-2].get("avg_tax_amount") or 0)
        else:
            item["avg_tax_change_amount"] = None

    annotated_routes.sort(
        key=lambda item: (
            -float(item.get("spread_amount") or 0),
            -abs(float(item.get("avg_tax_change_amount") or 0)),
            str(item.get("route_key") or ""),
        )
    )
    annotated_airlines.sort(
        key=lambda item: (
            -abs(float(item.get("avg_tax_change_amount") or 0)),
            -float(item.get("spread_amount") or 0),
            str(item.get("route_key") or ""),
            str(item.get("airline") or ""),
        )
    )

    return {
        "cycle_id": resolved_cycle_id,
        "rows": annotated_rows,
        "route_summaries": annotated_routes,
        "airline_summaries": annotated_airlines,
    }


def get_taxes(
    session: Session | None,
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    route_types: Sequence[str] | None = None,
    limit: int = 500,
    trend_limit: int = 8,
) -> dict[str, Any]:
    if _bigquery_ready():
        try:
            resolved_cycle_id = _resolve_cycle_id_bigquery(cycle_id)
            if not resolved_cycle_id:
                return {"cycle_id": None, "rows": [], "route_summaries": [], "airline_summaries": []}
            route_filters = ["cycle_id = @cycle_id", "tax_amount IS NOT NULL"]
            route_params: list[bigquery.ScalarQueryParameter] = [
                bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id),
            ]
            if airlines:
                route_filters.append("airline IN UNNEST(@airlines)")
                route_params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))
            if origins:
                route_filters.append("origin IN UNNEST(@origins)")
                route_params.append(bigquery.ArrayQueryParameter("origins", "STRING", _normalize_codes(origins)))
            if destinations:
                route_filters.append("destination IN UNNEST(@destinations)")
                route_params.append(bigquery.ArrayQueryParameter("destinations", "STRING", _normalize_codes(destinations)))

            route_summary_rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    WITH deduped_tax AS (
                      SELECT DISTINCT
                        cycle_id,
                        captured_at_utc,
                        airline,
                        origin,
                        destination,
                        route_key,
                        flight_number,
                        departure_utc,
                        cabin,
                        fare_basis,
                        tax_amount,
                        currency
                      FROM {_bq_table("fact_tax_snapshot")}
                      WHERE {' AND '.join(route_filters)}
                    )
                    SELECT
                      origin,
                      destination,
                      route_key,
                      COUNT(*) AS row_count,
                      COUNT(DISTINCT airline) AS airline_count,
                      MIN(tax_amount) AS min_tax_amount,
                      MAX(tax_amount) AS max_tax_amount,
                      AVG(tax_amount) AS avg_tax_amount,
                      MAX(tax_amount) - MIN(tax_amount) AS spread_amount,
                      MAX(captured_at_utc) AS latest_captured_at_utc
                    FROM deduped_tax
                    GROUP BY origin, destination, route_key
                    ORDER BY route_key
                    """,
                    route_params,
                )
            )
            route_summaries = _filter_route_type_records(_annotate_route_records(route_summary_rows), route_types)
            if not route_summaries:
                return {"cycle_id": resolved_cycle_id, "rows": [], "route_summaries": [], "airline_summaries": []}

            route_keys = [str(item["route_key"]) for item in route_summaries]
            detail_filters = ["cycle_id = @cycle_id", "tax_amount IS NOT NULL", "route_key IN UNNEST(@route_keys)"]
            detail_params: list[bigquery.ScalarQueryParameter] = [
                bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id),
                bigquery.ArrayQueryParameter("route_keys", "STRING", route_keys),
                bigquery.ScalarQueryParameter("row_limit", "INT64", limit),
            ]
            if airlines:
                detail_filters.append("airline IN UNNEST(@airlines)")
                detail_params.append(bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines)))

            rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    WITH deduped_tax AS (
                      SELECT DISTINCT
                        cycle_id,
                        captured_at_utc,
                        airline,
                        origin,
                        destination,
                        route_key,
                        flight_number,
                        departure_utc,
                        cabin,
                        fare_basis,
                        tax_amount,
                        currency
                      FROM {_bq_table("fact_tax_snapshot")}
                      WHERE {' AND '.join(detail_filters)}
                    )
                    SELECT
                      cycle_id,
                      captured_at_utc,
                      airline,
                      origin,
                      destination,
                      route_key,
                      flight_number,
                      departure_utc,
                      cabin,
                      fare_basis,
                      tax_amount,
                      currency
                    FROM deduped_tax
                    ORDER BY origin, destination, departure_utc, airline, flight_number
                    LIMIT @row_limit
                    """,
                    detail_params,
                )
            )

            airline_summary_rows = _serialize_warehouse_rows(
                _run_bigquery_query(
                    f"""
                    WITH deduped_tax AS (
                      SELECT DISTINCT
                        cycle_id,
                        captured_at_utc,
                        airline,
                        origin,
                        destination,
                        route_key,
                        flight_number,
                        departure_utc,
                        cabin,
                        fare_basis,
                        tax_amount,
                        currency
                      FROM {_bq_table("fact_tax_snapshot")}
                      WHERE cycle_id = @cycle_id
                        AND tax_amount IS NOT NULL
                        AND route_key IN UNNEST(@route_keys)
                        {"AND airline IN UNNEST(@airlines)" if airlines else ""}
                    )
                    SELECT
                      origin,
                      destination,
                      route_key,
                      airline,
                      COUNT(*) AS row_count,
                      MIN(tax_amount) AS min_tax_amount,
                      MAX(tax_amount) AS max_tax_amount,
                      AVG(tax_amount) AS avg_tax_amount,
                      MAX(tax_amount) - MIN(tax_amount) AS spread_amount,
                      MAX(captured_at_utc) AS latest_captured_at_utc
                    FROM deduped_tax
                    GROUP BY origin, destination, route_key, airline
                    ORDER BY route_key, airline
                    """,
                    [
                        bigquery.ScalarQueryParameter("cycle_id", "STRING", resolved_cycle_id),
                        bigquery.ArrayQueryParameter("route_keys", "STRING", route_keys),
                    ]
                    + (
                        [bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines))]
                        if airlines
                        else []
                    ),
                )
            )

            recent_cycles = get_recent_cycles(None, limit=trend_limit)
            trend_cycle_ids = [str(item.get("cycle_id") or "") for item in recent_cycles if item.get("cycle_id")]
            route_trend_rows: list[dict[str, Any]] = []
            airline_trend_rows: list[dict[str, Any]] = []
            if trend_cycle_ids:
                trend_params = [
                    bigquery.ArrayQueryParameter("cycle_ids", "STRING", trend_cycle_ids),
                    bigquery.ArrayQueryParameter("route_keys", "STRING", route_keys),
                ] + (
                    [bigquery.ArrayQueryParameter("airlines", "STRING", _normalize_codes(airlines))]
                    if airlines
                    else []
                )
                route_trend_rows = _serialize_warehouse_rows(
                    _run_bigquery_query(
                        f"""
                        WITH deduped_tax AS (
                          SELECT DISTINCT
                            cycle_id,
                            captured_at_utc,
                            airline,
                            origin,
                            destination,
                            route_key,
                            flight_number,
                            departure_utc,
                            cabin,
                            fare_basis,
                            tax_amount,
                            currency
                          FROM {_bq_table("fact_tax_snapshot")}
                          WHERE cycle_id IN UNNEST(@cycle_ids)
                            AND route_key IN UNNEST(@route_keys)
                            AND tax_amount IS NOT NULL
                            {"AND airline IN UNNEST(@airlines)" if airlines else ""}
                        )
                        SELECT
                          cycle_id,
                          origin,
                          destination,
                          route_key,
                          COUNT(*) AS row_count,
                          COUNT(DISTINCT airline) AS airline_count,
                          MIN(tax_amount) AS min_tax_amount,
                          MAX(tax_amount) AS max_tax_amount,
                          AVG(tax_amount) AS avg_tax_amount,
                          MAX(tax_amount) - MIN(tax_amount) AS spread_amount
                        FROM deduped_tax
                        GROUP BY cycle_id, origin, destination, route_key
                        ORDER BY route_key, cycle_id
                        """,
                        trend_params,
                    )
                )
                airline_trend_rows = _serialize_warehouse_rows(
                    _run_bigquery_query(
                        f"""
                        WITH deduped_tax AS (
                          SELECT DISTINCT
                            cycle_id,
                            captured_at_utc,
                            airline,
                            origin,
                            destination,
                            route_key,
                            flight_number,
                            departure_utc,
                            cabin,
                            fare_basis,
                            tax_amount,
                            currency
                          FROM {_bq_table("fact_tax_snapshot")}
                          WHERE cycle_id IN UNNEST(@cycle_ids)
                            AND route_key IN UNNEST(@route_keys)
                            AND tax_amount IS NOT NULL
                            {"AND airline IN UNNEST(@airlines)" if airlines else ""}
                        )
                        SELECT
                          cycle_id,
                          origin,
                          destination,
                          route_key,
                          airline,
                          COUNT(*) AS row_count,
                          MIN(tax_amount) AS min_tax_amount,
                          MAX(tax_amount) AS max_tax_amount,
                          AVG(tax_amount) AS avg_tax_amount,
                          MAX(tax_amount) - MIN(tax_amount) AS spread_amount
                        FROM deduped_tax
                        GROUP BY cycle_id, origin, destination, route_key, airline
                        ORDER BY route_key, cycle_id, airline
                        """,
                        trend_params,
                    )
                )

            return _build_tax_monitor_payload(
                resolved_cycle_id=resolved_cycle_id,
                detail_rows=rows,
                route_summaries=route_summaries,
                airline_summaries=airline_summary_rows,
                route_trend_rows=route_trend_rows,
                airline_trend_rows=airline_trend_rows,
                recent_cycles=recent_cycles,
            )
        except (GoogleAPIError, RuntimeError, ValueError):
            pass
    resolved_cycle_id = _resolve_cycle_id(session, cycle_id)
    if not resolved_cycle_id:
        return {"cycle_id": None, "rows": [], "route_summaries": [], "airline_summaries": []}
    if session is None:
        return {"cycle_id": None, "rows": [], "route_summaries": [], "airline_summaries": []}

    route_clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)", "frm.tax_amount IS NOT NULL"]
    route_params: dict[str, Any] = {"cycle_id": resolved_cycle_id}
    _apply_in_filter(route_clauses, route_params, "fo.airline", airlines, "tax_route_airline")
    _apply_in_filter(route_clauses, route_params, "fo.origin", origins, "tax_route_origin")
    _apply_in_filter(route_clauses, route_params, "fo.destination", destinations, "tax_route_destination")

    route_summary_rows = session.execute(
        text(
            f"""
            SELECT
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                COUNT(*) AS row_count,
                COUNT(DISTINCT fo.airline) AS airline_count,
                CAST(MIN(frm.tax_amount) AS NUMERIC(12, 2)) AS min_tax_amount,
                CAST(MAX(frm.tax_amount) AS NUMERIC(12, 2)) AS max_tax_amount,
                CAST(AVG(frm.tax_amount) AS NUMERIC(12, 2)) AS avg_tax_amount,
                CAST(MAX(frm.tax_amount) - MIN(frm.tax_amount) AS NUMERIC(12, 2)) AS spread_amount,
                MAX(fo.scraped_at) AS latest_captured_at_utc
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta frm
                ON frm.flight_offer_id = fo.id
            WHERE {' AND '.join(route_clauses)}
            GROUP BY fo.origin, fo.destination
            ORDER BY route_key
            """
        ),
        route_params,
    ).mappings().all()
    route_summaries = _filter_route_type_records(_annotate_route_records(_rows_to_dicts([dict(row) for row in route_summary_rows])), route_types)
    if not route_summaries:
        return {"cycle_id": resolved_cycle_id, "rows": [], "route_summaries": [], "airline_summaries": []}

    route_keys = [str(item["route_key"]) for item in route_summaries]
    detail_clauses = ["fo.scrape_id = CAST(:cycle_id AS uuid)", "frm.tax_amount IS NOT NULL"]
    detail_params: dict[str, Any] = {"cycle_id": resolved_cycle_id, "limit": limit}
    _apply_in_filter(detail_clauses, detail_params, "(fo.origin || '-' || fo.destination)", route_keys, "tax_detail_route")
    _apply_in_filter(detail_clauses, detail_params, "fo.airline", airlines, "tax_detail_airline")

    detail_rows = session.execute(
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
            WHERE {' AND '.join(detail_clauses)}
            ORDER BY fo.origin, fo.destination, fo.departure, fo.airline, fo.flight_number
            LIMIT :limit
            """
        ),
        detail_params,
    ).mappings().all()

    airline_filter_values = _normalize_codes(airlines)
    airline_summary_params = {
        "cycle_id": resolved_cycle_id,
        **{f"tax_airline_route_{idx}": value for idx, value in enumerate(route_keys)},
    }
    if airline_filter_values:
        airline_summary_params.update(
            {f"tax_airline_filter_{idx}": value for idx, value in enumerate(airline_filter_values)}
        )

    airline_summary_rows = session.execute(
        text(
            f"""
            SELECT
                fo.origin,
                fo.destination,
                (fo.origin || '-' || fo.destination) AS route_key,
                fo.airline,
                COUNT(*) AS row_count,
                CAST(MIN(frm.tax_amount) AS NUMERIC(12, 2)) AS min_tax_amount,
                CAST(MAX(frm.tax_amount) AS NUMERIC(12, 2)) AS max_tax_amount,
                CAST(AVG(frm.tax_amount) AS NUMERIC(12, 2)) AS avg_tax_amount,
                CAST(MAX(frm.tax_amount) - MIN(frm.tax_amount) AS NUMERIC(12, 2)) AS spread_amount,
                MAX(fo.scraped_at) AS latest_captured_at_utc
            FROM flight_offers fo
            LEFT JOIN flight_offer_raw_meta frm
                ON frm.flight_offer_id = fo.id
            WHERE fo.scrape_id = CAST(:cycle_id AS uuid)
              AND frm.tax_amount IS NOT NULL
              AND (fo.origin || '-' || fo.destination) IN ({', '.join(f':tax_airline_route_{idx}' for idx in range(len(route_keys)))})
              {"AND fo.airline IN (" + ', '.join(f':tax_airline_filter_{idx}' for idx in range(len(_normalize_codes(airlines)))) + ")" if airlines else ""}
            GROUP BY fo.origin, fo.destination, fo.airline
            ORDER BY route_key, fo.airline
            """
        ),
        airline_summary_params,
    ).mappings().all()

    recent_cycles = get_recent_cycles(session, limit=trend_limit)
    trend_cycle_ids = [str(item.get("cycle_id") or "") for item in recent_cycles if item.get("cycle_id")]
    route_trend_rows: list[dict[str, Any]] = []
    airline_trend_rows: list[dict[str, Any]] = []
    if trend_cycle_ids:
        cycle_placeholders = ", ".join(f":tax_cycle_{idx}" for idx in range(len(trend_cycle_ids)))
        route_placeholders = ", ".join(f":tax_route_{idx}" for idx in range(len(route_keys)))
        trend_params = {
            **{f"tax_cycle_{idx}": value for idx, value in enumerate(trend_cycle_ids)},
            **{f"tax_route_{idx}": value for idx, value in enumerate(route_keys)},
        }
        if airline_filter_values:
            trend_params.update(
                {f"tax_trend_airline_{idx}": value for idx, value in enumerate(airline_filter_values)}
            )
        route_trend_rows = _rows_to_dicts(
            [
                dict(row)
                for row in session.execute(
                    text(
                        f"""
                        SELECT
                            fo.scrape_id::text AS cycle_id,
                            fo.origin,
                            fo.destination,
                            (fo.origin || '-' || fo.destination) AS route_key,
                            COUNT(*) AS row_count,
                            COUNT(DISTINCT fo.airline) AS airline_count,
                            CAST(MIN(frm.tax_amount) AS NUMERIC(12, 2)) AS min_tax_amount,
                            CAST(MAX(frm.tax_amount) AS NUMERIC(12, 2)) AS max_tax_amount,
                            CAST(AVG(frm.tax_amount) AS NUMERIC(12, 2)) AS avg_tax_amount,
                            CAST(MAX(frm.tax_amount) - MIN(frm.tax_amount) AS NUMERIC(12, 2)) AS spread_amount
                        FROM flight_offers fo
                        LEFT JOIN flight_offer_raw_meta frm
                            ON frm.flight_offer_id = fo.id
                        WHERE fo.scrape_id::text IN ({cycle_placeholders})
                          AND frm.tax_amount IS NOT NULL
                          AND (fo.origin || '-' || fo.destination) IN ({route_placeholders})
                          {"AND fo.airline IN (" + ', '.join(f':tax_trend_airline_{idx}' for idx in range(len(_normalize_codes(airlines)))) + ")" if airlines else ""}
                        GROUP BY fo.scrape_id, fo.origin, fo.destination
                        ORDER BY route_key, cycle_id
                        """
                    ),
                    trend_params,
                ).mappings().all()
            ]
        )
        airline_trend_rows = _rows_to_dicts(
            [
                dict(row)
                for row in session.execute(
                    text(
                        f"""
                        SELECT
                            fo.scrape_id::text AS cycle_id,
                            fo.origin,
                            fo.destination,
                            (fo.origin || '-' || fo.destination) AS route_key,
                            fo.airline,
                            COUNT(*) AS row_count,
                            CAST(MIN(frm.tax_amount) AS NUMERIC(12, 2)) AS min_tax_amount,
                            CAST(MAX(frm.tax_amount) AS NUMERIC(12, 2)) AS max_tax_amount,
                            CAST(AVG(frm.tax_amount) AS NUMERIC(12, 2)) AS avg_tax_amount,
                            CAST(MAX(frm.tax_amount) - MIN(frm.tax_amount) AS NUMERIC(12, 2)) AS spread_amount
                        FROM flight_offers fo
                        LEFT JOIN flight_offer_raw_meta frm
                            ON frm.flight_offer_id = fo.id
                        WHERE fo.scrape_id::text IN ({cycle_placeholders})
                          AND frm.tax_amount IS NOT NULL
                          AND (fo.origin || '-' || fo.destination) IN ({route_placeholders})
                          {"AND fo.airline IN (" + ', '.join(f':tax_trend_airline_{idx}' for idx in range(len(_normalize_codes(airlines)))) + ")" if airlines else ""}
                        GROUP BY fo.scrape_id, fo.origin, fo.destination, fo.airline
                        ORDER BY route_key, cycle_id, fo.airline
                        """
                    ),
                    trend_params,
                ).mappings().all()
            ]
        )

    return _build_tax_monitor_payload(
        resolved_cycle_id=resolved_cycle_id,
        detail_rows=_rows_to_dicts([dict(row) for row in detail_rows]),
        route_summaries=route_summaries,
        airline_summaries=_rows_to_dicts([dict(row) for row in airline_summary_rows]),
        route_trend_rows=route_trend_rows,
        airline_trend_rows=airline_trend_rows,
        recent_cycles=recent_cycles,
    )
