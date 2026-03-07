from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


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
