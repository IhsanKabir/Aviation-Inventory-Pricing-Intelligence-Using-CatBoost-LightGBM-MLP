"""
repositories/gds.py - GDS fare and tax data from BigQuery

Queries the three GDS tables written by travelport-automation/bigquery_pusher.py:
    fact_gds_fare_snapshot   - per-run fare rows
    fact_gds_change_event    - fare change events between runs
    fact_gds_tax_snapshot    - airport tax rates per run

All functions accept a google.cloud.bigquery.Client and return
plain list[dict] so they are easy to cache and serialise.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any

from google.cloud import bigquery

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _gds_table(name: str) -> str:
    project = os.environ.get("BIGQUERY_PROJECT_ID", "aeropulseintelligence")
    dataset = os.environ.get("BIGQUERY_DATASET", "aviation_intel")
    return f"`{project}.{dataset}.{name}`"


def _serialize(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _rows_to_dicts(rows) -> list[dict]:
    return [{k: _serialize(v) for k, v in dict(row.items()).items()} for row in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Fare snapshot queries
# ─────────────────────────────────────────────────────────────────────────────


def get_latest_fare_run(client: bigquery.Client) -> dict | None:
    """Return metadata for the most recent GDS fare extraction run."""
    query = f"""
        SELECT
            cycle_id,
            MAX(captured_at_utc)                          AS captured_at_utc,
            COUNT(DISTINCT route_key)                     AS total_routes,
            COUNT(DISTINCT airline)                       AS total_airlines,
            COUNTIF(is_sold_out = FALSE AND base_fare > 0) AS total_fares
        FROM {_gds_table('fact_gds_fare_snapshot')}
        WHERE DATE(captured_at_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        GROUP BY cycle_id
        ORDER BY captured_at_utc DESC
        LIMIT 1
    """
    rows = _rows_to_dicts(client.query(query).result())
    return rows[0] if rows else None


def get_fare_runs(
    client: bigquery.Client,
    limit: int = 20,
) -> list[dict]:
    """Return a summary of recent GDS fare extraction runs."""
    query = f"""
        SELECT
            cycle_id,
            MAX(captured_at_utc)                           AS captured_at_utc,
            COUNT(DISTINCT route_key)                      AS total_routes,
            COUNT(DISTINCT airline)                        AS total_airlines,
            COUNTIF(is_sold_out = FALSE AND base_fare > 0) AS total_fares
        FROM {_gds_table('fact_gds_fare_snapshot')}
        GROUP BY cycle_id
        ORDER BY captured_at_utc DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    return _rows_to_dicts(client.query(query, job_config=job_config).result())


def get_fares(
    client: bigquery.Client,
    airline: str | None = None,
    origin: str | None = None,
    destination: str | None = None,
    cabin: str | None = None,
    journey_type: str | None = None,
    cycle_id: str | None = None,
    limit: int = 500,
) -> list[dict]:
    """
    Return fare rows from the latest cycle (or a specified cycle_id).
    Supports filtering by airline, route, cabin and journey type.
    """
    params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]

    # If no cycle_id supplied, default to the most recent cycle
    cycle_filter = ""
    if cycle_id:
        cycle_filter = "AND cycle_id = @cycle_id"
        params.append(bigquery.ScalarQueryParameter("cycle_id", "STRING", cycle_id))
    else:
        cycle_filter = """
            AND cycle_id = (
                SELECT cycle_id FROM {table}
                ORDER BY captured_at_utc DESC
                LIMIT 1
            )
        """.format(table=_gds_table("fact_gds_fare_snapshot"))

    airline_filter = ""
    if airline:
        airline_filter = "AND airline = @airline"
        params.append(
            bigquery.ScalarQueryParameter("airline", "STRING", airline.upper())
        )

    origin_filter = ""
    if origin:
        origin_filter = "AND origin = @origin"
        params.append(bigquery.ScalarQueryParameter("origin", "STRING", origin.upper()))

    dest_filter = ""
    if destination:
        dest_filter = "AND destination = @destination"
        params.append(
            bigquery.ScalarQueryParameter("destination", "STRING", destination.upper())
        )

    cabin_filter = ""
    if cabin:
        cabin_filter = "AND cabin = @cabin"
        params.append(bigquery.ScalarQueryParameter("cabin", "STRING", cabin))

    jt_filter = ""
    if journey_type:
        jt_filter = "AND journey_type = @journey_type"
        params.append(
            bigquery.ScalarQueryParameter(
                "journey_type", "STRING", journey_type.upper()
            )
        )

    query = f"""
        SELECT
            cycle_id,
            captured_at_utc,
            airline,
            origin,
            destination,
            route_key,
            rbd,
            cabin,
            fare_basis,
            journey_type,
            base_fare,
            total_taxes,
            total_fare,
            currency,
            is_sold_out,
            is_unsaleable
        FROM {_gds_table('fact_gds_fare_snapshot')}
        WHERE TRUE
            {cycle_filter}
            {airline_filter}
            {origin_filter}
            {dest_filter}
            {cabin_filter}
            {jt_filter}
        ORDER BY airline, route_key, rbd, journey_type
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return _rows_to_dicts(client.query(query, job_config=job_config).result())


def get_fare_history(
    client: bigquery.Client,
    route_key: str,
    airline: str,
    rbd: str,
    journey_type: str = "OW",
    days: int = 30,
) -> list[dict]:
    """Return fare history for a specific route/airline/RBD over N days."""
    query = f"""
        SELECT
            cycle_id,
            captured_at_utc,
            base_fare,
            total_taxes,
            total_fare,
            currency,
            is_sold_out
        FROM {_gds_table('fact_gds_fare_snapshot')}
        WHERE route_key     = @route_key
          AND airline       = @airline
          AND rbd           = @rbd
          AND journey_type  = @journey_type
          AND DATE(captured_at_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        ORDER BY captured_at_utc ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("route_key", "STRING", route_key),
            bigquery.ScalarQueryParameter("airline", "STRING", airline.upper()),
            bigquery.ScalarQueryParameter("rbd", "STRING", rbd.upper()),
            bigquery.ScalarQueryParameter(
                "journey_type", "STRING", journey_type.upper()
            ),
            bigquery.ScalarQueryParameter("days", "INT64", days),
        ]
    )
    return _rows_to_dicts(client.query(query, job_config=job_config).result())


# ─────────────────────────────────────────────────────────────────────────────
# Change event queries
# ─────────────────────────────────────────────────────────────────────────────


def get_change_events(
    client: bigquery.Client,
    airline: str | None = None,
    origin: str | None = None,
    destination: str | None = None,
    change_type: str | None = None,
    days: int = 7,
    limit: int = 200,
) -> list[dict]:
    """Return recent fare change events."""
    params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("days", "INT64", days),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]

    airline_filter = ""
    origin_filter = ""
    dest_filter = ""
    ct_filter = ""

    if airline:
        airline_filter = "AND airline = @airline"
        params.append(
            bigquery.ScalarQueryParameter("airline", "STRING", airline.upper())
        )
    if origin:
        origin_filter = "AND origin = @origin"
        params.append(bigquery.ScalarQueryParameter("origin", "STRING", origin.upper()))
    if destination:
        dest_filter = "AND destination = @destination"
        params.append(
            bigquery.ScalarQueryParameter("destination", "STRING", destination.upper())
        )
    if change_type:
        ct_filter = "AND change_type = @change_type"
        params.append(
            bigquery.ScalarQueryParameter("change_type", "STRING", change_type)
        )

    query = f"""
        SELECT
            detected_at_utc,
            report_day,
            airline,
            origin,
            destination,
            route_key,
            rbd,
            cabin,
            change_type,
            old_ow_fare,
            new_ow_fare,
            old_rt_fare,
            new_rt_fare
        FROM {_gds_table('fact_gds_change_event')}
        WHERE report_day >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
            {airline_filter}
            {origin_filter}
            {dest_filter}
            {ct_filter}
        ORDER BY detected_at_utc DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return _rows_to_dicts(client.query(query, job_config=job_config).result())


def get_change_summary(
    client: bigquery.Client,
    days: int = 7,
) -> list[dict]:
    """Return daily change counts grouped by type for the last N days."""
    query = f"""
        SELECT
            report_day,
            change_type,
            COUNT(*) AS change_count
        FROM {_gds_table('fact_gds_change_event')}
        WHERE report_day >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        GROUP BY report_day, change_type
        ORDER BY report_day DESC, change_count DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", days)]
    )
    return _rows_to_dicts(client.query(query, job_config=job_config).result())


# ─────────────────────────────────────────────────────────────────────────────
# Tax snapshot queries
# ─────────────────────────────────────────────────────────────────────────────


def get_tax_rates(
    client: bigquery.Client,
    airport_code: str,
    status: str = "current",
) -> list[dict]:
    """
    Return tax rates for an airport.
    status: "current" | "future" | "expired" | "all"
    """
    params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("airport_code", "STRING", airport_code.upper()),
    ]

    # Latest cycle for this airport
    status_filter = "" if status == "all" else "AND status = @status"
    if status != "all":
        params.append(bigquery.ScalarQueryParameter("status", "STRING", status))

    query = f"""
        WITH latest AS (
            SELECT MAX(cycle_id) AS cycle_id
            FROM {_gds_table('fact_gds_tax_snapshot')}
            WHERE airport_code = @airport_code
        )
        SELECT
            t.cycle_id,
            t.captured_at_utc,
            t.airport_code,
            t.tax_code,
            t.tax_name,
            t.category,
            t.subcategory,
            t.condition,
            t.currency,
            t.amount,
            t.status
        FROM {_gds_table('fact_gds_tax_snapshot')} t
        JOIN latest USING (cycle_id)
        WHERE t.airport_code = @airport_code
            {status_filter}
        ORDER BY t.tax_code, t.category, t.status
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return _rows_to_dicts(client.query(query, job_config=job_config).result())


def get_tax_airports(client: bigquery.Client) -> list[dict]:
    """Return the list of airports that have tax data, with run timestamp."""
    query = f"""
        SELECT
            airport_code,
            MAX(captured_at_utc) AS last_updated,
            COUNT(DISTINCT tax_code) AS tax_count
        FROM {_gds_table('fact_gds_tax_snapshot')}
        GROUP BY airport_code
        ORDER BY airport_code
    """
    return _rows_to_dicts(client.query(query).result())
