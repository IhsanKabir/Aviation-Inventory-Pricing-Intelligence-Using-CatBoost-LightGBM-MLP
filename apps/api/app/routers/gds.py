"""
routers/gds.py - GDS fare and tax API endpoints

Mount in main.py with:
    from app.routers import gds as gds_router
    app.include_router(gds_router.router, prefix="/gds", tags=["GDS"])
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from google.cloud import bigquery

from ..repositories import gds as gds_repo

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# Dependency: BigQuery client
# ─────────────────────────────────────────────────────────────────────────────


def get_bq_client() -> bigquery.Client:
    """
    Return a BigQuery client. Reuse the same approach as reporting.py
    (instantiate per-request; Cloud Run handles auth via workload identity).
    """
    import os

    project = os.environ.get("BIGQUERY_PROJECT_ID", "aeropulseintelligence")
    return bigquery.Client(project=project)


# ─────────────────────────────────────────────────────────────────────────────
# Runs
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/runs")
async def list_runs(
    limit: int = Query(default=20, ge=1, le=100),
    client: bigquery.Client = Depends(get_bq_client),
):
    """Return the N most recent GDS fare extraction runs."""
    try:
        return gds_repo.get_fare_runs(client, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/latest")
async def latest_run(client: bigquery.Client = Depends(get_bq_client)):
    """Return metadata for the most recent GDS fare extraction run."""
    try:
        result = gds_repo.get_latest_fare_run(client)
        if result is None:
            raise HTTPException(status_code=404, detail="No GDS runs found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Fares
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/fares")
async def list_fares(
    airline: str | None = Query(default=None, description="2-3 char IATA airline code"),
    origin: str | None = Query(default=None, description="3-char IATA airport"),
    destination: str | None = Query(default=None, description="3-char IATA airport"),
    cabin: str | None = Query(
        default=None, description="Economy | Business | First | Premium Economy"
    ),
    journey_type: str | None = Query(default=None, description="OW | RT"),
    cycle_id: str | None = Query(
        default=None, description="Specific cycle, e.g. gds_run_42"
    ),
    limit: int = Query(default=500, ge=1, le=5000),
    client: bigquery.Client = Depends(get_bq_client),
):
    """
    Return fare rows from the latest GDS extraction (or a specific cycle).
    All filters are optional — omit to return all fares.
    """
    try:
        return gds_repo.get_fares(
            client,
            airline=airline,
            origin=origin,
            destination=destination,
            cabin=cabin,
            journey_type=journey_type,
            cycle_id=cycle_id,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fares/history")
async def fare_history(
    route_key: str = Query(description="Route key, e.g. DAC-MCT"),
    airline: str = Query(description="2-3 char IATA airline code"),
    rbd: str = Query(description="Reservation Booking Designator, e.g. Y"),
    journey_type: str = Query(default="OW", description="OW | RT"),
    days: int = Query(default=30, ge=1, le=365),
    client: bigquery.Client = Depends(get_bq_client),
):
    """Return fare history for a specific route/airline/RBD over N days."""
    try:
        return gds_repo.get_fare_history(
            client,
            route_key=route_key,
            airline=airline,
            rbd=rbd,
            journey_type=journey_type,
            days=days,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Change events
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/changes")
async def list_changes(
    airline: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    destination: str | None = Query(default=None),
    change_type: str | None = Query(
        default=None, description="new | removed | price_change | sold_out | available"
    ),
    days: int = Query(default=7, ge=1, le=90),
    limit: int = Query(default=200, ge=1, le=2000),
    client: bigquery.Client = Depends(get_bq_client),
):
    """Return recent GDS fare change events."""
    try:
        return gds_repo.get_change_events(
            client,
            airline=airline,
            origin=origin,
            destination=destination,
            change_type=change_type,
            days=days,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/changes/summary")
async def change_summary(
    days: int = Query(default=7, ge=1, le=90),
    client: bigquery.Client = Depends(get_bq_client),
):
    """Return daily change counts grouped by change type."""
    try:
        return gds_repo.get_change_summary(client, days=days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Taxes
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/taxes")
async def list_tax_airports(client: bigquery.Client = Depends(get_bq_client)):
    """Return all airports that have GDS tax data."""
    try:
        return gds_repo.get_tax_airports(client)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/taxes/{airport_code}")
async def tax_rates(
    airport_code: str,
    status: str = Query(
        default="current", description="current | future | expired | all"
    ),
    client: bigquery.Client = Depends(get_bq_client),
):
    """
    Return tax rates for an airport.
    Defaults to current (active) rates only.
    """
    try:
        rows = gds_repo.get_tax_rates(client, airport_code=airport_code, status=status)
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tax data found for airport '{airport_code.upper()}'",
            )
        return rows
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
