from __future__ import annotations

from datetime import date

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, Response
from sqlalchemy.orm import Session

from .config import settings
from .db import get_optional_db
from .repositories import reporting


app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    docs_url="/docs",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_origins) or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _cap_limit(limit: int) -> int:
    if limit < 1:
        return settings.default_limit
    return min(limit, settings.max_limit)


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs", status_code=307)


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api", include_in_schema=False)
def api_root() -> JSONResponse:
    return JSONResponse(
        {
            "name": settings.api_title,
            "version": settings.api_version,
            "docs_url": "/docs",
            "health_url": "/health",
        }
    )


@app.get("/health")
def health(db: Session | None = Depends(get_optional_db)) -> dict:
    return reporting.get_health(db)


@app.get("/api/v1/reporting/cycle-health")
def cycle_health(db: Session | None = Depends(get_optional_db)) -> dict:
    return reporting.get_cycle_health(db)


@app.get("/api/v1/meta/airlines")
def meta_airlines(db: Session | None = Depends(get_optional_db)) -> dict:
    return {"items": reporting.list_airlines(db)}


@app.get("/api/v1/meta/routes")
def meta_routes(db: Session | None = Depends(get_optional_db)) -> dict:
    return {"items": reporting.list_routes(db)}


@app.get("/api/v1/reporting/cycles/latest")
def latest_cycle(db: Session | None = Depends(get_optional_db)) -> dict:
    payload = reporting.get_latest_cycle(db)
    if not payload:
        raise HTTPException(status_code=404, detail="No cycle data found")
    return payload


@app.get("/api/v1/reporting/cycles/recent")
def recent_cycles(
    limit: int = Query(default=10, ge=1, le=100),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return {"items": reporting.get_recent_cycles(db, limit=limit)}


@app.get("/api/v1/reporting/current-snapshot")
def current_snapshot(
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    cabin: list[str] | None = Query(default=None),
    limit: int = Query(default=settings.default_limit, ge=1),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return reporting.get_current_snapshot(
        db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        cabins=cabin,
        limit=_cap_limit(limit),
    )


@app.get("/api/v1/reporting/route-monitor-matrix")
def route_monitor_matrix(
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    cabin: list[str] | None = Query(default=None),
    route_limit: int = Query(default=8, ge=1, le=24),
    history_limit: int = Query(default=12, ge=1, le=48),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return reporting.get_route_monitor_matrix(
        db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        cabins=cabin,
        route_limit=route_limit,
        history_limit=history_limit,
    )


@app.get("/api/v1/reporting/route-summary")
def route_summary(
    start_date: date | None = None,
    end_date: date | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    cabin: list[str] | None = Query(default=None),
    limit: int = Query(default=settings.default_limit, ge=1),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return {
        "items": reporting.get_route_summary(
            db,
            start_date=start_date,
            end_date=end_date,
            airlines=airline,
            origins=origin,
            destinations=destination,
            cabins=cabin,
            limit=_cap_limit(limit),
        )
    }


@app.get("/api/v1/reporting/change-events")
def change_events(
    start_date: date | None = None,
    end_date: date | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    domain: list[str] | None = Query(default=None),
    change_type: list[str] | None = Query(default=None),
    direction: list[str] | None = Query(default=None),
    limit: int = Query(default=settings.default_limit, ge=1),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return {
        "items": reporting.get_change_events(
            db,
            start_date=start_date,
            end_date=end_date,
            airlines=airline,
            origins=origin,
            destinations=destination,
            domains=domain,
            change_types=change_type,
            directions=direction,
            limit=_cap_limit(limit),
        )
    }


@app.get("/api/v1/reporting/penalties")
def penalties(
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    limit: int = Query(default=settings.default_limit, ge=1),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return reporting.get_penalties(
        db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        limit=_cap_limit(limit),
    )


@app.get("/api/v1/reporting/taxes")
def taxes(
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    limit: int = Query(default=settings.default_limit, ge=1),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return reporting.get_taxes(
        db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        limit=_cap_limit(limit),
    )


@app.get("/api/v1/reporting/forecasting/latest")
def forecasting_latest() -> dict:
    return reporting.get_forecasting_payload()
