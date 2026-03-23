from __future__ import annotations

import logging
import time
from datetime import date

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse
from sqlalchemy.orm import Session

from .config import settings
from .db import get_optional_db
from .repositories import exporting, reporting

LOG = logging.getLogger("api.http")

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

if settings.gzip_enabled:
    app.add_middleware(GZipMiddleware, minimum_size=settings.gzip_minimum_size)


@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    started_at = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    response.headers["X-Process-Time-Ms"] = f"{elapsed_ms:.1f}"
    path_template = request.scope.get("route").path if request.scope.get("route") else request.url.path
    response.headers["X-Route-Template"] = str(path_template)
    if settings.request_timing_log_enabled:
        LOG.info(
            "request_timing method=%s path=%s route=%s status=%s total_ms=%.1f",
            request.method,
            request.url.path,
            path_template,
            getattr(response, "status_code", "-"),
            elapsed_ms,
        )
    return response


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
def meta_routes(
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    cabin: list[str] | None = Query(default=None),
    trip_type: list[str] | None = Query(default=None),
    origin_prefix: str | None = None,
    destination_prefix: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=200),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return {
        "items": reporting.list_routes(
            db,
            cycle_id=cycle_id,
            airlines=airline,
            cabins=cabin,
            trip_types=trip_type,
            origin_prefix=origin_prefix,
            destination_prefix=destination_prefix,
            limit=limit,
        )
    }


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
    trip_type: list[str] | None = Query(default=None),
    return_date: date | None = None,
    return_date_start: date | None = None,
    return_date_end: date | None = None,
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
        trip_types=trip_type,
        return_date=return_date,
        return_date_start=return_date_start,
        return_date_end=return_date_end,
        route_limit=route_limit,
        history_limit=history_limit,
    )


@app.get("/api/v1/reporting/route-date-availability")
def route_date_availability(
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    cabin: list[str] | None = Query(default=None),
    trip_type: list[str] | None = Query(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return reporting.get_route_date_availability(
        db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        cabins=cabin,
        trip_types=trip_type,
    )


@app.get("/api/v1/reporting/airline-operations")
def airline_operations(
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    via_airport: list[str] | None = Query(default=None),
    route_type: list[str] | None = Query(default=None),
    start_date: date | None = None,
    end_date: date | None = None,
    route_limit: int = Query(default=4, ge=1, le=12),
    trend_limit: int = Query(default=8, ge=1, le=20),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return reporting.get_airline_operations(
        db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        via_airports=via_airport,
        route_types=route_type,
        start_date=start_date,
        end_date=end_date,
        route_limit=route_limit,
        trend_limit=trend_limit,
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


@app.get("/api/v1/reporting/change-dashboard")
def change_dashboard(
    start_date: date | None = None,
    end_date: date | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    domain: list[str] | None = Query(default=None),
    change_type: list[str] | None = Query(default=None),
    direction: list[str] | None = Query(default=None),
    top_n: int = Query(default=8, ge=1, le=20),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return reporting.get_change_dashboard(
        db,
        start_date=start_date,
        end_date=end_date,
        airlines=airline,
        origins=origin,
        destinations=destination,
        domains=domain,
        change_types=change_type,
        directions=direction,
        top_n=top_n,
    )


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
    route_type: list[str] | None = Query(default=None),
    limit: int = Query(default=settings.default_limit, ge=1),
    trend_limit: int = Query(default=8, ge=1, le=20),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    return reporting.get_taxes(
        db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        route_types=route_type,
        limit=_cap_limit(limit),
        trend_limit=trend_limit,
    )


@app.get("/api/v1/reporting/forecasting/latest")
def forecasting_latest() -> dict:
    return reporting.get_forecasting_payload()


@app.get("/api/v1/reporting/export.xlsx")
def export_reporting_workbook(
    include: list[str] | None = Query(default=None),
    cycle_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    route_type: list[str] | None = Query(default=None),
    trip_type: list[str] | None = Query(default=None),
    return_date: date | None = None,
    cabin: list[str] | None = Query(default=None),
    domain: list[str] | None = Query(default=None),
    change_type: list[str] | None = Query(default=None),
    direction: list[str] | None = Query(default=None),
    route_limit: int = Query(default=8, ge=1, le=24),
    history_limit: int = Query(default=12, ge=1, le=48),
    limit: int = Query(default=settings.default_limit, ge=1),
    db: Session | None = Depends(get_optional_db),
) -> StreamingResponse:
    payload, filename = exporting.build_reporting_workbook(
        db,
        sections=include or (),
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        route_types=route_type,
        trip_types=trip_type,
        return_date=return_date,
        cabins=cabin,
        start_date=start_date,
        end_date=end_date,
        domains=domain,
        change_types=change_type,
        directions=direction,
        route_limit=route_limit,
        history_limit=history_limit,
        limit=_cap_limit(limit),
    )
    return StreamingResponse(
        iter([payload]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
