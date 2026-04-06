from __future__ import annotations

import logging
import time
from datetime import date

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.orm import Session

from .config import settings
from .db import engine, get_optional_db
from .repositories import access_requests, exporting, reporting

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
    allow_origin_regex=settings.cors_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if settings.gzip_enabled:
    app.add_middleware(GZipMiddleware, minimum_size=settings.gzip_minimum_size)


class AccessRequestCreateBody(BaseModel):
    page_key: str = Field(default="routes")
    requester_name: str | None = None
    requester_contact: str | None = None
    requested_start_date: date | None = None
    requested_end_date: date | None = None
    notes: str | None = None
    request_scope: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_dates(self):
        if self.requested_start_date and self.requested_end_date and self.requested_end_date < self.requested_start_date:
            raise ValueError("requested_end_date must be on or after requested_start_date")
        return self


class AccessRequestUpdateBody(BaseModel):
    status: str
    decision_note: str | None = None


@app.on_event("startup")
def startup() -> None:
    access_requests.ensure_tables(engine)


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


def _apply_reporting_metric_headers(response: Response, metrics: dict, prefix: str) -> None:
    normalized_prefix = prefix.replace("_", "-").title()
    header_prefix = f"X-{normalized_prefix}-"
    for key, value in metrics.items():
        if not key.startswith(prefix):
            continue
        suffix = key[len(prefix):].strip("_").replace("_", "-").title()
        response.headers[f"{header_prefix}{suffix}"] = str(value)


def _require_access_request_db(db: Session | None) -> Session:
    if db is None:
        raise HTTPException(status_code=503, detail="Access-request storage is not configured on this API instance.")
    return db


def _require_admin_token(x_admin_token: str | None) -> None:
    if not settings.report_access_admin_token:
        raise HTTPException(status_code=503, detail="Admin approval token is not configured.")
    if x_admin_token != settings.report_access_admin_token:
        raise HTTPException(status_code=403, detail="Invalid admin approval token.")


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


@app.post("/api/v1/access-requests")
def create_access_request(
    body: AccessRequestCreateBody,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    try:
        payload = access_requests.create_request(
            required_db,
            page_key=body.page_key,
            requester_name=body.requester_name,
            requester_contact=body.requester_contact,
            requested_start_date=body.requested_start_date,
            requested_end_date=body.requested_end_date,
            notes=body.notes,
            request_scope=body.request_scope,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return payload


@app.get("/api/v1/access-requests/{request_id}")
def get_access_request(
    request_id: str,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    payload = access_requests.get_request(required_db, request_id)
    if not payload:
        raise HTTPException(status_code=404, detail="Access request not found.")
    return payload


@app.get("/api/v1/access-requests")
def list_access_requests(
    status: str | None = None,
    page_key: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    x_admin_token: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    _require_admin_token(x_admin_token)
    required_db = _require_access_request_db(db)
    try:
        items = access_requests.list_requests(
            required_db,
            status=status,
            page_key=page_key,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"items": items}


@app.patch("/api/v1/access-requests/{request_id}")
def update_access_request(
    request_id: str,
    body: AccessRequestUpdateBody,
    x_admin_token: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    _require_admin_token(x_admin_token)
    required_db = _require_access_request_db(db)
    try:
        payload = access_requests.update_request_status(
            required_db,
            request_id=request_id,
            status=body.status,
            decision_note=body.decision_note,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not payload:
        raise HTTPException(status_code=404, detail="Access request not found.")
    return payload


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
    response: Response = None,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    reporting.clear_request_metrics()
    payload = {
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
    if response is not None:
        _apply_reporting_metric_headers(response, reporting.get_request_metrics(), "route_list")
    return payload


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
    request_id: str | None = None,
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    cabin: list[str] | None = Query(default=None),
    trip_type: list[str] | None = Query(default=None),
    return_date: date | None = None,
    return_date_start: date | None = None,
    return_date_end: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    departure_date: date | None = None,
    route_limit: int = Query(default=8, ge=1, le=24),
    history_limit: int = Query(default=12, ge=1, le=48),
    compact_history: bool = Query(default=True),
    response: Response = None,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    try:
        access_requests.require_approved_request(
            required_db,
            request_id=request_id,
            page_key="routes",
            scope={
                "cycle_id": cycle_id,
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "cabin": cabin,
                "trip_type": trip_type,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "return_date": return_date.isoformat() if return_date else None,
                "return_date_start": return_date_start.isoformat() if return_date_start else None,
                "return_date_end": return_date_end.isoformat() if return_date_end else None,
                "route_limit": route_limit,
                "history_limit": history_limit,
            },
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    reporting.clear_request_metrics()
    payload = reporting.get_route_monitor_matrix(
        required_db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        cabins=cabin,
        trip_types=trip_type,
        return_date=return_date,
        return_date_start=return_date_start,
        return_date_end=return_date_end,
        start_date=start_date,
        end_date=end_date,
        departure_date=departure_date,
        route_limit=route_limit,
        history_limit=history_limit,
        compact_history=compact_history,
    )
    if response is not None:
        _apply_reporting_metric_headers(response, reporting.get_request_metrics(), "route_matrix")
    return payload


@app.get("/api/v1/reporting/route-date-availability")
def route_date_availability(
    request_id: str | None = None,
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    cabin: list[str] | None = Query(default=None),
    trip_type: list[str] | None = Query(default=None),
    response: Response = None,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    try:
        access_requests.require_approved_request(
            required_db,
            request_id=request_id,
            page_key="routes",
            scope={
                "cycle_id": cycle_id,
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "cabin": cabin,
                "trip_type": trip_type,
            },
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    reporting.clear_request_metrics()
    payload = reporting.get_route_date_availability(
        required_db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        cabins=cabin,
        trip_types=trip_type,
    )
    if response is not None:
        _apply_reporting_metric_headers(response, reporting.get_request_metrics(), "route_date_availability")
    return payload


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
    request_id: str | None = None,
    cycle_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    route_type: list[str] | None = Query(default=None),
    trip_type: list[str] | None = Query(default=None),
    return_date: date | None = None,
    return_date_start: date | None = None,
    return_date_end: date | None = None,
    cabin: list[str] | None = Query(default=None),
    domain: list[str] | None = Query(default=None),
    change_type: list[str] | None = Query(default=None),
    direction: list[str] | None = Query(default=None),
    route_limit: int = Query(default=8, ge=1, le=24),
    history_limit: int = Query(default=12, ge=1, le=48),
    limit: int = Query(default=settings.default_limit, ge=1),
    db: Session | None = Depends(get_optional_db),
) -> StreamingResponse:
    requested_sections = tuple(include or ())
    if "routes" in requested_sections:
        required_db = _require_access_request_db(db)
        try:
            access_requests.require_approved_request(
                required_db,
                request_id=request_id,
                page_key="routes",
                scope={
                    "cycle_id": cycle_id,
                    "airline": airline,
                    "origin": origin,
                    "destination": destination,
                    "cabin": cabin,
                    "trip_type": trip_type,
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "return_date": return_date.isoformat() if return_date else None,
                    "return_date_start": return_date_start.isoformat() if return_date_start else None,
                    "return_date_end": return_date_end.isoformat() if return_date_end else None,
                    "route_limit": route_limit,
                    "history_limit": history_limit,
                },
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
    payload, filename = exporting.build_reporting_workbook(
        db,
        sections=requested_sections,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        route_types=route_type,
        trip_types=trip_type,
        return_date=return_date,
        return_date_start=return_date_start,
        return_date_end=return_date_end,
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
