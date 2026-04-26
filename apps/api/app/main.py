from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.orm import Session

from .config import settings
from .db import engine, get_optional_db
from .repositories import access_requests, exporting, reporting, user_accounts
from .routers import gds as gds_router
from .routers import travelport_feedback as travelport_feedback_router

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

app.include_router(gds_router.router, prefix="/gds", tags=["GDS"])
app.include_router(
    travelport_feedback_router.router,
    prefix="/travelport-agent",
    tags=["Travelport Feedback"],
)


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


class UserRegisterBody(BaseModel):
    email: str
    password: str
    full_name: str | None = None


class UserLoginBody(BaseModel):
    email: str
    password: str


class UserOAuthLoginBody(BaseModel):
    email: str
    full_name: str | None = None
    auth_provider: str
    provider_subject: str | None = None


class GoogleCodeExchangeBody(BaseModel):
    code: str
    code_verifier: str
    redirect_uri: str
    client_id: str


@app.on_event("startup")
def startup() -> None:
    access_requests.ensure_tables(engine)
    user_accounts.ensure_tables(engine)


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


def _enforce_report_access(
    db: Session | None,
    *,
    request_id: str | None,
    page_key: str,
    scope: dict,
) -> Session:
    required_db = _require_access_request_db(db)
    try:
        access_requests.require_approved_request(
            required_db,
            request_id=request_id,
            page_key=page_key,
            scope=scope,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return required_db


def _require_admin_token(x_admin_token: str | None) -> None:
    if not settings.report_access_admin_token:
        raise HTTPException(status_code=503, detail="Admin approval token is not configured.")
    if x_admin_token != settings.report_access_admin_token:
        raise HTTPException(status_code=403, detail="Invalid admin approval token.")


def _require_user_session(db: Session | None, x_user_session: str | None) -> dict:
    required_db = _require_access_request_db(db)
    payload = user_accounts.get_session_user(required_db, x_user_session, touch=True)
    if not payload:
        raise HTTPException(status_code=401, detail="Sign in is required.")
    return payload


_TPA_TOKEN_PREFIX = "tpa."
_TPA_TOKEN_TTL = 30 * 86400  # 30 days


def _make_stateless_token(email: str, name: str | None, sub: str, secret: str) -> str:
    payload_bytes = json.dumps(
        {"email": email, "name": name, "sub": sub, "iat": int(time.time())},
        separators=(",", ":"),
    ).encode()
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode()
    sig = hmac.new(secret.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{_TPA_TOKEN_PREFIX}{payload_b64}.{sig}"


def _verify_stateless_token(token: str, secret: str) -> dict | None:
    if not token.startswith(_TPA_TOKEN_PREFIX):
        return None
    rest = token[len(_TPA_TOKEN_PREFIX):]
    parts = rest.split(".", 1)
    if len(parts) != 2:
        return None
    payload_b64, sig = parts
    expected = hmac.new(secret.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        return None
    try:
        padding = (4 - len(payload_b64) % 4) % 4
        data = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * padding))
    except Exception:
        return None
    if time.time() - data.get("iat", 0) > _TPA_TOKEN_TTL:
        return None
    return data


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


@app.post("/api/v1/user-auth/register")
def register_user(
    body: UserRegisterBody,
    request: Request,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    try:
        user = user_accounts.register_user(
            required_db,
            email=body.email,
            password=body.password,
            full_name=body.full_name,
        )
        session_token, session_payload = user_accounts.create_session(
            required_db,
            user_id=user["user_id"],
            user_agent=request.headers.get("user-agent"),
            ip_address=request.client.host if request.client else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"user": user, "session_token": session_token, "session": session_payload}


@app.post("/api/v1/user-auth/login")
def login_user(
    body: UserLoginBody,
    request: Request,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    try:
        user = user_accounts.authenticate_user(required_db, email=body.email, password=body.password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    session_token, session_payload = user_accounts.create_session(
        required_db,
        user_id=user["user_id"],
        user_agent=request.headers.get("user-agent"),
        ip_address=request.client.host if request.client else None,
    )
    return {"user": user, "session_token": session_token, "session": session_payload}


@app.post("/api/v1/user-auth/oauth-login")
def oauth_login_user(
    body: UserOAuthLoginBody,
    request: Request,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    try:
        user = user_accounts.upsert_oauth_user(
            required_db,
            email=body.email,
            full_name=body.full_name,
            auth_provider=body.auth_provider,
            provider_subject=body.provider_subject,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    session_token, session_payload = user_accounts.create_session(
        required_db,
        user_id=user["user_id"],
        user_agent=request.headers.get("user-agent"),
        ip_address=request.client.host if request.client else None,
    )
    return {"user": user, "session_token": session_token, "session": session_payload}


_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"


@app.post("/api/v1/user-auth/google-code-exchange")
def google_code_exchange(
    body: GoogleCodeExchangeBody,
    request: Request,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    """Exchange a Google OAuth auth code for a TravelportAuto session token.

    The desktop sends the raw auth code and PKCE verifier; this endpoint
    completes the token exchange with Google using the server-stored
    GOOGLE_CLIENT_SECRET so the secret never travels to the client.
    """
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
    if not client_secret:
        raise HTTPException(
            status_code=503,
            detail="Google sign-in is not configured on this server.",
        )

    # Exchange the auth code with Google
    token_payload = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": body.code,
        "redirect_uri": body.redirect_uri,
        "client_id": body.client_id,
        "client_secret": client_secret,
        "code_verifier": body.code_verifier,
    }).encode("utf-8")
    token_req = urllib.request.Request(
        _GOOGLE_TOKEN_URL,
        data=token_payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(token_req, timeout=30) as resp:
            token_data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=400, detail=f"Google token exchange failed: {raw[:300]}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Google token exchange error: {exc}")

    access_token = token_data.get("access_token", "")
    if not access_token:
        raise HTTPException(status_code=502, detail="Google did not return an access token.")

    # Fetch userinfo
    userinfo_req = urllib.request.Request(
        _GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    try:
        with urllib.request.urlopen(userinfo_req, timeout=20) as resp:
            userinfo = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Google userinfo fetch failed: {exc}")

    email = userinfo.get("email", "")
    if not email:
        raise HTTPException(status_code=400, detail="Google did not return an email address.")

    name = userinfo.get("name")
    sub = userinfo.get("sub", "")

    # Prefer DB-backed session when a database is available; fall back to a
    # stateless HMAC-signed token so sign-in works even without a SQL DB.
    if db is not None:
        try:
            user = user_accounts.upsert_oauth_user(
                db,
                email=email,
                full_name=name,
                auth_provider="google",
                provider_subject=sub,
            )
            session_token, session_payload = user_accounts.create_session(
                db,
                user_id=user["user_id"],
                user_agent=request.headers.get("user-agent"),
                ip_address=request.client.host if request.client else None,
            )
            return {"user": user, "session_token": session_token, "session": session_payload}
        except Exception:
            pass  # fall through to stateless path

    session_token = _make_stateless_token(email, name, sub, client_secret)
    user = {"email": email, "full_name": name, "user_id": None}
    return {"user": user, "session_token": session_token, "session": {}}


@app.get("/api/v1/user-auth/me")
def current_user(
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    if x_user_session and x_user_session.startswith(_TPA_TOKEN_PREFIX):
        secret = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
        data = _verify_stateless_token(x_user_session, secret) if secret else None
        if not data:
            raise HTTPException(status_code=401, detail="Session token expired or invalid.")
        return {"user": {"email": data["email"], "full_name": data.get("name"), "user_id": None}}
    user = _require_user_session(db, x_user_session)
    return {"user": user}


@app.post("/api/v1/user-auth/logout")
def logout_user(
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    revoked = user_accounts.revoke_session(required_db, x_user_session)
    return {"ok": revoked}


@app.post("/api/v1/access-requests")
def create_access_request(
    body: AccessRequestCreateBody,
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _require_access_request_db(db)
    user = _require_user_session(required_db, x_user_session)
    try:
        payload = access_requests.create_request(
            required_db,
            page_key=body.page_key,
            requester_name=user.get("full_name") or body.requester_name,
            requester_contact=user.get("email") or body.requester_contact,
            requester_user_id=user.get("user_id"),
            requester_email=user.get("email"),
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
    route_pair: list[str] | None = Query(default=None),
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
        route_pairs=route_pair,
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
    route_pair: list[str] | None = Query(default=None),
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
    required_db = _enforce_report_access(
        db,
        request_id=request_id,
        page_key="routes",
        scope={
            "cycle_id": cycle_id,
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "route_pair": route_pair,
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
    reporting.clear_request_metrics()
    payload = reporting.get_route_monitor_matrix(
        required_db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        route_pairs=route_pair,
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
    route_pair: list[str] | None = Query(default=None),
    cabin: list[str] | None = Query(default=None),
    trip_type: list[str] | None = Query(default=None),
    response: Response = None,
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _enforce_report_access(
        db,
        request_id=request_id,
        page_key="routes",
        scope={
            "cycle_id": cycle_id,
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "route_pair": route_pair,
            "cabin": cabin,
            "trip_type": trip_type,
        },
    )
    reporting.clear_request_metrics()
    payload = reporting.get_route_date_availability(
        required_db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        route_pairs=route_pair,
        cabins=cabin,
        trip_types=trip_type,
    )
    if response is not None:
        _apply_reporting_metric_headers(response, reporting.get_request_metrics(), "route_date_availability")
    return payload


@app.get("/api/v1/reporting/airline-operations")
def airline_operations(
    request_id: str | None = None,
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
    required_db = _enforce_report_access(
        db,
        request_id=request_id,
        page_key="operations",
        scope={
            "cycle_id": cycle_id,
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "via_airport": via_airport,
            "route_type": route_type,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "route_limit": route_limit,
            "trend_limit": trend_limit,
        },
    )
    return reporting.get_airline_operations(
        required_db,
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
    request_id: str | None = None,
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
    required_db = _enforce_report_access(
        db,
        request_id=request_id,
        page_key="changes",
        scope={
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "domain": domain,
            "change_type": change_type,
            "direction": direction,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "limit": _cap_limit(limit),
        },
    )
    return {
        "items": reporting.get_change_events(
            required_db,
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
    request_id: str | None = None,
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
    required_db = _enforce_report_access(
        db,
        request_id=request_id,
        page_key="changes",
        scope={
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "domain": domain,
            "change_type": change_type,
            "direction": direction,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        },
    )
    return reporting.get_change_dashboard(
        required_db,
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
    request_id: str | None = None,
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    limit: int = Query(default=settings.default_limit, ge=1),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _enforce_report_access(
        db,
        request_id=request_id,
        page_key="penalties",
        scope={
            "cycle_id": cycle_id,
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "limit": _cap_limit(limit),
        },
    )
    return reporting.get_penalties(
        required_db,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        limit=_cap_limit(limit),
    )


@app.get("/api/v1/reporting/taxes")
def taxes(
    request_id: str | None = None,
    cycle_id: str | None = None,
    airline: list[str] | None = Query(default=None),
    origin: list[str] | None = Query(default=None),
    destination: list[str] | None = Query(default=None),
    route_type: list[str] | None = Query(default=None),
    limit: int = Query(default=settings.default_limit, ge=1),
    trend_limit: int = Query(default=8, ge=1, le=20),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    required_db = _enforce_report_access(
        db,
        request_id=request_id,
        page_key="taxes",
        scope={
            "cycle_id": cycle_id,
            "airline": airline,
            "origin": origin,
            "destination": destination,
            "route_type": route_type,
            "limit": _cap_limit(limit),
            "trend_limit": trend_limit,
        },
    )
    return reporting.get_taxes(
        required_db,
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
    route_pair: list[str] | None = Query(default=None),
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
    if requested_sections:
        required_db = _require_access_request_db(db)
        section_scopes = {
            "routes": {
                "cycle_id": cycle_id,
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "route_pair": route_pair,
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
            "operations": {
                "cycle_id": cycle_id,
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "route_type": route_type,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "route_limit": route_limit,
            },
            "changes": {
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "domain": domain,
                "change_type": change_type,
                "direction": direction,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "limit": _cap_limit(limit),
            },
            "penalties": {
                "cycle_id": cycle_id,
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "limit": _cap_limit(limit),
            },
            "taxes": {
                "cycle_id": cycle_id,
                "airline": airline,
                "origin": origin,
                "destination": destination,
                "route_type": route_type,
                "limit": _cap_limit(limit),
            },
        }
        for section in requested_sections:
            if section in section_scopes:
                _enforce_report_access(
                    required_db,
                    request_id=request_id,
                    page_key=section,
                    scope=section_scopes[section],
                )
    payload, filename = exporting.build_reporting_workbook(
        db,
        sections=requested_sections,
        cycle_id=cycle_id,
        airlines=airline,
        origins=origin,
        destinations=destination,
        route_pairs=route_pair,
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
