"""OTA discount comparison reports — team-canonical store + colored reads.

Flow: the desktop app builds the report LOCALLY (HARs never leave the machine),
sanitizes it (discount_engine.sanitize) and POSTs it here. The server re-sanitizes
(never trust the client), enforces a size cap, and upserts one canonical report
per (team, report_date). Reads return the report COLORED server-side
(discount_engine.highlight.apply_highlights against the stored previous report),
so green/blue/red is identical for every viewer — desktop, web, and xlsx.

Access: a signed-in user (X-User-Session) with an APPROVED "discount-comparison"
access request (matched by requester email — desktop clients don't track request
ids). Both writes and reads are gated.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask

from discount_engine.highlight import apply_highlights
from discount_engine.sanitize import sanitize_report_for_sync

from ..db import get_optional_db
from ..repositories import access_requests, discount_reports, user_accounts

router = APIRouter()

PAGE_KEY = "discount-comparison"
MAX_PAYLOAD_BYTES = 512 * 1024          # sanitized reports are ~100 KB; a HAR is not
REPORT_DATE_PAST_DAYS = 370             # allow generous backfill of past reports...
REPORT_DATE_FUTURE_DAYS = 2             # ...but a future date would SHADOW /latest for
                                        # the whole team (upsert is per-date and there is
                                        # no delete), so only tolerate clock/TZ skew.


class DiscountReportBody(BaseModel):
    report: dict[str, Any] = Field(..., description="sanitize_report_for_sync payload")
    client_version: str | None = None
    # Stable per-report id from the desktop, so an outbox retry after a network
    # blip does not bill a second metered use for the same report.
    sync_id: str | None = None


def _require_db(db: Session | None) -> Session:
    if db is None:
        raise HTTPException(status_code=503,
                            detail="Report storage is not configured on this API instance.")
    return db


def _require_user(db: Session, x_user_session: str | None) -> dict[str, Any]:
    user = user_accounts.get_session_user(db, x_user_session, touch=True)
    if not user:
        raise HTTPException(status_code=401, detail="Sign in is required.")
    return user


def _require_page_access(db: Session, user: dict[str, Any]) -> dict[str, Any]:
    """Returns the approved request (callers that meter usage need its quota)."""
    email = user.get("email")
    approved = access_requests.find_approved_request_for_email(
        db, page_key=PAGE_KEY, email=email)
    if approved:
        return approved
    # Tier-aware 403s: payment plans and expired windows get specific messages.
    payment = access_requests.find_latest_request_for_email(
        db, page_key=PAGE_KEY, email=email, statuses=("payment_required",))
    if payment:
        raise HTTPException(
            status_code=403,
            detail=payment.get("decision_note")
            or "Payment is required to activate your discount-report plan. "
               "Contact the admin to renew.")
    windowed = access_requests.find_latest_request_for_email(
        db, page_key=PAGE_KEY, email=email, statuses=("approved",))
    if windowed:   # approved once, but the date window doesn't cover today
        today = access_requests.local_today()
        start = windowed.get("requested_start_date")
        if start and str(start) > today.isoformat():
            raise HTTPException(
                status_code=403,
                detail=f"Your discount-report plan starts on {start}.")
        raise HTTPException(
            status_code=403,
            detail="Your discount-report access period has expired "
                   f"(ended {windowed.get('requested_end_date')}). Submit a new "
                   "request or ask the admin to renew your plan.")
    raise HTTPException(
        status_code=403,
        detail="An approved 'discount-comparison' access request is required. "
               "Submit one from the web app and ask an admin to approve it.")


def _parse_report_date(report: dict[str, Any]) -> date:
    raw = str(report.get("report_date") or "")
    try:
        parsed = datetime.strptime(raw, "%d/%m/%Y").date()
    except ValueError:
        raise HTTPException(status_code=422,
                            detail="report_date must be DD/MM/YYYY.") from None
    delta_days = (parsed - date.today()).days
    if delta_days > REPORT_DATE_FUTURE_DAYS or -delta_days > REPORT_DATE_PAST_DAYS:
        raise HTTPException(status_code=422,
                            detail="report_date is implausibly far from today.")
    return parsed


def _parse_query_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD.") from None


def _colored(db: Session, stored: dict[str, Any]) -> dict[str, Any]:
    """Attach server-computed highlights: diff against the stored PREVIOUS report."""
    current_date = date.fromisoformat(stored["report_date"])
    prev = discount_reports.get_previous_report(db, before_date=current_date)
    stored["report"] = apply_highlights(stored["report"], prev["report"] if prev else None)
    stored["prev_report_date"] = prev["report_date"] if prev else None
    return stored


@router.post("")
def submit_report(
    body: DiscountReportBody,
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict[str, Any]:
    required_db = _require_db(db)
    user = _require_user(required_db, x_user_session)
    approved = _require_page_access(required_db, user)

    sanitized = sanitize_report_for_sync(body.report)   # server-side: never trust the client
    # Cap AFTER sanitizing: the sanitizer can EXPAND terse fields (a short source
    # string becomes a provenance dict), so pre-sanitize size is not what gets stored.
    if len(json.dumps(body.report)) > MAX_PAYLOAD_BYTES \
            or len(json.dumps(sanitized)) > MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413,
                            detail="Report payload exceeds the 512 KB limit — sync the "
                                   "sanitized report, never raw capture data.")
    report_date = _parse_report_date(sanitized)

    # PER-USE METERING: a sync is the billable unit; reads stay free for the team.
    # Atomic + idempotent (sync_id), and committed TOGETHER with the report write
    # below (commit=False) so a storage failure can't burn a use.
    use = access_requests.consume_use(
        required_db, request=approved, action="sync",
        user_id=user.get("user_id"), email=user.get("email"),
        sync_id=body.sync_id, commit=False)
    if not use["allowed"]:
        required_db.rollback()
        raise HTTPException(
            status_code=403,
            detail=f"Your plan's included uses are exhausted "
                   f"({use['used']}/{use['quota']} synced reports). "
                   "Ask the admin to renew or extend your plan.")

    result = discount_reports.upsert_report(
        required_db,
        report_date=report_date,
        report_data=sanitized,
        submitted_by_user_id=user.get("user_id"),
        submitted_by_email=user.get("email"),
        commit=False,
    )
    required_db.commit()    # metering + storage land atomically
    result["normalized"] = bool(sanitized.get("normalized"))
    result["uses_remaining"] = use["remaining"]     # None = unlimited plan
    result["duplicate"] = use.get("duplicate", False)
    return result


@router.get("/latest")
def latest_report(
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict[str, Any]:
    required_db = _require_db(db)
    user = _require_user(required_db, x_user_session)
    _require_page_access(required_db, user)
    stored = discount_reports.get_report(required_db)
    if not stored:
        raise HTTPException(status_code=404, detail="No discount report has been synced yet.")
    return _colored(required_db, stored)


@router.get("/by-date")
def report_by_date(
    date_: str = Query(alias="date"),
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict[str, Any]:
    required_db = _require_db(db)
    user = _require_user(required_db, x_user_session)
    _require_page_access(required_db, user)
    stored = discount_reports.get_report(required_db, report_date=_parse_query_date(date_))
    if not stored:
        raise HTTPException(status_code=404, detail="No report stored for that date.")
    return _colored(required_db, stored)


@router.get("/history")
def report_history(
    limit: int = Query(default=60, ge=1, le=366),
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict[str, Any]:
    required_db = _require_db(db)
    user = _require_user(required_db, x_user_session)
    _require_page_access(required_db, user)
    return {"items": discount_reports.list_reports(required_db, limit=limit)}


@router.get("/xlsx")
def report_xlsx(
    date_: str | None = Query(default=None, alias="date"),
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> FileResponse:
    """Regenerate the colored single-sheet workbook from the STORED report (+ stored
    previous report for the red diff) — matches the web grid and the desktop xlsx."""
    from discount_engine.grid import write_single_sheet_xlsx

    required_db = _require_db(db)
    user = _require_user(required_db, x_user_session)
    _require_page_access(required_db, user)
    stored = discount_reports.get_report(required_db, report_date=_parse_query_date(date_))
    if not stored:
        raise HTTPException(status_code=404, detail="No report stored for that date.")
    prev = discount_reports.get_previous_report(
        required_db, before_date=date.fromisoformat(stored["report_date"]))

    out_dir = Path(tempfile.mkdtemp(prefix="discount_xlsx_"))
    out = write_single_sheet_xlsx(stored["report"], prev["report"] if prev else None,
                                  out_dir / f"OTA_Discount_{stored['report_date']}.xlsx")
    # Cloud Run's /tmp is RAM-backed tmpfs: without cleanup every download leaks the
    # tempdir into instance memory. FileResponse does NOT delete what it serves.
    return FileResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=out.name,
        background=BackgroundTask(shutil.rmtree, out_dir, ignore_errors=True),
    )
