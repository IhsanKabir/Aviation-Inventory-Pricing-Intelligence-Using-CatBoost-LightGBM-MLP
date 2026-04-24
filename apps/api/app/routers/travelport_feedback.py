"""
routers/travelport_feedback.py - Desktop feedback endpoints

Rate limiting: 10 POST requests per minute per IP (sliding-window, in-process).
Cloud Run may run multiple instances so the limit is per-instance; good enough to
block naive scripts without a Redis dependency.
"""

from __future__ import annotations

import logging
import threading
import time

LOG = logging.getLogger(__name__)
from collections import defaultdict, deque
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import bigquery
from pydantic import BaseModel, Field

from ..repositories import travelport_feedback as feedback_repo

router = APIRouter()

# ---------------------------------------------------------------------------
# Rate limiter — sliding window, no external dependency
# ---------------------------------------------------------------------------
_RATE_LIMIT = 10       # max requests
_RATE_WINDOW = 60.0    # per this many seconds
_rl_lock = threading.Lock()
_rl_buckets: dict[str, deque] = defaultdict(deque)


def _client_ip(request: Request) -> str:
    """Return the real client IP from Cloud Run's X-Forwarded-For.

    Cloud Run appends the genuine source IP as the *last* entry in
    X-Forwarded-For.  Taking the first entry allows a caller to spoof an
    arbitrary IP and bypass the rate limiter by setting the header themselves.
    """
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        # Rightmost entry is appended by the trusted Cloud Run ingress.
        return forwarded.split(",")[-1].strip()
    return request.client.host if request.client else "unknown"


def _check_rate_limit(ip: str) -> None:
    """Raise 429 if the IP has exceeded _RATE_LIMIT requests in _RATE_WINDOW seconds."""
    now = time.monotonic()
    cutoff = now - _RATE_WINDOW
    with _rl_lock:
        bucket = _rl_buckets[ip]
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= _RATE_LIMIT:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Max {_RATE_LIMIT} feedback submissions per minute.",
            )
        bucket.append(now)


# ---------------------------------------------------------------------------
# BigQuery client factory
# ---------------------------------------------------------------------------

def get_bq_client() -> bigquery.Client:
    import os

    project = os.environ.get("BIGQUERY_PROJECT_ID", "aeropulseintelligence")
    return bigquery.Client(project=project)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TravelportFeedbackCreate(BaseModel):
    category: str = Field(default="general", min_length=2, max_length=40)
    subject: str = Field(min_length=3, max_length=160)
    message: str = Field(min_length=5, max_length=5000)
    app_version: str | None = Field(default=None, max_length=40)
    device_id: str | None = Field(default=None, max_length=100)
    device_name: str | None = Field(default=None, max_length=100)
    hostname: str | None = Field(default=None, max_length=100)
    os_version: str | None = Field(default=None, max_length=200)
    source: str | None = Field(default="desktop_gui", max_length=50)
    submitted_at_utc: str | None = None
    context: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/feedback")
async def create_feedback(
    request: Request,
    payload: TravelportFeedbackCreate,
    client: bigquery.Client = Depends(get_bq_client),
):
    """Receive a feedback submission from the desktop GUI."""
    _check_rate_limit(_client_ip(request))
    try:
        payload_dict = (
            payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        )
        return feedback_repo.create_feedback(client, payload_dict)
    except Exception as e:
        LOG.exception("feedback POST failed: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/feedback")
async def list_feedback(
    limit: int = Query(default=100, ge=1, le=500),
    status: str = Query(default="all", description="all | new | reviewed | resolved"),
    client: bigquery.Client = Depends(get_bq_client),
):
    """Return feedback submissions for the admin page."""
    try:
        return feedback_repo.list_feedback(client, limit=limit, status=status)
    except Exception as e:
        LOG.exception("feedback GET failed: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")
