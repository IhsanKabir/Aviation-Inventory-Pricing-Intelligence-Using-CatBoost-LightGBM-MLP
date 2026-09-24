"""Shared authorization helpers usable from main.py AND routers (no circular import)."""

from __future__ import annotations

import hmac
import os

from fastapi import HTTPException

from .config import settings

#: Administrator accounts (by sign-in email). Gates the /usage dashboard and the
#: desktop app's LIVE searches (everyone else works from HAR captures). Set
#: USAGE_ADMIN_EMAILS on the API to change it - no app release needed.
ADMIN_EMAILS = {
    e.strip().lower()
    for e in os.environ.get("USAGE_ADMIN_EMAILS", "ihsankabir999@gmail.com").split(",")
    if e.strip()
}


def is_admin_email(email: str | None) -> bool:
    return bool(email) and str(email).strip().lower() in ADMIN_EMAILS


def require_admin_token(x_admin_token: str | None) -> None:
    """Constant-time admin-token check. 503 when unconfigured, 403 when wrong."""
    configured = str(settings.report_access_admin_token or "")
    if not configured:
        raise HTTPException(status_code=503, detail="Admin token is not configured.")
    if not hmac.compare_digest(str(x_admin_token or ""), configured):
        raise HTTPException(status_code=403, detail="Invalid admin token.")
