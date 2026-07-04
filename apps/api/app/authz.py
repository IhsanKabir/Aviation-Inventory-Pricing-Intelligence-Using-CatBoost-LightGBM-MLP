"""Shared authorization helpers usable from main.py AND routers (no circular import)."""

from __future__ import annotations

import hmac

from fastapi import HTTPException

from .config import settings


def require_admin_token(x_admin_token: str | None) -> None:
    """Constant-time admin-token check. 503 when unconfigured, 403 when wrong."""
    configured = str(settings.report_access_admin_token or "")
    if not configured:
        raise HTTPException(status_code=503, detail="Admin token is not configured.")
    if not hmac.compare_digest(str(x_admin_token or ""), configured):
        raise HTTPException(status_code=403, detail="Invalid admin token.")
