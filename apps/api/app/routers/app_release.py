"""routers/app_release.py - desktop-app self-update channel.

WHY THIS EXISTS
    The Travel Ops Console / IATA Code Validator desktop client used to
    check GitHub Releases directly for updates. Corporate networks (e.g.
    USBA) block github.com / api.github.com / objects.githubusercontent.com,
    so those users could never update — but they CAN reach this API (the
    client already authenticates against it). This router mirrors the GitHub
    release to firewalled clients: CI keeps publishing the .exe to GitHub,
    and the backend (which has internet) fronts it.

MOUNT in app/main.py (next to the other include_router calls, ~line 54):
    from .routers import app_release as app_release_router
    app.include_router(
        app_release_router.router, prefix="/api/v1/app", tags=["App Updates"],
    )

ENDPOINTS
    GET /api/v1/app/latest    -> {version, notes, download_url, sha256}  (auth)
    GET /api/v1/app/download  -> streams the latest .exe                 (auth)

The desktop client (src/updater.py) calls /latest FIRST, falls back to
GitHub on any failure, and verifies the streamed bytes against `sha256`
before swapping the running exe.

CLOUD RUN NOTE
    Streaming a ~420MB binary needs a longer request timeout than the 300s
    default. Deploy with `--timeout=600` (the CI deploy step's gcloud run
    deploy). Memory is fine — the stream is chunked, never buffered.

OPTIONAL ENV
    APP_RELEASE_GITHUB_REPO   default "IhsanKabir/iata-code-validator"
    APP_RELEASE_ASSET_NAME    default "IATACodeValidator.exe"
    APP_RELEASE_GITHUB_TOKEN  optional PAT to lift GitHub's 60/hr anon limit
    APP_PUBLIC_BASE_URL       optional explicit https base for download_url
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from ..db import get_optional_db
from ..repositories import user_accounts

log = logging.getLogger(__name__)
router = APIRouter()

_GH_REPO = os.environ.get("APP_RELEASE_GITHUB_REPO", "IhsanKabir/iata-code-validator")
_ASSET = os.environ.get("APP_RELEASE_ASSET_NAME", "IATACodeValidator.exe")
_GH_API = f"https://api.github.com/repos/{_GH_REPO}/releases/latest"
_GH_TOKEN = os.environ.get("APP_RELEASE_GITHUB_TOKEN", "").strip()
_PUBLIC_BASE = os.environ.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
_UA = "aero-pulse-api/app-release-proxy"

# Short in-process cache so up-to-10 Cloud Run instances don't blow GitHub's
# unauthenticated 60-req/hour/IP limit on the metadata call.
_CACHE_TTL = 300.0
_cache: dict[str, object] = {"at": 0.0, "data": None}


def _require_session(db: Session | None, x_user_session: str | None) -> dict:
    """Mirror main._require_user_session without importing main (circular)."""
    if db is None:
        raise HTTPException(status_code=503, detail="Database unavailable.")
    user = user_accounts.get_session_user(db, x_user_session, touch=True)
    if not user:
        raise HTTPException(status_code=401, detail="Sign in is required.")
    return user


def _gh_request(url: str, accept: str) -> urllib.request.Request:
    headers = {"Accept": accept, "User-Agent": _UA}
    if _GH_TOKEN:
        headers["Authorization"] = f"Bearer {_GH_TOKEN}"
    return urllib.request.Request(url, headers=headers)


def _fetch_latest_release() -> dict:
    """GitHub releases/latest, cached briefly. Raises on transport failure."""
    now = time.monotonic()
    cached = _cache.get("data")
    if cached is not None and (now - float(_cache.get("at", 0.0))) < _CACHE_TTL:
        return cached  # type: ignore[return-value]
    with urllib.request.urlopen(
        _gh_request(_GH_API, "application/vnd.github+json"), timeout=20,
    ) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    _cache["data"] = data
    _cache["at"] = now
    return data


def _asset_url(data: dict) -> str | None:
    for a in (data.get("assets") or []):
        if a.get("name") == _ASSET:
            return a.get("browser_download_url")
    return None


def _sha256_from_release(data: dict) -> str:
    """Read the sibling <asset>.sha256 release asset, if CI published one."""
    for a in (data.get("assets") or []):
        if a.get("name") == f"{_ASSET}.sha256":
            try:
                with urllib.request.urlopen(
                    _gh_request(a["browser_download_url"], "text/plain"), timeout=15,
                ) as resp:
                    return resp.read().decode("utf-8").strip().split()[0]
            except Exception:  # noqa: BLE001
                return ""
    return ""


@router.get("/latest")
def latest_version(
    request: Request,
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    """Version manifest for the desktop updater. Auth-gated."""
    _require_session(db, x_user_session)
    try:
        data = _fetch_latest_release()
    except Exception as exc:  # noqa: BLE001
        log.warning("app/latest: GitHub fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail="Upstream release unavailable.")
    if data.get("draft") or data.get("prerelease") or not _asset_url(data):
        raise HTTPException(status_code=404, detail="No published release asset.")
    tag = (data.get("tag_name") or "").strip()
    if not tag:
        raise HTTPException(status_code=404, detail="No published release.")
    base = _PUBLIC_BASE or str(request.base_url).rstrip("/")
    return {
        "version": tag.lstrip("v"),
        "notes": data.get("body") or "",
        "download_url": f"{base}/api/v1/app/download",
        "sha256": _sha256_from_release(data),
    }


@router.get("/download")
def download() -> StreamingResponse:
    """Stream the latest .exe through this (reachable) host. PUBLIC by design.

    Intentionally unauthenticated: the website Download button is a plain
    browser navigation that cannot send the X-User-Session header, and the
    binary is ALREADY public on the GitHub release (public repo) — so this
    mirror exposes nothing new. It simply makes the download reachable on
    corporate networks that block GitHub. The desktop updater also hits this
    route (its session header is just ignored here).

    A 302 redirect to GitHub would defeat the purpose — the corporate client
    can't reach GitHub — so we proxy the bytes, chunked (constant memory).
    """
    try:
        url = _asset_url(_fetch_latest_release())
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="Upstream release unavailable.")
    if not url:
        raise HTTPException(status_code=404, detail="No release asset.")

    upstream = urllib.request.urlopen(
        _gh_request(url, "application/octet-stream"), timeout=60,
    )

    def _stream():
        try:
            while True:
                chunk = upstream.read(1024 * 256)  # 256 KB
                if not chunk:
                    break
                yield chunk
        finally:
            upstream.close()

    headers = {"Content-Disposition": f'attachment; filename="{_ASSET}"'}
    length = upstream.headers.get("Content-Length")
    if length:
        headers["Content-Length"] = length
    return StreamingResponse(
        _stream(), media_type="application/octet-stream", headers=headers,
    )
