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

# MULTI-APP: one release channel per desktop product, selected by ?app=<key>.
# "iata" stays the default so existing updaters (no query param) keep working.
APPS: dict[str, dict[str, str]] = {
    "iata": {
        "repo": os.environ.get("APP_RELEASE_GITHUB_REPO", "IhsanKabir/iata-code-validator"),
        "asset": os.environ.get("APP_RELEASE_ASSET_NAME", "IATACodeValidator.exe"),
    },
    "discount-report": {
        "repo": os.environ.get(
            "DISCOUNT_APP_GITHUB_REPO",
            "IhsanKabir/Aviation-Inventory-Pricing-Intelligence-Using-CatBoost-LightGBM-MLP"),
        "asset": os.environ.get("DISCOUNT_APP_ASSET_NAME", "OTADiscountReport.exe"),
    },
}
_DEFAULT_APP = "iata"
_GH_TOKEN = os.environ.get("APP_RELEASE_GITHUB_TOKEN", "").strip()
_PUBLIC_BASE = os.environ.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
_UA = "aero-pulse-api/app-release-proxy"

# Short PER-APP in-process cache so up-to-10 Cloud Run instances don't blow
# GitHub's unauthenticated 60-req/hour/IP limit on the metadata call. Keyed by
# app so one product's cached release can never be served for another.
_CACHE_TTL = 300.0
_cache: dict[str, dict[str, object]] = {}


def _app_config(app_key: str | None) -> tuple[str, dict[str, str]]:
    key = (app_key or _DEFAULT_APP).strip().lower()
    cfg = APPS.get(key)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Unknown app '{key}'.")
    return key, cfg


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


_NEGATIVE_CACHE_TTL = 60.0   # failed lookups (rate limit, 404) back off briefly too


def _fetch_latest_release(app_key: str, cfg: dict[str, str]) -> dict:
    """GitHub releases/latest for one app, cached briefly per app (success AND
    failure — otherwise a rate-limited/absent release makes every request hammer
    GitHub and burn the anonymous quota even faster)."""
    now = time.monotonic()
    entry = _cache.get(app_key, {})
    if entry.get("data") is not None and (now - float(entry.get("at", 0.0))) < _CACHE_TTL:
        return entry["data"]  # type: ignore[return-value]
    if entry.get("failed_at") is not None \
            and (now - float(entry["failed_at"])) < _NEGATIVE_CACHE_TTL:
        raise RuntimeError("release lookup recently failed (negative cache)")
    api = f"https://api.github.com/repos/{cfg['repo']}/releases/latest"
    try:
        with urllib.request.urlopen(
            _gh_request(api, "application/vnd.github+json"), timeout=20,
        ) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        _cache[app_key] = {**entry, "failed_at": now}
        raise
    _cache[app_key] = {"data": data, "at": now}
    return data


def _asset_url(cfg: dict[str, str], data: dict) -> str | None:
    for a in (data.get("assets") or []):
        if a.get("name") == cfg["asset"]:
            return a.get("browser_download_url")
    return None


def _sha256_from_release(cfg: dict[str, str], data: dict) -> str:
    """Read the sibling <asset>.sha256 release asset, if CI published one."""
    for a in (data.get("assets") or []):
        if a.get("name") == f"{cfg['asset']}.sha256":
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
    app: str | None = None,
    x_user_session: str | None = Header(default=None),
    db: Session | None = Depends(get_optional_db),
) -> dict:
    """Version manifest for the desktop updater. Auth-gated. ?app=<key> selects
    the product (default 'iata' for backward compatibility)."""
    app_key, cfg = _app_config(app)
    _require_session(db, x_user_session)
    try:
        data = _fetch_latest_release(app_key, cfg)
    except Exception as exc:  # noqa: BLE001
        log.warning("app/latest[%s]: GitHub fetch failed: %s", app_key, exc)
        raise HTTPException(status_code=502, detail="Upstream release unavailable.")
    if data.get("draft") or data.get("prerelease") or not _asset_url(cfg, data):
        raise HTTPException(status_code=404, detail="No published release asset.")
    tag = (data.get("tag_name") or "").strip()
    if not tag:
        raise HTTPException(status_code=404, detail="No published release.")
    base = _PUBLIC_BASE or str(request.base_url).rstrip("/")
    suffix = "" if app_key == _DEFAULT_APP else f"?app={app_key}"
    return {
        "version": tag.lstrip("v"),
        "notes": data.get("body") or "",
        "download_url": f"{base}/api/v1/app/download{suffix}",
        "sha256": _sha256_from_release(cfg, data),
    }


@router.get("/download")
def download(app: str | None = None) -> StreamingResponse:
    """Stream the latest binary through this (reachable) host. PUBLIC by design.

    Intentionally unauthenticated: the website Download button is a plain
    browser navigation that cannot send the X-User-Session header, and the
    binary is ALREADY public on the GitHub release (public repo) — so this
    mirror exposes nothing new. It simply makes the download reachable on
    corporate networks that block GitHub. The desktop updater also hits this
    route (its session header is just ignored here). ?app=<key> selects the
    product (default 'iata').

    A 302 redirect to GitHub would defeat the purpose — the corporate client
    can't reach GitHub — so we proxy the bytes, chunked (constant memory).
    """
    app_key, cfg = _app_config(app)
    try:
        url = _asset_url(cfg, _fetch_latest_release(app_key, cfg))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"release lookup failed: {exc}")
    if not url:
        raise HTTPException(status_code=404, detail="No release asset.")

    # GitHub's asset CDN (objects.githubusercontent.com) can 403 a datacenter
    # request that carries a generic User-Agent, so use a browser-like one.
    # Fetch eagerly and surface any failure as 502 rather than an unhandled
    # 500. We deliberately do NOT set a manual Content-Length on the streaming
    # response — Starlette chunks it.
    asset_req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/octet-stream",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
        },
    )
    try:
        upstream = urllib.request.urlopen(asset_req, timeout=60)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"asset fetch failed: {exc}")

    def _stream():
        try:
            while True:
                chunk = upstream.read(1024 * 256)  # 256 KB
                if not chunk:
                    break
                yield chunk
        finally:
            upstream.close()

    return StreamingResponse(
        _stream(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{cfg["asset"]}"'},
    )
