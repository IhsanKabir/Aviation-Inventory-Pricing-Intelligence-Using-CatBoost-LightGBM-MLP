"""Small in-process sliding-window rate limiter (shared across routers).

Extracted from routers/travelport_feedback.py so auth endpoints (login, register,
set-password) can throttle brute-force / credential-stuffing and the PBKDF2
CPU-exhaustion lever. In-process is per-instance, not global — Cloud Run may run
several instances — but it meaningfully raises the cost of an attack from a single
source without any external dependency. Keyed by whatever caller-supplied string
you pass (IP, "login:<ip>", "login:<email>", …).
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request

_lock = threading.Lock()
_buckets: dict[str, deque[float]] = defaultdict(deque)


def client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[-1].strip()   # trusted ingress appends rightmost
    return request.client.host if request.client else "unknown"


def check(key: str, *, limit: int, window_seconds: float, detail: str | None = None) -> None:
    """Raise 429 if `key` has exceeded `limit` hits within `window_seconds`."""
    now = time.monotonic()
    cutoff = now - window_seconds
    with _lock:
        bucket = _buckets[key]
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= limit:
            raise HTTPException(
                status_code=429,
                detail=detail or "Too many attempts. Please wait and try again.",
            )
        bucket.append(now)


def enforce_auth(request: Request, email: str | None = None) -> None:
    """Throttle an auth attempt by IP (and by email when provided). Deliberately
    generic 429 text so it leaks nothing about which accounts exist."""
    ip = client_ip(request)
    check(f"auth:ip:{ip}", limit=20, window_seconds=60.0,
          detail="Too many attempts from your network. Wait a minute and retry.")
    if email:
        check(f"auth:email:{email.strip().lower()}", limit=8, window_seconds=60.0,
              detail="Too many attempts for this account. Wait a minute and retry.")
