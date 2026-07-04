"""System observability: error tracking + rolling request metrics.

Errors are persisted to `report_error_events` when a DB is available (durable +
aggregates across Cloud Run instances) and mirrored to a small in-memory ring as
a fallback when it isn't. Request/latency stats are in-memory per instance (cheap,
rough) and labelled as such in the dashboard.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

_PROCESS_STARTED_AT = time.time()
_INSTANCE_ID = uuid4().hex[:8]

_lock = threading.Lock()
_recent_requests: deque[tuple[float, int, float]] = deque(maxlen=8000)  # (ts, status, ms)
_mem_errors: deque[dict[str, Any]] = deque(maxlen=200)                  # DB-less fallback


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> str | None:
    """Postgres returns a datetime; SQLite (tests) returns a string — handle both."""
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def uptime_seconds() -> float:
    return time.time() - _PROCESS_STARTED_AT


def instance_id() -> str:
    return _INSTANCE_ID


def ensure_tables(engine: Engine | None) -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS report_error_events (
                    error_id TEXT PRIMARY KEY,
                    occurred_at_utc TIMESTAMPTZ NOT NULL,
                    method TEXT NULL,
                    path TEXT NULL,
                    status INTEGER NULL,
                    error_type TEXT NULL,
                    message TEXT NULL,
                    request_id TEXT NULL,
                    instance_id TEXT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_error_events_occurred
                ON report_error_events (occurred_at_utc DESC)
                """
            )
        )


def record_request(status: int, elapsed_ms: float) -> None:
    with _lock:
        _recent_requests.append((time.time(), int(status), float(elapsed_ms)))


def request_stats(window_seconds: int = 3600) -> dict[str, Any]:
    now = time.time()
    cutoff = now - window_seconds
    with _lock:
        rows = [r for r in _recent_requests if r[0] >= cutoff]
    total = len(rows)
    errors = sum(1 for _ts, status, _ms in rows if status >= 500)
    lat = sorted(ms for _ts, _status, ms in rows)

    def _pct(p: float) -> float:
        if not lat:
            return 0.0
        idx = min(len(lat) - 1, int(len(lat) * p))
        return round(lat[idx], 1)

    return {
        "window_seconds": window_seconds,
        "total_requests": total,
        "error_requests": errors,
        "error_rate": round(errors / total, 4) if total else 0.0,
        "latency_p50_ms": _pct(0.50),
        "latency_p95_ms": _pct(0.95),
        "per_instance": True,
        "instance_id": _INSTANCE_ID,
    }


def record_error(
    db: Session | None,
    *,
    method: str | None,
    path: str | None,
    status: int | None,
    error_type: str | None,
    message: str | None,
    request_id: str | None,
) -> None:
    """Best-effort — recording an error must never raise out of the request path."""
    event = {
        "error_id": str(uuid4()),
        "occurred_at_utc": _utcnow(),
        "method": method,
        "path": (path or "")[:400],
        "status": status,
        "error_type": (error_type or "")[:120] or None,
        "message": (message or "")[:1000] or None,
        "request_id": request_id,
        "instance_id": _INSTANCE_ID,
    }
    if db is not None:
        try:
            db.execute(
                text(
                    """
                    INSERT INTO report_error_events (
                        error_id, occurred_at_utc, method, path, status,
                        error_type, message, request_id, instance_id
                    ) VALUES (
                        :error_id, :occurred_at_utc, :method, :path, :status,
                        :error_type, :message, :request_id, :instance_id
                    )
                    """
                ),
                event,
            )
            db.commit()
            return
        except Exception:  # noqa: BLE001 — fall back to memory, never propagate
            try:
                db.rollback()
            except Exception:  # noqa: BLE001
                pass
    with _lock:
        _mem_errors.appendleft({**event, "occurred_at_utc": event["occurred_at_utc"].isoformat()})


def recent_errors(db: Session | None, limit: int = 50) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit), 200))
    if db is not None:
        try:
            rows = (
                db.execute(
                    text(
                        """
                        SELECT occurred_at_utc, method, path, status, error_type,
                               message, request_id, instance_id
                        FROM report_error_events
                        ORDER BY occurred_at_utc DESC
                        LIMIT :limit
                        """
                    ),
                    {"limit": limit},
                )
                .mappings()
                .all()
            )
            return [
                {**dict(r), "occurred_at_utc": _iso(r["occurred_at_utc"])}
                for r in rows
            ]
        except Exception:  # noqa: BLE001
            pass
    with _lock:
        return list(_mem_errors)[:limit]


def error_count(db: Session | None, hours: int = 24) -> int:
    cutoff = datetime.fromtimestamp(time.time() - hours * 3600, tz=timezone.utc)
    if db is not None:
        try:
            row = db.execute(
                text(
                    """
                    SELECT COUNT(*) AS n FROM report_error_events
                    WHERE occurred_at_utc >= :cutoff
                    """
                ),
                {"cutoff": cutoff},
            ).mappings().first()
            return int(row["n"]) if row else 0
        except Exception:  # noqa: BLE001
            pass
    with _lock:
        return len(_mem_errors)
