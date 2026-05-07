"""Per-app usage logging.

Receives one row per *batch* (not per-record) from desktop apps so the
admin can see who used which tool, when, and against how many inputs —
without flooding the database with millions of rows.

Design choices:
  - Single table `app_lookup_events` keyed by (event_id).
  - Indexed on (app_id, occurred_at_utc DESC) for the per-app dashboard
    and on (user_id, occurred_at_utc DESC) for per-user drill-down.
  - The `count` column is the batch size. The `target` column carries
    a short identifier of what was looked up (e.g., the input column
    name, or "full-list-export"). No PII payload — just the metric.
  - `app_id` is stored verbatim. Caller is expected to pass a known
    constant ("travelport-auto", "iata-validator", ...).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_tables(engine: Engine | None) -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS app_lookup_events (
                    event_id TEXT PRIMARY KEY,
                    user_id TEXT NULL,
                    app_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target TEXT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    occurred_at_utc TIMESTAMPTZ NOT NULL,
                    user_agent TEXT NULL,
                    notes TEXT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_app_lookup_events_app_time
                ON app_lookup_events (app_id, occurred_at_utc DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_app_lookup_events_user_time
                ON app_lookup_events (user_id, occurred_at_utc DESC)
                """
            )
        )


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def record_event(
    db: Session,
    *,
    user_id: str | None,
    app_id: str,
    action: str,
    target: str | None = None,
    count: int = 0,
    user_agent: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    if not app_id or not action:
        raise ValueError("app_id and action are required.")
    event_id = str(uuid4())
    now = _utcnow()
    db.execute(
        text(
            """
            INSERT INTO app_lookup_events (
                event_id, user_id, app_id, action, target, count,
                occurred_at_utc, user_agent, notes
            ) VALUES (
                :event_id, :user_id, :app_id, :action, :target, :count,
                :occurred_at_utc, :user_agent, :notes
            )
            """
        ),
        {
            "event_id": event_id,
            "user_id": user_id,
            "app_id": app_id.strip(),
            "action": action.strip(),
            "target": (target or "").strip() or None,
            "count": max(0, int(count or 0)),
            "occurred_at_utc": now,
            "user_agent": (user_agent or "").strip() or None,
            "notes": (notes or "").strip() or None,
        },
    )
    db.commit()
    return {
        "event_id": event_id,
        "occurred_at_utc": now.isoformat(),
    }


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def usage_summary(
    db: Session,
    *,
    days: int = 30,
) -> dict[str, Any]:
    """Aggregate counts for the admin dashboard.

    Returns:
        {
          "window_days": 30,
          "as_of_utc": "...",
          "totals": [
            { "app_id": "iata-validator", "events": 12, "rows": 9123 },
            { "app_id": "travelport-auto", "events": 28, "rows": 0 },
          ],
          "by_user": [
            { "app_id": "iata-validator", "user_email": "alice@…",
              "events": 5, "rows": 3000, "last_seen_utc": "..." },
            ...
          ],
          "recent": [
            { "occurred_at_utc": "...", "app_id": "...", "user_email": "...",
              "action": "...", "target": "...", "count": 9000 },
            ...
          ]
        }
    """
    days = max(1, min(int(days or 30), 365))
    since = _utcnow() - timedelta(days=days)

    # Per-app totals
    totals_rows = db.execute(
        text(
            """
            SELECT app_id, COUNT(*) AS events, COALESCE(SUM(count), 0) AS rows
            FROM app_lookup_events
            WHERE occurred_at_utc >= :since
            GROUP BY app_id
            ORDER BY events DESC
            """
        ),
        {"since": since},
    ).mappings().all()

    # Per-app per-user breakdown
    by_user_rows = db.execute(
        text(
            """
            SELECT e.app_id,
                   COALESCE(u.email, '<unknown>') AS user_email,
                   COUNT(*) AS events,
                   COALESCE(SUM(e.count), 0) AS rows,
                   MAX(e.occurred_at_utc) AS last_seen_utc
            FROM app_lookup_events e
            LEFT JOIN report_users u ON u.user_id = e.user_id
            WHERE e.occurred_at_utc >= :since
            GROUP BY e.app_id, COALESCE(u.email, '<unknown>')
            ORDER BY MAX(e.occurred_at_utc) DESC
            """
        ),
        {"since": since},
    ).mappings().all()

    # Most recent 50 events
    recent_rows = db.execute(
        text(
            """
            SELECT e.occurred_at_utc, e.app_id,
                   COALESCE(u.email, '<unknown>') AS user_email,
                   e.action, e.target, e.count
            FROM app_lookup_events e
            LEFT JOIN report_users u ON u.user_id = e.user_id
            WHERE e.occurred_at_utc >= :since
            ORDER BY e.occurred_at_utc DESC
            LIMIT 50
            """
        ),
        {"since": since},
    ).mappings().all()

    def _iso(d) -> str:
        if d is None:
            return ""
        if hasattr(d, "isoformat"):
            return d.isoformat()
        return str(d)

    return {
        "window_days": days,
        "as_of_utc": _utcnow().isoformat(),
        "totals": [
            {
                "app_id": (r["app_id"] or "travelport-auto"),
                "events": int(r["events"] or 0),
                "rows": int(r["rows"] or 0),
            }
            for r in totals_rows
        ],
        "by_user": [
            {
                "app_id": (r["app_id"] or "travelport-auto"),
                "user_email": r["user_email"],
                "events": int(r["events"] or 0),
                "rows": int(r["rows"] or 0),
                "last_seen_utc": _iso(r["last_seen_utc"]),
            }
            for r in by_user_rows
        ],
        "recent": [
            {
                "occurred_at_utc": _iso(r["occurred_at_utc"]),
                "app_id": (r["app_id"] or "travelport-auto"),
                "user_email": r["user_email"],
                "action": r["action"],
                "target": r["target"] or "",
                "count": int(r["count"] or 0),
            }
            for r in recent_rows
        ],
    }
