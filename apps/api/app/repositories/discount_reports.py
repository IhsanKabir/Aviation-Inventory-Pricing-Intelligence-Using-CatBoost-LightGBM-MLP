"""Team-canonical storage for OTA discount comparison reports.

One row per (team_id, report_date): the desktop app runs the report locally and
POSTs the SANITIZED payload (see discount_engine.sanitize — no HAR filenames, no
routes, no secrets); the server re-sanitizes on ingest and upserts the canonical
report for that date. Every approved viewer sees the same report; the red
change-diff is computed at READ time against the stored previous report, so the
coloring is deterministic for the whole team.

`submitted_by_*` records provenance of the LAST upsert (re-runs the same day
overwrite, matching the rolling-workbook semantics of the CLI).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

DEFAULT_TEAM_ID = "default"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_tables(engine: Engine | None) -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS discount_reports (
                    report_id TEXT PRIMARY KEY,
                    team_id TEXT NOT NULL,
                    report_date DATE NOT NULL,
                    report_data JSONB NOT NULL,
                    generated_at TEXT NULL,
                    submitted_by_user_id TEXT NULL,
                    submitted_by_email TEXT NULL,
                    created_at_utc TIMESTAMPTZ NOT NULL,
                    updated_at_utc TIMESTAMPTZ NOT NULL,
                    UNIQUE (team_id, report_date)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_discount_reports_team_date
                ON discount_reports (team_id, report_date DESC)
                """
            )
        )


def upsert_report(
    db: Session,
    *,
    report_date: date,
    report_data: dict[str, Any],
    submitted_by_user_id: str | None,
    submitted_by_email: str | None,
    team_id: str = DEFAULT_TEAM_ID,
    commit: bool = True,
) -> dict[str, Any]:
    """Insert or overwrite the canonical report for (team, date). Pass commit=False
    to let the caller commit this together with metering in one transaction."""
    now = _utcnow()
    row = (
        db.execute(
            text(
                """
                INSERT INTO discount_reports (
                    report_id, team_id, report_date, report_data, generated_at,
                    submitted_by_user_id, submitted_by_email,
                    created_at_utc, updated_at_utc
                ) VALUES (
                    :report_id, :team_id, :report_date, CAST(:report_data AS JSONB),
                    :generated_at, :submitted_by_user_id, :submitted_by_email,
                    :now, :now
                )
                ON CONFLICT (team_id, report_date) DO UPDATE SET
                    report_data = EXCLUDED.report_data,
                    generated_at = EXCLUDED.generated_at,
                    submitted_by_user_id = EXCLUDED.submitted_by_user_id,
                    submitted_by_email = EXCLUDED.submitted_by_email,
                    updated_at_utc = EXCLUDED.updated_at_utc
                RETURNING report_id, created_at_utc, updated_at_utc
                """
            ),
            {
                "report_id": str(uuid4()),
                "team_id": team_id,
                "report_date": report_date,
                "report_data": json.dumps(report_data),
                "generated_at": str(report_data.get("generated_at") or ""),
                "submitted_by_user_id": submitted_by_user_id,
                "submitted_by_email": submitted_by_email,
                "now": now,
            },
        )
        .mappings()
        .first()
    )
    if commit:
        db.commit()
    return {
        "report_id": row["report_id"],
        "report_date": report_date.isoformat(),
        "replaced_existing": row["created_at_utc"] != row["updated_at_utc"],
    }


def _row_to_meta(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "report_id": row["report_id"],
        "report_date": row["report_date"].isoformat() if row.get("report_date") else None,
        "generated_at": row.get("generated_at"),
        "submitted_by_email": row.get("submitted_by_email"),
        "updated_at_utc": row["updated_at_utc"].isoformat() if row.get("updated_at_utc") else None,
    }


def _load_payload(raw: Any) -> dict[str, Any]:
    return json.loads(raw) if isinstance(raw, str) else (raw or {})


def get_report(
    db: Session,
    *,
    report_date: date | None = None,
    team_id: str = DEFAULT_TEAM_ID,
) -> dict[str, Any] | None:
    """The report for a date, or the LATEST report when date is None."""
    where = "team_id = :team_id" + (" AND report_date = :report_date" if report_date else "")
    row = (
        db.execute(
            text(
                f"""
                SELECT report_id, report_date, report_data, generated_at,
                       submitted_by_email, updated_at_utc
                FROM discount_reports
                WHERE {where}
                ORDER BY report_date DESC
                LIMIT 1
                """
            ),
            {"team_id": team_id, "report_date": report_date},
        )
        .mappings()
        .first()
    )
    if not row:
        return None
    meta = _row_to_meta(dict(row))
    meta["report"] = _load_payload(row["report_data"])
    return meta


def get_previous_report(
    db: Session,
    *,
    before_date: date,
    team_id: str = DEFAULT_TEAM_ID,
) -> dict[str, Any] | None:
    """The latest stored report STRICTLY BEFORE a date (source of the red diff)."""
    row = (
        db.execute(
            text(
                """
                SELECT report_id, report_date, report_data, generated_at,
                       submitted_by_email, updated_at_utc
                FROM discount_reports
                WHERE team_id = :team_id AND report_date < :before_date
                ORDER BY report_date DESC
                LIMIT 1
                """
            ),
            {"team_id": team_id, "before_date": before_date},
        )
        .mappings()
        .first()
    )
    if not row:
        return None
    meta = _row_to_meta(dict(row))
    meta["report"] = _load_payload(row["report_data"])
    return meta


def list_reports(
    db: Session,
    *,
    limit: int = 60,
    team_id: str = DEFAULT_TEAM_ID,
) -> list[dict[str, Any]]:
    """History metadata (no payloads), newest first."""
    rows = (
        db.execute(
            text(
                """
                SELECT report_id, report_date, generated_at,
                       submitted_by_email, updated_at_utc
                FROM discount_reports
                WHERE team_id = :team_id
                ORDER BY report_date DESC
                LIMIT :limit
                """
            ),
            {"team_id": team_id, "limit": max(1, min(int(limit), 366))},
        )
        .mappings()
        .all()
    )
    return [_row_to_meta(dict(r)) for r in rows]
